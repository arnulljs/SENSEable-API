// routes.js ─────────────────────────────────────────────────────────────────
// HTTP surface. Existing read/mutation endpoints are unchanged; new endpoints
// added for the frozen command/ack lifecycle:
//
//   POST /commands          issue a downward command (actuate | bus_recovery |
//                           sensor_port_up | sensor_port_down). The backend
//                           resolves the broker `tid` from tenants.mqtt_tid,
//                           builds the wire envelope, logs it (cid), and
//                           publishes to usc/thesis/{tid}/{nid}/cmd if a broker
//                           is connected.
//   GET  /commands          recent command log with latest ack status.
//   POST /ingest/ack        broker-free ack injection (parallels telemetry).

import { Router } from 'express';
import {
  store, projectDevices,
  markNotificationRead, markAllNotificationsRead,
  createFormula, deleteFormula, assignChannel, saveMapSensors,
  findNode, recordCommand, applyActuatorCommand,
} from './store.js';
import { ingestTelemetry, ingestDiscovery, ingestAck, refreshAll } from './ingest.js';
import { fitLinear } from './calibration.js';
import { buildCommand, cmdTopic } from './commands.js';
import { publishCommand } from './mqtt.js';

export const router = Router();

function tenantOf(req) {
  return req.get('x-tenant-id') || req.query.tenant || null;
}

const wrap = (fn) => (req, res) =>
  Promise.resolve(fn(req, res)).catch((e) => {
    console.error('[routes]', e);
    res.status(500).json({ error: e.message });
  });

router.use((req, res, next) => {
  if (!store.ready) return res.status(503).json({ error: 'store hydrating, retry shortly' });
  next();
});

// --- Read models the frontend renders --------------------------------------
router.get('/devices', (req, res) => {
  refreshAll();
  res.json(projectDevices(tenantOf(req)));
});

router.get('/sensor-profiles', (_req, res) => res.json(store.sensorProfiles));

router.get('/notifications', (_req, res) => res.json(store.notifications));

router.post('/notifications/read-all', wrap(async (_req, res) => {
  await markAllNotificationsRead();
  res.json({ ok: true, unread: 0 });
}));

router.post('/notifications/:id/read', wrap(async (req, res) => {
  const ok = await markNotificationRead(req.params.id);
  if (!ok) return res.status(404).json({ error: 'not found' });
  res.json({ ok: true });
}));

router.get('/formulas', (_req, res) => res.json(store.savedFormulas));

router.post('/formulas', wrap(async (req, res) => {
  const { label, formula, tenant } = req.body ?? {};
  if (!label || !formula) {
    return res.status(400).json({ error: 'label and formula required' });
  }
  const slug = tenant || tenantOf(req) || Object.keys(store.tenants)[0];
  const entry = await createFormula(slug, label, formula);
  res.status(201).json(entry);
}));

router.delete('/formulas/:id', wrap(async (req, res) => {
  const removed = await deleteFormula(req.params.id);
  res.json({ ok: true, removed });
}));

router.post('/formulas/fit', (req, res) => {
  const fit = fitLinear(req.body?.points ?? []);
  if (!fit) return res.status(400).json({ error: 'need >= 2 valid {raw,value} points' });
  res.json(fit);
});

router.get('/channel-assignments', (_req, res) => res.json(store.channelAssignments));

router.put('/channel-assignments/:board/:channel', wrap(async (req, res) => {
  const { board, channel } = req.params;
  const { formulaLabel } = req.body ?? {};
  const updated = await assignChannel(board, channel, formulaLabel ?? null);
  res.json(updated);
}));

router.get('/map-sensors', (_req, res) => res.json(store.mapSensors));

router.put('/map-sensors', wrap(async (req, res) => {
  if (!Array.isArray(req.body)) return res.status(400).json({ error: 'expected an array' });
  res.json(await saveMapSensors(req.body));
}));

// --- Commands (downward) ----------------------------------------------------
// Body: { deviceId, action, ...branchParams }
//   actuate            { port|actuatorId, mode:'bin'|'pwm', state?, duty?, dur? }
//   bus_recovery       { busId }
//   sensor_port_up     { chip, ch }
//   sensor_port_down   { chip, ch }
router.post('/commands', wrap(async (req, res) => {
  const { deviceId, action } = req.body ?? {};
  if (!deviceId || !action) {
    return res.status(400).json({ error: 'deviceId and action are required' });
  }

  const dev = findNode(deviceId);
  if (!dev) return res.status(404).json({ error: `unknown device '${deviceId}'` });

  // Resolve the BROKER tenant string (tenant-123) from our tenant. The browser
  // never supplies this — it only knows the slug.
  const tenant = store.tenants[dev.tenantId];
  const tid = tenant?.mqttTid;
  if (!tid) {
    return res.status(409).json({
      error: `tenant '${dev.tenantId}' has no mqtt_tid mapping — set tenants.mqtt_tid (see db/seed_hw.sql)`,
    });
  }

  // For actuate, let the operator target either an actuatorId or a raw port.
  const params = { ...req.body };
  if (action === 'actuate' && params.port == null && params.actuatorId != null) {
    const act = (dev.actuators ?? []).find((a) => a.id === params.actuatorId);
    if (!act) return res.status(404).json({ error: `unknown actuator '${params.actuatorId}'` });
    params.port = act.port; // 'OUT1' → builder parses to int
  }

  let envelope;
  try {
    envelope = buildCommand(action, { tid, nid: dev.nodeId }, params);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }

  // Log the command (cid) so the ack can correlate back to it.
  const record = await recordCommand(dev, envelope);

  // Optimistically reflect an actuate on its actuator (last_ack='pending').
  if (action === 'actuate') {
    applyActuatorCommand(dev, envelope.port, {
      mode: envelope.mode,
      state: envelope.mode === 'bin' ? envelope.state : (envelope.duty > 0 ? 1 : 0),
      duty: envelope.duty,
      dur: envelope.dur,
    });
  }

  const topic = cmdTopic(tid, dev.nodeId);
  const published = publishCommand(topic, envelope);

  res.status(201).json({
    ok: true,
    cid: envelope.cid,
    topic,
    published,               // false ⇒ no broker connected; envelope logged only
    envelope,
    command: record,
  });
}));

router.get('/commands', (req, res) => {
  const t = tenantOf(req);
  const dev = req.query.device;
  let list = store.commands;
  if (t) list = list.filter((c) => c.tenantId === t);
  if (dev) list = list.filter((c) => c.deviceId === dev);
  res.json(list);
});

// --- Ingest (broker-free) ---------------------------------------------------
router.post('/ingest/telemetry', (req, res) => {
  const result = ingestTelemetry(req.body);
  res.status(result.ok ? 200 : 400).json(result);
});

router.post('/ingest/discovery', (req, res) => {
  const result = ingestDiscovery(req.body);
  res.status(result.ok ? 200 : 400).json(result);
});

router.post('/ingest/ack', (req, res) => {
  const result = ingestAck(req.body);
  res.status(result.ok ? 200 : 400).json(result);
});

// --- Ops --------------------------------------------------------------------
router.get('/health', (_req, res) => {
  res.json({
    ok: true,
    store: store.ready ? 'postgres' : 'hydrating',
    nodes: store.devices.length,
    tenants: Object.keys(store.tenants).length,
    commands: store.commands.length,
    now: new Date().toISOString(),
  });
});
