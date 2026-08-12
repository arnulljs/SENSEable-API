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
  renameDevice, renameModule, renameActuator,
  removeDevice, removeModule, removePort, setPortEnabled,
} from './store.js';
import { ingestTelemetry, ingestDiscovery, ingestAck, refreshAll } from './ingest.js';
import { fitLinear } from './calibration.js';
import { buildCommand, cmdTopic, CHIP_ADDRS } from './commands.js';
import { publishCommand, getMqttStats } from './mqtt.js';

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

// --- Renames (device / module / actuator) -----------------------------------
// PATCH { name }. Persists to Postgres (RLS-enforced) and updates the cache, so
// the new name shows everywhere the entity is referenced on the next poll.
// The updated device projection is returned so the client can reconcile.
function projectOne(dev) {
  return projectDevices(dev.tenantId).find((d) => d.id === dev.id) ?? null;
}

function resolveDevice(req, res) {
  const dev = findNode(req.params.deviceId);
  if (!dev) {
    res.status(404).json({ error: `unknown device '${req.params.deviceId}'` });
    return null;
  }
  return dev;
}

router.patch('/devices/:deviceId', wrap(async (req, res) => {
  const dev = resolveDevice(req, res);
  if (!dev) return;
  try {
    await renameDevice(dev, req.body?.name);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }
  res.json({ ok: true, device: projectOne(dev) });
}));

router.patch('/devices/:deviceId/modules/:moduleId', wrap(async (req, res) => {
  const dev = resolveDevice(req, res);
  if (!dev) return;
  try {
    await renameModule(dev, req.params.moduleId, req.body?.name);
  } catch (e) {
    const code = /unknown module/.test(e.message) ? 404 : 400;
    return res.status(code).json({ error: e.message });
  }
  res.json({ ok: true, device: projectOne(dev) });
}));

router.patch('/devices/:deviceId/actuators/:actuatorId', wrap(async (req, res) => {
  const dev = resolveDevice(req, res);
  if (!dev) return;
  try {
    await renameActuator(dev, req.params.actuatorId, req.body?.name);
  } catch (e) {
    const code = /unknown actuator/.test(e.message) ? 404 : 400;
    return res.status(code).json({ error: e.message });
  }
  res.json({ ok: true, device: projectOne(dev) });
}));

// --- Channel enable / disable ------------------------------------------------
// PATCH { enabled: bool, reason?: string }
//
// Silences an ADC channel with nothing wired to it. A floating ADS1115 input
// reads leakage voltage — typically a few thousand counts — which the pipeline
// would otherwise render as a healthy gauge for a socket with no sensor in it.
//
// This does TWO things, because either alone is insufficient:
//
//   1. Sends sensor_port_down/up to the node, so the firmware stops sampling the
//      channel at source. This is the real fix — the garbage never enters the
//      pipeline. It is best-effort: the current firmware may not implement the
//      command branch yet, and no broker may be attached.
//
//   2. Sets ports.enabled, which takes effect immediately and unconditionally.
//      The dashboard is correct from the next poll whether or not (1) landed.
router.patch('/devices/:deviceId/modules/:moduleId/ports/:portId/enabled',
  wrap(async (req, res) => {
    const dev = resolveDevice(req, res);
    if (!dev) return;

    const enabled = req.body?.enabled;
    if (typeof enabled !== 'boolean') {
      return res.status(400).json({ error: 'body must be { enabled: true | false }' });
    }

    let port;
    try {
      port = await setPortEnabled(dev, req.params.moduleId, req.params.portId,
                                  enabled, req.body?.reason ?? null);
    } catch (e) {
      return res.status(e.status ?? 400).json({ error: e.message });
    }

    // Best-effort firmware command. Chip index is the position of the board's
    // I2C address in the fixed 0x48..0x4B range the protocol defines.
    let command = null;
    try {
      const tenant = store.tenants[dev.tenantId];
      const mqttTid = tenant?.mqttTid;
      const chip = CHIP_ADDRS.indexOf(String(req.params.moduleId).toLowerCase());
      if (mqttTid && chip >= 0 && Number.isInteger(port.channel)) {
        const envelope = buildCommand(
          enabled ? 'sensor_port_up' : 'sensor_port_down',
          { tid: mqttTid, nid: dev.nodeId },
          { chip, ch: port.channel });
        const rec = await recordCommand(dev, envelope);
        const published = publishCommand(cmdTopic(mqttTid, dev.nodeId), envelope);
        command = { cid: envelope.cid, published, action: envelope.action, id: rec?.id ?? null };
      }
    } catch (e) {
      // A firmware that can't be told is not a failure: the server-side flag
      // already made the dashboard correct.
      console.warn('[routes] sensor_port toggle not sent:', e.message);
    }

    res.json({ ok: true, enabled: port.enabled, command, device: projectOne(dev) });
  }));

// --- Removal (operator-initiated) -------------------------------------------
// Hardware that goes quiet is KEPT and marked Offline — never auto-deleted —
// so a dropped connection can't destroy an operator's calibration work. These
// endpoints are the only way inventory is removed, and they refuse (409) while
// the target is still reporting. Deletes cascade: removing a board removes its
// channels, removing a node removes everything under it.
router.delete('/devices/:deviceId', wrap(async (req, res) => {
  const dev = resolveDevice(req, res);
  if (!dev) return;
  try {
    const r = await removeDevice(dev);
    res.json({ ok: true, ...r });
  } catch (e) {
    res.status(e.status ?? 400).json({ error: e.message });
  }
}));

router.delete('/devices/:deviceId/modules/:moduleId', wrap(async (req, res) => {
  const dev = resolveDevice(req, res);
  if (!dev) return;
  try {
    const r = await removeModule(dev, req.params.moduleId);
    res.json({ ok: true, ...r, device: projectOne(dev) });
  } catch (e) {
    res.status(e.status ?? 400).json({ error: e.message });
  }
}));

router.delete('/devices/:deviceId/modules/:moduleId/ports/:portId', wrap(async (req, res) => {
  const dev = resolveDevice(req, res);
  if (!dev) return;
  try {
    const r = await removePort(dev, req.params.moduleId, req.params.portId);
    res.json({ ok: true, ...r, device: projectOne(dev) });
  } catch (e) {
    res.status(e.status ?? 400).json({ error: e.message });
  }
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
router.post('/ingest/telemetry', wrap(async (req, res) => {
  const result = await ingestTelemetry(req.body);
  res.status(result.ok ? 200 : 400).json(result);
}));

router.post('/ingest/discovery', wrap(async (req, res) => {
  const result = await ingestDiscovery(req.body);
  res.status(result.ok ? 200 : 400).json(result);
}));

router.post('/ingest/ack', wrap(async (req, res) => {
  const result = await ingestAck(req.body);
  res.status(result.ok ? 200 : 400).json(result);
}));

// --- Ops --------------------------------------------------------------------
router.get('/health', (_req, res) => {
  // MQTT state is included because "the dashboard is frozen" has two very
  // different causes — the broker link is down, or the node stopped publishing —
  // and they are indistinguishable from the UI. `mqtt.lastPacketAgeMs` settles
  // it immediately, and `mqtt.refused` names any topic the broker's ACL denied.
  const mqtt = getMqttStats();
  const freshest = store.devices.reduce(
    (acc, d) => (d.lastSeen && d.lastSeen > acc ? d.lastSeen : acc), 0);

  res.json({
    ok: true,
    store: store.ready ? 'postgres' : 'hydrating',
    nodes: store.devices.length,
    tenants: Object.keys(store.tenants).length,
    commands: store.commands.length,
    // Which tids ingest will accept. If the node's tid isn't here, every packet
    // is rejected and the dashboard freezes with no other clue.
    acceptedTids: Object.keys(store.tenantByMqttTid),
    newestTelemetryAgeMs: freshest ? Date.now() - freshest : null,
    mqtt,
    now: new Date().toISOString(),
  });
});
