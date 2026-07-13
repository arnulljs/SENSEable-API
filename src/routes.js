// routes.js ─────────────────────────────────────────────────────────────────
// Same endpoints as before — the frontend needs no changes. The difference is
// that every mutation now goes through store.js's write-through helpers, so it
// lands in PostgreSQL instead of evaporating on restart.
//
// Tenant scoping: the frontend still filters per-org client-side, so /devices
// returns every tenant's devices (each carrying tenantId) by default. Pass
// ?tenant=aquatech or an x-tenant-id header to pre-filter server-side.

import { Router } from 'express';
import {
  store, projectDevices,
  markNotificationRead, markAllNotificationsRead,
  createFormula, deleteFormula, assignChannel, saveMapSensors,
} from './store.js';
import { ingestTelemetry, ingestDiscovery, refreshAll } from './ingest.js';
import { fitLinear } from './calibration.js';

export const router = Router();

function tenantOf(req) {
  return req.get('x-tenant-id') || req.query.tenant || null;
}

// Small wrapper so an async handler that throws becomes a 500 instead of an
// unhandled rejection that silently kills the request.
const wrap = (fn) => (req, res) =>
  Promise.resolve(fn(req, res)).catch((e) => {
    console.error('[routes]', e);
    res.status(500).json({ error: e.message });
  });

// Guard: reject reads/writes before the cache is hydrated from Postgres.
router.use((req, res, next) => {
  if (!store.ready) return res.status(503).json({ error: 'store hydrating, retry shortly' });
  next();
});

// --- Read models the frontend renders --------------------------------------
router.get('/devices', (req, res) => {
  refreshAll(); // keep staleness/status honest at read time
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
  // Formulas are a per-tenant resource now (they were one global bank before).
  const slug = tenant || tenantOf(req) || Object.keys(store.tenants)[0];
  const entry = await createFormula(slug, label, formula);
  res.status(201).json(entry);
}));

router.delete('/formulas/:id', wrap(async (req, res) => {
  // ports.formula_id is ON DELETE SET NULL, so the database itself clears the
  // formula from every channel it was assigned to.
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

// --- Ingest (broker-free) ---------------------------------------------------
// POST a payload exactly as the ESP32 publishes it — lets you exercise the
// raw-ADC → calibration → status → PostgreSQL pipeline with no broker.
router.post('/ingest/telemetry', (req, res) => {
  const result = ingestTelemetry(req.body);
  res.status(result.ok ? 200 : 400).json(result);
});

router.post('/ingest/discovery', (req, res) => {
  const result = ingestDiscovery(req.body);
  res.status(result.ok ? 200 : 400).json(result);
});

// --- Ops --------------------------------------------------------------------
router.get('/health', (_req, res) => {
  res.json({
    ok: true,
    store: store.ready ? 'postgres' : 'hydrating',
    nodes: store.devices.length,
    tenants: Object.keys(store.tenants).length,
    now: new Date().toISOString(),
  });
});
