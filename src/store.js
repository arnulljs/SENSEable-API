// store.js ──────────────────────────────────────────────────────────────────
// PostgreSQL-backed replacement for the in-memory store.
//
// DESIGN: hydrated cache + write-through.
//   * hydrate()  loads the whole tenant tree from Postgres into memory at boot.
//   * Reads (projectDevices, findNode, ...) are served from that cache, so the
//     ingest hot path and the 5s status sweep stay allocation-cheap and the
//     exported signatures are IDENTICAL to the old in-memory store — ingest.js
//     never learns the database exists.
//   * Writes go to Postgres first (through withTenant(), so Row-Level Security
//     enforces tenant isolation at the database), then update the cache.
//
// This is exactly the "local edge server + durable store" split the thesis
// describes: fast local state, durable source of truth.
//
// ── ID CONVENTION (important) ───────────────────────────────────────────────
// The frontend needs a stable, GLOBALLY-unique string id per device. node_id
// ('N001') is only unique WITHIN a tenant — the firmware hardcodes it, so two
// tenants will both ship an 'N001'. Un-namespaced, they'd collide in the
// frontend's `new Map(devices.map(d => [d.id, d]))` and silently overwrite
// each other. So:
//
//     device.id  = `${tenantSlug}:${nodeId}`   e.g. "aquatech:N001"
//     module.id  = i2c address                 e.g. "0x48"
//     port.id    = port code                   e.g. "A0"
//     channelKey = "aquatech:N001::0x48::A0"
//
// Every id traces to real hardware. Modules/ports need no namespace because
// they're only ever resolved within a device.

import { withTenant, adminPool } from '../db/pool.js';
import { applyCalibration } from './calibration.js';

const HISTORY_CAP = Number(process.env.HISTORY_CAP ?? 40);

function fmtTs(d) {
  return d.toLocaleString('en-US', {
    month: 'short', day: 'numeric',
    hour: 'numeric', minute: '2-digit', hour12: true,
  });
}

export const deviceKey = (slug, nodeId) => `${slug}:${nodeId}`;

// The cache. Same shape the old in-memory store exposed, so routes.js reads
// (store.notifications, store.savedFormulas, ...) keep working unchanged.
export const store = {
  tenants: {},            // slug -> { id(uuid), slug, name, mqttTid }
  tenantByMqttTid: {},    // 'tenant-123' -> tenant record   (ingest tenant resolution)
  devices: [],            // hydrated device tree (frontend shape + internal uuids)
  notifications: [],
  savedFormulas: [],
  channelAssignments: {},
  sensorProfiles: [],
  mapSensors: [],
  ready: false,
};

// ── Hydration ───────────────────────────────────────────────────────────────
// Loads every tenant's tree. Runs as the OWNER (RLS-exempt) because boot-time
// hydration legitimately spans all tenants; per-request writes still go through
// withTenant() and are RLS-enforced.
export async function hydrate() {
  const { rows: tenants } = await adminPool.query(
    `SELECT tenant_id, slug, name, mqtt_tid FROM tenants ORDER BY slug`
  );

  store.tenants = {};
  store.tenantByMqttTid = {};
  for (const t of tenants) {
    const rec = { id: t.tenant_id, slug: t.slug, name: t.name, mqttTid: t.mqtt_tid };
    store.tenants[t.slug] = rec;
    if (t.mqtt_tid) store.tenantByMqttTid[t.mqtt_tid] = rec;
  }

  // One flat join, assembled in JS — cheaper and clearer than three round trips.
  const { rows } = await adminPool.query(`
    SELECT
      d.device_id, d.node_id, d.name AS device_name, d.status AS device_status,
      d.comm_mode, d.uptime_s, d.rssi, d.free_heap, d.last_seen,
      t.tenant_id, t.slug AS tenant_slug,
      m.module_id, m.i2c_address, m.name AS module_name,
      p.port_id, p.port_code, p.port_index, p.label, p.unit,
      p.range_min, p.range_max, p.safe_min, p.safe_max,
      p.active_flag, p.cal_type, p.cal_slope, p.cal_offset,
      p.last_value, p.last_status,
      f.label AS formula_label, f.expression AS formula_expr
    FROM devices d
    JOIN tenants t ON t.tenant_id = d.tenant_id
    LEFT JOIN modules m ON m.device_id = d.device_id
    LEFT JOIN ports   p ON p.module_id = m.module_id
    LEFT JOIN calibration_formulas f ON f.formula_id = p.formula_id
    ORDER BY t.slug, d.node_id, m.i2c_address, p.port_index
  `);

  const byDevice = new Map();
  for (const r of rows) {
    const id = deviceKey(r.tenant_slug, r.node_id);
    if (!byDevice.has(id)) {
      byDevice.set(id, {
        id,
        _uuid: r.device_id,
        _tenantUuid: r.tenant_id,
        tenantId: r.tenant_slug,          // frontend tenant id == slug
        name: r.device_name,
        nodeId: r.node_id,
        status: r.device_status,
        commMode: r.comm_mode,
        uptime: Number(r.uptime_s ?? 0),
        rssi: r.rssi,
        freeHeap: r.free_heap,
        systemFault: 0,
        lastSeen: r.last_seen ? new Date(r.last_seen).getTime() : null,
        modules: [],
        actuators: [],
      });
    }
    const dev = byDevice.get(id);
    if (!r.module_id) continue;

    let mod = dev.modules.find((m) => m._uuid === r.module_id);
    if (!mod) {
      mod = {
        id: r.i2c_address, _uuid: r.module_id,
        address: r.i2c_address, name: r.module_name, ports: [],
      };
      dev.modules.push(mod);
    }
    if (!r.port_id) continue;

    mod.ports.push({
      id: r.port_code, _uuid: r.port_id,
      channel: r.port_index,
      label: r.label, unit: r.unit,
      rangeMin: r.range_min, rangeMax: r.range_max,
      safeMin: r.safe_min, safeMax: r.safe_max,
      activeFlag: r.active_flag,
      calibration: r.cal_type === 'expr' && r.formula_expr
        ? { type: 'expr', expr: r.formula_expr }
        : { type: 'linear', slope: r.cal_slope, offset: r.cal_offset },
      formulaLabel: r.formula_label ?? null,
      value: r.last_value,
      code: 0,
      status: r.last_status ?? 'Offline',
      lastSeen: null,          // no live telemetry yet this process
      history: [],             // filled by loadHistory() below
    });
  }
  store.devices = [...byDevice.values()];

  // Actuators (node-level siblings of modules — LEDC outputs, not I2C).
  const { rows: acts } = await adminPool.query(`
    SELECT a.*, d.node_id, t.slug FROM actuators a
    JOIN devices d ON d.device_id = a.device_id
    JOIN tenants t ON t.tenant_id = a.tenant_id`);
  for (const a of acts) {
    const dev = byDevice.get(deviceKey(a.slug, a.node_id));
    if (!dev) continue;
    dev.actuators.push({
      id: a.actuator_code, name: a.name, port: a.port,
      channel: a.channel, gpio: a.gpio, mode: a.mode,
      state: a.state, duty: a.duty, dur: a.dur,
      lastAck: a.last_ack, updatedAt: new Date(a.updated_at).getTime(),
    });
  }

  // Recent history per port (LIMIT replaces the old in-memory splice cap —
  // and unlike the RAM version, this survives a restart).
  await loadHistory();

  const { rows: notes } = await adminPool.query(`
    SELECT n.notification_id, n.type, n.title, n.message, n.is_read,
           n.created_at, t.slug
    FROM notifications n JOIN tenants t ON t.tenant_id = n.tenant_id
    ORDER BY n.created_at DESC`);
  store.notifications = notes.map((n) => ({
    id: n.notification_id, tenantId: n.slug, type: n.type,
    title: n.title, message: n.message,
    time: fmtTs(new Date(n.created_at)), read: n.is_read,
  }));

  const { rows: fs } = await adminPool.query(
    `SELECT f.formula_id, f.label, f.expression, t.slug
     FROM calibration_formulas f JOIN tenants t ON t.tenant_id=f.tenant_id
     ORDER BY f.created_at`
  );
  store.savedFormulas = fs.map((f) => ({
    id: f.formula_id, tenantId: f.slug, label: f.label, formula: f.expression,
  }));

  const { rows: sp } = await adminPool.query(
    `SELECT p.profile_id, p.name, p.label, p.unit, p.range_min, p.range_max,
            p.safe_min, p.safe_max, t.slug
     FROM sensor_profiles p JOIN tenants t ON t.tenant_id=p.tenant_id`
  );
  store.sensorProfiles = sp.map((p) => ({
    id: p.profile_id, tenantId: p.slug, name: p.name, label: p.label, unit: p.unit,
    rangeMin: p.range_min, rangeMax: p.range_max,
    safeMin: p.safe_min, safeMax: p.safe_max,
  }));

  rebuildChannelAssignments();
  await loadMapSensors();

  store.ready = true;
  const nPorts = store.devices.reduce(
    (n, d) => n + d.modules.reduce((k, m) => k + m.ports.length, 0), 0);
  console.log(`[store] hydrated: ${tenants.length} tenants, ${store.devices.length} devices, ${nPorts} ports`);
  return store;
}

async function loadHistory() {
  const { rows } = await adminPool.query(`
    SELECT port_id, ts, value, status FROM (
      SELECT port_id, ts, value, status,
             row_number() OVER (PARTITION BY port_id ORDER BY ts DESC) AS rn
      FROM readings
    ) x WHERE rn <= $1 ORDER BY ts ASC`, [HISTORY_CAP]);

  const byPort = new Map();
  for (const r of rows) {
    if (!byPort.has(r.port_id)) byPort.set(r.port_id, []);
    byPort.get(r.port_id).push({
      timestamp: fmtTs(new Date(r.ts)), value: r.value, status: r.status,
    });
  }
  for (const d of store.devices)
    for (const m of d.modules)
      for (const p of m.ports) p.history = byPort.get(p._uuid) ?? [];
}

async function loadMapSensors() {
  const { rows } = await adminPool.query(`
    SELECT ms.map_sensor_id, ms.x, ms.y, t.slug,
           d.node_id, m.i2c_address, p.port_code
    FROM map_sensors ms
    JOIN tenants t ON t.tenant_id = ms.tenant_id
    JOIN ports   p ON p.port_id   = ms.port_id
    JOIN modules m ON m.module_id = p.module_id
    JOIN devices d ON d.device_id = m.device_id`);
  store.mapSensors = rows.map((r) => ({
    id: r.map_sensor_id, x: r.x, y: r.y, tenantId: r.slug,
    deviceId: deviceKey(r.slug, r.node_id),
    moduleId: r.i2c_address,
    portId: r.port_code,
  }));
}

// channelAssignments is a derived view over ports.formula_id, keyed by module.
function rebuildChannelAssignments() {
  const out = {};
  for (const d of store.devices) {
    for (const m of d.modules) {
      out[m.id] ??= {};
      for (const p of m.ports) out[m.id][p.id] = p.formulaLabel ?? null;
    }
  }
  store.channelAssignments = out;
}

// ── Lookups (same signatures the old store exported) ────────────────────────
export function findNode(nid) {
  return store.devices.find((d) => d.id === nid || d.nodeId === nid) ?? null;
}

// Tenant-aware resolution. The firmware's `tid` ("tenant-123") maps to a tenant
// via tenants.mqtt_tid; we then match nid WITHIN that tenant. This is what stops
// one tenant's node from writing into another tenant's device.
export function findNodeScoped(tid, nid) {
  const tenant = tid ? store.tenantByMqttTid[tid] : null;
  if (!tenant) return null;
  return store.devices.find(
    (d) => d._tenantUuid === tenant.id && d.nodeId === nid
  ) ?? null;
}

export function findPortByChannel(node, moduleAddress, channelIndex) {
  const mod = node?.modules.find(
    (m) => m.address?.toLowerCase() === String(moduleAddress).toLowerCase()
  );
  return mod?.ports.find((p) => p.channel === channelIndex) ?? null;
}

// ── Write-through ───────────────────────────────────────────────────────────
// Persist a reading and the port's latest state. Fire-and-forget: a DB hiccup
// must never stall or crash telemetry ingest, so it logs and moves on.
export function pushHistory(port, value, status) {
  port.history.push({ timestamp: fmtTs(new Date()), value, status });
  if (port.history.length > HISTORY_CAP) {
    port.history.splice(0, port.history.length - HISTORY_CAP);
  }
  persistReading(port, value, status).catch((e) =>
    console.error('[store] persist reading failed:', e.message)
  );
}

async function persistReading(port, value, status) {
  const dev = deviceOfPort(port);
  if (!dev) return;
  await withTenant(dev._tenantUuid, async (c) => {
    await c.query(
      `INSERT INTO readings(port_id, tenant_id, raw_adc, value, status)
       VALUES ($1,$2,$3,$4,$5)`,
      [port._uuid, dev._tenantUuid, port.raw ?? 0, value, status]
    );
    await c.query(
      `UPDATE ports SET last_value=$2, last_status=$3 WHERE port_id=$1`,
      [port._uuid, value, status]
    );
  });
}

export async function persistDeviceState(dev) {
  await withTenant(dev._tenantUuid, (c) =>
    c.query(
      `UPDATE devices SET status=$2, comm_mode=$3, uptime_s=$4, rssi=$5,
              free_heap=$6, last_seen=to_timestamp($7/1000.0)
       WHERE device_id=$1`,
      [dev._uuid, dev.status, dev.commMode, dev.uptime ?? 0,
       dev.rssi, dev.freeHeap, dev.lastSeen ?? Date.now()]
    )
  );
}

function deviceOfPort(port) {
  for (const d of store.devices)
    for (const m of d.modules)
      if (m.ports.includes(port)) return d;
  return null;
}

// ── Persisted mutations used by routes.js ───────────────────────────────────
export async function markNotificationRead(id) {
  const n = store.notifications.find((x) => String(x.id) === String(id));
  if (!n) return false;
  const t = store.tenants[n.tenantId];
  await withTenant(t.id, (c) =>
    c.query(`UPDATE notifications SET is_read=true WHERE notification_id=$1`, [id]));
  n.read = true;
  return true;
}

export async function markAllNotificationsRead() {
  for (const t of Object.values(store.tenants)) {
    await withTenant(t.id, (c) => c.query(`UPDATE notifications SET is_read=true`));
  }
  store.notifications.forEach((n) => (n.read = true));
}

export async function createFormula(tenantSlug, label, expression) {
  const t = store.tenants[tenantSlug];
  if (!t) throw new Error(`unknown tenant '${tenantSlug}'`);
  const { rows } = await withTenant(t.id, (c) =>
    c.query(
      `INSERT INTO calibration_formulas(tenant_id, label, expression)
       VALUES ($1,$2,$3) RETURNING formula_id`,
      [t.id, label, expression]
    ));
  const entry = { id: rows[0].formula_id, tenantId: tenantSlug, label, formula: expression };
  store.savedFormulas.push(entry);
  return entry;
}

// Deleting a formula clears it from every channel it was assigned to — the DB
// does this itself via `ports.formula_id ON DELETE SET NULL`, which is the
// schema-level equivalent of the frontend's existing rule.
export async function deleteFormula(id) {
  const f = store.savedFormulas.find((x) => String(x.id) === String(id));
  if (!f) return 0;
  const t = store.tenants[f.tenantId];
  await withTenant(t.id, (c) =>
    c.query(`DELETE FROM calibration_formulas WHERE formula_id=$1`, [id]));

  store.savedFormulas = store.savedFormulas.filter((x) => String(x.id) !== String(id));
  for (const d of store.devices)
    for (const m of d.modules)
      for (const p of m.ports)
        if (p.formulaLabel === f.label) {
          p.formulaLabel = null;
          p.calibration = { type: 'linear', slope: 1, offset: 0 };
        }
  rebuildChannelAssignments();
  return 1;
}

export async function assignChannel(moduleId, portCode, formulaLabel) {
  let target = null, dev = null;
  for (const d of store.devices)
    for (const m of d.modules)
      if (m.id === moduleId)
        for (const p of m.ports)
          if (p.id === portCode) { target = p; dev = d; }
  if (!target) throw new Error(`unknown channel ${moduleId}/${portCode}`);

  const f = formulaLabel
    ? store.savedFormulas.find(
        (x) => x.label === formulaLabel && x.tenantId === dev.tenantId)
    : null;

  await withTenant(dev._tenantUuid, (c) =>
    c.query(
      `UPDATE ports SET formula_id=$2, cal_type=$3 WHERE port_id=$1`,
      [target._uuid, f?.id ?? null, f ? 'expr' : 'linear']
    ));

  target.formulaLabel = f?.label ?? null;
  target.calibration = f
    ? { type: 'expr', expr: f.formula }
    : { type: 'linear', slope: 1, offset: 0 };
  rebuildChannelAssignments();
  return store.channelAssignments[moduleId];
}

export async function saveMapSensors(sensors) {
  const byTenant = new Map();
  for (const s of sensors) {
    const slug = String(s.deviceId).split(':')[0];
    if (!byTenant.has(slug)) byTenant.set(slug, []);
    byTenant.get(slug).push(s);
  }
  for (const [slug, list] of byTenant) {
    const t = store.tenants[slug];
    if (!t) continue;
    await withTenant(t.id, async (c) => {
      await c.query(`DELETE FROM map_sensors WHERE tenant_id=$1`, [t.id]);
      for (const s of list) {
        const port = resolvePort(s.deviceId, s.moduleId, s.portId);
        if (!port) continue;
        await c.query(
          `INSERT INTO map_sensors(tenant_id, port_id, x, y) VALUES ($1,$2,$3,$4)
           ON CONFLICT (tenant_id, port_id) DO UPDATE SET x=$3, y=$4`,
          [t.id, port._uuid, s.x, s.y]
        );
      }
    });
  }
  await loadMapSensors();
  return store.mapSensors;
}

export function resolvePort(deviceId, moduleId, portId) {
  const d = store.devices.find((x) => x.id === deviceId);
  const m = d?.modules.find((x) => x.id === moduleId);
  return m?.ports.find((x) => x.id === portId) ?? null;
}

// ── Projection to the frontend shape ────────────────────────────────────────
// Internal uuids (_uuid/_tenantUuid) are stripped — the API surface stays the
// hardware-real, tenant-namespaced ids the React app consumes.
export function projectDevices(tenantId = null) {
  return store.devices
    .filter((d) => (tenantId ? d.tenantId === tenantId : true))
    .map((d) => ({
      id: d.id, tenantId: d.tenantId, name: d.name, nodeId: d.nodeId,
      status: d.status, commMode: d.commMode,
      uptime: d.uptime, rssi: d.rssi, freeHeap: d.freeHeap,
      modules: d.modules.map((m) => ({
        id: m.id, address: m.address, name: m.name,
        ports: m.ports.map((p) => ({
          id: p.id, label: p.label, unit: p.unit,
          value: p.value, rangeMin: p.rangeMin, rangeMax: p.rangeMax,
          safeMin: p.safeMin, safeMax: p.safeMax,
          status: p.status, history: p.history,
        })),
      })),
      actuators: d.actuators ?? [],
    }));
}

export { applyCalibration };
