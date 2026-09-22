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
import { STALE_MS, deriveModuleStatus } from './status.js';
import { staleMsFor } from './reconcile.js';
import { ownsSideEffects } from './role.js';

const HISTORY_CAP = Number(process.env.HISTORY_CAP ?? 40);

// Display timestamp formatting. Both tiers must format identically, so the
// timezone is explicit: left to the host, toLocaleString() would render the
// same reading eight hours apart on the UTC+8 edge server and a UTC Vercel
// function.
const TZ = process.env.DISPLAY_TZ ?? 'Asia/Manila';

export function fmtTs(d) {
  return d.toLocaleString('en-US', {
    month: 'short', day: 'numeric',
    hour: 'numeric', minute: '2-digit', hour12: true,
    timeZone: TZ,
  });
}



export const deviceKey = (slug, nodeId) => `${slug}:${nodeId}`;

// The cache. Same shape the old in-memory store exposed, so routes.js reads
// (store.notifications, store.savedFormulas, ...) keep working unchanged.
export const store = {
  tenants: {},            // slug -> { id(uuid), slug, name, mqttTid }
  tenantByMqttTid: {},    // 'tenant-123' -> tenant record   (ingest tenant resolution)
  tenantByNodeId: {},     // NODE-TENANT-OVERRIDE: node_id -> tenant record (migration 011)
  devices: [],            // hydrated device tree (frontend shape + internal uuids)
  notifications: [],
  savedFormulas: [],
  channelAssignments: {},
  sensorProfiles: [],
  mapSensors: [],
  commands: [],           // recent downward commands, keyed by cid (cmd↔ack)
  ready: false,
};

// ── Hydration ───────────────────────────────────────────────────────────────
// Loads every tenant's tree. Runs as the OWNER (RLS-exempt) because boot-time
// hydration legitimately spans all tenants; per-request writes still go through
// withTenant() and are RLS-enforced.
// `lite` is for short-lived runs (the AWS Lambda bridge): load only what ingest
// needs — tenants, pins, the device tree with calibration, actuators, recent
// commands for ack matching — and skip the dashboard-only caches. The history
// load in particular scans every reading ever stored and would grow with the
// database on every one-minute invocation.
export async function hydrate({ lite = false } = {}) {
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

  // NODE-TENANT-OVERRIDE: per-node tenant pins (migration 011). Checked before
  // tid resolution in ingest.js, so a physical board can be moved between
  // tenants from the database alone. Guarded like loadCommands(): a DB that
  // hasn't run the migration still boots, just without the feature.
  store.tenantByNodeId = {};
  try {
    const { rows: pins } = await adminPool.query(
      `SELECT a.node_id, t.slug FROM node_tenant_assignments a
       JOIN tenants t ON t.tenant_id = a.tenant_id`);
    for (const p of pins) {
      if (store.tenants[p.slug]) store.tenantByNodeId[p.node_id] = store.tenants[p.slug];
    }
  } catch (e) {
    if (e.code !== '42P01') throw e;
    console.warn('[store] node_tenant_assignments missing — run migration 1730000000011; ' +
                 'node-level tenant overrides disabled');
  }

  // One flat join, assembled in JS — cheaper and clearer than three round trips.
  const { rows } = await adminPool.query(`
    SELECT
      d.device_id, d.node_id, d.name AS device_name, d.status AS device_status,
      d.comm_mode, d.uptime_s, d.rssi, d.free_heap, d.last_seen, d.tlm_interval_ms,
      t.tenant_id, t.slug AS tenant_slug,
      m.module_id, m.i2c_address, m.name AS module_name,
      m.last_seen AS module_last_seen, m.configured AS module_configured,
      p.port_id, p.port_code, p.port_index, p.label, p.unit,
      p.range_min, p.range_max, p.safe_min, p.safe_max,
      p.active_flag, p.cal_type, p.cal_slope, p.cal_offset,
      p.last_value, p.last_status, p.last_seen AS port_last_seen,
      p.configured AS port_configured, p.enabled AS port_enabled,
      p.disabled_reason,
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
        // Declared telemetry cadence, so per-node staleness survives a restart
        // rather than reverting to the global default for a node we already
        // know publishes slowly.
        tlmIntervalMs: r.tlm_interval_ms ?? null,
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
        // Presence survives restarts: a board last heard from days ago stays in
        // the inventory reading Offline rather than vanishing.
        lastSeen: r.module_last_seen ? new Date(r.module_last_seen).getTime() : null,
        configured: r.module_configured ?? true,
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
      connState: r.active_flag ? 'CONNECTED' : 'DISCONNECTED',
      masked: false,
      // Restored from the DB rather than reset to null, so "last seen" is real
      // across restarts. Staleness still forces Offline until fresh telemetry.
      lastSeen: r.port_last_seen ? new Date(r.port_last_seen).getTime() : null,
      configured: r.port_configured ?? true,
      enabled: r.port_enabled ?? true,
      disabledReason: r.disabled_reason ?? null,
      history: [],             // filled by loadHistory() below
    });
  }
  store.devices = [...byDevice.values()];

  // Actuators (node-level siblings of modules — LEDC outputs, not I2C).
  const { rows: acts } = await adminPool.query(`
    SELECT a.*, d.node_id, t.slug FROM actuators a
    JOIN devices d ON d.device_id = a.device_id
    JOIN tenants t ON t.tenant_id = a.tenant_id
    -- ORDER BY is not cosmetic here. Without it Postgres returns heap order,
    -- and an UPDATE rewrites the row to the end of the heap — so commanding
    -- OUT1 made it jump to the bottom of the Control page on the next hydrate.
    -- read.js already orders by port; this query was the one that didn't.
    ORDER BY a.port`);
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
  if (lite) {
    await loadCommands();
    store.ready = true;
    return store;
  }

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
  await loadCommands();

  store.ready = true;
  const nPorts = store.devices.reduce(
    (n, d) => n + d.modules.reduce((k, m) => k + m.ports.length, 0), 0);
  console.log(`[store] hydrated: ${tenants.length} tenants, ${store.devices.length} devices, ${nPorts} ports, ` +
              `${Object.keys(store.tenantByNodeId).length} node overrides`);
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

// Recent downward commands + their latest ack status (cmd↔ack correlation).
const COMMAND_CAP = Number(process.env.COMMAND_CAP ?? 100);
async function loadCommands() {
  let rows;
  try {
    ({ rows } = await adminPool.query(`
      SELECT c.command_id, c.cid, c.action, c.mode, c.port, c.payload,
             c.status, c.msg, c.created_at, c.acked_at,
             t.slug, d.node_id
      FROM commands c
      JOIN tenants t ON t.tenant_id = c.tenant_id
      JOIN devices d ON d.device_id = c.device_id
      ORDER BY c.created_at DESC
      LIMIT $1`, [COMMAND_CAP]));
  } catch (e) {
    // Not-yet-migrated DB (migration 1730000000003) — boot with no command log.
    if (e.code === '42P01') {
      console.warn('[store] commands table missing — run migration 1730000000003; command log disabled');
      store.commands = [];
      return;
    }
    throw e;
  }
  store.commands = rows.map((c) => ({
    id: c.command_id, cid: c.cid, tenantId: c.slug,
    deviceId: deviceKey(c.slug, c.node_id),
    action: c.action, mode: c.mode, port: c.port,
    payload: c.payload, status: c.status, msg: c.msg,
    createdAt: new Date(c.created_at).getTime(),
    ackedAt: c.acked_at ? new Date(c.acked_at).getTime() : null,
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
/**
 * Append a sample to the in-memory ring and persist it.
 *
 * @param opts.origin 'cloud' when the packet arrived over the CLOUD broker (the
 *   normal cloud-first path, so Supabase already has it or is about to), or
 *   'local' when it arrived over the LOCAL broker during failover, which means
 *   this row is the only copy in existence and the cloud is owed it.
 * @param opts.ts  device-supplied sample instant. Falls back to ingest time.
 * @param opts.raw  the raw ADC count for THIS sample. Required for a replayed
 *   packet, which deliberately does not write port.raw — without it the stored
 *   row would carry the live raw count instead of the buffered one.
 * @param opts.replay  true for a sample the node buffered during an outage and
 *   is only now pushing. It is persisted, but kept out of the in-memory ring and
 *   out of ports.last_value — a three-hour-old reading must not become the
 *   number on the gauge, and appending it to the ring would put the live chart
 *   out of order.
 */
export function pushHistory(port, value, status, opts = {}) {
  const at = opts.ts instanceof Date ? opts.ts : new Date();

  if (!opts.replay) {
    port.history.push({ timestamp: fmtTs(at), value, status });
    if (port.history.length > HISTORY_CAP) {
      port.history.splice(0, port.history.length - HISTORY_CAP);
    }
  }

  persistReading(port, value, status, at, opts.origin ?? 'cloud', opts.replay === true,
                 opts.raw ?? port.raw ?? 0)
    .catch((e) => console.error('[store] persist reading failed:', e.message));
}

// Which tier this process is. On the cloud tier nothing is ever "owed" upward,
// so every row is born synced regardless of which broker delivered it.
const IS_CLOUD_TIER = process.env.TIER === 'cloud';

// BRIDGE MODE — the interim shape of cloud-first.
//
// The finished topology has something subscribing to the cloud broker and
// writing straight into Supabase. Until that exists, a packet published to the
// cloud broker reaches this edge, gets stored as origin='cloud', synced=true,
// and STOPS. synced=true means the sync worker will never offer it upward, so
// the row lives on the edge forever and the cloud dashboard never sees it.
// Correct once a bridge exists. A dead end until then.
//
// With EDGE_BRIDGES_CLOUD=true this server IS the bridge: cloud-origin rows are
// marked unsynced so the sync worker carries them to Supabase. The path becomes
// node -> cloud broker -> edge -> Supabase -> dashboard, which delivers the same
// observable behaviour with no new infrastructure.
//
// Turn it OFF the day a real bridge starts writing to Supabase, or both will
// insert the same rows. They would collide harmlessly on (port_id, ts), but
// paying twice for every reading is not a steady state.
const EDGE_BRIDGES_CLOUD = process.env.EDGE_BRIDGES_CLOUD === 'true';

async function persistReading(port, value, status, at, origin, replay = false, raw = 0) {
  const dev = deviceOfPort(port);
  if (!dev) return;
  // A 'cloud'-origin row on the edge is a MIRROR of something the cloud already
  // holds. Marking it synced is what stops sync.js from pushing it straight back
  // up, which would be an echo loop with the ingest path.
  const synced = IS_CLOUD_TIER || (origin === 'cloud' && !EDGE_BRIDGES_CLOUD);
  await withTenant(dev._tenantUuid, async (c) => {
    await c.query(
      `INSERT INTO readings(port_id, tenant_id, ts, raw_adc, value, status, origin, synced)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT (port_id, ts) DO NOTHING`,
      [port._uuid, dev._tenantUuid, at, raw, value, status, origin, synced]
    );
    // A replayed sample is history. Letting it write last_value would make the
    // dashboard show an outage-era reading as the current one.
    if (!replay) {
      await c.query(
        `UPDATE ports SET last_value=$2, last_status=$3 WHERE port_id=$1`,
        [port._uuid, value, status]
      );
    }
  });
}

// Store the node's declared publish cadence. Separate from persistDeviceState
// because it changes once at provisioning rather than on every packet, and
// writing it with the hot path would mean an UPDATE per telemetry message for
// a value that almost never changes.
export async function persistTlmInterval(node, ms) {
  if (!Number.isFinite(ms) || ms <= 0) return;
  if (node.tlmIntervalMs === ms) return;          // no change, no write
  node.tlmIntervalMs = ms;
  await withTenant(node._tenantUuid, (c) =>
    c.query('UPDATE devices SET tlm_interval_ms = $2 WHERE device_id = $1',
      [node._uuid, ms])).catch((e) =>
    console.error('[store] persist tlm_interval failed:', e.message));
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

// Persist a port's discovery-derived active_flag (fire-and-forget).
export async function persistPortActive(port, attached) {
  const dev = deviceOfPort(port);
  if (!dev) return;
  await withTenant(dev._tenantUuid, (c) =>
    c.query(`UPDATE ports SET active_flag=$2 WHERE port_id=$1`, [port._uuid, attached]));
}

// ── Renames (write-through, RLS-enforced) ───────────────────────────────────
// Each writes to Postgres FIRST (through withTenant → RLS), then updates the
// in-memory cache only on success, so a failed write never leaves the cache and
// the DB disagreeing. The name is the single source of truth: the /devices
// projection reads it straight from the cache, so every page that renders the
// device/board/actuator picks up the new name on its next poll.
function normName(name) {
  const s = String(name ?? '').trim();
  if (!s) throw new Error('name is required');
  if (s.length > 120) throw new Error('name too long (max 120 chars)');
  return s;
}

export async function renameDevice(dev, name) {
  const label = normName(name);
  await withTenant(dev._tenantUuid, (c) =>
    c.query(`UPDATE devices SET name=$2, configured=true WHERE device_id=$1`,
            [dev._uuid, label]));
  dev.name = label;
  dev.configured = true;
  return dev;
}

export async function renameModule(dev, moduleId, name) {
  const mod = dev.modules.find((m) => m.id === moduleId || m._uuid === moduleId);
  if (!mod) throw new Error(`unknown module '${moduleId}'`);
  const label = normName(name);
  await withTenant(dev._tenantUuid, (c) =>
    c.query(`UPDATE modules SET name=$2, configured=true WHERE module_id=$1`,
            [mod._uuid, label]));
  mod.name = label;
  mod.configured = true;
  return mod;
}

export async function renameActuator(dev, actuatorId, name) {
  const act = (dev.actuators ?? []).find((a) => a.id === actuatorId);
  if (!act) throw new Error(`unknown actuator '${actuatorId}'`);
  const label = normName(name);
  await withTenant(dev._tenantUuid, (c) =>
    c.query(
      `UPDATE actuators SET name=$3 WHERE device_id=$1 AND actuator_code=$2`,
      [dev._uuid, act.id, label]
    ));
  act.name = label;
  return act;
}

// ── Actuators + commands + ack correlation ──────────────────────────────────
const portNumOf = (p) => {
  const m = String(p).match(/(\d+)/);
  return m ? Number(m[1]) : NaN;
};

function findActuator(node, idOrPort) {
  const acts = node?.actuators ?? [];
  const asNum = portNumOf(idOrPort);
  return (
    acts.find((a) => a.id === idOrPort) ??
    acts.find((a) => portNumOf(a.port) === asNum) ??
    null
  );
}

// Insert a downward command as it's published, so the ack can later find it by
// cid. Returns the cache record.
export async function recordCommand(dev, envelope) {
  const { rows } = await withTenant(dev._tenantUuid, (c) =>
    c.query(
      `INSERT INTO commands(tenant_id, device_id, cid, action, mode, port, payload, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,'pending')
       RETURNING command_id, created_at`,
      [dev._tenantUuid, dev._uuid, envelope.cid, envelope.action,
       envelope.mode ?? null, envelope.port ?? null, JSON.stringify(envelope)]
    ));
  const rec = {
    id: rows[0].command_id, cid: envelope.cid, tenantId: dev.tenantId,
    deviceId: dev.id, action: envelope.action, mode: envelope.mode ?? null,
    port: envelope.port ?? null, payload: envelope, status: 'pending', msg: null,
    createdAt: new Date(rows[0].created_at).getTime(), ackedAt: null,
  };
  store.commands.unshift(rec);
  if (store.commands.length > COMMAND_CAP) store.commands.length = COMMAND_CAP;
  return rec;
}

// Update a command's lifecycle status from an incoming ack. Returns the cache
// record (with .action/.port) so ingest can reflect it on the actuator.
export function updateCommandStatus(cid, status, msg, terminal = false) {
  const rec = store.commands.find((c) => c.cid === cid);
  if (rec) {
    rec.status = status;
    rec.msg = msg;
    if (terminal) rec.ackedAt = Date.now();
  }
  // Persist without blocking ingest. We know the tenant from the cache record;
  // if the command wasn't in cache (e.g. issued before restart), skip the DB.
  const t = rec ? store.tenants[rec.tenantId] : null;
  if (t) {
    withTenant(t.id, (c) =>
      c.query(
        `UPDATE commands
         SET status=$2, msg=$3, acked_at = CASE WHEN $4 THEN now() ELSE acked_at END
         WHERE cid=$1`,
        [cid, status, msg, terminal]
      )).catch((e) => console.error('[store] persist ack failed:', e.message));
  }
  return rec ?? null;
}

// Reflect an ack (or an optimistic send) on the target actuator: last_ack, and
// optionally state. Updates cache + DB. `patch` = { status, state? }.
export function setActuatorAck(node, idOrPort, patch = {}) {
  const act = findActuator(node, idOrPort);
  if (!act) return null;
  if (patch.status != null) act.lastAck = patch.status;
  if (patch.state != null) act.state = patch.state ? 1 : 0;
  act.updatedAt = Date.now();

  persistActuator(node, act).catch((e) =>
    console.error('[store] persist actuator failed:', e.message));
  return act;
}

// Optimistically apply an actuate command's intent to the actuator at send time
// (before the ack arrives): mode/state/duty/dur + last_ack='pending'.
export function applyActuatorCommand(node, idOrPort, out = {}) {
  const act = findActuator(node, idOrPort);
  if (!act) return null;
  if (out.mode === 'bin' || out.mode === 'pwm') act.mode = out.mode;
  if (out.state != null) act.state = out.state ? 1 : 0;
  if (act.mode === 'pwm' && out.duty != null) {
    act.duty = Math.min(255, Math.max(0, Math.round(Number(out.duty) || 0)));
  }
  if (out.dur != null) act.dur = Math.max(0, Math.round(Number(out.dur) || 0));
  act.lastAck = 'pending';
  act.updatedAt = Date.now();

  persistActuator(node, act).catch((e) =>
    console.error('[store] persist actuator failed:', e.message));
  return act;
}

async function persistActuator(node, act) {
  await withTenant(node._tenantUuid, (c) =>
    c.query(
      `UPDATE actuators SET mode=$3, state=$4, duty=$5, dur=$6, last_ack=$7
       WHERE device_id=$1 AND actuator_code=$2`,
      [node._uuid, act.id, act.mode, act.state, act.duty, act.dur, act.lastAck]
    ));
}

// Insert a notification (e.g. on command failure). Scoped to the node's tenant.
export async function addNotification(node, { type, title, message }) {
  // With a cloud bridge running, the bridge raises notifications for packets
  // both tiers receive; raising them here too would put every alert on the
  // dashboard twice (event_uid is random, so sync cannot collapse them). The
  // edge still raises its own while failed over, when the bridge is blind.
  if (!ownsSideEffects()) return null;
  const { rows } = await withTenant(node._tenantUuid, (c) =>
    c.query(
      // synced follows the same rule as readings: on the cloud tier a row is
      // born final, on the edge it is owed upward until sync.js drains it.
      `INSERT INTO notifications(tenant_id, type, title, message, synced)
       VALUES ($1,$2,$3,$4,$5) RETURNING notification_id, created_at`,
      [node._tenantUuid, type, title, message, IS_CLOUD_TIER]
    ));
  store.notifications.unshift({
    id: rows[0].notification_id, tenantId: node.tenantId, type, title, message,
    time: fmtTs(new Date(rows[0].created_at)), read: false,
  });
  return rows[0].notification_id;
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

function resolvePort(deviceId, moduleId, portId) {
  const d = store.devices.find((x) => x.id === deviceId);
  const m = d?.modules.find((x) => x.id === moduleId);
  return m?.ports.find((x) => x.id === portId) ?? null;
}

// ── Routing refresh ─────────────────────────────────────────────────────────
// Tenant tid mappings and node pins used to be read once, at boot. With two
// tiers ingesting, a pin set on one reaches the other through the sync worker —
// straight into the database, behind this process's back — so both tiers
// re-read routing on a timer (server.js). Tenant records are updated IN PLACE,
// because devices and pins hold references to them.
export async function refreshRouting() {
  const { rows: tenants } = await adminPool.query(
    'SELECT tenant_id, slug, name, mqtt_tid FROM tenants');
  const byTid = {};
  for (const t of tenants) {
    const rec = store.tenants[t.slug] ?? (store.tenants[t.slug] = { slug: t.slug });
    Object.assign(rec, { id: t.tenant_id, name: t.name, mqttTid: t.mqtt_tid });
    if (t.mqtt_tid) byTid[t.mqtt_tid] = rec;
  }
  store.tenantByMqttTid = byTid;

  let pins = [];
  try {
    ({ rows: pins } = await adminPool.query(
      `SELECT a.node_id, t.slug FROM node_tenant_assignments a
       JOIN tenants t ON t.tenant_id = a.tenant_id`));
  } catch (e) {
    if (e.code !== '42P01') throw e;
  }
  const next = {};
  for (const p of pins) if (store.tenants[p.slug]) next[p.node_id] = store.tenants[p.slug];

  // A pin that arrived or moved since the last read retires the node's row in
  // every other tenant, exactly as a local assignment would.
  for (const [nid, t] of Object.entries(next)) {
    if (store.tenantByNodeId[nid]?.id !== t.id) retireRowsElsewhere(nid, t.id);
  }
  store.tenantByNodeId = next;
}

// ── NODE-TENANT-OVERRIDE: node tenant assignments (migration 011) ───────────
// Cross-tenant routing metadata, so it goes through adminPool (the same reason
// sync_state does) rather than any single tenant's RLS scope. Testing tool: it
// is not gated by x-tenant-id and must not be exposed to Designers as a feature.
export function listNodeAssignments() {
  return Object.entries(store.tenantByNodeId).map(([nodeId, t]) => ({
    nodeId, tenantSlug: t.slug, tenantName: t.name,
  }));
}

export async function assignNodeToTenant(nodeId, tenantSlug, note = null) {
  const nid = String(nodeId ?? '').trim();
  if (!nid) throw Object.assign(new Error('nodeId is required'), { status: 400 });
  const tenant = store.tenants[tenantSlug];
  if (!tenant) throw Object.assign(new Error(`unknown tenant '${tenantSlug}'`), { status: 404 });

  await adminPool.query(
    `INSERT INTO node_tenant_assignments (node_id, tenant_id, note, assigned_at)
     VALUES ($1, $2, $3, now())
     ON CONFLICT (node_id) DO UPDATE
       SET tenant_id = EXCLUDED.tenant_id, note = EXCLUDED.note, assigned_at = now()`,
    [nid, tenant.id, note]);

  store.tenantByNodeId[nid] = tenant;
  const retired = retireRowsElsewhere(nid, tenant.id);
  return { nodeId: nid, tenantSlug: tenant.slug, retired };
}

// After a node moves, the row it left behind was heard from seconds ago and
// would keep reading Connected for a full staleness window — two tenants
// appearing to own one physical board. Clearing its presence in memory makes it
// read Offline on the next sweep (raising the ordinary "went offline" notice in
// the tenant that lost it) and makes it deletable immediately. Nothing is
// deleted: its configuration stays, in case the node is moved back.
function retireRowsElsewhere(nid, keepTenantUuid) {
  const retired = [];
  for (const d of store.devices) {
    if (d.nodeId !== nid || d._tenantUuid === keepTenantUuid) continue;
    d.lastSeen = null;
    for (const m of d.modules) {
      m.lastSeen = null;
      for (const p of m.ports) p.lastSeen = null;
    }
    retired.push(d.id);
  }
  return retired;
}

export async function clearNodeAssignment(nodeId) {
  const nid = String(nodeId ?? '').trim();
  if (!nid) throw Object.assign(new Error('nodeId is required'), { status: 400 });
  await adminPool.query('DELETE FROM node_tenant_assignments WHERE node_id = $1', [nid]);
  delete store.tenantByNodeId[nid];

  // The node falls back to whatever tenant its own tid maps to. Retire the row
  // in every OTHER tenant, for the same reason as on assignment. The tid is
  // taken from the node's own packets; if it hasn't reported since boot there
  // is nothing live to retire from.
  const wire = store.devices.find((d) => d.nodeId === nid && d.wireTid)?.wireTid;
  const home = wire ? store.tenantByMqttTid[wire] : null;
  const retired = home ? retireRowsElsewhere(nid, home.id) : [];
  return { nodeId: nid, cleared: true, retired };
}

// The tid a node actually publishes and subscribes under. For an overridden
// node this differs from its tenant's mqtt_tid, and commands must go to the
// node's real topic or the board never sees them. Learned from the node's own
// packets (ingest.js stamps dev.wireTid), so it is unknown until the node has
// sent something since this process booted; until then the tenant's tid is the
// only candidate.
export function commandTidFor(dev) {
  return dev.wireTid ?? store.tenants[dev.tenantId]?.mqttTid ?? null;
}

// ── Projection to the frontend shape ────────────────────────────────────────
// Internal uuids (_uuid/_tenantUuid) are stripped — the API surface stays the
// hardware-real, tenant-namespaced ids the React app consumes.
// "Active" means currently reporting — seen within the staleness window. It is
// the ONLY thing that gates deletion: you may remove hardware that has gone
// quiet, never hardware that is live. Exposing it per entity lets the UI show or
// hide each remove button without a second round-trip.
const isPortActive   = (p, now = Date.now()) => p.lastSeen != null && now - p.lastSeen <= STALE_MS;
const isModuleActive = (m, now = Date.now()) =>
  (m.lastSeen != null && now - m.lastSeen <= STALE_MS) || m.ports.some((p) => isPortActive(p, now));
const isDeviceActive = (d, now = Date.now()) =>
  (d.lastSeen != null && now - d.lastSeen <= STALE_MS) || d.modules.some((m) => isModuleActive(m, now));

export function projectDevices(tenantId = null) {
  const now = Date.now();
  return store.devices
    .filter((d) => (tenantId ? d.tenantId === tenantId : true))
    .map((d) => ({
      id: d.id, tenantId: d.tenantId, name: d.name, nodeId: d.nodeId,
      status: d.status, commMode: d.commMode,
      uptime: d.uptime, rssi: d.rssi, freeHeap: d.freeHeap,
      lastSeen: d.lastSeen ?? null,
      active: isDeviceActive(d, now),
      configured: d.configured ?? true,
      modules: d.modules.map((m) => {
        const ports = m.ports.map((p) => ({
          id: p.id, label: p.label, unit: p.unit,
          value: p.value, rangeMin: p.rangeMin, rangeMax: p.rangeMax,
          safeMin: p.safeMin, safeMax: p.safeMax,
          // A switched-off channel reports 'Disabled' rather than a colour that
          // implies a judgement about a reading nobody is monitoring.
          status: p.enabled === false ? 'Disabled' : p.status,
          history: p.history,
          activeFlag: p.activeFlag, connState: p.connState ?? null, masked: !!p.masked,
          lastSeen: p.lastSeen ?? null,
          active: isPortActive(p, now),
          configured: p.configured ?? true,
          enabled: p.enabled !== false,
          disabledReason: p.disabledReason ?? null,
        }));
        return {
          id: m.id, address: m.address, name: m.name,
          lastSeen: m.lastSeen ?? null,
          active: isModuleActive(m, now),
          configured: m.configured ?? true,
          status: deriveModuleStatus({
            portStatuses: ports.map((p) => p.status),
            lastSeen: m.lastSeen, now,
            // Same per-node threshold the ports were judged by, or a board on a
            // slow-publishing node reads Offline while its own channels read
            // Normal — the rollup contradicting the things it rolls up.
            staleMs: staleMsFor(d, STALE_MS),
          }),
          ports,
        };
      }),
      actuators: d.actuators ?? [],
    }));
}

// ── Removal (operator-initiated, never automatic) ───────────────────────────
// Absence alone must never delete anything — a node that drops off for ten
// minutes would otherwise lose its calibration. So removal is explicit, and
// refused outright while the hardware is still reporting.
class ActiveHardwareError extends Error {
  constructor(what) {
    super(`${what} is currently reporting and cannot be removed — disconnect it first`);
    this.status = 409;
  }
}

// ── Enabling / disabling a channel ──────────────────────────────────────────
// An unconnected ADS1115 input floats: leakage current and channel-to-channel
// crosstalk push it to a few thousand counts, which the pipeline faithfully
// renders as a healthy-looking gauge. Only a human knows nothing is wired there,
// so this records that judgement. A disabled channel stops contributing to
// status, stops raising notifications, and stops writing readings.
export async function setPortEnabled(dev, moduleId, portId, enabled, reason = null) {
  const mod = dev.modules.find((m) => m.id === moduleId || m._uuid === moduleId);
  if (!mod) { const e = new Error(`unknown module '${moduleId}'`); e.status = 404; throw e; }
  const port = mod.ports.find((p) => p.id === portId || p._uuid === portId);
  if (!port) { const e = new Error(`unknown port '${portId}'`); e.status = 404; throw e; }

  const on = !!enabled;
  await withTenant(dev._tenantUuid, (c) =>
    c.query(
      `UPDATE ports SET enabled = $2,
              disabled_reason = CASE WHEN $2 THEN NULL ELSE $3 END,
              disabled_at     = CASE WHEN $2 THEN NULL ELSE now() END
       WHERE port_id = $1`,
      [port._uuid, on, reason]));

  port.enabled = on;
  port.disabledReason = on ? null : reason;
  if (!on) {
    // Freeze the last reading rather than clearing it — an operator may want to
    // see what the channel was showing when they switched it off.
    port.status = 'Disabled';
  }
  return port;
}

export async function removeDevice(dev) {
  if (isDeviceActive(dev)) throw new ActiveHardwareError(`device '${dev.name}'`);
  await withTenant(dev._tenantUuid, (c) =>
    c.query('DELETE FROM devices WHERE device_id = $1', [dev._uuid]));
  const i = store.devices.indexOf(dev);
  if (i >= 0) store.devices.splice(i, 1);

  // Removing a node from the tenant it is PINNED to also drops the pin. An
  // operator who removes a board from a tenant means "it doesn't belong here";
  // leaving the pin in place re-provisioned it into the same tenant on the very
  // next packet, which read as the Remove button not working. With the pin gone
  // the board falls back to its own tid's tenant. A pin pointing at a DIFFERENT
  // tenant is left alone: removing a stale leftover row must not undo a move.
  let unpinned = false;
  const pin = store.tenantByNodeId[dev.nodeId];
  if (pin && pin.id === dev._tenantUuid) {
    await adminPool.query('DELETE FROM node_tenant_assignments WHERE node_id = $1', [dev.nodeId]);
    delete store.tenantByNodeId[dev.nodeId];
    unpinned = true;
    console.log(`[store] ${dev.id} removed from its pinned tenant — pin cleared, ` +
                `'${dev.nodeId}' now routes by its own tid`);
  }
  return { removed: dev.id, unpinned };
}

export async function removeModule(dev, moduleId) {
  const mod = dev.modules.find((m) => m.id === moduleId || m._uuid === moduleId);
  if (!mod) { const e = new Error(`unknown module '${moduleId}'`); e.status = 404; throw e; }
  if (isModuleActive(mod)) throw new ActiveHardwareError(`board '${mod.name}'`);
  await withTenant(dev._tenantUuid, (c) =>
    c.query('DELETE FROM modules WHERE module_id = $1', [mod._uuid]));
  dev.modules.splice(dev.modules.indexOf(mod), 1);
  return { removed: `${dev.id}::${mod.id}` };
}

export async function removePort(dev, moduleId, portId) {
  const mod = dev.modules.find((m) => m.id === moduleId || m._uuid === moduleId);
  if (!mod) { const e = new Error(`unknown module '${moduleId}'`); e.status = 404; throw e; }
  const port = mod.ports.find((p) => p.id === portId || p._uuid === portId);
  if (!port) { const e = new Error(`unknown port '${portId}'`); e.status = 404; throw e; }
  if (isPortActive(port)) throw new ActiveHardwareError(`channel '${port.id}'`);
  await withTenant(dev._tenantUuid, (c) =>
    c.query('DELETE FROM ports WHERE port_id = $1', [port._uuid]));
  mod.ports.splice(mod.ports.indexOf(port), 1);
  return { removed: `${dev.id}::${mod.id}::${port.id}` };
}

export { applyCalibration };
