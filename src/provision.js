// provision.js ──────────────────────────────────────────────────────────────
// Turns OBSERVED hardware into persisted inventory.
//
// Before this, devices/boards/ports had to be seeded by hand and telemetry for
// anything unseeded was silently dropped (`unmatched++`). That made the
// dashboard a picture of what someone typed into seed.sql rather than what is
// physically wired up. Now the first packet from a node creates it, the first
// appearance of a chip address creates the board, and the first reading on a
// channel creates the port.
//
// THREE RULES THIS FILE ENFORCES
//
//  1. CREATE ON FIRST SIGHT, NEVER DELETE ON ABSENCE. Provisioning only ever
//     inserts. When hardware goes quiet the row stays and reads Offline; the
//     staleness sweep in ingest.js handles that. Deleting is a deliberate
//     operator action (routes.js), permitted only while a row is stale.
//
//  2. NEVER OVERWRITE HUMAN CONFIGURATION. Re-seeing a known channel updates
//     presence (last_seen) and nothing else. A sensor unplugged for a week and
//     returned to the same channel resumes with its label, unit, ranges and
//     calibration exactly as the operator left them.
//
//  3. HONEST PLACEHOLDERS. A new port has no discoverable identity — the wire
//     protocol carries a raw ADC count, not "dissolved oxygen in mg/L". So it is
//     created with identity calibration and `raw` units, showing the true count
//     until an operator configures it. Inventing a plausible-looking unit would
//     put a meaningless number on a dashboard someone might act on.
//
// Everything runs inside withTenant(), so provisioning obeys the same RLS
// isolation as every other write: a packet can only ever create hardware inside
// the tenant its `tid` maps to.

import { withTenant } from '../db/pool.js';
import { store, deviceKey } from './store.js';

// Node outputs are a fixed property of the ESP32 board per the frozen protocol
// (port 1..6 == OUT1..OUT6), not a detachable accessory that discovery reports.
// They're created with the device so the Control page has something to address.
// Set AUTO_PROVISION_ACTUATORS=false to leave the actuator list empty until you
// add outputs deliberately.
const AUTO_ACTUATORS = process.env.AUTO_PROVISION_ACTUATORS !== 'false';
const ACTUATOR_COUNT = Math.min(6, Math.max(0, Number(process.env.ACTUATOR_COUNT ?? 6)));

// Guards against two packets racing to create the same row. The DB has unique
// constraints as the real defence; this just avoids pointless round-trips and
// duplicate cache entries within one process.
const inFlight = new Map();
function once(key, fn) {
  if (inFlight.has(key)) return inFlight.get(key);
  const p = fn().finally(() => inFlight.delete(key));
  inFlight.set(key, p);
  return p;
}

// ── Devices ─────────────────────────────────────────────────────────────────
/**
 * Find (or create) the device a packet belongs to. Returns null when the tid
 * isn't mapped to a tenant — an unknown tenant must never be able to conjure
 * hardware into existence.
 */
export async function ensureDevice(tid, nid) {
  const tenant = tid ? store.tenantByMqttTid[tid] : null;
  if (!tenant || !nid) return null;

  const existing = store.devices.find(
    (d) => d._tenantUuid === tenant.id && d.nodeId === nid);
  if (existing) return existing;

  return once(`dev:${tenant.id}:${nid}`, async () => {
    // Re-check inside the guard: another packet may have created it while we
    // were awaiting.
    const raced = store.devices.find(
      (d) => d._tenantUuid === tenant.id && d.nodeId === nid);
    if (raced) return raced;

    const row = await withTenant(tenant.id, async (c) => {
      const { rows } = await c.query(
        `INSERT INTO devices (tenant_id, node_id, name, status, first_seen, last_seen)
         VALUES ($1, $2, $3, 'online', now(), now())
         ON CONFLICT (tenant_id, node_id) DO UPDATE SET last_seen = now()
         RETURNING device_id, node_id, name, status, comm_mode,
                   uptime_s, rssi, free_heap, configured`,
        [tenant.id, nid, `ESP32 ${nid}`]);
      return rows[0];
    });

    const dev = {
      id: deviceKey(tenant.slug, row.node_id),
      _uuid: row.device_id,
      _tenantUuid: tenant.id,
      tenantId: tenant.slug,
      name: row.name,
      nodeId: row.node_id,
      status: row.status,
      commMode: row.comm_mode,
      uptime: Number(row.uptime_s ?? 0),
      rssi: row.rssi,
      freeHeap: row.free_heap,
      // false until an operator renames it — the UI uses this to distinguish a
      // generated placeholder name from one somebody actually chose.
      configured: row.configured ?? false,
      systemFault: 0,
      lastSeen: Date.now(),
      modules: [],
      actuators: [],
    };
    store.devices.push(dev);
    console.log(`[provision] new node '${nid}' → ${dev.id}`);

    if (AUTO_ACTUATORS && ACTUATOR_COUNT > 0) {
      await provisionActuators(tenant.id, dev).catch((e) =>
        console.error('[provision] actuators failed:', e.message));
    }
    return dev;
  });
}

async function provisionActuators(tenantUuid, dev) {
  const rows = await withTenant(tenantUuid, async (c) => {
    const out = [];
    for (let n = 1; n <= ACTUATOR_COUNT; n += 1) {
      const { rows: r } = await c.query(
        `INSERT INTO actuators (device_id, tenant_id, actuator_code, name, port,
                                channel, mode, state, duty, dur, last_ack)
         VALUES ($1, $2, $3, $4, $5, $6, 'bin', 0, 0, 0, 'pending')
         ON CONFLICT (device_id, port) DO NOTHING
         RETURNING actuator_code, name, port, channel, gpio, mode,
                   state, duty, dur, last_ack, updated_at`,
        [dev._uuid, tenantUuid, `out${n}`, `Output ${n}`, `OUT${n}`, n - 1]);
      if (r[0]) out.push(r[0]);
    }
    return out;
  });

  for (const a of rows) {
    dev.actuators.push({
      id: a.actuator_code, name: a.name, port: a.port,
      channel: a.channel, gpio: a.gpio, mode: a.mode,
      state: a.state, duty: a.duty, dur: a.dur,
      lastAck: a.last_ack, updatedAt: new Date(a.updated_at).getTime(),
    });
  }
}

// ── Modules (expansion boards) ──────────────────────────────────────────────
export async function ensureModule(dev, addr) {
  const norm = String(addr).toLowerCase();
  const existing = dev.modules.find((m) => m.address?.toLowerCase() === norm);
  if (existing) return existing;

  return once(`mod:${dev._uuid}:${norm}`, async () => {
    const raced = dev.modules.find((m) => m.address?.toLowerCase() === norm);
    if (raced) return raced;

    const row = await withTenant(dev._tenantUuid, async (c) => {
      const { rows } = await c.query(
        `INSERT INTO modules (device_id, tenant_id, i2c_address, name,
                              first_seen, last_seen)
         VALUES ($1, $2, $3, $4, now(), now())
         ON CONFLICT (device_id, i2c_address) DO UPDATE SET last_seen = now()
         RETURNING module_id, i2c_address, name, configured`,
        [dev._uuid, dev._tenantUuid, norm, `Expansion Board ${norm}`]);
      return rows[0];
    });

    const mod = {
      id: row.i2c_address,
      _uuid: row.module_id,
      address: row.i2c_address,
      name: row.name,
      ports: [],
      lastSeen: Date.now(),
      configured: row.configured ?? false,
    };
    dev.modules.push(mod);
    // Stable display order regardless of which chip answered first.
    dev.modules.sort((a, b) => a.address.localeCompare(b.address));
    console.log(`[provision] new board ${norm} on ${dev.id}`);
    return mod;
  });
}

// ── Ports (ADC channels) ────────────────────────────────────────────────────
export async function ensurePort(dev, mod, channel) {
  const ch = Number(channel);
  if (!Number.isInteger(ch) || ch < 0) return null;

  const existing = mod.ports.find((p) => p.channel === ch);
  if (existing) return existing;

  return once(`port:${mod._uuid}:${ch}`, async () => {
    const raced = mod.ports.find((p) => p.channel === ch);
    if (raced) return raced;

    const code = `A${ch}`;
    const row = await withTenant(dev._tenantUuid, async (c) => {
      // Descriptive columns are omitted deliberately so the migration-006
      // defaults apply: identity calibration, `raw` units, full ADS1115 span.
      const { rows } = await c.query(
        `INSERT INTO ports (module_id, tenant_id, port_code, port_index,
                            active_flag, first_seen, last_seen)
         VALUES ($1, $2, $3, $4, true, now(), now())
         ON CONFLICT (module_id, port_code) DO UPDATE SET last_seen = now()
         RETURNING port_id, port_code, port_index, label, unit,
                   range_min, range_max, safe_min, safe_max,
                   cal_type, cal_slope, cal_offset, active_flag,
                   last_value, last_status, configured`,
        [mod._uuid, dev._tenantUuid, code, ch]);
      return rows[0];
    });

    const port = {
      id: row.port_code,
      _uuid: row.port_id,
      channel: row.port_index,
      label: row.label,
      unit: row.unit,
      rangeMin: row.range_min,
      rangeMax: row.range_max,
      safeMin: row.safe_min,
      safeMax: row.safe_max,
      activeFlag: row.active_flag,
      configured: row.configured,
      calibration: row.cal_type === 'expr'
        ? { type: 'expr', expr: null }
        : { type: 'linear', slope: row.cal_slope, offset: row.cal_offset },
      formulaLabel: null,
      value: row.last_value,
      code: 0,
      status: row.last_status ?? 'Offline',
      connState: 'CONNECTED',
      masked: false,
      lastSeen: null,
      history: [],
    };
    mod.ports.push(port);
    mod.ports.sort((a, b) => a.channel - b.channel);
    console.log(`[provision] new channel ${code} on ${mod.address} (${dev.id})`);
    return port;
  });
}

// ── Presence bookkeeping ────────────────────────────────────────────────────
// Called on every packet. Throttled because telemetry arrives every couple of
// seconds and a write per packet per port would swamp the DB for information
// that only matters at STALE_MS resolution.
const PRESENCE_MIN_INTERVAL_MS = Number(process.env.PRESENCE_WRITE_MS ?? 15_000);
const lastPresenceWrite = new Map();

export function touchPresence(dev, mod = null, port = null) {
  const now = Date.now();
  if (mod) mod.lastSeen = now;
  if (port) port.lastSeen = now;

  const key = port?._uuid ?? mod?._uuid ?? dev._uuid;
  const prev = lastPresenceWrite.get(key) ?? 0;
  if (now - prev < PRESENCE_MIN_INTERVAL_MS) return;
  lastPresenceWrite.set(key, now);

  withTenant(dev._tenantUuid, async (c) => {
    if (port) {
      await c.query('UPDATE ports SET last_seen = now() WHERE port_id = $1', [port._uuid]);
    }
    if (mod) {
      await c.query('UPDATE modules SET last_seen = now() WHERE module_id = $1', [mod._uuid]);
    }
  }).catch((e) => console.error('[provision] presence write failed:', e.message));
}
