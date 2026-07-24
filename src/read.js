// src/read.js ───────────────────────────────────────────────────────────────
// Direct-SQL read models. These return JSON *byte-identical* to what the
// cache-backed projections in store.js produce, so the same frontend can be
// served by either tier without noticing which one answered.
//
// WHY THIS EXISTS
// store.js keeps a hydrated in-memory cache and projects out of it. That's the
// right design for the edge server: one long-lived process, telemetry arriving
// continuously, reads served from RAM. It cannot work in a serverless function,
// which starts cold, serves one request, and dies — there is no process to hold
// a cache, and hydrating the whole store per request would be absurd.
//
// So the cloud read API queries Postgres directly. Both tiers import THIS file,
// which is the point: one implementation, so the two can't drift into
// disagreeing about what a device looks like.
//
// TENANT SCOPING
// These functions never set app.current_tenant themselves — the caller owns
// that, because the two tiers do it differently:
//
//   edge/Express    withTenant(uuid, c => readDevices(c))     RLS-enforced
//   cloud/Vercel    BEGIN; set_config(...); readDevices(c)    RLS-enforced
//   edge hydration  adminPool + { tenantSlug } filter         RLS-exempt
//
// The optional tenantSlug filter is what makes the third case work; under RLS
// it's redundant but harmless, and it keeps one code path for all three.

const HISTORY_CAP = Number(process.env.HISTORY_CAP ?? 40);
const STALE_MS = Number(process.env.STALE_MS ?? 30_000);

// Mirrors store.js: "active" = reported within the staleness window. The UI uses
// it to decide whether a remove button is offered, so both tiers must agree.
const freshly = (ts, now) => ts != null && now - new Date(ts).getTime() <= STALE_MS;

// Board-status rollup — must match deriveModuleStatus() in status.js exactly, or
// the same board would show a different colour depending on which tier answered.
const RANK = { Offline: 3, Fault: 2, Warning: 1, Normal: 0 };
const UI_TO_DEVICE = { Offline: 'offline', Fault: 'fault', Warning: 'warning', Normal: 'online' };

// The frontend's device id is `${slug}:${nodeId}` — globally unique across
// tenants, unlike the firmware's hardcoded node_id.
export const deviceKey = (slug, nodeId) => `${slug}:${nodeId}`;

// Timestamp rendering is DISPLAY formatting, and it must not depend on where
// the code happens to run. toLocaleString() with no timeZone uses the host's
// zone: UTC+8 on the on-site edge server, UTC on a Vercel function. Left
// implicit, the same reading would render eight hours apart depending on which
// tier answered — the kind of bug that looks like corrupt data. Pin it.
const TZ = process.env.DISPLAY_TZ ?? 'Asia/Manila';
export function fmtTs(d) {
  return d.toLocaleString('en-US', {
    month: 'short', day: 'numeric',
    hour: 'numeric', minute: '2-digit', hour12: true,
    timeZone: TZ,
  });
}

// ── Devices (the big one) ───────────────────────────────────────────────────
export async function readDevices(client, { tenantSlug = null } = {}) {
  const filter = tenantSlug ? 'WHERE t.slug = $1' : '';
  const params = tenantSlug ? [tenantSlug] : [];

  const { rows } = await client.query(`
    SELECT
      d.device_id, d.node_id, d.name AS device_name, d.status AS device_status,
      d.comm_mode, d.uptime_s, d.rssi, d.free_heap, d.last_seen,
      t.tenant_id, t.slug AS tenant_slug,
      m.module_id, m.i2c_address, m.name AS module_name,
      m.last_seen AS module_last_seen, m.configured AS module_configured,
      p.port_id, p.port_code, p.port_index, p.label, p.unit,
      p.range_min, p.range_max, p.safe_min, p.safe_max, p.active_flag,
      p.last_value, p.last_status, p.last_seen AS port_last_seen,
      p.configured AS port_configured, p.enabled AS port_enabled,
      p.disabled_reason,
      d.configured AS device_configured,
      f.label AS formula_label
    FROM devices d
    JOIN tenants t ON t.tenant_id = d.tenant_id
    LEFT JOIN modules m ON m.device_id = d.device_id
    LEFT JOIN ports   p ON p.module_id = m.module_id
    LEFT JOIN calibration_formulas f ON f.formula_id = p.formula_id
    ${filter}
    ORDER BY t.slug, d.node_id, m.i2c_address, p.port_index`, params);

  const byDevice = new Map();
  const portUuids = [];

  for (const r of rows) {
    const id = deviceKey(r.tenant_slug, r.node_id);
    if (!byDevice.has(id)) {
      byDevice.set(id, {
        id,
        tenantId: r.tenant_slug,
        name: r.device_name,
        nodeId: r.node_id,
        status: r.device_status,
        commMode: r.comm_mode,
        uptime: Number(r.uptime_s ?? 0),
        rssi: r.rssi,
        freeHeap: r.free_heap,
        lastSeen: r.last_seen ? new Date(r.last_seen).getTime() : null,
        configured: r.device_configured ?? true,
        modules: [],
        actuators: [],
        _uuid: r.device_id,
        _lastSeenRaw: r.last_seen,
      });
    }
    const dev = byDevice.get(id);
    // A device with no expansion board LEFT JOINs to nulls — it must still
    // appear, with modules: []. The frontend renders "No modules detected".
    if (!r.module_id) continue;

    let mod = dev.modules.find((m) => m._uuid === r.module_id);
    if (!mod) {
      mod = { id: r.i2c_address, address: r.i2c_address, name: r.module_name,
              ports: [], _uuid: r.module_id,
              lastSeen: r.module_last_seen ? new Date(r.module_last_seen).getTime() : null,
              configured: r.module_configured ?? true,
              _lastSeenRaw: r.module_last_seen };
      dev.modules.push(mod);
    }
    if (!r.port_id) continue;

    portUuids.push(r.port_id);
    mod.ports.push({
      id: r.port_code,
      label: r.label,
      unit: r.unit,
      value: r.last_value,
      rangeMin: r.range_min,
      rangeMax: r.range_max,
      safeMin: r.safe_min,
      safeMax: r.safe_max,
      status: r.last_status ?? 'Offline',
      history: [],
      activeFlag: r.active_flag,
      // The cloud tier has no live socket, so connState is derived from the
      // last persisted discovery result rather than an in-flight packet.
      connState: r.active_flag ? 'CONNECTED' : 'DISCONNECTED',
      masked: false,
      lastSeen: r.port_last_seen ? new Date(r.port_last_seen).getTime() : null,
      configured: r.port_configured ?? true,
      enabled: r.port_enabled ?? true,
      disabledReason: r.disabled_reason ?? null,
      _uuid: r.port_id,
      _lastSeenRaw: r.port_last_seen,
      _formulaLabel: r.formula_label ?? null,
    });
  }

  await attachHistory(client, byDevice, portUuids);
  await attachActuators(client, byDevice, tenantSlug);

  // Strip internals so the payload matches projectDevices() exactly.
  const now = Date.now();
  return [...byDevice.values()].map((d) => {
    const modules = d.modules.map((m) => {
      const ports = m.ports.map((p) => ({
        id: p.id, label: p.label, unit: p.unit,
        value: p.value, rangeMin: p.rangeMin, rangeMax: p.rangeMax,
        safeMin: p.safeMin, safeMax: p.safeMax,
        // last_status is the value persisted at the last successful reading, so
        // it stays "Normal" forever once a sensor dies. The edge tier recomputes
        // status live against STALE_MS; without the same check here, the cloud
        // dashboard would cheerfully show a green gauge for a channel that
        // stopped reporting days ago.
        status: p.enabled === false
          ? 'Disabled'
          : (freshly(p._lastSeenRaw, now) ? p.status : 'Offline'),
        history: p.history,
        activeFlag: p.activeFlag, connState: p.connState ?? null, masked: !!p.masked,
        lastSeen: p.lastSeen ?? null,
        active: freshly(p._lastSeenRaw, now),
        configured: p.configured ?? true,
        enabled: p.enabled !== false,
        disabledReason: p.disabledReason ?? null,
      }));
      const modActive = freshly(m._lastSeenRaw, now) || ports.some((p) => p.active);
      const considered = ports.map((p) => p.status).filter((x) => x !== 'Disabled');
      let worst = 'Normal';
      for (const st of considered) {
        // Matches deriveModuleStatus(): a single dead sensor is a warning about
        // the board, not proof the board itself is gone.
        const asNode = st === 'Offline' ? 'Warning' : st;
        if (RANK[asNode] > RANK[worst]) worst = asNode;
      }
      return {
        id: m.id, address: m.address, name: m.name,
        lastSeen: m.lastSeen ?? null,
        active: modActive,
        configured: m.configured ?? true,
        status: (!modActive || !considered.length) ? 'offline' : (UI_TO_DEVICE[worst] ?? 'offline'),
        ports,
      };
    });
    return {
      id: d.id, tenantId: d.tenantId, name: d.name, nodeId: d.nodeId,
      status: d.status, commMode: d.commMode,
      uptime: d.uptime, rssi: d.rssi, freeHeap: d.freeHeap,
      lastSeen: d.lastSeen ?? null,
      active: freshly(d._lastSeenRaw, now) || modules.some((m) => m.active),
      configured: d.configured ?? true,
      modules,
      actuators: d.actuators ?? [],
    };
  });
}

// Most-recent HISTORY_CAP readings per port, oldest-first (chart order).
// row_number() windows per port so one query covers every port rather than
// N round-trips — which matters a lot more over a WAN than on localhost.
async function attachHistory(client, byDevice, portUuids) {
  if (!portUuids.length) return;

  const { rows } = await client.query(`
    SELECT port_id, ts, value, status FROM (
      SELECT port_id, ts, value, status,
             row_number() OVER (PARTITION BY port_id ORDER BY ts DESC) AS rn
      FROM readings
      WHERE port_id = ANY($1::uuid[])
    ) x WHERE rn <= $2 ORDER BY ts ASC`, [portUuids, HISTORY_CAP]);

  const byPort = new Map();
  for (const r of rows) {
    if (!byPort.has(r.port_id)) byPort.set(r.port_id, []);
    byPort.get(r.port_id).push({
      timestamp: fmtTs(new Date(r.ts)), value: r.value, status: r.status,
    });
  }
  for (const d of byDevice.values())
    for (const m of d.modules)
      for (const p of m.ports) p.history = byPort.get(p._uuid) ?? [];
}

async function attachActuators(client, byDevice, tenantSlug) {
  const filter = tenantSlug ? 'WHERE t.slug = $1' : '';
  const params = tenantSlug ? [tenantSlug] : [];
  const { rows } = await client.query(`
    SELECT a.actuator_code, a.name, a.port, a.channel, a.gpio, a.mode,
           a.state, a.duty, a.dur, a.last_ack, a.updated_at,
           d.node_id, t.slug
    FROM actuators a
    JOIN devices d ON d.device_id = a.device_id
    JOIN tenants t ON t.tenant_id = a.tenant_id
    ${filter}
    ORDER BY a.port`, params);

  for (const a of rows) {
    const dev = byDevice.get(deviceKey(a.slug, a.node_id));
    if (!dev) continue;
    dev.actuators.push({
      id: a.actuator_code, name: a.name, port: a.port,
      channel: a.channel, gpio: a.gpio, mode: a.mode,
      state: a.state, duty: a.duty, dur: a.dur,
      lastAck: a.last_ack, updatedAt: new Date(a.updated_at).getTime(),
    });
  }
}

// ── Smaller read models ─────────────────────────────────────────────────────
export async function readNotifications(client, { tenantSlug = null } = {}) {
  const filter = tenantSlug ? 'WHERE t.slug = $1' : '';
  const params = tenantSlug ? [tenantSlug] : [];
  const { rows } = await client.query(`
    SELECT n.notification_id, n.type, n.title, n.message, n.is_read,
           n.created_at, t.slug
    FROM notifications n JOIN tenants t ON t.tenant_id = n.tenant_id
    ${filter}
    ORDER BY n.created_at DESC`, params);
  return rows.map((n) => ({
    id: n.notification_id, tenantId: n.slug, type: n.type,
    title: n.title, message: n.message,
    time: fmtTs(new Date(n.created_at)), read: n.is_read,
  }));
}

export async function readFormulas(client, { tenantSlug = null } = {}) {
  const filter = tenantSlug ? 'WHERE t.slug = $1' : '';
  const params = tenantSlug ? [tenantSlug] : [];
  const { rows } = await client.query(`
    SELECT f.formula_id, f.label, f.expression, t.slug
    FROM calibration_formulas f JOIN tenants t ON t.tenant_id = f.tenant_id
    ${filter}
    ORDER BY f.created_at`, params);
  return rows.map((f) => ({
    id: f.formula_id, tenantId: f.slug, label: f.label, formula: f.expression,
  }));
}

export async function readSensorProfiles(client, { tenantSlug = null } = {}) {
  const filter = tenantSlug ? 'WHERE t.slug = $1' : '';
  const params = tenantSlug ? [tenantSlug] : [];
  const { rows } = await client.query(`
    SELECT p.profile_id, p.name, p.label, p.unit, p.range_min, p.range_max,
           p.safe_min, p.safe_max, t.slug
    FROM sensor_profiles p JOIN tenants t ON t.tenant_id = p.tenant_id
    ${filter}`, params);
  return rows.map((p) => ({
    id: p.profile_id, tenantId: p.slug, name: p.name, label: p.label, unit: p.unit,
    rangeMin: p.range_min, rangeMax: p.range_max,
    safeMin: p.safe_min, safeMax: p.safe_max,
  }));
}

// Derived view over ports.formula_id, keyed by module then port:
//   { "0x48": { "A0": "DO", "A1": null } }
export async function readChannelAssignments(client, { tenantSlug = null } = {}) {
  const filter = tenantSlug ? 'WHERE t.slug = $1' : '';
  const params = tenantSlug ? [tenantSlug] : [];
  const { rows } = await client.query(`
    SELECT m.i2c_address, p.port_code, f.label AS formula_label
    FROM ports p
    JOIN modules m ON m.module_id = p.module_id
    JOIN tenants t ON t.tenant_id = p.tenant_id
    LEFT JOIN calibration_formulas f ON f.formula_id = p.formula_id
    ${filter}
    ORDER BY m.i2c_address, p.port_index`, params);

  const out = {};
  for (const r of rows) {
    out[r.i2c_address] ??= {};
    out[r.i2c_address][r.port_code] = r.formula_label ?? null;
  }
  return out;
}

export async function readMapSensors(client, { tenantSlug = null } = {}) {
  const filter = tenantSlug ? 'WHERE t.slug = $1' : '';
  const params = tenantSlug ? [tenantSlug] : [];
  const { rows } = await client.query(`
    SELECT ms.map_sensor_id, ms.x, ms.y, t.slug,
           d.node_id, m.i2c_address, p.port_code
    FROM map_sensors ms
    JOIN tenants t ON t.tenant_id = ms.tenant_id
    JOIN ports   p ON p.port_id   = ms.port_id
    JOIN modules m ON m.module_id = p.module_id
    JOIN devices d ON d.device_id = m.device_id
    ${filter}`, params);
  return rows.map((r) => ({
    id: r.map_sensor_id, x: r.x, y: r.y, tenantId: r.slug,
    deviceId: deviceKey(r.slug, r.node_id),
    moduleId: r.i2c_address,
    portId: r.port_code,
  }));
}

export async function readCommands(client, { tenantSlug = null, deviceId = null } = {}) {
  const where = [];
  const params = [];
  if (tenantSlug) { params.push(tenantSlug); where.push(`t.slug = $${params.length}`); }
  const clause = where.length ? `WHERE ${where.join(' AND ')}` : '';
  params.push(Number(process.env.COMMAND_CAP ?? 100));

  const { rows } = await client.query(`
    SELECT c.command_id, c.cid, c.action, c.mode, c.port, c.payload,
           c.status, c.msg, c.created_at, c.acked_at, t.slug, d.node_id
    FROM commands c
    JOIN tenants t ON t.tenant_id = c.tenant_id
    JOIN devices d ON d.device_id = c.device_id
    ${clause}
    ORDER BY c.created_at DESC
    LIMIT $${params.length}`, params);

  const out = rows.map((c) => ({
    id: c.command_id, cid: c.cid, tenantId: c.slug,
    deviceId: deviceKey(c.slug, c.node_id),
    action: c.action, mode: c.mode, port: c.port, payload: c.payload,
    status: c.status, msg: c.msg,
    createdAt: new Date(c.created_at).getTime(),
    ackedAt: c.acked_at ? new Date(c.acked_at).getTime() : null,
  }));
  return deviceId ? out.filter((c) => c.deviceId === deviceId) : out;
}

// Resolve a tenant slug to its uuid — needed to set app.current_tenant, since
// the RLS policies compare against tenant_id, not slug. Runs RLS-exempt on the
// edge; on the cloud tier the serverless wrapper uses a privileged lookup
// before dropping into the tenant-scoped transaction.
export async function resolveTenantUuid(client, slug) {
  if (!slug) return null;
  const { rows } = await client.query(
    'SELECT tenant_id FROM tenants WHERE slug = $1', [slug]);
  return rows[0]?.tenant_id ?? null;
}
