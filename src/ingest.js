// ingest.js ─────────────────────────────────────────────────────────────────
// Turns raw wire-protocol packets into updated state. Aligned with the FROZEN
// "Payload Schemas" spec (Sasil / Hatulan) and SENSEable-HW's MQTT scripts.
//
//   tlm    usc/thesis/{tid}/{nid}/tlm    raw ADC + per-port status codes
//   disco  usc/thesis/{tid}/{nid}/disco  per-chip port connection map
//   ack    usc/thesis/{tid}/{nid}/ack    command lifecycle feedback (cid-keyed)
//
// Everything resolves the tenant BEFORE matching `nid` — from a node_id pin if
// one exists, otherwise from `tid` (→ tenants.mqtt_tid) — so a node claiming
// "N001" can never write into another tenant's device.

import {
  store, findNode, findPortByChannel, pushHistory, persistLwt, persistWireTid, persistHwConfig,
  applyCalibration, persistDeviceState, persistPortActive, persistTlmInterval,
  updateCommandStatus, markCommandDelivered, setActuatorAck, addNotification,
} from './store.js';
import { derivePortStatus, deriveNodeStatus, describeConnState, STALE_MS } from './status.js';
import { ensureDeviceForTenant, ensureModule, ensurePort, touchPresence } from './provision.js';
import { reconcileNode, checkTopologyConsistency, staleMsFor } from './reconcile.js';
import { claimIfFloating, announceClaim } from './claim.js';

// Auto-provisioning: create inventory rows the first time real hardware
// announces itself. Set AUTO_PROVISION=false to go back to strict declared-only
// mode, where telemetry for unseeded hardware is counted as unmatched.
const AUTO_PROVISION = process.env.AUTO_PROVISION !== 'false';

// '48' | '0X4A' -> '0x4a'
function normAddr(a) {
  const s = String(a).toLowerCase();
  return s.startsWith('0x') ? s : `0x${s}`;
}

// Reject packets whose tenant can't be resolved. Default ON. Set
// INGEST_STRICT_TENANT=false only for a single-tenant bring-up bench.
const STRICT_TENANT = process.env.INGEST_STRICT_TENANT !== 'false';

// NODE-TENANT-OVERRIDE: tenant resolution now has two sources, first wins:
//   1. store.tenantByNodeId — a per-node pin (node_tenant_assignments, set via
//      /api/node-assignments). node_id is MAC-derived, so it is unique per
//      physical board even though every board ships the same compiled tid.
//   2. store.tenantByMqttTid — the normal tid -> tenants.mqtt_tid path.
// Both still require a KNOWN mapping, so an unmapped tid on an unpinned node is
// rejected exactly as before.
async function resolvePacketNode(pkt, { provision = true } = {}) {
  // CLAIM_ON_CONNECT (claim.js): an unpinned board is claimed by the single
  // organization logged in right now, and the claim becomes its pin. Returns
  // null when the mode is off or the choice is ambiguous, leaving the normal
  // routing below untouched.
  const claimed = await claimIfFloating(pkt.nid);
  const pinned = pkt.nid ? store.tenantByNodeId[pkt.nid] : null;
  const tenant = pinned ?? (pkt.tid ? store.tenantByMqttTid[pkt.tid] : null);

  let node = tenant
    ? store.devices.find((d) => d._tenantUuid === tenant.id && d.nodeId === pkt.nid) ?? null
    : null;

  // Known tenant + unknown node ⇒ this is a node we've simply never met. Create
  // it rather than dropping its data on the floor. The tenant check above is
  // what keeps this safe: an unmapped tid can never provision anything.
  if (!node && tenant && AUTO_PROVISION && provision && pkt.nid) {
    node = await ensureDeviceForTenant(tenant, pkt.nid);
  }
  if (node && claimed) announceClaim(node);

  if (node) {
    // Remember the tid this board really uses, so commands for a pinned node
    // go to the topic it subscribes to rather than its new tenant's tid.
    // Persisted (migration 018) so the cloud API can publish to it directly.
    // Not from replayed rows (r:1): after a tenant change the node drains its
    // old backlog under the OLD tid, and learning that would send commands to
    // a topic the node no longer listens on.
    const replayed = pkt.r === 1 || pkt.r === true;
    if (pkt.tid && !replayed) persistWireTid(node, pkt.tid);
    return { node, scoped: true, error: null };
  }

  if (!tenant) {
    const msg =
      `unmapped tid '${pkt.tid}' — add a row to tenants.mqtt_tid mapping it ` +
      `to a tenant (see db/seed_hw.sql), or pin node '${pkt.nid}' with ` +
      `POST /api/node-assignments`;
    if (STRICT_TENANT) {
      console.warn(`[ingest] REJECTED: ${msg}`);
      return { node: null, scoped: false, error: msg };
    }
    console.warn(`[ingest] ${msg} — falling back to unscoped nid lookup (NOT tenant-safe)`);
    return { node: findNode(pkt.nid), scoped: false, error: null };
  }

  return {
    node: null, scoped: false,
    error: pinned
      ? `node '${pkt.nid}' is pinned to tenant '${tenant.slug}' but could not be provisioned there`
      : `node '${pkt.nid}' not registered to tid '${pkt.tid}'`,
  };
}

// The interface that carried this packet ("net" in tlm and disco). Accepts the
// firmware's "wifi" | "cell" and tolerates "cellular"; anything else, or no key
// at all (older firmware), is null — unknown is recorded as unknown.
function packetNet(pkt) {
  const n = String(pkt?.net ?? '').trim().toLowerCase();
  if (n === 'wifi' || n === 'wi-fi') return 'wifi';
  if (n === 'cell' || n === 'cellular') return 'cell';
  return null;
}
const COMM_MODE = { wifi: 'Wi-Fi', cell: 'Cellular' };   // devices.comm_mode enum

// Sample instant. The frozen tlm schema carries no timestamp today, so this
// falls back to ingest time — but ingest time differs between the cloud path and
// the failover path, and (port_id, ts) is the key reconciliation dedupes on. A
// device-supplied `ts` (epoch ms or ISO-8601) is honoured the moment the
// firmware starts sending one, and until then a sample ingested by both tiers
// can appear twice. See docs/CLOUD-FIRST.md.
//
// PLAUSIBILITY. The firmware stamps `ts` from a DS3231 RTC, falling back to
// time(NULL). A board whose RTC was never set reports a clock that starts at the
// 1970 epoch (or garbage from an unpowered RTC), and trusting it would file every
// reading decades in the past, where no chart looks and where (port_id, ts)
// dedupe stops meaning anything. A device clock is therefore only honoured when
// it lands in a believable window; otherwise the sample is marked `untrusted`
// and the caller decides what to do with it.
const TS_FLOOR_MS = Date.UTC(2024, 0, 1);
const TS_FUTURE_SLACK_MS = 10 * 60_000;

function sampleTime(pkt) {
  const raw = pkt?.ts;
  if (raw == null) return { at: new Date(), trusted: false, supplied: false };
  const d = typeof raw === 'number' ? new Date(raw < 1e12 ? raw * 1000 : raw) : new Date(raw);
  const ms = d.getTime();
  if (Number.isNaN(ms) || ms < TS_FLOOR_MS || ms > Date.now() + TS_FUTURE_SLACK_MS) {
    return { at: new Date(), trusted: false, supplied: true };
  }
  return { at: d, trusted: true, supplied: true };
}

// Logged once per node, so a board with an unset RTC produces one warning
// rather than one every ten seconds.
const warnedClock = new Set();
function warnClockOnce(nid, raw) {
  if (warnedClock.has(nid)) return;
  warnedClock.add(nid);
  console.warn(`[ingest] ${nid}: device ts=${raw} is not a believable time — its RTC is ` +
               'probably unset. Live samples use server time; replayed samples are dropped.');
}

// One port's window statistics (firmware 2026-10-28): a port row is
// [ch, mean, status, n, min, max] over win_ms, sampled once a second. Older
// firmware sends [ch, raw, status]; the extra fields are then null. A row whose
// every read failed is [ch, -9999, 3, 0, -9999, -9999]: it has no value at all —
// calibrating -9999 would put a made-up number on the chart.
const READ_FAILED = -9999;
const intOrNull = (v) => (Number.isInteger(v) ? v : null);
function windowStats(port, [, raw, code, n, rawMin, rawMax], winMs) {
  const failed = raw === READ_FAILED || code === 3;
  const value = failed ? null : applyCalibration(raw, port.calibration);
  const hasRange = !failed && Number.isInteger(rawMin) && Number.isInteger(rawMax) &&
                   rawMin !== READ_FAILED && rawMax !== READ_FAILED;
  const a = hasRange ? applyCalibration(rawMin, port.calibration) : null;
  const b = hasRange ? applyCalibration(rawMax, port.calibration) : null;
  const ok = a != null && b != null;
  return {
    value,
    stats: {
      code: intOrNull(code), n: intOrNull(n),
      winMs: Number.isFinite(Number(winMs)) && winMs !== null ? Math.round(Number(winMs)) : null,
      rawMin: hasRange ? rawMin : null, rawMax: hasRange ? rawMax : null,
      valueMin: ok ? Math.min(a, b) : null, valueMax: ok ? Math.max(a, b) : null,
    },
  };
}

// --- Telemetry ('tlm') ------------------------------------------------------
/**
 * @param opts.origin 'cloud' (packet came over the cloud broker — the normal
 *   cloud-first path) or 'local' (came over the local broker during failover,
 *   so this tier is the only holder and owes the row upward).
 */
export async function ingestTelemetry(pkt, opts = {}) {
  if (!pkt || pkt.t !== 'tlm') return { ok: false, error: 'not a telemetry packet' };
  const origin = opts.origin === 'local' ? 'local' : 'cloud';
  const clock = sampleTime(pkt);
  const ts = clock.at;

  // FIFO REPLAY. After a WAN outage the firmware pushes its LittleFS spool,
  // each sample tagged "r":1 (backlog_replay_task). Those readings are real and
  // belong in the time series — but they are HISTORY, not the state of the tank.
  // They are persisted at their own timestamps and nothing else: no gauge value,
  // no status, no live history ring, no presence. Otherwise a replay walks the
  // dashboard through the outage, showing old values as current and raising
  // alerts for conditions that already resolved. scripts/test-replay.js is the
  // contract for this branch, and mqtt.js skips its presence/broadcast pass
  // when the result carries `replay: true`.
  const replay = pkt.r === 1 || pkt.r === true;

  if (clock.supplied && !clock.trusted) warnClockOnce(pkt.nid, pkt.ts);

  const { node, scoped, error } = await resolvePacketNode(pkt);
  if (error) return { ok: false, error };
  if (!node) return { ok: false, error: `unknown node '${pkt.nid}' (tid '${pkt.tid}')` };

  const net = packetNet(pkt);
  if (replay) return ingestReplay(node, pkt, { origin, clock, scoped, net });

  const now = Date.now();
  node.lastSeen = now;
  // Live packets say which interface the node is on NOW; persistDeviceState
  // below writes it with the rest of the device row.
  if (net) node.commMode = COMM_MODE[net];

  // Optional node-level status block (the frozen tlm schema has no `st`; kept
  // tolerant in case the firmware adds one later).
  if (pkt.st && typeof pkt.st === 'object') {
    if (Number.isFinite(pkt.st.up)) node.uptime = pkt.st.up;
    if (Number.isFinite(pkt.st.rs)) node.rssi = pkt.st.rs;
    if (Number.isFinite(pkt.st.hp)) node.freeHeap = pkt.st.hp;
    if (Number.isFinite(pkt.st.f))  node.systemFault = pkt.st.f;
  }

  let matched = 0, unmatched = 0, provisioned = 0, skipped = 0;

  for (const module of pkt.adc ?? []) {
    const addr = normAddr(module.a);

    // A board that reports data exists, whether or not anyone declared it.
    let mod = node.modules.find((m) => m.address?.toLowerCase() === addr);
    if (!mod && AUTO_PROVISION) { mod = await ensureModule(node, addr); if (mod) provisioned++; }
    if (!mod) { unmatched += (module.p ?? []).length; continue; }

    // The board itself is present: the firmware lists every chip it can reach,
    // with an empty `p` when none of its channels are switched on. Touching
    // presence only per channel made a board with every channel off look
    // unplugged.
    touchPresence(node, mod, null);

    for (const row of module.p ?? []) {
      const [channel, raw, code] = row;
      let port = mod.ports.find((p) => p.channel === Number(channel));
      if (!port && AUTO_PROVISION) { port = await ensurePort(node, mod, channel); if (port) provisioned++; }
      if (!port) { unmatched++; continue; }

      touchPresence(node, mod, port);

      // A channel the operator switched off is still SAMPLED by the firmware
      // (until sensor_port_down is honoured), so packets keep arriving for it.
      // We record that it's still present, then drop the sample: an unwired
      // input reads leakage voltage, and persisting that would fill the
      // time-series with noise and let it colour the board's status.
      if (port.enabled === false) {
        port.raw = raw;
        port.lastSeen = now;
        port.status = 'Disabled';
        skipped++;
        continue;
      }

      port.code = code ?? 0;
      port.raw = raw;              // frozen protocol: keep the raw count
      port.lastSeen = now;

      // All engineering-unit conversion happens HERE, server-side.
      const { value, stats } = windowStats(port, row, pkt.win_ms);
      // A failed window keeps the gauge on the last good value (its status says
      // the read failed); the reading itself is stored without a value.
      if (value != null) port.value = value;

      port.status = derivePortStatus({
        code: port.code, value: port.value,
        safeMin: port.safeMin, safeMax: port.safeMax,
        lastSeen: port.lastSeen, now,
      });

      pushHistory(port, value, port.status, { origin, ts, net, stats }); // → INSERT INTO readings
      matched++;
    }
  }

  refreshNodeStatus(node, now);
  persistDeviceState(node).catch((e) =>
    console.error('[ingest] persist device state failed:', e.message));

  return { ok: true, node: node.id, tenantScoped: scoped, matched, unmatched, provisioned, skipped };
}

// Persist one buffered sample per channel, touching no live state. A replayed
// sample without a believable device timestamp cannot be placed on the timeline
// at all (stamping it "now" would put outage-era data on top of the present and
// collide with the live sample on (port_id, ts)), so it is dropped and counted.
async function ingestReplay(node, pkt, { origin, clock, scoped, net }) {
  let stored = 0, unmatched = 0, provisioned = 0, skipped = 0, undatable = 0;

  for (const module of pkt.adc ?? []) {
    const addr = normAddr(module.a);
    let mod = node.modules.find((m) => m.address?.toLowerCase() === addr);
    if (!mod && AUTO_PROVISION) { mod = await ensureModule(node, addr); if (mod) provisioned++; }
    if (!mod) { unmatched += (module.p ?? []).length; continue; }

    for (const row of module.p ?? []) {
      const [channel, raw, code] = row;
      let port = mod.ports.find((p) => p.channel === Number(channel));
      if (!port && AUTO_PROVISION) { port = await ensurePort(node, mod, channel); if (port) provisioned++; }
      if (!port) { unmatched++; continue; }
      if (port.enabled === false) { skipped++; continue; }
      if (!clock.trusted) { undatable++; continue; }

      const { value, stats } = windowStats(port, row, pkt.win_ms);
      // Judged as of its own moment, so a buffered sample is not filed as
      // Offline merely for being old.
      const status = derivePortStatus({
        code: code ?? 0, value,
        safeMin: port.safeMin, safeMax: port.safeMax,
        lastSeen: clock.at.getTime(), now: clock.at.getTime(),
      });
      // A replay is history: it records the network each sample came in on,
      // but never changes the device's current interface.
      pushHistory(port, value, status, { origin, ts: clock.at, replay: true, raw, net, stats });
      stored++;
    }
  }

  return {
    ok: true, replay: true, node: node.id, tenantScoped: scoped,
    stored, unmatched, provisioned, skipped, undatable,
  };
}

// --- Discovery ('disco') ----------------------------------------------------
// Frozen schema: buses[] with a per-chip ports object p0..p3 whose values are
// CONNECTED | DISCONNECTED | DISABLED. We map that onto each port's activeFlag
// (has a live sensor) + connState (for the UI's "masked vs unplugged" nuance).
export async function ingestDiscovery(pkt) {
  if (!pkt || pkt.t !== 'disco') return { ok: false, error: 'not a discovery packet' };

  const { node, scoped, error } = await resolvePacketNode(pkt);
  if (error) return { ok: false, error };
  if (!node) return { ok: false, error: `unknown node '${pkt.nid}' (tid '${pkt.tid}')` };

  node.lastSeen = Date.now();

  // Discovery is sent on every (re)connect, so it is the first to report an
  // interface change — e.g. a node rebooted from Wi-Fi into cellular mode.
  const net = packetNet(pkt);
  if (net && node.commMode !== COMM_MODE[net]) {
    node.commMode = COMM_MODE[net];   // persisted by the persistDeviceState at the end
  }

  let connected = 0, disconnected = 0, disabled = 0, unmatched = 0, provisioned = 0;

  for (const bus of pkt.buses ?? []) {
    const addr = normAddr(bus.a);
    let mod = node.modules.find((m) => m.address?.toLowerCase() === addr);
    if (!mod && AUTO_PROVISION) { mod = await ensureModule(node, addr); if (mod) provisioned++; }
    if (!mod) { unmatched++; continue; }
    touchPresence(node, mod, null);

    for (const [key, state] of Object.entries(bus.ports ?? {})) {
      const ch = Number(String(key).replace(/^p/i, ''));   // 'p2' -> 2
      let port = mod.ports.find((p) => p.channel === ch);
      const conn = describeConnState(state);
      // Every channel the node reports is created, whatever its state. The
      // firmware keeps ports switched off until an operator turns them on, and a
      // switched-off port is left out of telemetry — so discovery is the only
      // place it ever appears. New channels start disabled (provision.js), so
      // this adds switches for the operator, not readings. CONNECTED means
      // "readable", not "a sensor is attached": the node cannot tell.
      if (!port && AUTO_PROVISION && conn.name !== 'UNKNOWN') {
        port = await ensurePort(node, mod, ch);
        if (port) provisioned++;
      }
      if (!port) { unmatched++; continue; }

      port.masked = conn.masked;
      if (port.activeFlag !== conn.attached || port.connState !== conn.name) {
        port.activeFlag = conn.attached;
        port.connState = conn.name;
        persistPortActive(port, conn.attached, conn.name).catch((e) =>
          console.error('[ingest] persist port state failed:', e.message));
      }

      if (conn.name === 'CONNECTED') connected++;
      else if (conn.name === 'DISABLED') disabled++;
      else disconnected++;
    }
  }

  // The node's own publish cadence, when it declares one. Feeds staleMsFor().
  if (Number.isFinite(Number(pkt.tlm_interval_ms))) {
    persistTlmInterval(node, Number(pkt.tlm_interval_ms)).catch((e) =>
      console.error('[ingest] persist tlm_interval failed:', e.message));
  }

  // Discovery is edge-triggered, so an absent board is ambiguous. Cross-check
  // the node's own chip count before treating this as a full snapshot.
  const topology = checkTopologyConsistency(node, pkt);

  // Correct any drift between what the firmware is doing and what the operator
  // asked for. Wrapped: a reconciliation problem must never reject a discovery
  // packet, which is still good data regardless.
  let drift = [];
  try {
    drift = await reconcileNode(node, pkt);
  } catch (e) {
    console.warn('[ingest] reconcile failed:', e.message);
  }

  refreshNodeStatus(node, Date.now());
  // Persist presence even though discovery carries no readings. A node with no
  // ADCs sends ONLY discovery (never telemetry), so without this its last_seen
  // never advances in the database — the edge's live subscription looked online
  // while the cloud (which only sees what's written) stayed frozen. This is the
  // one write that makes a sensorless-but-alive node show online in the cloud.
  persistDeviceState(node).catch((e) =>
    console.error('[ingest] persist device state (disco) failed:', e.message));
  return {
    ok: true, node: node.id, tenantScoped: scoped,
    detectedChips: pkt.detected_chips ?? null,
    connected, disconnected, disabled, unmatched, provisioned,
    topology,
    drift: drift.length ? drift : undefined,
  };
}

// --- Acknowledgement ('ack') ------------------------------------------------
// Closed-loop feedback for downward commands, correlated by `cid`. Updates the
// command record's lifecycle status, reflects the result on the target actuator
// (for `actuate`), and raises a notification on failure.
//
// status ∈ started | completed | stopped | failed | error | success
const ACK_OK       = new Set(['started', 'completed', 'stopped', 'success']);
const ACK_FAIL     = new Set(['failed', 'error', 'expired']);
// A command reaches a terminal state on these; actuator returns to ground on
// completed/stopped (the timer elapsed or a manual stop overrode it).
const ACK_TERMINAL = new Set(['completed', 'stopped', 'success', 'failed', 'error', 'expired']);
const ACK_GROUNDED = new Set(['completed', 'stopped']);

export async function ingestAck(pkt) {
  if (!pkt || pkt.t !== 'ack') return { ok: false, error: 'not an ack packet' };
  if (!pkt.cid) return { ok: false, error: 'ack missing cid' };
  // Ping replies belong to the standalone latency listener, which never logs a
  // command; ingesting them would warn "orphan ack" every 10 s.
  if (pkt.action === 'ping') return { ok: true, ignored: 'ping' };

  const { node, scoped, error } = await resolvePacketNode(pkt);
  if (error) return { ok: false, error };
  if (!node) return { ok: false, error: `unknown node '${pkt.nid}' (tid '${pkt.tid}')` };

  node.lastSeen = Date.now();

  // The firmware sends its text as `details` (send_command_ack); `msg` is the
  // older name, still accepted.
  const text = pkt.details || pkt.msg || null;

  // failed + "expired": the node refused a command more than 5 minutes old, on
  // purpose. Recorded as its own status ('expired' is in the actuator_ack enum)
  // so the dashboard can say "try again" instead of a generic failure.
  let status = String(pkt.status ?? '').toLowerCase();
  if (status === 'failed' && /expired/i.test(text ?? '')) status = 'expired';
  const terminal = ACK_TERMINAL.has(status);

  // "Command already executed; redelivery ignored" (handover 3.1) arrives as
  // `completed` but says nothing about the command's outcome — only that the
  // node got it again. Recording it as completed would end a timed actuation
  // on the dashboard while the output is still running. It is a delivery
  // receipt: stop re-sending, change nothing else.
  if (/already executed/i.test(text ?? '')) {
    await markCommandDelivered(pkt.cid, node);
    return { ok: true, node: node.id, tenantScoped: scoped, redelivery: true };
  }

  // 1. Update the command record (fire-and-forget persistence).
  const cmd = await updateCommandStatus(pkt.cid, status, text, terminal, node);

  // 2. Reflect on the target actuator, if this ack is for an actuate command.
  let actuator = null;
  const action = pkt.action ?? cmd?.action;
  if (action === 'actuate' && cmd?.port != null) {
    const patch = { status };
    if (ACK_GROUNDED.has(status)) patch.state = 0; // timed run ended / stopped
    actuator = setActuatorAck(node, cmd.port, patch);
  }

  // 3. Notify on failure so it surfaces in the Notifications page.
  if (ACK_FAIL.has(status)) {
    addNotification(node, {
      type: 'fault',
      title: `Command ${status}`,
      message: status === 'expired'
        ? `Command expired — ${action ?? 'it'} was more than 5 minutes old when ${node.nodeId} received it, ` +
          'so the node ignored it. Please try again.'
        : text || `Command ${pkt.cid} (${action ?? 'unknown'}) ${status}.`,
    }).catch((e) => console.error('[ingest] ack notification failed:', e.message));
  }

  // An ack whose cid matches no logged command is an orphan. In normal
  // operation this cannot happen — the backend stamps a cid on everything it
  // issues — so it means a command injected by other means (a bench
  // mosquitto_pub) or the firmware's `cid ? cid : "unknown"` fallback firing.
  // Worth saying out loud: discarded silently, it looks like the hardware
  // ignored the command.
  if (!cmd) {
    console.warn(`[ingest] orphan ack from ${node.id}: cid '${pkt.cid}' matches no ` +
                 `logged command (status '${status}')`);
  }

  return {
    ok: true, node: node.id, tenantScoped: scoped,
    cid: pkt.cid, status,
    matchedCommand: Boolean(cmd),
    orphan: cmd ? undefined : true,
    actuator: actuator?.id ?? null,
    result: ACK_FAIL.has(status) ? 'fail' : (ACK_OK.has(status) ? 'ok' : 'unknown'),
  };
}

// --- Status / LWT ('status') ------------------------------------------------
// The firmware sets an MQTT Last Will on usc/thesis/{tid}/{nid}/status: the
// broker publishes {"t":"lwt","status":"offline"} the instant the node's socket
// drops, and the node itself publishes {"status":"online"} (retained) on
// connect. This is a definitive presence signal — a clean disconnect is known
// immediately, instead of waiting out the staleness window. The staleness sweep
// stays as the fallback for a node that vanishes without the broker noticing
// (power-cut on the broker link, say), so lwtOnline is advisory: it forces
// Offline when false, but never masks a node the sweep already considers dead.
export async function ingestStatus(pkt, opts = {}) {
  if (!pkt) return { ok: false, error: 'empty status packet' };
  const raw = String(pkt.status ?? '').toLowerCase();
  if (raw !== 'online' && raw !== 'offline') {
    return { ok: false, error: `status must be 'online' or 'offline', got '${pkt.status}'` };
  }
  const online = raw === 'online';

  // Never provisions (same as config): status is retained, and on a tenant
  // change the node leaves a retained 'offline' on the OLD tenant's topic. With
  // provisioning on, that replay re-created a node the operator had removed.
  // A status for a node we don't hold carries nothing to keep: ignored, not an error.
  const { node, scoped, error } = await resolvePacketNode(pkt, { provision: false });
  if (!node) return { ok: true, ignored: error ?? `unknown node '${pkt.nid}'` };

  node.lwtOnline = online;
  node.lwtAt = Date.now();
  if (online) node.lastSeen = Date.now();   // an online LWT is a fresh sighting
  persistLwt(node, online).catch((e) => console.error('[ingest] persist lwt failed:', e.message));
  // Recompute from the cleared/!cleared LWT gate: offline pins it Offline, online
  // hands the decision back to the ports + staleness so the dot is correct at once.
  refreshNodeStatus(node);

  return { ok: true, node: node.id, tenantScoped: scoped, presence: raw, lwt: true };
}

// --- Configuration twin ('config') ------------------------------------------
// The node publishes its network setup, retained, on usc/thesis/{tid}/{nid}/config:
//   { t:"config", v:1, tid, nid, active_hw_mode, wifi_ssid, cell_apn,
//     target_broker, ts }
// Retained because it is the node's current state, not an event, so unlike
// tlm/disco/ack a retained copy IS ingested. The broker replays it on every
// subscribe (each Lambda run), so an unchanged payload writes nothing. It never
// creates a device: a replay for a board someone deleted must not bring it back.
const cfgStr = (v, max) => (typeof v === 'string' && v.trim() ? v.trim().slice(0, max) : null);

// last_disconnect (handover 2.3): the node's last NETWORK outage, not a reboot.
// Only known fields, bounded, so a malformed config can't put arbitrary JSON in
// the devices row. null when absent (no outage since boot) or unusable.
const cfgNum = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);
function cleanDisconnect(d) {
  if (!d || typeof d !== 'object' || Array.isArray(d)) return null;
  const out = {
    cause: cfgStr(d.cause, 10)?.toLowerCase() ?? null,      // wifi | mqtt
    reason: cfgStr(d.reason, 40),
    reasonCode: Number.isInteger(d.reason_code) ? d.reason_code : null,
    at: Number.isInteger(d.at) && d.at > 1_600_000_000 ? d.at : null,   // absent if the clock wasn't set
    offlineS: cfgNum(d.offline_s),
    rowsSpooled: Number.isInteger(d.rows_spooled) ? d.rows_spooled : null,
  };
  return Object.values(out).some((v) => v != null) ? out : null;
}
// Field by field: the copy loaded from jsonb has its keys in a different order.
const DISCONNECT_KEYS = ['cause', 'reason', 'reasonCode', 'at', 'offlineS', 'rowsSpooled'];
const sameDisconnect = (a, b) => (!a || !b) ? !a === !b
  : DISCONNECT_KEYS.every((k) => (a[k] ?? null) === (b[k] ?? null));

export async function ingestConfig(pkt) {
  if (!pkt || pkt.t !== 'config') return { ok: false, error: 'not a config packet' };
  const { node, scoped, error } = await resolvePacketNode(pkt, { provision: false });
  if (error) return { ok: false, error };
  if (!node) return { ok: true, ignored: `unknown node '${pkt.nid}'` };

  const clock = sampleTime(pkt);
  const cfg = {
    mode: cfgStr(pkt.active_hw_mode, 20)?.toUpperCase() ?? null,
    ssid: cfgStr(pkt.wifi_ssid, 100),
    apn: cfgStr(pkt.cell_apn, 100),
    // A broker URI may carry user:password@ — never stored or shown.
    broker: cfgStr(pkt.target_broker, 255)?.replace(/\/\/[^/@]*@/, '//') ?? null,
    // Why this boot started; after a crash also where the previous one was
    // (last_step/last_task) and how long it had run. Absent fields clear the
    // previous crash: the config describes the current boot.
    reset: cfgStr(pkt.last_reset, 30)?.toLowerCase() ?? null,
    crashStep: cfgStr(pkt.last_step, 80),
    crashTask: cfgStr(pkt.last_task, 40),
    prevUptimeS: Number.isFinite(Number(pkt.prev_uptime_s)) && pkt.prev_uptime_s !== null
      ? Number(pkt.prev_uptime_s) : null,
    lastDisconnect: cleanDisconnect(pkt.last_disconnect),
    syncedAt: clock.at.getTime(),
  };
  const prev = node.hwConfig;
  const unchanged = prev && prev.mode === cfg.mode && prev.ssid === cfg.ssid &&
    prev.apn === cfg.apn && prev.broker === cfg.broker &&
    prev.reset === cfg.reset && prev.crashStep === cfg.crashStep &&
    prev.crashTask === cfg.crashTask && prev.prevUptimeS === cfg.prevUptimeS &&
    sameDisconnect(prev.lastDisconnect, cfg.lastDisconnect) &&
    // No usable ts: the receive time would differ on every replay, so keep the
    // first one rather than rewriting the row each minute.
    (!clock.trusted || prev.syncedAt === cfg.syncedAt);
  if (unchanged) return { ok: true, node: node.id, tenantScoped: scoped, changed: false };

  node.hwConfig = cfg;
  persistHwConfig(node).catch((e) => console.error('[ingest] persist config failed:', e.message));
  return { ok: true, node: node.id, tenantScoped: scoped, changed: true };
}

// Recompute one node's status from its ports + staleness.
export function refreshNodeStatus(node, now = Date.now()) {
  // Per-node staleness; see staleMsFor() in reconcile.js for why one global
  // threshold cannot serve both a fast and a low-power node.
  const staleMs = staleMsFor(node, STALE_MS);

  const portStatuses = [];
  for (const m of node.modules) {
    for (const p of m.ports) {
      // Disabled channels are excluded from the rollup entirely — switching off
      // an unwired input must never drag the node's status down.
      if (p.enabled === false) { p.status = 'Disabled'; continue; }
      p.status = derivePortStatus({
        code: p.code, value: p.value,
        safeMin: p.safeMin, safeMax: p.safeMax,
        lastSeen: p.lastSeen, now, staleMs,
      });
      portStatuses.push(p.status);
    }
  }
  // A node the broker reported OFFLINE via LWT stays Offline until it reconnects
  // (an online LWT or fresh telemetry clears it), whatever its last-known ports
  // said — the socket is provably down.
  node.status = node.lwtOnline === false
    ? 'offline'
    : deriveNodeStatus({
        portStatuses, systemFault: node.systemFault, lastSeen: node.lastSeen, now, staleMs,
      });
}

export function refreshAll(now = Date.now()) {
  for (const node of store.devices) refreshNodeStatus(node, now);
}
