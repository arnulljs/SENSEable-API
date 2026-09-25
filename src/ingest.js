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
  store, findNode, findPortByChannel, pushHistory,
  applyCalibration, persistDeviceState, persistPortActive, persistTlmInterval,
  updateCommandStatus, setActuatorAck, addNotification,
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
async function resolvePacketNode(pkt) {
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
  if (!node && tenant && AUTO_PROVISION && pkt.nid) {
    node = await ensureDeviceForTenant(tenant, pkt.nid);
  }
  if (node && claimed) announceClaim(node);

  if (node) {
    // Remember the tid this board really uses, so commands for a pinned node
    // go to the topic it subscribes to rather than its new tenant's tid.
    if (pkt.tid) node.wireTid = pkt.tid;
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

    for (const [channel, raw, code] of module.p ?? []) {
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
      const value = applyCalibration(raw, port.calibration);
      if (value != null) port.value = value;

      port.status = derivePortStatus({
        code: port.code, value: port.value,
        safeMin: port.safeMin, safeMax: port.safeMax,
        lastSeen: port.lastSeen, now,
      });

      pushHistory(port, port.value, port.status, { origin, ts, net }); // → INSERT INTO readings
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

    for (const [channel, raw, code] of module.p ?? []) {
      let port = mod.ports.find((p) => p.channel === Number(channel));
      if (!port && AUTO_PROVISION) { port = await ensurePort(node, mod, channel); if (port) provisioned++; }
      if (!port) { unmatched++; continue; }
      if (port.enabled === false) { skipped++; continue; }
      if (!clock.trusted) { undatable++; continue; }

      const value = applyCalibration(raw, port.calibration);
      // Judged as of its own moment, so a buffered sample is not filed as
      // Offline merely for being old.
      const status = derivePortStatus({
        code: code ?? 0, value,
        safeMin: port.safeMin, safeMax: port.safeMax,
        lastSeen: clock.at.getTime(), now: clock.at.getTime(),
      });
      // A replay is history: it records the network each sample came in on,
      // but never changes the device's current interface.
      pushHistory(port, value, status, { origin, ts: clock.at, replay: true, raw, net });
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
    node.commMode = COMM_MODE[net];
    persistDeviceState(node).catch((e) =>
      console.error('[ingest] persist comm mode failed:', e.message));
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
      // Discovery enumerates every channel on the chip, including empty ones.
      // Only provision channels the firmware says something is attached to —
      // otherwise every node would sprout four ports whether wired or not.
      if (!port && AUTO_PROVISION && String(state).toUpperCase() === 'CONNECTED') {
        port = await ensurePort(node, mod, ch);
        if (port) provisioned++;
      }
      if (!port) { unmatched++; continue; }

      const conn = describeConnState(state);
      port.connState = conn.name;
      port.masked = conn.masked;
      if (port.activeFlag !== conn.attached) {
        port.activeFlag = conn.attached;
        persistPortActive(port, conn.attached).catch((e) =>
          console.error('[ingest] persist active_flag failed:', e.message));
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
const ACK_FAIL     = new Set(['failed', 'error']);
// A command reaches a terminal state on these; actuator returns to ground on
// completed/stopped (the timer elapsed or a manual stop overrode it).
const ACK_TERMINAL = new Set(['completed', 'stopped', 'success', 'failed', 'error']);
const ACK_GROUNDED = new Set(['completed', 'stopped']);

export async function ingestAck(pkt) {
  if (!pkt || pkt.t !== 'ack') return { ok: false, error: 'not an ack packet' };
  if (!pkt.cid) return { ok: false, error: 'ack missing cid' };

  const { node, scoped, error } = await resolvePacketNode(pkt);
  if (error) return { ok: false, error };
  if (!node) return { ok: false, error: `unknown node '${pkt.nid}' (tid '${pkt.tid}')` };

  node.lastSeen = Date.now();

  const status = String(pkt.status ?? '').toLowerCase();
  const terminal = ACK_TERMINAL.has(status);

  // 1. Update the command record (fire-and-forget persistence).
  const cmd = updateCommandStatus(pkt.cid, status, pkt.msg ?? null, terminal);

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
      message: pkt.msg || `Command ${pkt.cid} (${action ?? 'unknown'}) ${status}.`,
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

  const { node, scoped, error } = await resolvePacketNode(pkt);
  if (error) return { ok: false, error };
  if (!node) return { ok: false, error: `unknown node '${pkt.nid}' (tid '${pkt.tid}')` };

  node.lwtOnline = online;
  node.lwtAt = Date.now();
  if (online) node.lastSeen = Date.now();   // an online LWT is a fresh sighting
  // Recompute from the cleared/!cleared LWT gate: offline pins it Offline, online
  // hands the decision back to the ports + staleness so the dot is correct at once.
  refreshNodeStatus(node);

  return { ok: true, node: node.id, tenantScoped: scoped, presence: raw, lwt: true };
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
