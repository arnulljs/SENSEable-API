// ingest.js ─────────────────────────────────────────────────────────────────
// Turns raw wire-protocol packets into updated state. Aligned with the FROZEN
// "Payload Schemas" spec (Sasil / Hatulan) and SENSEable-HW's MQTT scripts.
//
//   tlm    usc/thesis/{tid}/{nid}/tlm    raw ADC + per-port status codes
//   disco  usc/thesis/{tid}/{nid}/disco  per-chip port connection map
//   ack    usc/thesis/{tid}/{nid}/ack    command lifecycle feedback (cid-keyed)
//
// Everything resolves the tenant from `tid` (→ tenants.mqtt_tid) BEFORE matching
// `nid`, so a node claiming "N001" can never write into another tenant's device.

import {
  store, findNode, findNodeScoped, findPortByChannel, pushHistory,
  applyCalibration, persistDeviceState, persistPortActive,
  updateCommandStatus, setActuatorAck, addNotification,
} from './store.js';
import { derivePortStatus, deriveNodeStatus, describeConnState } from './status.js';

// '48' | '0X4A' -> '0x4a'
function normAddr(a) {
  const s = String(a).toLowerCase();
  return s.startsWith('0x') ? s : `0x${s}`;
}

// Reject packets whose tenant can't be resolved. Default ON. Set
// INGEST_STRICT_TENANT=false only for a single-tenant bring-up bench.
const STRICT_TENANT = process.env.INGEST_STRICT_TENANT !== 'false';

function resolvePacketNode(pkt) {
  const scoped = findNodeScoped(pkt.tid, pkt.nid);
  if (scoped) return { node: scoped, scoped: true, error: null };

  const tenantKnown = pkt.tid && store.tenantByMqttTid[pkt.tid];

  if (!tenantKnown) {
    const msg =
      `unmapped tid '${pkt.tid}' — add a row to tenants.mqtt_tid mapping it ` +
      `to a tenant (see db/seed_hw.sql)`;
    if (STRICT_TENANT) {
      console.warn(`[ingest] REJECTED: ${msg}`);
      return { node: null, scoped: false, error: msg };
    }
    console.warn(`[ingest] ${msg} — falling back to unscoped nid lookup (NOT tenant-safe)`);
    return { node: findNode(pkt.nid), scoped: false, error: null };
  }

  return { node: null, scoped: false, error: `node '${pkt.nid}' not registered to tid '${pkt.tid}'` };
}

// --- Telemetry ('tlm') ------------------------------------------------------
export function ingestTelemetry(pkt) {
  if (!pkt || pkt.t !== 'tlm') return { ok: false, error: 'not a telemetry packet' };

  const { node, scoped, error } = resolvePacketNode(pkt);
  if (error) return { ok: false, error };
  if (!node) return { ok: false, error: `unknown node '${pkt.nid}' (tid '${pkt.tid}')` };

  const now = Date.now();
  node.lastSeen = now;

  // Optional node-level status block (the frozen tlm schema has no `st`; kept
  // tolerant in case the firmware adds one later).
  if (pkt.st && typeof pkt.st === 'object') {
    if (Number.isFinite(pkt.st.up)) node.uptime = pkt.st.up;
    if (Number.isFinite(pkt.st.rs)) node.rssi = pkt.st.rs;
    if (Number.isFinite(pkt.st.hp)) node.freeHeap = pkt.st.hp;
    if (Number.isFinite(pkt.st.f))  node.systemFault = pkt.st.f;
  }

  let matched = 0, unmatched = 0;

  for (const module of pkt.adc ?? []) {
    const addr = normAddr(module.a);
    for (const [channel, raw, code] of module.p ?? []) {
      const port = findPortByChannel(node, addr, channel);
      if (!port) { unmatched++; continue; }

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

      pushHistory(port, port.value, port.status);   // → INSERT INTO readings
      matched++;
    }
  }

  refreshNodeStatus(node, now);
  persistDeviceState(node).catch((e) =>
    console.error('[ingest] persist device state failed:', e.message));

  return { ok: true, node: node.id, tenantScoped: scoped, matched, unmatched };
}

// --- Discovery ('disco') ----------------------------------------------------
// Frozen schema: buses[] with a per-chip ports object p0..p3 whose values are
// CONNECTED | DISCONNECTED | DISABLED. We map that onto each port's activeFlag
// (has a live sensor) + connState (for the UI's "masked vs unplugged" nuance).
export function ingestDiscovery(pkt) {
  if (!pkt || pkt.t !== 'disco') return { ok: false, error: 'not a discovery packet' };

  const { node, scoped, error } = resolvePacketNode(pkt);
  if (error) return { ok: false, error };
  if (!node) return { ok: false, error: `unknown node '${pkt.nid}' (tid '${pkt.tid}')` };

  node.lastSeen = Date.now();

  let connected = 0, disconnected = 0, disabled = 0, unmatched = 0;

  for (const bus of pkt.buses ?? []) {
    const addr = normAddr(bus.a);
    const mod = node.modules.find((m) => m.address?.toLowerCase() === addr);
    if (!mod) { unmatched++; continue; }

    for (const [key, state] of Object.entries(bus.ports ?? {})) {
      const ch = Number(String(key).replace(/^p/i, ''));   // 'p2' -> 2
      const port = mod.ports.find((p) => p.channel === ch);
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

  refreshNodeStatus(node, Date.now());
  return {
    ok: true, node: node.id, tenantScoped: scoped,
    detectedChips: pkt.detected_chips ?? null,
    connected, disconnected, disabled, unmatched,
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

export function ingestAck(pkt) {
  if (!pkt || pkt.t !== 'ack') return { ok: false, error: 'not an ack packet' };
  if (!pkt.cid) return { ok: false, error: 'ack missing cid' };

  const { node, scoped, error } = resolvePacketNode(pkt);
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

  return {
    ok: true, node: node.id, tenantScoped: scoped,
    cid: pkt.cid, status,
    matchedCommand: Boolean(cmd),
    actuator: actuator?.id ?? null,
    result: ACK_FAIL.has(status) ? 'fail' : (ACK_OK.has(status) ? 'ok' : 'unknown'),
  };
}

// Recompute one node's status from its ports + staleness.
export function refreshNodeStatus(node, now = Date.now()) {
  const portStatuses = [];
  for (const m of node.modules) {
    for (const p of m.ports) {
      p.status = derivePortStatus({
        code: p.code, value: p.value,
        safeMin: p.safeMin, safeMax: p.safeMax,
        lastSeen: p.lastSeen, now,
      });
      portStatuses.push(p.status);
    }
  }
  node.status = deriveNodeStatus({
    portStatuses, systemFault: node.systemFault, lastSeen: node.lastSeen, now,
  });
}

export function refreshAll(now = Date.now()) {
  for (const node of store.devices) refreshNodeStatus(node, now);
}
