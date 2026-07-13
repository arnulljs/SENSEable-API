// ingest.js ─────────────────────────────────────────────────────────────────
// Turns raw wire-protocol packets into updated state. Verified against the
// ACTUAL firmware (SENSEable-HW/blink/main/blink_example_main.c).
//
// FIXES vs the previous version:
//   1. Discovery packet type is 't':'disco' — the old code checked for 'dsc',
//      so EVERY discovery packet was rejected ("not a discovery packet").
//   2. Discovery carries a TOPOLOGY BITMAP (+ detected_chips), not a `modules`
//      array. Bit layout from the firmware's disco task:
//         bit 0        chip 0 online
//         bits 1..4    chip 0 ports 0..3 attached
//         bit 5        chip 1 online
//         bits 6..9    chip 1 ports 0..3 attached
//         ... (5 bits per chip, 4 chips => 20 bits)
//   3. Telemetry now resolves the tenant from `tid` BEFORE matching `nid`.
//      The old code matched nid alone, so any node claiming "N001" wrote into
//      whichever tenant happened to own N001 — a cross-tenant write.
//   4. The raw ADC count is stashed on the port so store.js can persist it
//      (the frozen protocol says the node ships raw counts; the DB keeps them).

import {
  store, findNode, findNodeScoped, findPortByChannel, pushHistory,
  applyCalibration, persistDeviceState,
} from './store.js';
import { derivePortStatus, deriveNodeStatus } from './status.js';

// '48' | '0X4A' -> '0x4a'
function normAddr(a) {
  const s = String(a).toLowerCase();
  return s.startsWith('0x') ? s : `0x${s}`;
}

// Reject packets whose tenant can't be resolved. Default ON — a node whose
// `tid` isn't mapped in tenants.mqtt_tid must NOT be allowed to write into a
// tenant picked by nid alone (that's a cross-tenant write). Set
// INGEST_STRICT_TENANT=false only for a single-tenant bring-up bench.
const STRICT_TENANT = process.env.INGEST_STRICT_TENANT !== 'false';

// Resolve the node a packet belongs to, tenant-first.
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

  // Tenant is known but has no such node — a real "unknown node", not a
  // tenancy problem.
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

  // Node-level status block (st) — the current firmware doesn't send one, so
  // these stay at their hydrated values. Kept for the frozen spec's sake.
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

      // All engineering-unit conversion happens HERE, server-side. The node
      // never sends a calibrated value.
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
// The firmware publishes its I2C topology as a bitmap. We decode it and mark
// ports active/inactive — this is the "newly detected ports appear as
// unconfigured resources" behavior from the thesis.
const CHIPS = 4, CHANNELS = 4;

export function decodeTopology(bitmap) {
  const chips = [];
  let bit = 0;
  for (let c = 0; c < CHIPS; c++) {
    const online = Boolean((bitmap >> bit) & 1);
    bit++;
    const ports = [];
    for (let ch = 0; ch < CHANNELS; ch++) {
      ports.push(Boolean((bitmap >> bit) & 1));
      bit++;
    }
    chips.push({ online, ports });
  }
  return chips;
}

export function ingestDiscovery(pkt) {
  // The firmware sends 'disco'. 'dsc' accepted too, in case the frozen spec's
  // shorter name is ever adopted.
  if (!pkt || (pkt.t !== 'disco' && pkt.t !== 'dsc')) {
    return { ok: false, error: 'not a discovery packet' };
  }

  const { node, error } = resolvePacketNode(pkt);
  if (error) return { ok: false, error };
  if (!node) return { ok: false, error: `unknown node '${pkt.nid}' (tid '${pkt.tid}')` };

  node.lastSeen = Date.now();
  if (pkt.net) node.commMode = pkt.net === 'cell' ? 'Cellular' : 'Wi-Fi';

  let activated = 0, deactivated = 0;

  const bitmap = Number(pkt.topology ?? pkt.topo ?? 0);
  if (bitmap) {
    const chips = decodeTopology(bitmap);
    // Chip index -> module. The firmware scans a fixed address list
    // (0x48,0x49,0x4a,0x4b), so chip index i maps to that address.
    const ADDRS = ['0x48', '0x49', '0x4a', '0x4b'];
    chips.forEach((chip, i) => {
      const mod = node.modules.find((m) => m.address.toLowerCase() === ADDRS[i]);
      if (!mod) return;
      chip.ports.forEach((attached, ch) => {
        const port = mod.ports.find((p) => p.channel === ch);
        if (!port) return;
        if (port.activeFlag !== attached) {
          port.activeFlag = attached;
          attached ? activated++ : deactivated++;
        }
      });
    });
  }

  refreshNodeStatus(node, Date.now());
  return {
    ok: true, node: node.id,
    detectedChips: pkt.detected_chips ?? null,
    activated, deactivated,
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

// Sweep every node so nodes flip to Offline when telemetry stops arriving.
export function refreshAll(now = Date.now()) {
  for (const node of store.devices) refreshNodeStatus(node, now);
}
