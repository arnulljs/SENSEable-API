// reconcile.js ───────────────────────────────────────────────────────────────
// Desired-state reconciliation between the operator's intent and what the
// firmware is actually doing.
//
// THE PROBLEM
// Two independent records of the same fact exist, and they drift.
//
//   Firmware:  port_active[chip][channel] — a RAM flag, mutated by
//              sensor_port_up / sensor_port_down. A deactivated channel is
//              omitted from telemetry entirely and reported as "DISABLED" in
//              discovery.
//
//   Backend:   ports.enabled (+ disabled_reason, disabled_at) — durable,
//              written by PATCH .../ports/:id/enabled, which ALSO fires a
//              sensor_port_down at the node.
//
// They agree only when a change originates from the dashboard AND the command
// arrives. They fall out of step whenever:
//
//   * the ESP32 reboots — port_active[] returns to its compiled default while
//     the database still records the operator's disable
//   * an ack never comes back (broker drop, node offline at the time) — the
//     database moved on, the hardware never heard
//   * a channel is disabled while the node is offline — the command is logged
//     and published into the void, and nothing re-sends it on reconnect
//
// WHY THE DRIFT IS WORSE THAN IT SOUNDS
// A channel disabled in the database but still sampling in firmware keeps
// arriving in telemetry and is treated as live data — the operator believes
// they switched off a floating input, and its leakage voltage is still being
// recorded and calibrated as if it were a reading.
//
// The mirror case raises a false alarm: a channel disabled in firmware but
// enabled in the database goes stale, and the presence system reports it as
// "stopped reporting" — an incident notification for hardware behaving exactly
// as instructed. During UAT that reads as an unreliable system.
//
// THE APPROACH
// Discovery is the ONLY moment both views are visible in the same packet, so it
// is the only place the drift can be detected. When they disagree, the DATABASE
// wins — it holds the operator's intent, and intent is not something the
// hardware gets a vote on. The firmware is corrected by re-issuing the command
// rather than by silently rewriting either record.
//
// This is desired-state reconciliation in the ordinary sense: observe actual,
// compare to desired, emit the correction, repeat. It converges because
// discovery repeats; a correction lost to a dropped broker connection is simply
// re-derived from the next discovery packet.

import { recordCommand, commandTidFor } from './store.js';
import { buildCommand, cmdTopic, CHIP_ADDRS } from './commands.js';
import { publishCommand } from './mqtt.js';

// Don't re-issue the same correction faster than this. A node that is ignoring
// the command (wrong firmware, dead I2C bus) would otherwise get a fresh one on
// every discovery packet, and the command log would fill with identical rows
// that tell you nothing you didn't know after the first.
const REISSUE_COOLDOWN_MS = 60_000;

// key -> timestamp of last correction attempt
const lastAttempt = new Map();

const stats = {
  checked: 0,
  drifted: 0,
  corrected: 0,
  suppressed: 0,     // within cooldown
  failed: 0,
};

export function getReconcileStats() {
  return { ...stats, pending: lastAttempt.size };
}

/**
 * Compare the firmware's reported channel state against the operator's stored
 * intent, and re-issue commands where they disagree.
 *
 * Called from ingestDiscovery AFTER the packet has been applied, so it compares
 * settled values. Returns a summary for the ingest result rather than throwing:
 * a reconciliation failure must never reject a discovery packet, because the
 * packet itself is still good information.
 */
export async function reconcileNode(node, pkt) {
  const drift = [];

  for (const bus of pkt.buses ?? []) {
    const addr = String(bus.a ?? '').toLowerCase();
    const mod = node.modules.find((m) => m.address?.toLowerCase() === addr);
    if (!mod) continue;

    for (const [key, rawState] of Object.entries(bus.ports ?? {})) {
      const ch = Number(String(key).replace(/^p/i, ''));
      const port = mod.ports.find((p) => p.channel === ch);
      if (!port) continue;

      stats.checked += 1;

      // What the firmware says it is doing.
      const firmwareDisabled = String(rawState).toUpperCase() === 'DISABLED';
      // What the operator asked for. `enabled` defaults true, so an undefined
      // value is "no instruction given" and never counts as drift.
      const operatorDisabled = port.enabled === false;

      const key2 = `${node.id}::${mod.address}::${ch}`;

      if (firmwareDisabled === operatorDisabled) {
        // Agreed. Drop any cooldown entry so a genuine future drift isn't
        // suppressed by one left over from an old correction.
        lastAttempt.delete(key2);
        continue;
      }

      stats.drifted += 1;
      drift.push({
        address: mod.address,
        channel: ch,
        port: port.id,
        firmware: firmwareDisabled ? 'DISABLED' : 'ACTIVE',
        intent: operatorDisabled ? 'disabled' : 'enabled',
      });

      const now = Date.now();
      const prev = lastAttempt.get(key2);
      if (prev && now - prev < REISSUE_COOLDOWN_MS) {
        stats.suppressed += 1;
        continue;
      }
      lastAttempt.set(key2, now);

      try {
        await reissue(node, mod, ch, !operatorDisabled);
        stats.corrected += 1;
        console.log(`[reconcile] ${node.id} ${mod.address}/p${ch}: firmware says ` +
                    `${firmwareDisabled ? 'DISABLED' : 'ACTIVE'} but operator wants ` +
                    `${operatorDisabled ? 'disabled' : 'enabled'} — re-issued`);
      } catch (e) {
        stats.failed += 1;
        console.warn(`[reconcile] ${node.id} ${mod.address}/p${ch}: could not re-issue —`, e.message);
      }
    }
  }

  return drift;
}

async function reissue(node, mod, channel, enable) {
  // The tid the node itself publishes with, which differs from its tenant's
  // mqtt_tid when the node is pinned (node_tenant_assignments). Using the
  // tenant's tid sent every correction for a pinned node to a topic the board
  // does not subscribe to — or failed outright for a tenant with no mqtt_tid.
  const mqttTid = commandTidFor(node);
  if (!mqttTid) throw new Error('no tid known for this node');

  const chip = CHIP_ADDRS.indexOf(String(mod.address).toLowerCase());
  if (chip < 0) throw new Error(`address ${mod.address} outside the 0x48..0x4B range`);

  const envelope = buildCommand(
    enable ? 'sensor_port_up' : 'sensor_port_down',
    { tid: mqttTid, nid: node.nodeId },
    { chip, ch: channel });

  // Logged like any other command so the correction is auditable — an operator
  // asking "why did this channel switch off by itself" gets a real answer.
  await recordCommand(node, envelope);
  const published = publishCommand(cmdTopic(mqttTid, node.nodeId), envelope);
  if (!published) throw new Error('no broker connection');
  return envelope.cid;
}

/**
 * Cross-check the node's own chip count against the number of bus entries it
 * actually sent.
 *
 * Discovery is edge-triggered — published on topology change, not on a timer —
 * so an absent board is ambiguous: it may have been removed, or the packet may
 * simply be partial. `detected_chips` is the node's own count of what it can
 * see, so a mismatch means the packet should NOT be trusted as a complete
 * topology snapshot. Without this the backend cannot tell "this board is gone"
 * from "this packet didn't mention it", and quietly relies on the staleness
 * sweep to notice.
 */
export function checkTopologyConsistency(node, pkt) {
  const declared = pkt.detected_chips;
  if (declared == null) return { consistent: true, reason: 'no detected_chips field' };

  const present = (pkt.buses ?? []).length;
  if (declared === present) return { consistent: true, declared, present };

  console.warn(`[disco] ${node.id}: detected_chips=${declared} but ${present} bus ` +
               'entry(ies) present — treating as a PARTIAL snapshot, not a full ' +
               'topology replacement');
  return {
    consistent: false,
    declared,
    present,
    reason: 'detected_chips disagrees with buses[] length',
  };
}

/**
 * Per-node staleness threshold: three missed publishes at the cadence the node
 * declared in discovery. One global threshold cannot serve both a 10 s node and
 * a 60 s low-power node — pick 30 s and the slow one reads permanently offline,
 * pick 180 s and the fast one's outage goes unnoticed for three minutes.
 *
 * Falls back to the global STALE_MS when the node hasn't declared an interval,
 * so firmware that predates `tlm_interval_ms` behaves exactly as before.
 */
export function staleMsFor(node, fallbackMs) {
  const interval = Number(node?.tlmIntervalMs);
  if (!Number.isFinite(interval) || interval <= 0) return fallbackMs;
  return Math.max(fallbackMs, interval * 3);
}
