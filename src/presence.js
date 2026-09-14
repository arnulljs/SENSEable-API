// presence.js ────────────────────────────────────────────────────────────────
// Raises notifications when hardware appears, disappears, or changes health.
//
// WHY THIS IS SEPARATE FROM refreshNodeStatus()
// The status sweep runs every SWEEP_MS and recomputes every status from scratch.
// Notifying from inside it would raise the same "node offline" alert on every
// pass — hundreds of identical rows for one unplugged ESP32. Presence has to be
// EDGE-TRIGGERED: fire once when the state changes, then stay quiet until it
// changes again. This module owns that transition tracking so the sweep stays a
// pure recomputation.
//
// WHY A NODE GOING OFFLINE ISN'T JUST "one more warning"
// A sensor reading outside its safe range is the system working: it saw
// something and told you. A node going silent is the system losing the ability
// to see at all, and it is indistinguishable from a healthy pond until someone
// checks. That is the failure an aquaculture operator most needs pushed at them,
// which is why offline transitions are raised at 'fault' rather than 'warning'.
//
// SUPPRESSION RULES, and the reasoning behind each
//  1. First observation is never an alert. On boot every node is "new", and a
//     restart must not manufacture a wall of alerts about hardware that was
//     always there.
//  2. Child transitions are suppressed while the parent is offline. When an
//     ESP32 drops, all four of its boards and all sixteen channels go stale
//     within the same sweep. One "node offline" is the actionable fact; the
//     other twenty are noise describing the same cable.
//  3. Recovery is reported, at 'success'. Knowing something came back matters
//     as much as knowing it left — and without it the Notifications page reads
//     like an unresolved outage forever.

import { store, addNotification } from './store.js';

// Last announced state per entity. Keyed by a stable identity string rather
// than an object reference: provisioning replaces port objects on re-discovery,
// and a WeakMap keyed on the object would forget everything at that moment.
const lastState = new Map();        // key -> status string

// Entities seen at least once. Rule 1 above depends on distinguishing "changed
// to offline" from "first ever seen, and it happens to be offline".
const known = new Set();

const OFFLINE = new Set(['Offline', 'offline']);
const FAULT = new Set(['Fault', 'fault']);

const nodeKey = (n) => `node:${n.id}`;
const modKey = (n, m) => `mod:${n.id}:${m.id}`;
const portKey = (n, m, p) => `port:${n.id}:${m.id}:${p.id}`;

function isOffline(s) { return OFFLINE.has(s); }
function isFault(s) { return FAULT.has(s); }

// Fire-and-forget: a notification failing to insert must never break the status
// sweep, which is what keeps the dashboard honest.
function raise(node, type, title, message) {
  addNotification(node, { type, title, message })
    .catch((e) => console.error('[presence] notification failed:', e.message));
}

/**
 * Compare current status against the last announced status for every entity
 * under `node`, and raise notifications for genuine transitions.
 *
 * Call AFTER refreshNodeStatus() has recomputed statuses, so this reads settled
 * values rather than racing the computation that produces them.
 */
function checkPresence(node) {
  const nKey = nodeKey(node);
  const nPrev = lastState.get(nKey);
  const nNow = node.status;
  const nodeWasKnown = known.has(nKey);
  const nodeOffline = isOffline(nNow);

  // ── Node ────────────────────────────────────────────────────────────────
  if (!nodeWasKnown) {
    known.add(nKey);
    // A node discovered for the first time IS worth announcing — it means new
    // hardware was plugged in — but only when it arrives healthy. A node that
    // is offline the moment we first see it is almost always a restart
    // observing hardware that was already gone.
    if (!nodeOffline) {
      raise(node, 'success', `${node.name} online`,
        `Node ${node.nodeId} is reporting on ${node.commMode ?? 'the network'}.`);
    }
  } else if (nPrev !== nNow) {
    if (nodeOffline) {
      raise(node, 'fault', `${node.name} went offline`,
        `Node ${node.nodeId} stopped reporting. Its channels cannot be monitored ` +
        'until it reconnects — check power, Wi-Fi, and the broker.');
    } else if (isOffline(nPrev)) {
      raise(node, 'success', `${node.name} is back online`,
        `Node ${node.nodeId} resumed reporting.`);
    } else if (isFault(nNow)) {
      raise(node, 'fault', `${node.name} reporting a fault`,
        `Node ${node.nodeId} is online but reporting a system fault.`);
    }
  }
  lastState.set(nKey, nNow);

  // ── Modules ─────────────────────────────────────────────────────────────
  for (const m of node.modules ?? []) {
    const mKey = modKey(node, m);
    const mPrev = lastState.get(mKey);
    const mNow = m.status;
    const mWasKnown = known.has(mKey);

    if (!mWasKnown) {
      known.add(mKey);
      if (!isOffline(mNow) && nodeWasKnown) {
        // A board appearing on a node we already knew about is a real event:
        // someone attached an expansion board. On a node we're seeing for the
        // first time it's just part of that node's inventory.
        raise(node, 'info', `Expansion board ${m.address} detected`,
          `A new board was discovered at I2C address ${m.address} on ${node.name}.`);
      }
    } else if (mPrev !== mNow && !nodeOffline) {
      // Rule 2: only report board transitions when the node itself is up.
      // Otherwise every board on a dropped node reports independently about
      // the same unplugged cable.
      if (isOffline(mNow)) {
        raise(node, 'warning', `Board ${m.address} went offline`,
          `${m.name} on ${node.name} stopped responding while the node is still ` +
          'reporting — check the I2C wiring and the board\'s power.');
      } else if (isOffline(mPrev)) {
        raise(node, 'success', `Board ${m.address} recovered`,
          `${m.name} on ${node.name} is responding again.`);
      }
    }
    lastState.set(mKey, mNow);

    // ── Ports ─────────────────────────────────────────────────────────────
    const moduleOffline = isOffline(mNow);
    for (const p of m.ports ?? []) {
      const pKey = portKey(node, m, p);
      const pPrev = lastState.get(pKey);
      const pNow = p.status;

      if (!known.has(pKey)) { known.add(pKey); lastState.set(pKey, pNow); continue; }

      // Rule 2 again, one level down. A channel is only newsworthy on its own
      // when its parents are healthy — otherwise it's describing the parent's
      // outage.
      if (pPrev !== pNow && !nodeOffline && !moduleOffline) {
        // A channel the operator deliberately switched off is not an incident.
        if (pNow === 'Disabled' || pPrev === 'Disabled') {
          lastState.set(pKey, pNow);
          continue;
        }
        const label = p.label && p.label !== 'Unassigned channel'
          ? `${p.label} (${p.id})` : `Channel ${p.id}`;

        if (isOffline(pNow)) {
          raise(node, 'warning', `${label} stopped reporting`,
            `${label} on board ${m.address} has gone stale while the rest of ` +
            `${node.name} is still reporting — the sensor may be disconnected.`);
        } else if (isOffline(pPrev)) {
          raise(node, 'success', `${label} is reporting again`,
            `${label} on board ${m.address} resumed sending readings.`);
        }
      }
      lastState.set(pKey, pNow);
    }
  }
}

export function checkAllPresence() {
  for (const node of store.devices) {
    try { checkPresence(node); }
    catch (e) { console.error(`[presence] ${node.id}:`, e.message); }
  }
}

// Exposed for tests and for /api/health, so "why did I get no alert" is
// answerable without reading the source.
export function getPresenceStats() {
  return { tracked: lastState.size, known: known.size };
}
