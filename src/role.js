// role.js ───────────────────────────────────────────────────────────────────
// Which tier this process is, and which one owns the side effects.
//
// CLOUD-FIRST TOPOLOGY
//   bridge (TIER=cloud)  an always-on copy of this server, subscribed to the
//                        cloud broker and writing straight into Supabase. This
//                        is the "[bridge]" box in docs/CLOUD-FIRST.md that was
//                        missing, which is why the laptop used to be in the
//                        cloud's data path (EDGE_BRIDGES_CLOUD).
//   edge (TIER unset)    the on-site server. With a bridge running it is a
//                        MIRROR in normal operation and the only ingest point
//                        during failover.
//
// SIDE EFFECTS
// Both tiers ingest the same packets from the cloud broker, so both would raise
// the same notification and both would publish the same queued command. Exactly
// one may. The rule:
//   * the bridge always does;
//   * an edge with no bridge (CLOUD_BRIDGE_RUNNING unset) always does, since it
//     is the only writer — the pre-bridge behaviour, unchanged;
//   * an edge WITH a bridge does only while the hardware is observed on the
//     local broker, i.e. failed over, when the bridge cannot see it.

export const TIER = process.env.TIER === 'cloud' ? 'cloud' : 'edge';
export const IS_BRIDGE = TIER === 'cloud';
export const CLOUD_BRIDGE_RUNNING = !IS_BRIDGE && process.env.CLOUD_BRIDGE_RUNNING === 'true';

// Updated by mqtt.js on every LIVE packet (never on retained replays).
let route = 'unknown';
export function setObservedRoute(origin) {
  route = origin === 'local' ? 'ROUTE_LOCAL_FAILOVER' : 'ROUTE_CLOUD_FIRST';
}
export const observedRoute = () => route;

// BRIDGE_SIDE_EFFECTS=false makes a bridge data-only: it writes readings and
// hardware state but never raises notifications or publishes commands. The AWS
// Lambda bridge runs that way, leaving alerts and command dispatch to the edge
// server exactly as before (so the edge keeps CLOUD_BRIDGE_RUNNING unset).
const BRIDGE_SIDE_EFFECTS = process.env.BRIDGE_SIDE_EFFECTS !== 'false';

export function ownsSideEffects() {
  if (IS_BRIDGE) return BRIDGE_SIDE_EFFECTS;
  if (!CLOUD_BRIDGE_RUNNING) return true;
  return route === 'ROUTE_LOCAL_FAILOVER';
}

export function describeRole() {
  return {
    tier: TIER,
    cloudBridgeRunning: IS_BRIDGE ? null : CLOUD_BRIDGE_RUNNING,
    observedRoute: route,
    ownsSideEffects: ownsSideEffects(),
  };
}
