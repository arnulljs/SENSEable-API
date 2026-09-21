// claim.js ──────────────────────────────────────────────────────────────────
// CLAIM-ON-CONNECT: bench-testing ownership for boards that share one compiled
// tid. Enabled with CLAIM_ON_CONNECT=true; OFF by default.
//
// THE MODEL
//   * A board with no pin is FLOATING.
//   * When a floating board reports in, it is claimed by the organization whose
//     dashboard is open on this edge server right now, and the claim is written
//     as a pin (node_tenant_assignments), so it STICKS: logging out, logging in
//     elsewhere, unplugging and replugging all leave it where it is.
//   * Removing the board from that organization's dashboard clears the pin
//     (store.removeDevice), and the board floats again. The next organization
//     with a dashboard open when it reports claims it.
//
// "WHOSE DASHBOARD IS OPEN" is observed, not guessed: a websocket subscribed to
// a tenant, or a GET /api/devices carrying x-tenant-id within the last
// ACTIVE_WINDOW_MS. If exactly ONE organization is active, it gets the board.
// If none or several are, nothing is claimed — the board is not assigned to
// whichever tab happened to poll last — and it routes by its tid as it always
// did, until the situation is unambiguous.
//
// WHY THIS IS A TESTING MODE AND NOT THE PRODUCT
// It hands any board on the broker to any organization that happens to be
// looking. On a shared bench that is exactly the convenience wanted; in a real
// deployment it would let one farm's login capture another farm's hardware.
// The production answer is an explicit claim step (a code printed on the board,
// entered by the owner). Keep this flag off anywhere that is not your bench.
//
// Edge-only: dashboards served by the cloud tier are invisible here.

import { store, assignNodeToTenant, addNotification } from './store.js';
import { getActiveTenantSlugs } from './realtime.js';

export const CLAIM_ON_CONNECT = process.env.CLAIM_ON_CONNECT === 'true';
const ACTIVE_WINDOW_MS = Number(process.env.CLAIM_ACTIVE_WINDOW_MS ?? 20_000);

// tenant slug -> last GET /api/devices from that tenant's dashboard
const restSeen = new Map();

const stats = { claimed: 0, ambiguous: 0, noDashboard: 0, last: null };

export function noteDashboardActivity(slug) {
  if (slug && store.tenants[slug]) restSeen.set(slug, Date.now());
}

export function activeTenants(now = Date.now()) {
  const active = new Set(getActiveTenantSlugs());
  for (const [slug, at] of restSeen) if (now - at <= ACTIVE_WINDOW_MS) active.add(slug);
  return [...active].filter((s) => store.tenants[s]);
}

// One claim per node at a time: telemetry and discovery from a freshly
// plugged board arrive together, and both must not race to write the pin.
const inFlight = new Map();
const quietUntil = new Map();   // nid -> ts; rate-limits the "can't claim" log

/**
 * If claim mode is on and `nid` is unpinned, try to claim it for the single
 * active organization. Returns that tenant record, or null to fall back to the
 * normal tid route.
 */
export async function claimIfFloating(nid) {
  if (!CLAIM_ON_CONNECT || !nid || store.tenantByNodeId[nid]) return null;
  if (inFlight.has(nid)) return inFlight.get(nid);

  const p = (async () => {
    const active = activeTenants();
    if (active.length !== 1) {
      const now = Date.now();
      const qk = `${nid}:${active.length === 0 ? 'none' : 'many'}`;
      if ((quietUntil.get(qk) ?? 0) < now) {
        quietUntil.set(qk, now + 60_000);
        if (active.length === 0) {
          stats.noDashboard += 1;
          console.log(`[claim] '${nid}' is floating but no dashboard is open — routing by tid until one is`);
        } else {
          stats.ambiguous += 1;
          console.warn(`[claim] '${nid}' is floating and ${active.length} organizations are logged in ` +
                       `(${active.join(', ')}) — not guessing; routing by tid. Close all but one to claim it.`);
        }
      }
      return null;
    }

    const slug = active[0];
    const { retired } = await assignNodeToTenant(nid, slug, 'claimed on connect');
    stats.claimed += 1;
    stats.last = { nodeId: nid, tenant: slug, at: new Date().toISOString() };
    console.log(`[claim] '${nid}' claimed by ${slug} (the only organization logged in)` +
                (retired.length ? `; retired ${retired.join(', ')}` : ''));
    return store.tenants[slug];
  })().finally(() => inFlight.delete(nid));

  inFlight.set(nid, p);
  return p;
}

// Called once the claimed node's device row exists, so the notice lands in the
// organization that now owns it.
export function announceClaim(node) {
  addNotification(node, {
    type: 'info',
    title: `${node.name} added to this organization`,
    message: `Node ${node.nodeId} was plugged in while this organization was logged in, so it ` +
             'was assigned here. Remove it from the dashboard to release it.',
  }).catch((e) => console.error('[claim] notification failed:', e.message));
}

export function getClaimStats() {
  return { enabled: CLAIM_ON_CONNECT, activeTenants: activeTenants(), ...stats };
}
