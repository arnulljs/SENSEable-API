// dispatch.js ───────────────────────────────────────────────────────────────
// The downlink half of cloud-first: gets cloud-authored commands onto a broker.
//
// THE PROBLEM THIS SOLVES
// Under cloud-first the dashboard is served from Vercel and writes to Supabase.
// A Vercel function cannot hold an MQTT connection open, so it cannot publish a
// command; it writes a row to the `commands` outbox with published_at NULL. The
// sync worker's downward pass brings that row to the edge, and this dispatcher
// puts it on whichever broker the hardware is actually using.
//
// Without this, actuator control silently stops working the moment the dashboard
// stops talking to the edge directly — which is the exact scenario cloud-first
// creates, and which the architecture documents do not address at all.
//
// WHY published_at RATHER THAN status
// `status` is the ACK lifecycle: pending until the node confirms. That is about
// the hardware. published_at is about transport: has this envelope been put on a
// wire by anyone. A command can be published and still pending, and the two must
// not be conflated or a command awaiting ack would be re-sent on every tick.
//
// RLS: this runs on adminPool, which is RLS-exempt, for the same reason the sync
// worker does — dispatch legitimately spans every tenant, and nothing here is
// reachable from an HTTP request.

import { adminPool } from '../db/pool.js';
import { publishCommand } from './mqtt.js';
import { cmdTopic } from './commands.js';
import { store, commandTidFor } from './store.js';

// A command that has been sitting in the outbox for longer than this is not
// dispatched. An actuator instruction authored an hour ago and delivered now is
// worse than one never delivered: the operator has moved on, and the tank has
// not. Expired rows are marked so they stop being scanned.
const MAX_AGE_MS = Number(process.env.COMMAND_MAX_AGE_MS ?? 10 * 60_000);
const BATCH = Number(process.env.COMMAND_DISPATCH_BATCH ?? 50);

const stats = { dispatched: 0, expired: 0, failed: 0, lastRunAt: null, lastError: null };

export const getDispatchStats = () => ({ ...stats });

// One pass at a time. Publishing is fast, but a stalled broker write plus a 5s
// sweep would otherwise stack passes and send duplicates.
let running = false;

export async function dispatchPendingCommands() {
  if (running) return 0;
  running = true;
  stats.lastRunAt = Date.now();

  try {
    const { rows } = await adminPool.query(
      `SELECT c.command_id, c.cid, c.payload, c.created_at,
              d.device_id, d.node_id, t.mqtt_tid
         FROM commands c
         JOIN devices d ON d.device_id = c.device_id
         JOIN tenants t ON t.tenant_id = c.tenant_id
        WHERE c.published_at IS NULL
        ORDER BY c.created_at
        LIMIT $1`, [BATCH]);
    if (!rows.length) return 0;

    const sent = [];
    const expired = [];

    for (const row of rows) {
      const age = Date.now() - new Date(row.created_at).getTime();
      if (age > MAX_AGE_MS) { expired.push(row.command_id); continue; }

      // Address the node on the tid it actually uses. For a pinned node that is
      // learned from its own packets (dev.wireTid) and differs from the
      // tenant's mqtt_tid; for everything else the two are the same.
      const dev = store.devices.find((d) => d._uuid === row.device_id);
      const tid = (dev && commandTidFor(dev)) ?? row.mqtt_tid;

      // A node with no known tid cannot be addressed on the wire at all. Treat
      // it as expired rather than retrying forever — the fix is a database row
      // (or the node reporting in), not another delivery attempt.
      if (!tid) { expired.push(row.command_id); continue; }

      const topic = cmdTopic(tid, row.node_id);
      // The stored payload IS the frozen-schema envelope, written once by
      // whichever tier accepted the command. Publishing it verbatim keeps a
      // single construction site for the wire format.
      if (publishCommand(topic, row.payload)) {
        sent.push(row.command_id);
        console.log(`[dispatch] published ${row.cid} → ${topic}`);
      }
      // No broker connected: leave published_at NULL and try again next sweep.
      // This is the normal state during a total outage and is not an error.
    }

    if (sent.length) {
      await adminPool.query(
        'UPDATE commands SET published_at = now() WHERE command_id = ANY($1::uuid[])', [sent]);
      stats.dispatched += sent.length;
    }
    if (expired.length) {
      await adminPool.query(
        `UPDATE commands
            SET published_at = now(), status = 'failed',
                msg = coalesce(msg, 'expired in outbox before a broker was available')
          WHERE command_id = ANY($1::uuid[])`, [expired]);
      stats.expired += expired.length;
      console.warn(`[dispatch] expired ${expired.length} stale command(s)`);
    }
    return sent.length;
  } catch (err) {
    stats.failed += 1;
    stats.lastError = err.message;
    console.error('[dispatch] pass failed:', err.message);
    return 0;
  } finally {
    running = false;
  }
}

/**
 * Mark a command this process has just published itself, so the dispatcher does
 * not send it a second time. Called from routes.js on the edge's own write path.
 */
export async function markPublished(cid) {
  if (!cid) return;
  await adminPool.query(
    'UPDATE commands SET published_at = now() WHERE cid = $1 AND published_at IS NULL',
    [cid]).catch((e) => console.error('[dispatch] markPublished failed:', e.message));
}
