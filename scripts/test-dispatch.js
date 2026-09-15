import 'dotenv/config';
import { adminPool } from '../db/pool.js';
import { dispatchPendingCommands, getDispatchStats } from '../src/dispatch.js';

let pass = 0, fail = 0;
const is = (n, g, w) => { const ok = g === w; console.log(`  ${ok?'PASS':'FAIL'}  ${n}${ok?'':` (got ${g}, want ${w})`}`); ok?pass++:fail++; };
const n = async (sql) => (await adminPool.query(sql)).rows[0].n;

// Seed our own outbox rather than depending on whatever an earlier test left
// behind. A test that only passes on a particular database state is not a test.
await adminPool.query('DELETE FROM commands');
const { rows: dev } = await adminPool.query(
  'SELECT device_id, tenant_id FROM devices LIMIT 1');
if (!dev.length) { console.error('no device provisioned — run test:cloudfirst first'); process.exit(1); }
for (const cid of ['t-dispatch-1', 't-dispatch-2']) {
  await adminPool.query(
    `INSERT INTO commands (tenant_id, device_id, cid, action, payload, status)
     VALUES ($1,$2,$3,'actuate',$4,'pending')`,
    [dev[0].tenant_id, dev[0].device_id, cid,
     JSON.stringify({ t: 'cmd', v: 1, cid, action: 'actuate', port: 1, state: 1 })]);
}

// No broker is connected in this process.
await dispatchPendingCommands();
is('no broker: commands stay queued for retry', await n("SELECT count(*)::int n FROM commands WHERE published_at IS NULL"), 2);
is('nothing reported as dispatched', getDispatchStats().dispatched, 0);

// Age them past the outbox window.
await adminPool.query("UPDATE commands SET created_at = now() - interval '2 hours'");
await dispatchPendingCommands();
is('stale commands expire instead of firing late', await n("SELECT count(*)::int n FROM commands WHERE status='failed'"), 2);
is('expired commands stop being scanned', await n("SELECT count(*)::int n FROM commands WHERE published_at IS NULL"), 0);

await dispatchPendingCommands();
is('idempotent: nothing left to do', getDispatchStats().expired, 2);

console.log(`\n  ${pass} passed, ${fail} failed`);
await adminPool.end();
process.exit(fail ? 1 : 0);
