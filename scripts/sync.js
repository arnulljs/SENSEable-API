// scripts/sync.js ───────────────────────────────────────────────────────────
// Edge → cloud replication worker (thesis two-tier redundancy model).
//
// The local edge server is the PRIMARY store: telemetry lands there first and
// monitoring keeps working with no internet at all. This worker asynchronously
// pushes those rows up to the Supabase repository, resuming from a watermark so
// a WAN outage costs nothing but delay — when the link returns, the backlog
// drains automatically.
//
// Direction is strictly ONE-WAY (local → cloud). The cloud tier is a read
// replica for remote monitoring and off-site backup; it is never authoritative.
// (The downward command path is a separate concern — see the commands outbox.)
//
// ROLES: both ends connect as *_owner, which is RLS-EXEMPT. That's deliberate
// and matches the existing adminPool rationale in db/pool.js: replication
// legitimately spans every tenant, so it cannot run under a per-tenant policy.
// Nothing here is reachable from an HTTP request.
//
//   node scripts/sync.js              # run forever, every SYNC_INTERVAL_MS
//   node scripts/sync.js --once       # single pass, then exit (cron-friendly)
//   node scripts/sync.js --status     # print watermarks and exit
//   node scripts/sync.js --backfill   # reset watermarks, re-push everything
//   node scripts/sync.js --dry-run    # report what WOULD move, change nothing

import 'dotenv/config';
import dotenv from 'dotenv';
import pg from 'pg';

dotenv.config({ path: '.env.cloud' });   // cloud URLs live in a separate file

const { Pool } = pg;
const args = new Set(process.argv.slice(2));
const ONCE = args.has('--once');
const STATUS = args.has('--status');
const BACKFILL = args.has('--backfill');
const DRY = args.has('--dry-run');

const INTERVAL_MS = Number(process.env.SYNC_INTERVAL_MS ?? 30_000);
const BATCH = Number(process.env.SYNC_BATCH_SIZE ?? 500);

// Replication order is FK-safe: a child never ships before its parent, or the
// cloud would reject it on a foreign-key violation.
//
//   identity  → append-only, chased by monotonic bigint PK (id > watermark).
//               Cheap and exact; rows are immutable once written.
//   timestamp → mutable, chased by updated_at and applied as an UPSERT, so a
//               rename or status flip propagates rather than duplicating.
//   static    → tiny lookup table, re-upserted wholesale every pass.
const TABLES = [
  { name: 'roles',                strategy: 'static'    },
  { name: 'tenants',              strategy: 'timestamp' },
  { name: 'users',                strategy: 'timestamp' },
  { name: 'sensor_profiles',      strategy: 'timestamp' },
  { name: 'devices',              strategy: 'timestamp' },
  { name: 'modules',              strategy: 'timestamp' },
  { name: 'calibration_formulas', strategy: 'timestamp' },
  { name: 'ports',                strategy: 'timestamp' },
  { name: 'actuators',            strategy: 'timestamp' },
  { name: 'readings',             strategy: 'identity'  },
  { name: 'notifications',        strategy: 'identity'  },
  { name: 'map_profiles',         strategy: 'timestamp' },
  { name: 'map_sensors',          strategy: 'timestamp' },
  { name: 'commands',             strategy: 'timestamp' },
];

const localPool = new Pool({
  connectionString: process.env.DATABASE_URL_OWNER ?? process.env.DATABASE_URL,
  max: 4,
});
const cloudPool = new Pool({
  connectionString: process.env.CLOUD_DATABASE_URL_OWNER,
  max: 4,
  // Cloud is across the WAN; fail fast rather than hanging a whole pass.
  connectionTimeoutMillis: 15_000,
  idleTimeoutMillis: 10_000,
});

const log = (...a) => console.log(new Date().toISOString(), ...a);

// ── Schema introspection ────────────────────────────────────────────────────
// Column lists are read from the live database rather than hardcoded, so adding
// a column in a future migration doesn't silently stop replicating it (a class
// of bug that's invisible until you need the backup).
const schemaCache = new Map();
async function describe(table) {
  if (schemaCache.has(table)) return schemaCache.get(table);

  const { rows: cols } = await localPool.query(
    `SELECT column_name, is_identity
       FROM information_schema.columns
      WHERE table_schema='public' AND table_name=$1
      ORDER BY ordinal_position`, [table]);
  if (!cols.length) throw new Error(`table '${table}' not found locally`);

  const { rows: pk } = await localPool.query(
    `SELECT a.attname
       FROM pg_index i
       JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      WHERE i.indrelid = $1::regclass AND i.indisprimary`, [table]);

  const meta = {
    columns: cols.map((c) => c.column_name),
    // GENERATED ALWAYS AS IDENTITY refuses an explicit value unless the INSERT
    // says OVERRIDING SYSTEM VALUE — and we must preserve ids so the watermark
    // stays meaningful and rows don't duplicate on re-push.
    hasAlwaysIdentity: cols.some((c) => c.is_identity === 'YES'),
    pk: pk.map((r) => r.attname),
  };
  schemaCache.set(table, meta);
  return meta;
}

// ── Watermarks ──────────────────────────────────────────────────────────────
// Timestamps are read back as TEXT. A JS Date carries only milliseconds, so
// letting the driver hydrate a microsecond-precision timestamptz silently
// truncates it — and a truncated watermark is always slightly BEHIND the rows
// it already copied, so every mutable row would re-ship on every pass forever.
async function getWatermark(table) {
  const { rows } = await localPool.query(
    `SELECT last_synced_id, last_synced_at::text AS last_synced_at, last_synced_key
       FROM sync_state WHERE table_name=$1`, [table]);
  return rows[0] ?? { last_synced_id: null, last_synced_at: null, last_synced_key: null };
}

async function setWatermark(table, { id, at, key, added, error }) {
  if (DRY) return;
  await localPool.query(
    `INSERT INTO sync_state (table_name, last_synced_id, last_synced_at, last_synced_key,
                             rows_synced, last_run_at, last_error)
     VALUES ($1,$2,$3::timestamptz,$4,$5, now(), $6)
     ON CONFLICT (table_name) DO UPDATE SET
       last_synced_id  = COALESCE(EXCLUDED.last_synced_id,  sync_state.last_synced_id),
       last_synced_at  = COALESCE(EXCLUDED.last_synced_at,  sync_state.last_synced_at),
       last_synced_key = COALESCE(EXCLUDED.last_synced_key, sync_state.last_synced_key),
       rows_synced     = sync_state.rows_synced + EXCLUDED.rows_synced,
       last_run_at     = now(),
       last_error      = EXCLUDED.last_error`,
    [table, id ?? null, at ?? null, key ?? null, added ?? 0, error ?? null]);
}

// ── Push a batch of rows to the cloud ───────────────────────────────────────
async function push(table, rows, meta, mode) {
  if (!rows.length) return 0;

  const cols = meta.columns;
  const quoted = cols.map((c) => `"${c}"`).join(', ');
  const params = [];
  const tuples = rows.map((row) => {
    const slots = cols.map((c) => {
      params.push(row[c]);
      return `$${params.length}`;
    });
    return `(${slots.join(', ')})`;
  });

  const overriding = meta.hasAlwaysIdentity ? 'OVERRIDING SYSTEM VALUE' : '';
  const conflict = meta.pk.map((c) => `"${c}"`).join(', ');

  // Append-only rows never change, so a collision means "already replicated" —
  // skip it. Mutable rows must overwrite, or an edit made locally would never
  // reach the cloud copy.
  const action = mode === 'identity'
    ? 'DO NOTHING'
    : `DO UPDATE SET ${cols.filter((c) => !meta.pk.includes(c))
        .map((c) => `"${c}" = EXCLUDED."${c}"`).join(', ')}`;

  const sql = `INSERT INTO "${table}" (${quoted}) ${overriding}
               VALUES ${tuples.join(', ')}
               ON CONFLICT (${conflict}) ${action}`;

  if (DRY) return rows.length;
  await cloudPool.query(sql, params);
  return rows.length;
}

// ── Per-strategy sync ───────────────────────────────────────────────────────
async function syncIdentity(table, meta) {
  const wm = await getWatermark(table);
  const since = wm.last_synced_id ?? 0;
  const idCol = meta.pk[0];

  let total = 0;
  let cursor = since;
  for (;;) {
    const { rows } = await localPool.query(
      `SELECT * FROM "${table}" WHERE "${idCol}" > $1 ORDER BY "${idCol}" LIMIT $2`,
      [cursor, BATCH]);
    if (!rows.length) break;

    total += await push(table, rows, meta, 'identity');
    cursor = rows[rows.length - 1][idCol];
    // Checkpoint each batch: an interrupted pass resumes mid-backlog instead of
    // restarting, which matters when a long outage leaves thousands of rows.
    await setWatermark(table, { id: cursor, added: rows.length });
    if (rows.length < BATCH) break;
  }
  return total;
}

async function syncTimestamp(table, meta) {
  const wm = await getWatermark(table);
  // Epoch on first run ⇒ everything is "changed since", i.e. a full seed.
  let curAt = wm.last_synced_at ?? '1970-01-01 00:00:00+00';
  let curKey = wm.last_synced_key ?? '';
  const pk = meta.pk[0];

  let total = 0;
  for (;;) {
    // Keyset pagination on the COMPOSITE cursor. The row-value comparison
    // `(updated_at, pk) > (at, key)` is strictly ordered even when hundreds of
    // rows share one updated_at (which a bulk UPDATE guarantees, since
    // set_updated_at() stamps the transaction timestamp). ORDER BY must use the
    // same expressions as the comparison or paging skips rows.
    //
    // pk is cast to text so one code path covers both uuid and bigint keys; the
    // ordering only needs to be CONSISTENT, not semantically numeric, because
    // it serves purely as a tie-breaker within an identical timestamp.
    const { rows } = await localPool.query(
      `SELECT *, updated_at::text AS _wm_at, "${pk}"::text AS _wm_key
         FROM "${table}"
        WHERE (updated_at, "${pk}"::text) > ($1::timestamptz, $2)
        ORDER BY updated_at, "${pk}"::text
        LIMIT $3`,
      [curAt, curKey, BATCH]);
    if (!rows.length) break;

    total += await push(table, rows, meta, 'timestamp');
    const last = rows[rows.length - 1];
    curAt = last._wm_at;
    curKey = last._wm_key;
    await setWatermark(table, { at: curAt, key: curKey, added: rows.length });
    if (rows.length < BATCH) break;
  }
  return total;
}

async function syncStatic(table, meta) {
  // Tiny immutable lookup (roles). Re-upserting the whole table each pass costs
  // nothing and removes the need for a change marker on a table that has none.
  const { rows } = await localPool.query(`SELECT * FROM "${table}"`);
  const n = await push(table, rows, meta, 'timestamp');
  await setWatermark(table, { added: 0 });
  return n;
}

// ── One full pass ───────────────────────────────────────────────────────────
async function runOnce() {
  const started = Date.now();
  let moved = 0;
  const failures = [];

  for (const { name, strategy } of TABLES) {
    try {
      const meta = await describe(name);
      if (!meta.pk.length) { log(`  ${name}: no PK, skipped`); continue; }

      const n = strategy === 'identity'  ? await syncIdentity(name, meta)
              : strategy === 'static'    ? await syncStatic(name, meta)
              :                            await syncTimestamp(name, meta);
      moved += n;
      if (n) log(`  ${name}: ${n} row(s)${DRY ? ' (dry-run)' : ''}`);
    } catch (err) {
      // One bad table must not abort the rest — a FK hiccup on `commands`
      // shouldn't stop telemetry from reaching the backup.
      failures.push(`${name}: ${err.message}`);
      await setWatermark(name, { error: err.message }).catch(() => {});
      log(`  ${name}: FAILED — ${err.message}`);
    }
  }

  const secs = ((Date.now() - started) / 1000).toFixed(1);
  log(failures.length
    ? `pass complete in ${secs}s — ${moved} row(s), ${failures.length} table(s) failed`
    : `pass complete in ${secs}s — ${moved} row(s) replicated`);
  return { moved, failures };
}

async function printStatus() {
  const { rows } = await localPool.query(
    `SELECT table_name, last_synced_id, last_synced_at::text AS last_synced_at,
            last_synced_key, rows_synced, last_run_at, last_error
       FROM sync_state ORDER BY table_name`);
  if (!rows.length) return log('sync_state is empty — no pass has run yet.');
  console.table(rows.map((r) => ({
    table: r.table_name,
    watermark: r.last_synced_id ?? r.last_synced_at ?? null,
    rows: r.rows_synced,
    last_run: r.last_run_at?.toISOString?.() ?? null,
    error: r.last_error ?? '',
  })));
}

async function main() {
  if (!process.env.CLOUD_DATABASE_URL_OWNER) {
    console.error('CLOUD_DATABASE_URL_OWNER is not set — check .env.cloud');
    process.exit(1);
  }

  if (STATUS) { await printStatus(); return; }

  if (BACKFILL) {
    log('--backfill: clearing watermarks, every row will be re-pushed');
    if (!DRY) await localPool.query('TRUNCATE sync_state');
  }

  // Verify the cloud is actually reachable before claiming to replicate.
  try {
    const { rows } = await cloudPool.query('SELECT current_user, now()');
    log(`cloud reachable as ${rows[0].current_user}`);
  } catch (err) {
    log(`cloud UNREACHABLE — ${err.message}`);
    if (ONCE) process.exit(1);
    log(`will retry every ${INTERVAL_MS}ms; local logging is unaffected`);
  }

  await runOnce();
  if (ONCE || DRY) return;

  log(`replicating every ${INTERVAL_MS}ms — Ctrl+C to stop`);
  // A pass that outruns the interval must NOT start a second one. Each pass
  // opens connections per table; overlapping passes multiply that against a
  // pooler capped at 15 backend connections, and the first pass to finish
  // tears down pools the others are still using. The observed failure is a
  // cascade of "connection timeout" then "cannot use a pool after end" that
  // wedges the pooler for ~15 minutes — and restarting the worker only feeds
  // it. Skipping a tick is always cheaper: the watermark means the next pass
  // picks up exactly where this one left off.
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) {
      log(`previous pass still running after ${INTERVAL_MS}ms — skipping this tick`);
      return;
    }
    inFlight = true;
    runOnce()
      .catch((e) => log('pass error:', e.message))
      .finally(() => { inFlight = false; });
  }, INTERVAL_MS);

  const shutdown = async () => {
    clearInterval(timer);
    await Promise.allSettled([localPool.end(), cloudPool.end()]);
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main()
  .then(async () => {
    if (ONCE || STATUS || DRY) {
      await Promise.allSettled([localPool.end(), cloudPool.end()]);
    }
  })
  .catch(async (err) => {
    console.error('sync failed:', err);
    await Promise.allSettled([localPool.end(), cloudPool.end()]);
    process.exit(1);
  });
