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

dotenv.config({ path: '.env.cloud', override: true });   // cloud URLs live in a separate file — override: true so a stray var of the same name in .env can't silently win

const { Pool } = pg;
const args = new Set(process.argv.slice(2));
const ONCE = args.has('--once');
const STATUS = args.has('--status');
const BACKFILL = args.has('--backfill');
const DRY = args.has('--dry-run');
// The downward (cloud → edge) pass. On by default because cloud-first means the
// cloud is where telemetry lands; set SYNC_PULL=false or pass --no-pull to run
// the worker in the legacy one-way edge → cloud mode.
const PULL = !args.has('--no-pull') && process.env.SYNC_PULL !== 'false';

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
  // Pins must agree on both tiers now that both ingest (migration 013).
  { name: 'node_tenant_assignments', strategy: 'timestamp' },
  { name: 'users',                strategy: 'timestamp' },
  { name: 'sensor_profiles',      strategy: 'timestamp' },
  { name: 'devices',              strategy: 'timestamp' },
  { name: 'modules',              strategy: 'timestamp' },
  { name: 'calibration_formulas', strategy: 'timestamp' },
  { name: 'ports',                strategy: 'timestamp' },
  { name: 'actuators',            strategy: 'timestamp' },
  // QUEUE tables are the failover backlog. Their bigint identity PK is now
  // minted independently on BOTH tiers (the cloud ingests live, the edge
  // ingests during an outage), so it can no longer be carried across or used as
  // a conflict target — it is dropped on the wire and the destination assigns
  // its own. `synced` is the queue: a row leaves it only once the cloud has
  // confirmed it.
  { name: 'readings',             strategy: 'queue',
    queue: { flag: 'synced', id: 'reading_id',
             omit: ['reading_id'], conflict: 'port_id, ts',
             // Do not pull back rows this edge itself pushed up. They are
             // already here by definition, and fetching them just to have every
             // one bounce off the unique index wastes a WAN round trip per
             // batch during backlog recovery.
             pullWhere: "origin = 'cloud'" } },
  { name: 'notifications',        strategy: 'queue',
    queue: { flag: 'synced', id: 'notification_id',
             omit: ['notification_id'], conflict: 'event_uid',
             pullWhere: null } },
  { name: 'map_profiles',         strategy: 'timestamp' },
  { name: 'map_sensors',          strategy: 'timestamp' },
  { name: 'commands',             strategy: 'timestamp' },
];

const localPool = new Pool({
  connectionString: process.env.DATABASE_URL_OWNER ?? process.env.DATABASE_URL,
  max: 4,
});
// SSL is forced here rather than via ?sslmode= in the URL. pg-connection-string
// treats 'require'/'prefer'/'verify-ca' as aliases for 'verify-full' (see the
// SECURITY WARNING pg prints on connect), so any URL-based sslmode short of a
// literal disable still attempts full chain verification. That verification
// path hung indefinitely (AggregateError [ETIMEDOUT], empty message) on this
// network even though psql, using its own TLS stack, connected to the same
// host/port instantly. Setting ssl explicitly on the Pool bypasses
// pg-connection-string's URL parsing entirely — this is the same pattern
// api/_db.js already uses successfully for the Vercel read tier, which has
// no reliable way to reference a checked-in CA file either.
const cloudPool = new Pool({
  connectionString: process.env.CLOUD_DATABASE_URL_OWNER,
  ssl: { rejectUnauthorized: false },
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
    `SELECT column_name, is_identity, data_type
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
    identity: cols.filter((c) => c.is_identity === 'YES').map((c) => c.column_name),
    // Timestamp columns need special handling on the way OUT — see selectList().
    tsCols: cols.filter((c) => c.data_type.startsWith('timestamp')).map((c) => c.column_name),
    pk: pk.map((r) => r.attname),
  };
  schemaCache.set(table, meta);
  return meta;
}

/**
 * Column list for a replication SELECT, with every timestamp cast to text.
 *
 * node-postgres parses a timestamptz into a JavaScript Date, and a Date holds
 * MILLISECONDS. Postgres stores MICROSECONDS. So a plain `SELECT *` silently
 * truncates 15:31:52.149413 to 15:31:52.149 before the value ever reaches the
 * INSERT — the driver is lossy, not the database.
 *
 * That was survivable when `ts` was only carried alongside a row identified by
 * its bigint id. It is not survivable now: (port_id, ts) IS the identity of a
 * reading. A round trip that alters ts gives the same physical sample two
 * identities, so the pull-down inserts a near-duplicate of every row it already
 * has, forever.
 *
 * Casting to text on the way out sidesteps the Date entirely. The value travels
 * as '2026-09-10 15:31:52.149413+08' and Postgres reparses it at full precision
 * on the way in.
 */
function selectList(meta) {
  const ts = new Set(meta.tsCols);
  return meta.columns
    .map((c) => (ts.has(c) ? `"${c}"::text AS "${c}"` : `"${c}"`))
    .join(', ');
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

// ── Push a batch of rows to a destination ───────────────────────────────────
// Direction is a parameter now, not an assumption. Cloud-first makes
// configuration flow DOWN (the dashboard writes to Supabase; the edge needs
// calibration and inventory locally to keep converting raw counts during an
// outage) while the failover backlog flows UP. Same upsert either way.
async function push(table, rows, meta, mode, opts = {}) {
  if (!rows.length) return 0;

  const dest = opts.dest ?? cloudPool;
  const omit = new Set(opts.omit ?? []);
  const cols = meta.columns.filter((c) => !omit.has(c));
  const override = opts.override ?? {};

  const quoted = cols.map((c) => `"${c}"`).join(', ');
  const params = [];
  const tuples = rows.map((row) => {
    const slots = cols.map((c) => {
      params.push(c in override ? override[c] : row[c]);
      return `$${params.length}`;
    });
    return `(${slots.join(', ')})`;
  });

  // An identity column can only be written explicitly with OVERRIDING SYSTEM
  // VALUE — and only when we are actually carrying it. Queue tables omit it on
  // purpose so the destination mints its own, which is what makes two
  // independently-numbering tiers safe to merge.
  const carriesIdentity = meta.hasAlwaysIdentity && cols.some((c) => meta.identity.includes(c));
  const overriding = carriesIdentity ? 'OVERRIDING SYSTEM VALUE' : '';

  // Conflict target: the primary key for replicated rows that carry their id,
  // a NATURAL key for queue rows that do not.
  const conflict = opts.conflict ?? meta.pk.map((c) => `"${c}"`).join(', ');
  const keyCols = opts.conflict
    ? opts.conflict.split(',').map((c) => c.trim().replace(/"/g, ''))
    : meta.pk;

  // Append-only rows never change, so a collision means "already replicated" —
  // skip it. Mutable rows must overwrite, or an edit made on one side would
  // never reach the other.
  const updatable = cols.filter((c) => !keyCols.includes(c));
  // The WHERE turns a no-op upsert into a genuine no-op: without it every pass
  // rewrites every row it offers, rowCount always equals the batch size, and the
  // worker reports (and NOTIFYs) work it did not do. With a downward pass added
  // that noise doubled — `roles` alone woke every dashboard socket every tick.
  const action = (mode === 'identity' || mode === 'queue' || mode === 'insert-only') || !updatable.length
    ? 'DO NOTHING'
    : `DO UPDATE SET ${updatable.map((c) => `"${c}" = EXCLUDED."${c}"`).join(', ')}
       WHERE _t.* IS DISTINCT FROM EXCLUDED.*`;

  const sql = `INSERT INTO "${table}" AS _t (${quoted}) ${overriding}
               VALUES ${tuples.join(', ')}
               ON CONFLICT (${conflict}) ${action}`;

  if (DRY) return rows.length;

  // app.sync_replay tells set_updated_at() to preserve the source timestamp
  // rather than stamping now(). Without it a replicated row looks freshly
  // edited to the OTHER direction's watermark and bounces back on the next
  // pass, forever. SET LOCAL scopes it to this transaction only.
  const client = await dest.connect();
  try {
    await client.query('BEGIN');
    await client.query("SET LOCAL app.sync_replay = 'on'");
    const result = await client.query(sql, params);
    await client.query('COMMIT');
    // Report what actually LANDED, not what was offered. A row that bounces off
    // the unique index was already there, and counting it as replicated makes
    // the log claim work that did not happen.
    return result.rowCount ?? 0;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// ── Per-strategy sync ───────────────────────────────────────────────────────

// QUEUE: the failover backlog. Rows the cloud does not have yet, drained in
// primary-key order and marked only once the destination has accepted them.
//
// This replaces the old identity-watermark strategy, which cannot survive
// cloud-first. Under it, `readings` shipped its locally-minted reading_id with
// OVERRIDING SYSTEM VALUE and conflicted on that id. Now that the cloud mints
// its own ids for live telemetry, an edge failover row would arrive carrying an
// id the cloud had already issued to a DIFFERENT reading, hit
// ON CONFLICT DO NOTHING, and be silently discarded — a backlog that reports
// success and loses every row. The id is dropped on the wire instead and the
// conflict target is the natural key (port_id, ts).
// The destination mints its own id for queue rows, which only works if its
// SEQUENCE knows how far the table already goes.
//
// It usually does not. Under the old topology every replicated row arrived with
// an explicit id via OVERRIDING SYSTEM VALUE, and an explicit insert does NOT
// advance the identity sequence. So a cloud table holding 20,000 rows can still
// have its sequence sitting at 1. The first row that asks for a generated id
// gets 1, collides with the primary key, and the whole batch fails with
// "duplicate key value violates unique constraint" — which looks like a data
// problem and is really a bookkeeping one.
//
// setval() to the current maximum is idempotent and costs one indexed lookup,
// so it runs once per table per pass rather than being a one-off repair someone
// has to remember.
async function alignSequence(dest, table, idCol) {
  if (DRY) return;
  await dest.query(
    `SELECT setval(
       pg_get_serial_sequence($1, $2),
       GREATEST(coalesce((SELECT max("${idCol}") FROM "${table}"), 0), 1))
     WHERE pg_get_serial_sequence($1, $2) IS NOT NULL`,
    [table, idCol]);
}

async function syncQueue(table, meta, q) {
  await alignSequence(cloudPool, table, q.id);
  let total = 0;
  for (;;) {
    const { rows } = await localPool.query(
      `SELECT ${selectList(meta)} FROM "${table}"
        WHERE NOT "${q.flag}" ORDER BY "${q.id}" LIMIT $1`,
      [BATCH]);
    if (!rows.length) break;

    // Push FIRST, mark second. If the push throws, nothing is marked and the
    // same rows are retried next pass — the backlog is never lost to a
    // half-finished transfer.
    // synced is per-tier bookkeeping, not data. Copying the edge's `false`
    // upward marks rows in the DESTINATION as still owed to the cloud, which is
    // nonsense — the cloud IS the cloud. Nothing drains them there, so it was
    // harmless, but it made the cloud report a permanent phantom backlog and
    // hid the one case that genuinely matters: a tier running without
    // TIER=cloud.
    await push(table, rows, meta, 'queue', {
      omit: q.omit, conflict: q.conflict, override: { synced: true },
    });

    if (!DRY) {
      await localPool.query(
        `UPDATE "${table}" SET "${q.flag}" = true WHERE "${q.id}" = ANY($1::bigint[])`,
        [rows.map((r) => r[q.id])]);
    }
    total += rows.length;
    await setWatermark(table, { added: rows.length });

    if (rows.length < BATCH) break;
    if (DRY) break;            // dry-run marks nothing, so it would loop forever
  }
  return total;
}

// TIMESTAMP: mutable rows, chased by (updated_at, pk) and applied as an upsert.
// Direction is a parameter — configuration flows DOWN under cloud-first, while
// everything else still flows up.
// Tables describing physical hardware. Their rows mix live state (last_seen,
// last_value, last_status) that the INGESTING tier writes every few seconds with
// operator settings (enabled, labels, ranges) edited on either dashboard.
// Once the cloud ingests on its own (EDGE_BRIDGES_CLOUD=false: the Lambda
// bridge), both tiers write these rows, and a whole-row upsert lets whichever
// pushed last win: the edge's copy — touched every 15 s by presence — kept
// overwriting the cloud's, reverting dashboard edits and flipping channels
// between Disabled, Offline and stale statuses on the deployed site. So in that
// mode the edge only ADDS rows the cloud has never seen (hardware first met
// during a failover) and otherwise takes the cloud's version on the pull.
const HARDWARE_TABLES = new Set(['devices', 'modules', 'ports', 'actuators']);
const CLOUD_OWNS_HARDWARE = process.env.EDGE_BRIDGES_CLOUD === 'false';

async function syncTimestamp(table, meta, dir = {}) {
  const src = dir.src ?? localPool;
  const dest = dir.dest ?? cloudPool;
  const wmKey = dir.wmKey ?? table;

  const wm = await getWatermark(wmKey);
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
    const { rows } = await src.query(
      `SELECT ${selectList(meta)}, updated_at::text AS _wm_at, "${pk}"::text AS _wm_key
         FROM "${table}"
        WHERE (updated_at, "${pk}"::text) > ($1::timestamptz, $2)
        ORDER BY updated_at, "${pk}"::text
        LIMIT $3`,
      [curAt, curKey, BATCH]);
    if (!rows.length) break;

    total += await push(table, rows, meta, dir.insertOnly ? 'insert-only' : 'timestamp', { dest });
    const last = rows[rows.length - 1];
    curAt = last._wm_at;
    curKey = last._wm_key;
    await setWatermark(wmKey, { at: curAt, key: curKey, added: rows.length });
    if (rows.length < BATCH) break;
  }
  return total;
}

async function syncStatic(table, meta, dir = {}) {
  // Tiny immutable lookup (roles). Re-upserting the whole table each pass costs
  // nothing and removes the need for a change marker on a table that has none.
  const src = dir.src ?? localPool;
  const dest = dir.dest ?? cloudPool;
  const { rows } = await src.query(`SELECT ${selectList(meta)} FROM "${table}"`);
  const n = await push(table, rows, meta, 'timestamp', { dest });
  await setWatermark(dir.wmKey ?? table, { added: 0 });
  return n;
}

// ── Downward pass: closing the mirror gap ───────────────────────────────────
// The edge is a MIRROR under cloud-first, and a mirror built only from a live
// MQTT subscription has permanent holes: if the edge loses its own WAN link
// while a cellular node keeps publishing to the cloud, that telemetry lands in
// Supabase and the edge simply never hears it. MQTT will not replay it.
//
// So the edge pulls down what it missed, using the same watermark machinery in
// reverse. Watermarks are namespaced 'down:<table>' so the two directions never
// share a cursor.
//
// Configuration comes down for a second reason: the dashboard writes to the
// cloud now, and the edge needs calibration, safe ranges and inventory LOCALLY
// or it cannot convert raw ADC counts during the next outage.
// In bridge mode the edge is the SOURCE of cloud telemetry, not a mirror of it,
// so pulling readings back down only re-fetches rows it pushed moments earlier
// to have every one bounce off the unique index. Configuration still comes down;
// only the telemetry pull is skipped.
const BRIDGE_MODE = process.env.EDGE_BRIDGES_CLOUD === 'true';

async function pullQueue(table, meta, q) {
  if (BRIDGE_MODE && table === 'readings') return 0;
  // Same problem in the other direction: the edge assigns ids for rows it pulls
  // down, and its sequence is just as likely to be behind.
  await alignSequence(localPool, table, q.id);
  const wmKey = `down:${table}`;
  const wm = await getWatermark(wmKey);
  let cursor = wm.last_synced_id ?? 0;
  let total = 0;

  for (;;) {
    const { rows } = await cloudPool.query(
      `SELECT ${selectList(meta)} FROM "${table}"
        WHERE "${q.id}" > $1 ${q.pullWhere ? `AND ${q.pullWhere}` : ''}
        ORDER BY "${q.id}" LIMIT $2`,
      [cursor, BATCH]);
    if (!rows.length) break;

    // synced=true: a mirrored row is already in the cloud by definition, and
    // marking it otherwise would push it straight back — the echo loop.
    // origin='cloud' records that it arrived over the cloud path, which the
    // dashboard surfaces to distinguish live data from recovered data.
    await push(table, rows, meta, 'queue', {
      dest: localPool,
      omit: q.omit,
      conflict: q.conflict,
      override: { synced: true, origin: 'cloud' },
    });

    cursor = rows[rows.length - 1][q.id];
    total += rows.length;
    await setWatermark(wmKey, { id: cursor, added: rows.length });
    if (rows.length < BATCH) break;
  }
  return total;
}

// ── One full pass ───────────────────────────────────────────────────────────
// ── Deletions (migration 012) ───────────────────────────────────────────────
// Upserts cannot express "this row is gone", so edge-side deletes are recorded
// as tombstones by a trigger and applied here as cloud DELETEs. Runs BEFORE the
// upsert pass, and skips any tombstone whose row exists again locally: ids are
// deterministic, so a node removed and then re-provisioned comes back with the
// same device_id, and deleting it in the cloud would erase the live row.
const DELETABLE = new Set(['devices', 'modules', 'ports', 'actuators', 'node_tenant_assignments']);

// Direction-agnostic: `src` holds the tombstones, `dest` receives the DELETEs.
async function syncDeletions(src = localPool, dest = cloudPool) {
  let rows;
  try {
    ({ rows } = await src.query(
      'SELECT id, table_name, pk_col, pk FROM sync_deletions ORDER BY id LIMIT $1', [BATCH]));
  } catch (err) {
    if (err.code === '42P01') return 0;   // migration 012 not applied on this edge yet
    throw err;
  }
  if (!rows.length) return 0;

  let applied = 0;
  const done = [];
  for (const r of rows) {
    // Identifiers are interpolated, so accept only the four known tables and a
    // plain column name — never whatever happens to be in the row.
    if (!DELETABLE.has(r.table_name) || !/^[a-z_]+$/.test(r.pk_col)) { done.push(r.id); continue; }

    const { rows: back } = await src.query(
      `SELECT 1 FROM "${r.table_name}" WHERE "${r.pk_col}" = $1`, [r.pk]);
    if (back.length) { done.push(r.id); continue; }

    if (!DRY) {
      const c = await dest.connect();
      try {
        await c.query('BEGIN');
        await c.query("SET LOCAL app.sync_replay = 'on'");   // don't tombstone the replicated delete
        const res = await c.query(`DELETE FROM "${r.table_name}" WHERE "${r.pk_col}" = $1`, [r.pk]);
        await c.query('COMMIT');
        applied += res.rowCount ?? 0;
      } catch (err) {
        await c.query('ROLLBACK').catch(() => {});
        throw err;
      } finally {
        c.release();
      }
    }
    done.push(r.id);
  }
  if (!DRY && done.length) {
    await src.query('DELETE FROM sync_deletions WHERE id = ANY($1::bigint[])', [done]);
  }
  return applied;
}

async function runOnce() {
  const started = Date.now();
  let moved = 0;
  const failures = [];

  try {
    const d = await syncDeletions(localPool, cloudPool);
    moved += d;
    if (d) log(`  ↑ deletions: ${d} row(s)${DRY ? ' (dry-run)' : ''}`);
  } catch (err) {
    failures.push(`deletions: ${err.message}`);
    log(`  deletions: FAILED — ${err.message}`);
  }
  // Deletes made on the cloud tier (the bridge, or the Vercel dashboard) come
  // down before anything is pulled, for the same re-creation reason as above.
  if (PULL) {
    try {
      const d = await syncDeletions(cloudPool, localPool);
      moved += d;
      if (d) log(`  ↓ deletions: ${d} row(s)${DRY ? ' (dry-run)' : ''}`);
    } catch (err) {
      failures.push(`down:deletions: ${err.message}`);
      log(`  ↓ deletions: FAILED — ${err.message}`);
    }
  }

  // ── UP: failover backlog and anything still authored on the edge ──────────
  for (const { name, strategy, queue } of TABLES) {
    try {
      const meta = await describe(name);
      if (!meta.pk.length) { log(`  ${name}: no PK, skipped`); continue; }

      const n = strategy === 'queue'   ? await syncQueue(name, meta, queue)
              : strategy === 'static'  ? await syncStatic(name, meta)
              :                          await syncTimestamp(name, meta, { insertOnly: CLOUD_OWNS_HARDWARE && HARDWARE_TABLES.has(name) });
      moved += n;
      if (n) log(`  ↑ ${name}: ${n} row(s)${DRY ? ' (dry-run)' : ''}`);
    } catch (err) {
      // One bad table must not abort the rest — a FK hiccup on `commands`
      // shouldn't stop telemetry from reaching the backup.
      failures.push(`${name}: ${err.message}`);
      await setWatermark(name, { error: err.message }).catch(() => {});
      log(`  ${name}: FAILED — ${err.message}`);
    }
  }

  // ── DOWN: mirror what the cloud ingested while this edge was not listening ─
  // Skipped entirely with --no-pull or SYNC_PULL=false, which is the right
  // setting for a deployment still running edge-primary.
  if (PULL) {
    for (const { name, strategy, queue } of TABLES) {
      try {
        const meta = await describe(name);
        if (!meta.pk.length) continue;

        const n = strategy === 'queue'
          ? await pullQueue(name, meta, queue)
          : strategy === 'static'
            ? await syncStatic(name, meta,
                { src: cloudPool, dest: localPool, wmKey: `down:${name}` })
            : await syncTimestamp(name, meta,
                { src: cloudPool, dest: localPool, wmKey: `down:${name}` });
        moved += n;
        if (n) log(`  ↓ ${name}: ${n} row(s)${DRY ? ' (dry-run)' : ''}`);
      } catch (err) {
        failures.push(`down:${name}: ${err.message}`);
        await setWatermark(`down:${name}`, { error: err.message }).catch(() => {});
        log(`  ↓ ${name}: FAILED — ${err.message}`);
      }
    }
  }

  const secs = ((Date.now() - started) / 1000).toFixed(1);
  log(failures.length
    ? `pass complete in ${secs}s — ${moved} row(s), ${failures.length} table(s) failed`
    : `pass complete in ${secs}s — ${moved} row(s) replicated`);

  // Tell the cloud read tier that Supabase moved.
  //
  // The edge server can push to its own dashboards the instant a packet lands,
  // because it IS the ingest point. The Vercel tier has no such signal — it only
  // reads a replica, and nothing in that replica announces itself. Without this
  // the cloud dashboard has no choice but to poll.
  //
  // NOTIFY is issued on the CLOUD connection (not the local one) because the
  // listener lives in a Vercel function attached to Supabase. Payload is capped
  // at 8000 bytes by Postgres, so this carries a summary only — the listener
  // re-reads through the normal tenant-scoped read model rather than trusting
  // anything in the message.
  if (moved > 0 && !DRY) {
    try {
      await cloudPool.query('SELECT pg_notify($1, $2)', [
        'senseable_sync',
        JSON.stringify({ moved, at: Date.now() }),
      ]);
    } catch (err) {
      // A failed notification must never fail the pass — replication already
      // succeeded, and the cloud dashboard degrades to polling on its own.
      log(`  notify failed (replication still OK) — ${err.message}`);
    }
  }

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
