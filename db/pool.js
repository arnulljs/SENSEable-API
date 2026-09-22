// db/pool.js — connection pools + the Row-Level Security bridge.
//
// TWO pools, deliberately:
//
//   pool       (senseable_app)   — RLS-ENFORCED. Every per-request read/write
//                                  goes through withTenant(), which sets the
//                                  app.current_tenant GUC for one transaction.
//                                  A query with no tenant set sees ZERO rows
//                                  (fail-closed), and a write aimed at another
//                                  tenant is REJECTED by the database itself.
//
//   adminPool  (senseable_owner) — RLS-exempt. Used ONLY for boot-time
//                                  hydration, which legitimately spans every
//                                  tenant (the dashboard serves all orgs and
//                                  filters client-side). Never used to service
//                                  a user request.
//
// Keeping these separate is the point: a bug in a route handler cannot reach
// across tenants, because the pool it holds physically cannot.
import 'dotenv/config';
import pg from 'pg';

const { Pool } = pg;

if (!process.env.DATABASE_URL) {
  console.warn('[pg] DATABASE_URL is not set — check your .env');
}

// PG_SSL=no-verify: TLS on, chain not verified. Needed when the pools point at
// Supabase (the cloud bridge). A URL sslmode is not used for this because
// pg-connection-string upgrades 'require' to verify-full, which hung on this
// project's network — the same reason scripts/sync.js and api/_db.js set ssl
// explicitly. Unset for local Postgres.
const ssl = process.env.PG_SSL === 'no-verify' ? { rejectUnauthorized: false } : undefined;

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,          // senseable_app
  ssl,
  max: Number(process.env.PG_POOL_MAX ?? 10),
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

// Falls back to DATABASE_URL so a dev box that set only one URL still boots.
export const adminPool = new Pool({
  connectionString: process.env.DATABASE_URL_OWNER ?? process.env.DATABASE_URL,
  ssl,
  max: Number(process.env.PG_ADMIN_POOL_MAX ?? 4),
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

for (const [name, p] of [['app', pool], ['admin', adminPool]]) {
  p.on('error', (err) => console.error(`[pg:${name}] idle client error:`, err.message));
}

/**
 * Run work scoped to one tenant. Everything inside `fn` sees only that tenant's
 * rows, and writes to any other tenant are rejected by PostgreSQL itself.
 *
 *   await withTenant(tenantUuid, async (c) => {
 *     const { rows } = await c.query('SELECT * FROM devices'); // no WHERE needed
 *     return rows;
 *   });
 */
// Every withTenant() transaction in flight. Ingest persists readings, device
// state and presence fire-and-forget so the MQTT hot path never waits on the
// database; a long-running server doesn't care when they land, but a Lambda
// that returns early is frozen mid-write. drainWrites() lets it wait them out.
const inflight = new Set();

export async function drainWrites() {
  while (inflight.size) await Promise.allSettled([...inflight]);
}

export function withTenant(tenantId, fn) {
  const p = runWithTenant(tenantId, fn);
  inflight.add(p);
  p.then(() => inflight.delete(p), () => inflight.delete(p));
  return p;
}

async function runWithTenant(tenantId, fn) {
  if (!tenantId) throw new Error('withTenant: tenantId is required');
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // is_local = true → discarded at COMMIT/ROLLBACK, so the setting can never
    // leak to the next borrower of this pooled connection.
    await client.query("SELECT set_config('app.current_tenant', $1, true)", [tenantId]);
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/** One-off tenant-scoped query. */
async function tenantQuery(tenantId, text, params = []) {
  return withTenant(tenantId, (c) => c.query(text, params));
}

/** Graceful shutdown. */
export async function closePool() {
  await Promise.allSettled([pool.end(), adminPool.end()]);
}
