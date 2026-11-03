// tc-3-2-isolation.mjs ───────────────────────────────────────────────────────
// TC-3.2: tenant isolation, as evidence. Two halves, each skipped if its
// settings are missing:
//
//   API  against the deployed app: no/invalid session → 401; a real member
//        naming ANOTHER organisation (header or ?tenant=) → 403 on reads and
//        writes; their own request → 200 with only their own devices.
//   RLS  straight against the database AS THE APP'S ROLE (senseable_app):
//        nothing visible with no tenant set; only tenant A's rows with A set;
//        writes aimed at tenant B blocked or matching nothing.
//
// Nothing is changed: every write it sends is either refused (the point of the
// test) or aimed at an id that does not exist, and the database half runs in
// one transaction that is always rolled back.
//
// A terminal tool, run from the senseable-api folder; nothing here is deployed.
//   cp scripts/tc-3-2.env.example .env.tc32    # fill it in (.env* is git-ignored)
//   node --env-file=.env.tc32 scripts/tc-3-2-isolation.mjs
//
// Results print as PASS/FAIL and are saved to tc-3-2-out/results-<time>.csv.
// Exit code 0 only if every check passed.

import fs from 'node:fs';
import pg from 'pg';

const E = process.env;
const rows = [];   // [id, check, expected, got, pass]
const rec = (id, check, expected, got, pass) => {
  rows.push([id, check, expected, String(got), pass ? 'PASS' : 'FAIL']);
  console.log(`  ${pass ? 'PASS' : 'FAIL'}  ${id}  ${check}  (expected ${expected}, got ${got})`);
};

// ── API half ────────────────────────────────────────────────────────────────
async function signIn(email, password) {
  const r = await fetch(`${E.TC_SUPABASE_URL}/auth/v1/token?grant_type=password`, {
    method: 'POST',
    headers: { apikey: E.TC_SUPABASE_ANON, 'content-type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j.access_token) throw new Error(`sign-in failed for ${email}: ${j.error_description ?? j.msg ?? r.status}`);
  return j.access_token;
}

async function api(method, path, { token, tenant, query = '' } = {}) {
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  if (tenant) headers['x-tenant-id'] = tenant;
  const r = await fetch(`${E.TC_API_BASE.replace(/\/$/, '')}/api/${path}${query}`, {
    method, headers, body: method === 'GET' ? undefined : '{}',
  });
  let body = null;
  try { body = await r.json(); } catch { /* not json */ }
  return { status: r.status, body };
}

async function apiChecks() {
  const need = ['TC_API_BASE', 'TC_SUPABASE_URL', 'TC_SUPABASE_ANON', 'TC_A_EMAIL', 'TC_A_PASSWORD', 'TC_A_SLUG', 'TC_B_SLUG'];
  const missing = need.filter((k) => !E[k]);
  if (missing.length) { console.log(`API half skipped (missing ${missing.join(', ')})`); return; }
  console.log(`\nAPI checks against ${E.TC_API_BASE}`);
  const A = E.TC_A_SLUG, B = E.TC_B_SLUG;
  const tok = await signIn(E.TC_A_EMAIL, E.TC_A_PASSWORD);
  let s;

  s = await api('GET', 'devices');
  rec('API-01', 'read devices with no session', 401, s.status, s.status === 401);
  s = await api('GET', 'devices', { token: 'not-a-real-token' });
  rec('API-02', 'read devices with an invalid token', 401, s.status, s.status === 401);

  s = await api('GET', 'devices', { token: tok, tenant: A });
  const list = Array.isArray(s.body) ? s.body : [];
  const foreign = list.filter((d) => d.tenantId !== A).length;
  rec('API-03', `member of ${A} reads own devices`, '200', s.status, s.status === 200);
  rec('API-04', `devices returned that belong to another org (of ${list.length})`, 0, foreign, s.status === 200 && foreign === 0);

  let n = 5;
  for (const res of ['devices', 'notifications', 'formulas', 'commands', 'map-sensors', 'sensor-profiles']) {
    s = await api('GET', res, { token: tok, tenant: B });
    rec(`API-${String(n++).padStart(2, '0')}`, `member of ${A} reads ${res} of ${B} (header)`, 403, s.status, s.status === 403);
  }
  s = await api('GET', 'devices', { token: tok, query: `?tenant=${encodeURIComponent(B)}` });
  rec(`API-${n++}`, `member of ${A} reads devices of ${B} (?tenant=)`, 403, s.status, s.status === 403);

  // Writes: the empty body and the made-up device id mean nothing could happen
  // even if the check were broken. The cross-org check runs before either.
  s = await api('POST', 'commands', { token: tok, tenant: B });
  rec(`API-${n++}`, `member of ${A} sends a command to ${B}`, 403, s.status, s.status === 403);
  s = await api('PATCH', 'devices/tc32-no-such-device', { token: tok, tenant: B });
  rec(`API-${n++}`, `member of ${A} renames a device of ${B}`, 403, s.status, s.status === 403);
  s = await api('POST', 'commands');
  rec(`API-${n++}`, 'send a command with no session', 401, s.status, s.status === 401);

  if (E.TC_NOORG_EMAIL && E.TC_NOORG_PASSWORD) {
    const t2 = await signIn(E.TC_NOORG_EMAIL, E.TC_NOORG_PASSWORD);
    s = await api('GET', 'devices', { token: t2 });
    rec(`API-${n++}`, 'confirmed account in no organization reads devices', 403, s.status, s.status === 403);
  } else {
    console.log('  (no-organization account not set: that 403 case skipped)');
  }
}

// ── RLS half ────────────────────────────────────────────────────────────────
async function rlsChecks() {
  const need = ['TC_APP_DB_URL', 'TC_A_SLUG', 'TC_B_SLUG'];
  const missing = need.filter((k) => !E[k]);
  if (missing.length) { console.log(`RLS half skipped (missing ${missing.join(', ')})`); return; }
  console.log('\nRLS checks against the database');
  const db = new pg.Client({
    connectionString: E.TC_APP_DB_URL,
    ssl: E.TC_DB_NO_SSL === 'true' ? false : { rejectUnauthorized: false },
  });
  await db.connect();
  try {
    // A superuser, a BYPASSRLS role, or the tables' owner sees everything
    // regardless of policy, so a run as one proves nothing. Stop instead.
    const { rows: [me] } = await db.query(`
      SELECT current_user AS u, r.rolsuper, r.rolbypassrls,
             EXISTS (SELECT 1 FROM pg_class c WHERE c.relowner = r.oid AND c.relname = 'readings') AS owner
      FROM pg_roles r WHERE r.rolname = current_user`);
    const valid = !me.rolsuper && !me.rolbypassrls && !me.owner;
    rec('RLS-00', `connected as ${me.u}: not superuser, not BYPASSRLS, not table owner`, 'true', valid, valid);
    if (!valid) return;

    // pg_catalog, not information_schema: the latter hides tables this role
    // has no privileges on, and those should show up as "denied".
    const { rows: tabs } = await db.query(`
      SELECT c.relname AS table_name FROM pg_attribute a
      JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace s ON s.oid = c.relnamespace
      WHERE s.nspname = 'public' AND c.relkind IN ('r', 'p') AND a.attname = 'tenant_id'
        AND NOT a.attisdropped ORDER BY 1`);
    const { rows: [ids] } = await db.query('SELECT resolve_tenant($1) AS a, resolve_tenant($2) AS b', [E.TC_A_SLUG, E.TC_B_SLUG]);
    if (!ids.a || !ids.b) throw new Error(`unknown org slug: ${!ids.a ? E.TC_A_SLUG : E.TC_B_SLUG}`);

    await db.query('BEGIN');
    // A failed statement aborts the transaction; a savepoint per check keeps
    // one denial from failing every check after it.
    const q = async (sql, params) => {
      await db.query('SAVEPOINT c');
      try { const r = await db.query(sql, params); await db.query('RELEASE SAVEPOINT c'); return { r }; }
      catch (e) { await db.query('ROLLBACK TO SAVEPOINT c'); return { e }; }
    };
    const denied = (e) => e && /permission denied|row-level security/i.test(e.message);

    // 1. No tenant set: every tenant table looks empty (or is not readable).
    for (const { table_name: t } of tabs) {
      const { r, e } = await q(`SELECT count(*)::int AS n FROM "${t}"`);
      const got = e ? (denied(e) ? 'denied' : e.message) : r.rows[0].n;
      rec('RLS-01', `no org set: rows visible in ${t}`, '0 or denied', got, got === 0 || got === 'denied');
    }

    // 2. Org A set: nothing from any other org is visible.
    await db.query("SELECT set_config('app.current_tenant', $1, true)", [ids.a]);
    for (const { table_name: t } of tabs) {
      const { r, e } = await q(`SELECT count(*) FILTER (WHERE tenant_id <> $1)::int AS other,
                                       count(*)::int AS own FROM "${t}"`, [ids.a]);
      const got = e ? (denied(e) ? 'denied' : e.message) : r.rows[0].other;
      const own = e ? '' : ` (own rows: ${r.rows[0].own})`;
      rec('RLS-02', `org ${E.TC_A_SLUG} set: other orgs' rows in ${t}${own}`, '0 or denied', got, got === 0 || got === 'denied');
    }

    // 3. Org A set: writes aimed at org B.
    {
      const { e } = await q(`INSERT INTO notifications(tenant_id, type, title, message)
                             VALUES ($1, 'info', 'tc-3.2', 'tc-3.2 isolation probe')`, [ids.b]);
      rec('RLS-03', `org ${E.TC_A_SLUG} set: insert a notification for ${E.TC_B_SLUG}`, 'blocked', e ? 'blocked' : 'inserted', denied(e));
    }
    for (const [id, what, sql] of [
      ['RLS-04', 'update notifications', 'UPDATE notifications SET is_read = is_read WHERE tenant_id = $1'],
      ['RLS-05', 'delete notifications', 'DELETE FROM notifications WHERE tenant_id = $1'],
      ['RLS-06', 'update devices',       'UPDATE devices SET name = name WHERE tenant_id = $1'],
    ]) {
      const { r, e } = await q(sql, [ids.b]);
      const got = e ? (denied(e) ? 'denied' : e.message) : r.rowCount;
      rec(id, `org ${E.TC_A_SLUG} set: ${what} of ${E.TC_B_SLUG}`, '0 or denied', got, got === 0 || got === 'denied');
    }
  } finally {
    await db.query('ROLLBACK').catch(() => {});
    await db.end();
  }
}

// ── Run ─────────────────────────────────────────────────────────────────────
const started = new Date();
try {
  await apiChecks();
  await rlsChecks();
} catch (e) {
  rec('ERROR', 'run aborted', 'no error', e.message, false);
}
if (!rows.length) { console.error('\nNothing ran: fill in .env.tc32 (see scripts/tc-3-2.env.example).'); process.exit(2); }

fs.mkdirSync('tc-3-2-out', { recursive: true });
const file = `tc-3-2-out/results-${started.toISOString().replace(/[:.]/g, '-')}.csv`;
const csv = (v) => `"${String(v).replace(/"/g, '""')}"`;
fs.writeFileSync(file, [
  `# TC-3.2 tenant isolation, run ${started.toISOString()}, API ${E.TC_API_BASE ?? '(skipped)'}, org A ${E.TC_A_SLUG}, org B ${E.TC_B_SLUG}`,
  'id,check,expected,got,result',
  ...rows.map((r) => r.map(csv).join(',')),
].join('\n') + '\n');
const failed = rows.filter((r) => r[4] === 'FAIL').length;
console.log(`\n${rows.length - failed}/${rows.length} passed. Saved ${file}`);
process.exit(failed ? 1 : 0);
