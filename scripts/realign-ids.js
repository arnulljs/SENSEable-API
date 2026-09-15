// scripts/realign-ids.js ────────────────────────────────────────────────────
// Bring one database's surrogate ids onto the deterministic scheme from
// migration 009, ONE TABLE PER TRANSACTION.
//
// WHY THIS IS NOT PART OF THE MIGRATION
// Rewriting a primary key cascades. Changing tenants.tenant_id carries
// readings.tenant_id on every row; changing ports.port_id carries
// readings.port_id on every row again. Inside a single migration transaction
// that is one enormous statement chain against the largest table in the
// database — slow, un-resumable, and the first thing a managed platform's
// timeout or a flaky link kills. Half an hour later you have nothing.
//
// Here each table is its own transaction. A dropped connection costs you one
// step, not the whole job, and re-running skips everything already aligned.
//
// WHEN YOU NEED IT
// Only when two tiers disagree about an id for the same row. Check first:
//
//   SELECT slug, tenant_id FROM tenants ORDER BY slug;   -- on BOTH tiers
//
// If they match, you do not need this. Old random ids and new derived ids
// coexist fine; what matters is that both tiers agree on each row.
//
// USAGE
//   node scripts/realign-ids.js --url "$CLOUD" --dry-run
//   node scripts/realign-ids.js --url "$CLOUD"
//
// Stop the edge server and the sync worker first. A concurrent write to a row
// whose key is being rewritten will block, and lock_timeout will abort that
// step.

import pg from 'pg';

const args = process.argv.slice(2);
const flag = (name) => args.includes(name);
const opt = (name, fallback = null) => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};

const URL = opt('--url', process.env.DATABASE_URL_OWNER);
const DRY = flag('--dry-run');
if (!URL) {
  console.error('pass --url <connection string>, or set DATABASE_URL_OWNER');
  process.exit(1);
}

// Parents before children. Each entry: table, key column, and the expression
// that derives the correct value. Order matters — a child's derived id is a
// function of its parent's id, so the parent has to settle first.
const STEPS = [
  ['tenants',              'tenant_id',      "senseable_uuid('tenant', slug)"],
  ['users',                'user_id',        "senseable_uuid('user', lower(email::text))"],
  ['devices',              'device_id',      "senseable_uuid('device', tenant_id::text, node_id)"],
  ['modules',              'module_id',      "senseable_uuid('module', device_id::text, lower(i2c_address))"],
  ['ports',                'port_id',        "senseable_uuid('port', module_id::text, port_code)"],
  ['actuators',            'actuator_id',    "senseable_uuid('actuator', device_id::text, port)"],
  ['calibration_formulas', 'formula_id',     "senseable_uuid('formula', tenant_id::text, label)"],
  ['sensor_profiles',      'profile_id',     "senseable_uuid('profile', tenant_id::text, name)"],
  ['map_sensors',          'map_sensor_id',  "senseable_uuid('mapsensor', tenant_id::text, port_id::text)"],
  ['map_profiles',         'map_profile_id', "senseable_uuid('mapprofile', tenant_id::text, name)"],
  // commands last: it is the table most likely to hold rows created before the
  // deterministic scheme existed, and its natural key (tenant_id, cid) is what
  // a mismatched id collides with on the next replication pass.
  ['commands',             'command_id',     "senseable_uuid('command', tenant_id::text, cid)"],
];

const pool = new pg.Pool({
  connectionString: URL,
  max: 1,
  // Supabase terminates TLS with its own CA; this script runs from an operator's
  // machine against a direct connection, so encrypt without chain pinning.
  ssl: /supabase|amazonaws|\d+\.\d+\.\d+\.\d+/.test(URL) && !/localhost|127\.0\.0\.1/.test(URL)
    ? { rejectUnauthorized: false }
    : false,
});

const secs = (t) => ((Date.now() - t) / 1000).toFixed(1);

async function main() {
  const { rows: v } = await pool.query(
    "SELECT to_regprocedure('senseable_uuid(text,text[])') IS NOT NULL AS ok");
  if (!v[0].ok) {
    console.error('senseable_uuid() is missing — run migration 009 on this database first');
    process.exit(1);
  }

  const { rows: sz } = await pool.query('SELECT count(*)::bigint AS n FROM readings');
  console.log(`readings holds ${sz[0].n} row(s). Key rewrites cascade into this ` +
              'table, so that number is what sets the pace.\n');

  let touched = 0;
  for (const [table, key, expr] of STEPS) {
    const t0 = Date.now();

    const { rows: pending } = await pool.query(
      `SELECT count(*)::int AS n FROM "${table}" WHERE "${key}" IS DISTINCT FROM ${expr}`);
    if (!pending[0].n) { console.log(`  ${table}: already aligned`); continue; }

    if (DRY) {
      console.log(`  ${table}: ${pending[0].n} row(s) would be realigned (dry-run)`);
      touched += pending[0].n;
      continue;
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      // Bounded: if something else holds the row, fail this step rather than
      // block the database for everyone.
      await client.query("SET LOCAL lock_timeout = '30s'");
      await client.query('SET LOCAL statement_timeout = 0');
      const res = await client.query(
        `UPDATE "${table}" SET "${key}" = ${expr} WHERE "${key}" IS DISTINCT FROM ${expr}`);
      await client.query('COMMIT');
      touched += res.rowCount;
      console.log(`  ${table}: ${res.rowCount} row(s) in ${secs(t0)}s`);
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      console.error(`  ${table}: FAILED after ${secs(t0)}s — ${err.message}`);
      console.error('  nothing from this step was applied; fix the cause and re-run');
      process.exitCode = 1;
      break;
    } finally {
      client.release();
    }
  }

  console.log(`\n${touched} row(s) ${DRY ? 'would be ' : ''}realigned`);
  console.log('Verify against the OTHER tier:  SELECT slug, tenant_id FROM tenants ORDER BY slug;');
  await pool.end();
}

main().catch(async (e) => { console.error(e.message); await pool.end(); process.exit(1); });
