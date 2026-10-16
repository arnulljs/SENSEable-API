// floating-report.js ─────────────────────────────────────────────────────────
// READ-ONLY. Prints, per ADC channel, the statistics that separate an empty
// (floating) ADS1115 input from a wired sensor, computed from readings already
// in the database. Nothing is written.
//
//   node scripts/floating-report.js            # cloud (.env.cloud), last 6 h
//   node scripts/floating-report.js --hours 24
//   node scripts/floating-report.js --local    # edge database (.env)
//
// WHAT THE COLUMNS MEAN
// The firmware converts channels one at a time, chip 0x48..0x4B, A0..A3, each
// a single conversion right after the previous one. An empty input has nothing
// to drive the ADC's sampling capacitor, so it reports roughly what the
// channel sampled just before it left behind.
//
//   jitter     median |Δraw| between consecutive packets. Tank water changes
//              slowly; an empty input wanders.
//   gap_prev   median |raw − raw of the channel sampled just before it in the
//              same packet|. Small = it sits on top of its predecessor.
//   corr_prev  correlation with that predecessor over the window. Near 1 =
//              it follows its predecessor around.
//
// A channel with a small gap_prev AND a high corr_prev is the ghost signature.
// Mark which channels are physically empty, compare their rows with the wired
// ones, and the thresholds for a "likely no sensor" badge come from this data
// instead of a guess. Only channels whose readings are stored appear: ingest
// drops samples for disabled channels.

import dotenv from 'dotenv';
import pg from 'pg';

const args = process.argv.slice(2);
const LOCAL = args.includes('--local');
const hi = args.indexOf('--hours');
const HOURS = hi >= 0 ? Number(args[hi + 1]) : 6;
if (!(HOURS > 0)) { console.error('--hours must be a positive number'); process.exit(1); }

dotenv.config({ path: LOCAL ? '.env' : '.env.cloud', override: true });
const url = LOCAL ? (process.env.DATABASE_URL_OWNER ?? process.env.DATABASE_URL)
                  : process.env.CLOUD_DATABASE_URL_OWNER;
if (!url) { console.error(`no database URL — check ${LOCAL ? '.env' : '.env.cloud'}`); process.exit(1); }

// Same SSL handling as scripts/sync.js (see the note there).
const pool = new pg.Pool({
  connectionString: url,
  ssl: LOCAL ? undefined : { rejectUnauthorized: false },
  max: 1,
  connectionTimeoutMillis: 15_000,
});

const SQL = `
WITH r AS (
  SELECT t.slug, d.node_id, lower(m.i2c_address) AS addr, p.port_index, p.port_code,
         p.port_id, p.enabled, p.formula_id IS NOT NULL AS calibrated,
         x.ts, x.raw_adc
    FROM readings x
    JOIN ports p   ON p.port_id = x.port_id
    JOIN modules m ON m.module_id = p.module_id
    JOIN devices d ON d.device_id = m.device_id
    JOIN tenants t ON t.tenant_id = d.tenant_id
   WHERE x.ts > now() - make_interval(hours => $1::int)
), s AS (
  SELECT *,
         raw_adc - lag(raw_adc) OVER (PARTITION BY port_id ORDER BY ts)                AS d_raw,
         lag(raw_adc)          OVER (PARTITION BY node_id, ts ORDER BY addr, port_index) AS prev_raw
    FROM r
)
SELECT slug, node_id, addr, port_code, enabled, calibrated,
       count(*)::int                                                              AS n,
       min(raw_adc)                                                               AS raw_min,
       max(raw_adc)                                                               AS raw_max,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(d_raw))                    AS jitter,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(raw_adc - prev_raw))       AS gap_prev,
       corr(raw_adc, prev_raw)                                                    AS corr_prev
  FROM s
 GROUP BY slug, node_id, addr, port_index, port_code, enabled, calibrated
 ORDER BY slug, node_id, addr, port_index`;

const fmt = (v, d = 0) => (v == null || Number.isNaN(Number(v)) ? '—' : Number(v).toFixed(d));

try {
  // A session that cannot write, so this script provably changes nothing.
  const c = await pool.connect();
  await c.query('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY');
  const { rows } = await c.query(SQL, [Math.ceil(HOURS)]);
  c.release();

  console.log(`${LOCAL ? 'edge' : 'cloud'} database, last ${Math.ceil(HOURS)} h — ${rows.length} channel(s) with stored readings\n`);
  console.table(rows.map((r) => ({
    tenant: r.slug, node: r.node_id, board: r.addr, ch: r.port_code,
    enabled: r.enabled, calibrated: r.calibrated, n: r.n,
    raw_min: r.raw_min, raw_max: r.raw_max,
    jitter: fmt(r.jitter, 1), gap_prev: fmt(r.gap_prev, 1), corr_prev: fmt(r.corr_prev, 3),
  })));
} catch (e) {
  console.error('report failed:', e.message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
