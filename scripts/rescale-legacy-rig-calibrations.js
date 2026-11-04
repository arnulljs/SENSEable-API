// rescale-legacy-rig-calibrations.js ─────────────────────────────────────────
// One-off. Formulas calibrated with the rig while it still ran at ±4.096 V
// (0.125 mV/count) have raw_adc 1.5× the field nodes' (±6.144 V, 0.1875
// mV/count), so on a node they read 2/3 of the true value. This rescales their
// points to the node scale and refits the fitted (linear) ones.
//
//   node scripts/rescale-legacy-rig-calibrations.js --before 2026-11-04T06:00:00Z          dry run (default)
//   node scripts/rescale-legacy-rig-calibrations.js --before 2026-11-04T06:00:00Z --apply  writes
//
// --before is when the rig firmware was updated; only formulas whose points were
// measured earlier (calibration.calibrated_at) are considered. If unsure, use the
// current time: points already at the new scale fail the old-scale check below
// and are skipped.
//
// HOW
//   * A point's new raw_adc is millivolts ÷ 0.1875. The rig's millivolts was the
//     physical voltage at either scale (validated against raw × 0.125 when it was
//     taken), so this is exact; raw × 2/3 would only be approximate.
//   * unguided / 2-point are refitted by least squares, 1-point through the
//     origin, exactly as Calibration.jsx fits them. The new slope comes out 1.5×
//     the old one with the offset and R² unchanged, which the report shows.
//   * preset / custom expressions are reported, never changed. So is any
//     formula with a hand-typed point (no millivolts: its scale is unknown) or a
//     point not at the old scale.
//   * The original points are kept under calibration.legacy_points (the dashboard
//     drops that key if the formula is re-saved there). A formula that already
//     has it is never selected again.
//
// Runs against the CLOUD database as the owner (all tenants), from .env.cloud.
// The sync worker carries the change to the edge database. Readings already
// stored keep their old value (calibration is applied at ingest); every reading
// after the change uses the new formula.

import dotenv from 'dotenv';
import pg from 'pg';

dotenv.config({ path: '.env.cloud', override: false });
const argv = process.argv.slice(2);
const bi = argv.indexOf('--before');
const BEFORE = bi >= 0 ? argv[bi + 1] : null;
const APPLY = argv.includes('--apply');
if (!BEFORE || Number.isNaN(Date.parse(BEFORE)) || !process.env.CLOUD_DATABASE_URL_OWNER) {
  console.error('usage: node scripts/rescale-legacy-rig-calibrations.js --before <ISO time> [--apply]\n' +
                '       (run from senseable-api; needs CLOUD_DATABASE_URL_OWNER in .env.cloud)');
  process.exit(2);
}

const NODE_MV = 0.1875;              // field nodes and the updated rig
const OLD_MV = 0.125;                // the rig before its update
const FITTED = new Set(['unguided', '2-point', '1-point']);

// Same expression format the Calibration page writes.
const fmt = (slope, offset) =>
  `x * ${slope.toExponential(6)} ${offset >= 0 ? '+' : '-'} ${Math.abs(offset).toExponential(6)}`;

function fit(method, pts) {
  if (method === '1-point') {
    const k = pts[0].unit / pts[0].raw_adc;
    return { slope: k, offset: 0, r2: null, expression: `x * ${k.toExponential(6)}` };
  }
  const n = pts.length;
  const sx = pts.reduce((a, p) => a + p.raw_adc, 0), sy = pts.reduce((a, p) => a + p.unit, 0);
  const sxx = pts.reduce((a, p) => a + p.raw_adc ** 2, 0), sxy = pts.reduce((a, p) => a + p.raw_adc * p.unit, 0);
  const denom = n * sxx - sx * sx;
  if (!denom) return null;
  const slope = (n * sxy - sx * sy) / denom, offset = (sy - slope * sx) / n;
  const ybar = sy / n, ssTot = pts.reduce((a, p) => a + (p.unit - ybar) ** 2, 0);
  const ssRes = pts.reduce((a, p) => a + (p.unit - (slope * p.raw_adc + offset)) ** 2, 0);
  return { slope, offset, r2: ssTot ? 1 - ssRes / ssTot : 1, expression: fmt(slope, offset) };
}

const db = new pg.Client({
  connectionString: process.env.CLOUD_DATABASE_URL_OWNER,
  ssl: process.env.CLOUD_DB_NO_SSL === 'true' ? false : { rejectUnauthorized: false },
});
await db.connect();
let changed = 0;
try {
  const { rows } = await db.query(`
    SELECT f.formula_id, t.slug, f.label, f.expression, f.calibration,
           (SELECT count(*)::int FROM ports p WHERE p.formula_id = f.formula_id) AS ports_using
      FROM calibration_formulas f JOIN tenants t ON t.tenant_id = f.tenant_id
     WHERE f.calibration ->> 'rig_id' IS NOT NULL
       AND NOT (f.calibration ? 'legacy_points')
       AND (f.calibration ->> 'calibrated_at')::timestamptz < $1::timestamptz
     ORDER BY t.slug, f.label`, [BEFORE]);

  console.log(`${rows.length} rig-calibrated formula(s) measured before ${BEFORE}` +
              `${APPLY ? '' : '   (DRY RUN: nothing is written)'}\n`);

  for (const f of rows) {
    const c = f.calibration;
    const pts = Array.isArray(c.points) ? c.points : [];
    console.log(`■ ${f.slug} / "${f.label}"   method ${c.method}   rig ${c.rig_id}   ` +
                `calibrated ${c.calibrated_at}   used by ${f.ports_using} port(s)`);
    console.log(`  now:  ${f.expression}   R² ${c.r2 ?? '—'}`);

    if (!FITTED.has(c.method)) { console.log(`  SKIP: '${c.method}' is an expression, not a fit. Review it by hand.\n`); continue; }
    const hand = pts.filter((p) => p.millivolts == null);
    if (!pts.length || hand.length) {
      console.log(`  SKIP: ${pts.length ? `${hand.length} point(s) typed by hand (no millivolts): scale unknown` : 'no points'}. Review it by hand.\n`);
      continue;
    }
    const off = pts.filter((p) => Math.abs(p.millivolts - p.raw_adc * OLD_MV) > 1);
    if (off.length) {
      console.log(`  SKIP: ${off.length} point(s) are not at the old 0.125 mV/count scale (already node scale?).\n`);
      continue;
    }

    const scaled = pts.map((p) => ({ ...p, raw_adc: Math.round(p.millivolts / NODE_MV) }));
    pts.forEach((p, i) => console.log(`    unit ${p.unit}   ${p.millivolts} mV   raw ${p.raw_adc} → ${scaled[i].raw_adc}`));
    const r = fit(c.method, scaled);
    if (!r) { console.log('  SKIP: the rescaled points cannot be fitted (all share one raw value).\n'); continue; }
    console.log(`  new:  ${r.expression}   R² ${r.r2 == null ? '—' : r.r2.toFixed(6)}`);

    if (APPLY) {
      const cal = {
        ...c, points: scaled, r2: r.r2 ?? c.r2 ?? null, legacy_points: pts,
        rescaled: { at: new Date().toISOString(), from_mv_per_count: OLD_MV, to_mv_per_count: NODE_MV,
                    legacy_expression: f.expression },
      };
      await db.query('UPDATE calibration_formulas SET expression = $2, calibration = $3::jsonb WHERE formula_id = $1',
                     [f.formula_id, r.expression, JSON.stringify(cal)]);
      changed++;
      console.log('  WRITTEN');
    }
    console.log('');
  }
} finally {
  await db.end();
}
console.log(APPLY ? `${changed} formula(s) updated.` : 'Dry run complete. Re-run with --apply to write.');
