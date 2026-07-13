// calibration.js ────────────────────────────────────────────────────────────
// The edge node transmits ONLY raw 16-bit ADC counts. All engineering-unit
// conversion happens here, in the backend. Two paths:
//
//   1. Linear (default, deterministic):  value = slope * raw + offset
//   2. Expression (optional):            arbitrary formula in `x` (= raw count)
//
// The expression path uses mathjs with a security lockdown so a stored formula
// can't reach process/require/import. NOTE the ordering below: we capture the
// real evaluate() BEFORE disabling the dangerous functions on the math object —
// capturing after the lockdown would grab a neutered reference.

import { create, all } from 'mathjs';

const math = create(all, {});

// Capture a working evaluate() first...
const rawEvaluate = math.evaluate.bind(math);

// ...then harden the instance: neuter anything that could escape the sandbox.
const DISABLED = ['import', 'createUnit', 'evaluate', 'parse', 'simplify', 'derivative'];
const blocked = () => { throw new Error('function is disabled for security'); };
math.import(DISABLED.reduce((acc, name) => ((acc[name] = blocked), acc), {}), {
  override: true,
});

// safeEvaluate now points at the captured, still-functional evaluator, while
// the math namespace exposed to formulas has the escape hatches removed.
const safeEvaluate = rawEvaluate;

// Firmware / thesis formulas are written Python-style. Normalize the common
// cases to mathjs syntax so a formula pasted from the calibration bank runs.
//   x**2      -> x^2
//   np.floor  -> floor      (and other np.* -> bare fn)
//   math.foo  -> foo
function normalizeExpression(expr) {
  return String(expr)
    .replace(/\*\*/g, '^')
    .replace(/\bnp\./g, '')
    .replace(/\bmath\./g, '');
}

// Apply a calibration to a single raw count. `cal` is either:
//   { type: 'linear', slope, offset }
//   { type: 'expr', expr: 'x^2 * 0.001 + 3' }
// Returns a finite Number, or null if it can't be computed.
export function applyCalibration(raw, cal) {
  if (!Number.isFinite(raw)) return null;

  if (!cal || cal.type === 'linear') {
    const slope = cal?.slope ?? 1;
    const offset = cal?.offset ?? 0;
    const v = slope * raw + offset;
    return Number.isFinite(v) ? round4(v) : null;
  }

  if (cal.type === 'expr' && cal.expr) {
    try {
      const v = safeEvaluate(normalizeExpression(cal.expr), { x: raw });
      const num = typeof v === 'number' ? v : Number(v);
      return Number.isFinite(num) ? round4(num) : null;
    } catch {
      return null; // bad formula → treat as no reading rather than crash ingest
    }
  }

  return null;
}

// Fit a linear calibration from manual (raw, value) sample points via least
// squares — mirrors the frontend Calibration page's "unguided" mode, so a
// saved regression there produces the same slope/offset here.
export function fitLinear(points) {
  const pts = points.filter(
    (p) => Number.isFinite(p?.raw) && Number.isFinite(p?.value)
  );
  const n = pts.length;
  if (n < 2) return null;

  let sx = 0, sy = 0, sxx = 0, sxy = 0;
  for (const { raw, value } of pts) {
    sx += raw; sy += value; sxx += raw * raw; sxy += raw * value;
  }
  const denom = n * sxx - sx * sx;
  if (denom === 0) return null;

  const slope = (n * sxy - sx * sy) / denom;
  const offset = (sy - slope * sx) / n;

  // R² for reporting
  const meanY = sy / n;
  let ssRes = 0, ssTot = 0;
  for (const { raw, value } of pts) {
    const pred = slope * raw + offset;
    ssRes += (value - pred) ** 2;
    ssTot += (value - meanY) ** 2;
  }
  const r2 = ssTot === 0 ? 1 : 1 - ssRes / ssTot;

  return { type: 'linear', slope: round6(slope), offset: round6(offset), r2: round4(r2) };
}

const round4 = (v) => Math.round(v * 1e4) / 1e4;
const round6 = (v) => Math.round(v * 1e6) / 1e6;
