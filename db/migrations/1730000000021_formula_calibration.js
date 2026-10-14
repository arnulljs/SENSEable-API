// 1730000000021_formula_calibration.js
// How a formula was obtained, kept with the formula (the calibration module
// spec's audit trail: rig_id, raw_adc, millivolts, calibrated_at). One jsonb
// column rather than the spec's separate sensor_calibration_profiles table,
// which the spec itself marks optional: the formula is what gets assigned to
// channels, so its evidence belongs on the same row. NULL for formulas saved
// before this or typed in by hand.
//   { method, rig_id, points: [{ unit, raw_adc, millivolts }], r2, calibrated_at }
// The sync worker reads columns from information_schema, so this replicates.
export const up = (pgm) => {
  pgm.sql('ALTER TABLE calibration_formulas ADD COLUMN IF NOT EXISTS calibration jsonb;');
};
export const down = (pgm) => {
  pgm.sql('ALTER TABLE calibration_formulas DROP COLUMN IF EXISTS calibration;');
};
