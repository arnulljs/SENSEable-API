// 1730000000024_window_stats_crash_info.js
// Firmware 2026-10-28/29: telemetry is a window average, and config carries why
// the node last restarted.
//
//   readings  code              port status code for the window (0..3)
//             n                 samples behind the mean (1 per second)
//             win_ms            the window actually covered, so n can be judged
//             raw_min, raw_max  extremes in ADC counts, as sent
//             value_min/max     the same, calibrated (ordered, so an inverting
//                               formula still gives min <= max)
//   ports     last_code         the latest window's code, for the dashboard label
//   devices   last_reset, last_crash_step, last_crash_task, prev_uptime_s
//
// All nullable: rows from older firmware stay valid. The sync worker copies
// the columns the on-site database has, so run this on the cloud database
// FIRST, then on the on-site one.
export const up = (pgm) => {
  pgm.sql(`
    ALTER TABLE readings
      ADD COLUMN IF NOT EXISTS code      smallint,
      ADD COLUMN IF NOT EXISTS n         integer,
      ADD COLUMN IF NOT EXISTS win_ms    integer,
      ADD COLUMN IF NOT EXISTS raw_min   integer,
      ADD COLUMN IF NOT EXISTS raw_max   integer,
      ADD COLUMN IF NOT EXISTS value_min double precision,
      ADD COLUMN IF NOT EXISTS value_max double precision;
    ALTER TABLE ports   ADD COLUMN IF NOT EXISTS last_code smallint;
    ALTER TABLE devices
      ADD COLUMN IF NOT EXISTS last_reset      text,
      ADD COLUMN IF NOT EXISTS last_crash_step text,
      ADD COLUMN IF NOT EXISTS last_crash_task text,
      ADD COLUMN IF NOT EXISTS prev_uptime_s   double precision;`);
};
export const down = (pgm) => {
  pgm.sql(`
    ALTER TABLE readings DROP COLUMN IF EXISTS code, DROP COLUMN IF EXISTS n,
      DROP COLUMN IF EXISTS win_ms, DROP COLUMN IF EXISTS raw_min, DROP COLUMN IF EXISTS raw_max,
      DROP COLUMN IF EXISTS value_min, DROP COLUMN IF EXISTS value_max;
    ALTER TABLE ports DROP COLUMN IF EXISTS last_code;
    ALTER TABLE devices DROP COLUMN IF EXISTS last_reset, DROP COLUMN IF EXISTS last_crash_step,
      DROP COLUMN IF EXISTS last_crash_task, DROP COLUMN IF EXISTS prev_uptime_s;`);
};
