// 1730000000008_units_and_intervals.js ───────────────────────────────────────
// Documentation-only corrections plus one new column. No data is rewritten.
//
// WHY
// The `actuators.dur` column has carried a misleading comment since the initial
// migration: `-- auto-off seconds`. The wire protocol and the firmware both
// treat `dur` as MILLISECONDS — `vTaskDelay(pdMS_TO_TICKS(args->duration_ms))`
// — and the backend stores the wire value unconverted. The column is typed
// `integer` (max ≈ 2.1 billion) so millisecond values have always fit; nothing
// was ever stored incorrectly. But the comment actively misinformed anyone
// reading the schema, and it is the direct cause of a real bug: the dashboard's
// auto-off field was labelled "sec" while sending the raw value, so typing 10
// produced a 10 ms pulse rather than a 10 second one. On a dosing pump that
// does not look like a units error — it looks like the actuator failed to fire.
//
// The column is left as `dur` rather than renamed to `dur_ms`, because the sync
// worker discovers its column list from the catalog and the name appears in the
// frozen wire protocol. A precise comment carries the same information without
// touching either.

export async function up(pgm) {
  pgm.sql(`
    COMMENT ON COLUMN actuators.dur IS
      'Auto-off window in MILLISECONDS. Matches the wire protocol and the '
      'firmware''s vTaskDelay(pdMS_TO_TICKS(dur)). The dashboard displays '
      'seconds and converts at its UI boundary. 0 = hold until the next '
      'command.';
  `);

  pgm.sql(`
    COMMENT ON COLUMN actuators.duty IS
      '8-bit PWM duty, 0..255 — NOT an operator percentage. Matches the LEDC '
      'timer configured at LEDC_TIMER_8_BIT in firmware. Ignored when mode = '
      'bin.';
  `);

  // Telemetry cadence, as declared by the node in its discovery packet.
  //
  // Staleness was previously judged against a single global STALE_MS, which
  // cannot serve both a node publishing every 10 s and a low-power node
  // publishing every 60 s: one threshold makes the slow node permanently
  // "offline", the other lets the fast node's outage go unnoticed for minutes.
  // Storing the node's own cadence lets the threshold be derived per node
  // (3 x interval), and it survives a restart so a node that has gone quiet is
  // still judged by ITS interval rather than reverting to the default.
  //
  // Nullable on purpose: firmware that predates the field simply doesn't set
  // it, and those nodes keep using the global default.
  pgm.sql(`
    ALTER TABLE devices
      ADD COLUMN IF NOT EXISTS tlm_interval_ms integer
      CHECK (tlm_interval_ms IS NULL OR tlm_interval_ms > 0);
  `);

  pgm.sql(`
    COMMENT ON COLUMN devices.tlm_interval_ms IS
      'Telemetry publish interval in milliseconds, as declared by the node in '
      'its discovery packet. NULL means the node did not declare one and the '
      'global STALE_MS applies. Staleness is derived as 3x this value.';
  `);
}

export async function down(pgm) {
  pgm.sql(`ALTER TABLE devices DROP COLUMN IF EXISTS tlm_interval_ms;`);
  // Comments are documentation; restoring the previous inaccurate text would
  // serve no one, so they are simply cleared.
  pgm.sql(`COMMENT ON COLUMN actuators.dur IS NULL;`);
  pgm.sql(`COMMENT ON COLUMN actuators.duty IS NULL;`);
}
