// 1730000000006_hardware_presence.js
// Shifts the hardware inventory from DECLARED (seeded by hand) to OBSERVED
// (created by discovery/telemetry), while making absence non-destructive.
//
// THE MODEL
//   A device/board/port row is created the first time real hardware announces
//   it, and is NEVER auto-deleted. When the hardware stops reporting the row
//   stays and simply reads Offline, preserving every user edit — label, unit,
//   ranges, calibration assignment. Plug the same sensor back into the same
//   channel and it resumes with its old configuration intact.
//
//   Removal is therefore a deliberate human act, not a side effect of a loose
//   cable. That's the whole point: an aquaculture node that drops off the bus
//   for ten minutes must not silently lose its calibration.
//
// WHAT THIS ADDS
//   first_seen  — when the hardware was first observed (audit / "installed on")
//   last_seen   — when it last reported. `devices` already had this; modules and
//                 ports did not, so there was no way to say WHICH board went
//                 quiet, only that the node did.
//
// last_seen also underpins the deletion guard: a row may only be removed while
// it is stale (not currently reporting), which is enforced in routes.js. Storing
// it means that rule survives a backend restart rather than depending on
// whatever happens to be in RAM.

export const up = (pgm) => {
  pgm.sql(`
-- ── Presence columns ────────────────────────────────────────────────────────
ALTER TABLE modules ADD COLUMN IF NOT EXISTS last_seen  timestamptz;
ALTER TABLE modules ADD COLUMN IF NOT EXISTS first_seen timestamptz NOT NULL DEFAULT now();
ALTER TABLE ports   ADD COLUMN IF NOT EXISTS last_seen  timestamptz;
ALTER TABLE ports   ADD COLUMN IF NOT EXISTS first_seen timestamptz NOT NULL DEFAULT now();
ALTER TABLE devices ADD COLUMN IF NOT EXISTS first_seen timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS ix_modules_last_seen ON modules (last_seen);
CREATE INDEX IF NOT EXISTS ix_ports_last_seen   ON ports (last_seen);

-- ── Auto-provisioning defaults ──────────────────────────────────────────────
-- A freshly discovered port has NO sensor metadata: the wire protocol carries a
-- raw ADC count and nothing that says "this is dissolved oxygen in mg/L". So the
-- defaults must be honest rather than invented — identity calibration, raw
-- units, full ADS1115 span. The dashboard then shows the true raw count until an
-- operator assigns a real label and calibration formula, instead of displaying a
-- confident-looking number that means nothing.
--
-- cal_type/cal_slope/cal_offset already default to ('linear', 1, 0) from
-- migration 002, which IS the identity transform (value = raw). Nothing to
-- change there; these defaults just let INSERT omit the descriptive columns.
ALTER TABLE ports ALTER COLUMN label     SET DEFAULT 'Unassigned channel';
ALTER TABLE ports ALTER COLUMN unit      SET DEFAULT 'raw';
ALTER TABLE ports ALTER COLUMN range_min SET DEFAULT 0;
ALTER TABLE ports ALTER COLUMN range_max SET DEFAULT 32767;
ALTER TABLE ports ALTER COLUMN safe_min  SET DEFAULT 0;
ALTER TABLE ports ALTER COLUMN safe_max  SET DEFAULT 32767;

-- Distinguishes "the operator named this" from "the system generated a
-- placeholder". The UI uses it to nudge toward configuring a new channel, and it
-- keeps auto-provisioning from ever overwriting a human-chosen name.
ALTER TABLE ports   ADD COLUMN IF NOT EXISTS configured boolean NOT NULL DEFAULT false;
ALTER TABLE modules ADD COLUMN IF NOT EXISTS configured boolean NOT NULL DEFAULT false;
ALTER TABLE devices ADD COLUMN IF NOT EXISTS configured boolean NOT NULL DEFAULT false;

-- Rows that already exist were placed there deliberately (seed or manual), so
-- mark them configured — auto-provisioning must never rename them.
UPDATE ports    SET configured = true WHERE configured = false;
UPDATE modules  SET configured = true WHERE configured = false;
UPDATE devices  SET configured = true WHERE configured = false;
`);
};

export const down = (pgm) => {
  pgm.sql(`
ALTER TABLE ports   ALTER COLUMN label     DROP DEFAULT;
ALTER TABLE ports   ALTER COLUMN unit      DROP DEFAULT;
ALTER TABLE ports   ALTER COLUMN range_min DROP DEFAULT;
ALTER TABLE ports   ALTER COLUMN range_max DROP DEFAULT;
ALTER TABLE ports   ALTER COLUMN safe_min  DROP DEFAULT;
ALTER TABLE ports   ALTER COLUMN safe_max  DROP DEFAULT;

DROP INDEX IF EXISTS ix_modules_last_seen;
DROP INDEX IF EXISTS ix_ports_last_seen;

ALTER TABLE ports   DROP COLUMN IF EXISTS configured;
ALTER TABLE modules DROP COLUMN IF EXISTS configured;
ALTER TABLE devices DROP COLUMN IF EXISTS configured;
ALTER TABLE ports   DROP COLUMN IF EXISTS first_seen;
ALTER TABLE ports   DROP COLUMN IF EXISTS last_seen;
ALTER TABLE modules DROP COLUMN IF EXISTS first_seen;
ALTER TABLE modules DROP COLUMN IF EXISTS last_seen;
ALTER TABLE devices DROP COLUMN IF EXISTS first_seen;
`);
};
