// 1730000000007_port_enable.js
// Lets an operator switch off ADC channels that have nothing wired to them.
//
// WHY THIS IS NEEDED — IT'S AN ELECTRICAL PROBLEM, NOT A UI ONE
// An ADS1115 input with nothing connected does not read zero. The pin floats:
// its voltage is set by input leakage current, capacitive coupling to adjacent
// traces, and crosstalk from whichever channel the mux visited last. In practice
// it settles a few thousand counts above ground and drifts. The firmware dutifully
// samples it, the backend dutifully calibrates it, and the dashboard shows a
// confident green "Normal" gauge for a channel with nothing plugged into it.
//
// THREE LAYERS OF FIX, IN ORDER OF CORRECTNESS
//
//   1. HARDWARE (best). A 10kΩ pull-down from each unused input to GND ties the
//      pin to a known 0 V, so an empty channel reads ~0 instead of garbage. This
//      is the textbook remedy and costs pennies. Nothing in software beats it.
//
//   2. FIRMWARE. The frozen protocol already carries sensor_port_up /
//      sensor_port_down (chip 0-3, ch 0-3), and the node firmware already gates
//      its sampling loop on a port_active[] array. Disabling a channel there
//      stops it being sampled or transmitted at all — the cleanest software fix,
//      because the garbage never enters the pipeline.
//
//   3. SERVER (this column). `enabled` records the operator's intent
//      independently of whether the firmware honoured, or even implements, the
//      command. A disabled channel is excluded from status rollups, raises no
//      notifications, and — importantly — stops writing readings, so the
//      time-series table isn't filled with noise from empty inputs.
//
// The UI issues (2) and (3) together: the command goes out, and the server-side
// flag guarantees the dashboard is correct immediately regardless of firmware
// support.
//
// DISTINCT FROM THE FLAGS THAT ALREADY EXIST
//   active_flag  — discovery says a sensor is attached      (hardware reports)
//   masked       — discovery says the channel is DISABLED   (hardware reports)
//   enabled      — the operator says to monitor it          (human intent)
// Only the last one is authoritative for what the dashboard shows, because only
// a human knows that channel A2 has nothing on it.

export const up = (pgm) => {
  pgm.sql(`
ALTER TABLE ports ADD COLUMN IF NOT EXISTS enabled boolean NOT NULL DEFAULT true;

-- Why a channel was switched off, so the next person doesn't have to guess.
ALTER TABLE ports ADD COLUMN IF NOT EXISTS disabled_reason text;
ALTER TABLE ports ADD COLUMN IF NOT EXISTS disabled_at     timestamptz;

CREATE INDEX IF NOT EXISTS ix_ports_enabled ON ports (enabled) WHERE enabled = false;

COMMENT ON COLUMN ports.enabled IS
  'Operator intent: monitor this channel. false ⇒ excluded from status rollups '
  'and notifications, and telemetry for it is discarded rather than stored. '
  'Used to silence floating (unconnected) ADC inputs that read leakage voltage.';
`);
};

export const down = (pgm) => {
  pgm.sql(`
DROP INDEX IF EXISTS ix_ports_enabled;
ALTER TABLE ports DROP COLUMN IF EXISTS disabled_at;
ALTER TABLE ports DROP COLUMN IF EXISTS disabled_reason;
ALTER TABLE ports DROP COLUMN IF EXISTS enabled;
`);
};
