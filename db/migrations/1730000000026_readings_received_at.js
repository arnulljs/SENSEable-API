// 1730000000026_readings_received_at.js
// When the row reached THIS database, separate from `ts` (the node's window
// end). received_at - ts is node -> database delay; joined with the latency
// listener's recv_ms it splits out broker -> database (Lambda schedule incl.).
// Rows synced up from the edge carry the edge's own received_at (the sync worker
// copies every column). Old rows stay NULL: no honest value exists for them.
// Run on the cloud database FIRST, then the on-site one (the sync worker copies
// the columns the on-site database has).
export const up = (pgm) => {
  // Two steps on purpose: ADD COLUMN ... DEFAULT now() would stamp every
  // EXISTING row with the migration time. Added bare, old rows stay NULL; the
  // default then applies to new inserts only.
  pgm.sql('ALTER TABLE readings ADD COLUMN IF NOT EXISTS received_at timestamptz;');
  pgm.sql('ALTER TABLE readings ALTER COLUMN received_at SET DEFAULT now();');
};
export const down = (pgm) => {
  pgm.sql('ALTER TABLE readings DROP COLUMN IF EXISTS received_at;');
};
