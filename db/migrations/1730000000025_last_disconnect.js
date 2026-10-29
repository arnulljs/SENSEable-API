// 1730000000025_last_disconnect.js
// Config carries the node's last network outage (firmware 2026-10-29, handover
// 2.3): {cause, reason, reason_code, at, offline_s, rows_spooled}. Kept whole as
// jsonb, cleaned by ingestConfig, shown on the device card. Nullable: absent
// until the node's first outage since boot.
// Run on the cloud database FIRST, then the on-site one (the sync worker copies
// the columns the on-site database has).
export const up = (pgm) => {
  pgm.sql('ALTER TABLE devices ADD COLUMN IF NOT EXISTS last_disconnect jsonb;');
};
export const down = (pgm) => {
  pgm.sql('ALTER TABLE devices DROP COLUMN IF EXISTS last_disconnect;');
};
