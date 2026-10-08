// 1730000000019_hw_config.js
// Configuration twin: the network setup each node reports, retained, on
// usc/thesis/{tid}/{nid}/config — which interface it is on, the Wi-Fi SSID or
// cellular APN, and the broker it targets. Read-only on the dashboard. The sync
// worker reads columns from information_schema, so these replicate with the rest
// of the devices row once this runs on both databases.
export const up = (pgm) => {
  pgm.sql(`
    ALTER TABLE devices
      ADD COLUMN IF NOT EXISTS active_hw_mode   text,
      ADD COLUMN IF NOT EXISTS wifi_ssid        text,
      ADD COLUMN IF NOT EXISTS cell_apn         text,
      ADD COLUMN IF NOT EXISTS target_broker    text,
      ADD COLUMN IF NOT EXISTS last_config_sync timestamptz;`);
};
export const down = (pgm) => {
  pgm.sql(`
    ALTER TABLE devices
      DROP COLUMN IF EXISTS active_hw_mode,
      DROP COLUMN IF EXISTS wifi_ssid,
      DROP COLUMN IF EXISTS cell_apn,
      DROP COLUMN IF EXISTS target_broker,
      DROP COLUMN IF EXISTS last_config_sync;`);
};
