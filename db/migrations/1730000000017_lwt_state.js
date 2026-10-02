// 1730000000017_lwt_state.js
// Persist the broker Last-Will presence so the CLOUD tier judges online/offline
// the same way the edge does. lwtOnline lived only in edge memory, so the cloud
// used staleness alone — showing a node online up to a window after the edge
// dropped it on LWT. NULL=no LWT seen · true=connected · false=offline now.
export const up = (pgm) => { pgm.sql(`ALTER TABLE devices ADD COLUMN IF NOT EXISTS lwt_online boolean;`); };
export const down = (pgm) => { pgm.sql(`ALTER TABLE devices DROP COLUMN IF EXISTS lwt_online;`); };
