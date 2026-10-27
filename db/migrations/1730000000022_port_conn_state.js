// 1730000000022_port_conn_state.js
// The node's own report of each ADC input, from its discovery packet:
// CONNECTED (readable), DISCONNECTED (not readable) or DISABLED (switched off on
// the node). active_flag could only say CONNECTED or not, so the cloud
// dashboard showed a port the node had switched off as merely disconnected.
// Stored as reported; ports.enabled stays the operator's intent. The sync
// worker reads columns from information_schema, so this replicates once it has
// run on both databases. NULL until the node's next discovery packet; readers
// fall back to active_flag meanwhile.
export const up = (pgm) => {
  pgm.sql('ALTER TABLE ports ADD COLUMN IF NOT EXISTS conn_state text;');
};
export const down = (pgm) => {
  pgm.sql('ALTER TABLE ports DROP COLUMN IF EXISTS conn_state;');
};
