// 1730000000023_cmd_set_interval.js
// Firmware 2026-10-29 added `set_interval` (telemetry interval, 5 s – 1 h).
// commands.action was CHECKed to the original four actions (migration 003), so
// queuing one failed at INSERT. `ping` is deliberately not added: it is sent by
// the standalone latency listener straight to the broker, never through here.
const ACTIONS = ['bus_recovery', 'actuate', 'sensor_port_up', 'sensor_port_down'];
const check = (list) => `
ALTER TABLE commands DROP CONSTRAINT IF EXISTS commands_action_check;
ALTER TABLE commands ADD CONSTRAINT commands_action_check
  CHECK (action IN (${list.map((a) => `'${a}'`).join(',')}));`;

export const up = (pgm) => { pgm.sql(check([...ACTIONS, 'set_interval'])); };
export const down = (pgm) => {
  pgm.sql(`DELETE FROM commands WHERE action = 'set_interval';${check(ACTIONS)}`);
};
