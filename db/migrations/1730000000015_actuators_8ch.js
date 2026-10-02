// 1730000000015_actuators_8ch.js
// Hardware grew from 6 to 8 PWM outputs (firmware NUM_ACTUATORS 8, LEDC 0-7 on
// GPIOs 4,25,13,19,26,27,33,18). Backend addresses outputs by logical OUT1..OUT8;
// only commands.port still capped at 6, rejecting OUT7/OUT8 at insert. Widen to 8.
export const up = (pgm) => { pgm.sql(`
ALTER TABLE commands DROP CONSTRAINT IF EXISTS commands_port_check;
ALTER TABLE commands ADD CONSTRAINT commands_port_check CHECK (port IS NULL OR port BETWEEN 1 AND 8);`); };
export const down = (pgm) => { pgm.sql(`
ALTER TABLE commands DROP CONSTRAINT IF EXISTS commands_port_check;
ALTER TABLE commands ADD CONSTRAINT commands_port_check CHECK (port IS NULL OR port BETWEEN 1 AND 6);`); };
