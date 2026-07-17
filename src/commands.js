// commands.js ───────────────────────────────────────────────────────────────
// Builds DOWNWARD command envelopes exactly as the frozen "cmd" schema defines
// them, and exactly as KorinneMargaret/SENSEable-HW's MQTTpublisher.py emits
// them. Pure + side-effect-free so it can be unit-tested and reused by both the
// HTTP route and the MQTT publisher.
//
// Common envelope (mandatory on every command):
//   { t:"cmd", v:1, tid, nid, cid, ts, action }
//
// Branches (discriminated by `action`):
//   bus_recovery        { bus_id:0|1 }
//   actuate             { port:1..6, mode:"bin"|"pwm", dur:>=0,
//                         state:0|1        (mode=bin),
//                         duty:0..255      (mode=pwm) }
//   sensor_port_up|down { chip:0..3, ch:0..3 }
//
// IMPORTANT — tid is the BROKER tenant string ("tenant-123"), NOT our slug
// ("aquatech"). It must be resolved server-side from tenants.mqtt_tid; the
// browser is never trusted to stamp it. Callers pass the already-resolved tid.

import { randomUUID } from 'node:crypto';

export const SCHEMA_VERSION = 1;
export const TOPIC_BASE = process.env.MQTT_TOPIC_BASE ?? 'usc/thesis';

// Chip index (0..3) ↔ ADS1115 I2C address, per the schema.
export const CHIP_ADDRS = ['0x48', '0x49', '0x4a', '0x4b'];

export function newCid(prefix = 'cmd') {
  return `${prefix}-${randomUUID()}`;
}

// Downward command topic: usc/thesis/{tid}/{nid}/cmd
export function cmdTopic(tid, nid, base = TOPIC_BASE) {
  return `${base}/${tid}/${nid}/cmd`;
}

// Accept "OUT1".."OUT6", "out3", or a bare number; return the integer 1..6.
export function parsePortNumber(port) {
  if (typeof port === 'number') return port;
  const m = String(port).match(/(\d+)/);
  return m ? Number(m[1]) : NaN;
}

function reqInt(v, name) {
  const n = Number(v);
  if (!Number.isInteger(n)) throw new Error(`${name} must be an integer`);
  return n;
}

function base(action, { tid, nid, cid, ts }) {
  if (!tid) throw new Error('tid is required (resolve from tenants.mqtt_tid)');
  if (!nid) throw new Error('nid is required');
  return {
    t: 'cmd',
    v: SCHEMA_VERSION,
    tid,
    nid,
    cid: cid ?? newCid(action),
    ts: ts ?? Math.floor(Date.now() / 1000),
    action,
  };
}

// Branch A — bit-bang I2C recovery.
export function buildBusRecovery({ tid, nid, busId = 0, cid, ts }) {
  const b = reqInt(busId, 'bus_id');
  if (b !== 0 && b !== 1) throw new Error('bus_id must be 0 or 1');
  return { ...base('bus_recovery', { tid, nid, cid, ts }), bus_id: b };
}

// Branch B — multi-channel actuator driver.
export function buildActuate({ tid, nid, port, mode, state, duty, dur = 0, cid, ts }) {
  const p = parsePortNumber(port);
  if (!Number.isInteger(p) || p < 1 || p > 6) {
    throw new Error('port must map to OUT1..OUT6 (integer 1..6)');
  }
  const m = String(mode).toLowerCase();
  if (m !== 'bin' && m !== 'pwm') throw new Error(`mode must be "bin" or "pwm" (got "${mode}")`);

  const d = reqInt(dur, 'dur');
  if (d < 0) throw new Error('dur must be >= 0 (0 = hold indefinitely)');

  const cmd = { ...base('actuate', { tid, nid, cid, ts }), port: p, mode: m, dur: d };

  if (m === 'bin') {
    const s = reqInt(state, 'state');
    if (s !== 0 && s !== 1) throw new Error('state must be 0 or 1 for mode "bin"');
    cmd.state = s;
  } else {
    const dy = reqInt(duty, 'duty');
    if (dy < 0 || dy > 255) throw new Error('duty must be 0..255 (8-bit) for mode "pwm"');
    cmd.duty = dy;
  }
  return cmd;
}

// Branch C — sensor port mask toggle (up = enable, down = disable).
export function buildSensorPortToggle({ tid, nid, direction, chip, ch, cid, ts }) {
  const dir = String(direction).toLowerCase();
  if (dir !== 'up' && dir !== 'down') throw new Error('direction must be "up" or "down"');
  const c = reqInt(chip, 'chip');
  const k = reqInt(ch, 'ch');
  if (c < 0 || c > 3) throw new Error('chip must be 0..3 (0x48..0x4B)');
  if (k < 0 || k > 3) throw new Error('ch must be 0..3 (AIN0..AIN3)');
  return { ...base(`sensor_port_${dir}`, { tid, nid, cid, ts }), chip: c, ch: k };
}

// Single dispatch entry the HTTP route uses. `ctx` carries {tid, nid}; `params`
// is the branch-specific body. Throws on any invalid field (route → HTTP 400).
export function buildCommand(action, ctx, params = {}) {
  const a = String(action);
  switch (a) {
    case 'bus_recovery':
      return buildBusRecovery({ ...ctx, ...params });
    case 'actuate':
      return buildActuate({ ...ctx, ...params });
    case 'sensor_port_up':
      return buildSensorPortToggle({ ...ctx, direction: 'up', ...params });
    case 'sensor_port_down':
      return buildSensorPortToggle({ ...ctx, direction: 'down', ...params });
    default:
      throw new Error(
        `unknown action "${a}" — expected bus_recovery | actuate | sensor_port_up | sensor_port_down`
      );
  }
}
