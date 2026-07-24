// status.js ────────────────────────────────────────────────────────────────
// Single source of truth for the hardware status vocabulary and for turning
// raw port status codes + safe-range checks + staleness into the UI's four
// states (Normal | Warning | Fault | Offline).
//
// CANONICAL SOURCE: "Environment-Adaptable IoT Monitoring Framework: Payload
// Schemas" (Sasil / Hatulan). That document is now the FROZEN protocol, so its
// telemetry status codes are the default here:
//
//   0  NORMAL            healthy analog reading
//   1  OPEN              floating pin / empty terminal (4500..5000 LSB window)
//   2  FAULT_OOR         voltage saturated the ADC registers (±32760)
//   3  HARDWARE_OFFLINE  I2C line fault / comm drop (node reports -9999)
//
// The old wide "spec dictionary" (SYS_NORMAL / CELLULAR_ACTIVE / ...) mis-read
// code 1 as CELLULAR_ACTIVE, which is wrong under the frozen schema. It's kept
// below ONLY as a legacy profile you can opt into with STATUS_PROFILE=legacy;
// the default is the schema table.

export const STATUS_PROFILE =
  process.env.STATUS_PROFILE === 'legacy' ? 'legacy' : 'schema';

// --- Frozen telemetry status codes (adc[].p[] third element) ----------------
export const SCHEMA_CODES = {
  0: { name: 'NORMAL',           severity: 'normal'  },
  1: { name: 'OPEN',             severity: 'offline' }, // unplugged / floating
  2: { name: 'FAULT_OOR',        severity: 'fault'   }, // ADC saturation
  3: { name: 'HARDWARE_OFFLINE', severity: 'offline' }, // I2C fault (-9999)
};

// Back-compat alias — earlier code imported FIRMWARE_CODES.
export const FIRMWARE_CODES = SCHEMA_CODES;

// --- Legacy wide dictionary (opt-in via STATUS_PROFILE=legacy) ---------------
export const LEGACY_CODES = {
  0: { name: 'SYS_NORMAL',           severity: 'normal'  },
  1: { name: 'CELLULAR_ACTIVE',      severity: 'normal'  },
  2: { name: 'I2C_BUS_TIMEOUT',      severity: 'warning' },
  3: { name: 'DRV_THERMAL_SHUTDOWN', severity: 'fault'   },
  4: { name: 'ADC_CONV_TIMEOUT',     severity: 'fault'   },
  5: { name: 'PORT_OPEN_CIRCUIT',    severity: 'offline' },
  6: { name: 'RAIL_VOLTAGE_FLUC',    severity: 'fault'   },
  7: { name: 'RSSI_CRITICAL',        severity: 'warning' },
  8: { name: 'HEAP_EXHAUST_WARN',    severity: 'warning' },
  9: { name: 'EEPROM_WRITE_FAIL',    severity: 'warning' },
};

export function describeCode(code) {
  const table = STATUS_PROFILE === 'legacy' ? LEGACY_CODES : SCHEMA_CODES;
  return table[code] ?? { name: `UNKNOWN_${code}`, severity: 'warning' };
}

// --- Discovery connection states (disco buses[].ports.pN) -------------------
// CONNECTED     a sensor is physically attached and producing values
// DISCONNECTED  nothing attached; port floats inside the leak window
// DISABLED      channel was manually masked down (sensor_port_down)
//
// `attached` drives ports.active_flag; `masked` records a deliberate override
// so the UI can distinguish "nothing plugged in" from "operator turned it off".
export function describeConnState(state) {
  switch (String(state).toUpperCase()) {
    case 'CONNECTED':    return { attached: true,  masked: false, name: 'CONNECTED' };
    case 'DISABLED':     return { attached: false, masked: true,  name: 'DISABLED' };
    case 'DISCONNECTED': return { attached: false, masked: false, name: 'DISCONNECTED' };
    default:             return { attached: false, masked: false, name: 'UNKNOWN' };
  }
}

// severity → UI status string used across the frontend
const SEVERITY_TO_UI = {
  normal:  'Normal',
  warning: 'Warning',
  fault:   'Fault',
  offline: 'Offline',
};

// How long since last telemetry before a node/port is considered stale/offline.
export const STALE_MS = Number(process.env.STALE_MS ?? 30_000);

// Resolve a single port's UI status from: its last health code, whether its
// calibrated value sits inside the safe band, and how fresh the reading is.
// Order mirrors the spec: gray(offline) → red(fault) → amber(warn) → green.
export function derivePortStatus({ code, value, safeMin, safeMax, lastSeen, now = Date.now() }) {
  if (lastSeen == null || now - lastSeen > STALE_MS) return 'Offline';

  const { severity } = describeCode(code ?? 0);
  if (severity === 'offline') return 'Offline';
  if (severity === 'fault')   return 'Fault';

  const outsideSafe =
    Number.isFinite(value) &&
    Number.isFinite(safeMin) &&
    Number.isFinite(safeMax) &&
    (value < safeMin || value > safeMax);

  if (severity === 'warning' || outsideSafe) return 'Warning';
  return 'Normal';
}

const RANK = { Offline: 3, Fault: 2, Warning: 1, Normal: 0 };
const UI_TO_DEVICE = { Offline: 'offline', Fault: 'fault', Warning: 'warning', Normal: 'online' };

// Board-level status. The expansion board is a real, separately-failing piece of
// hardware — its I2C bus can drop while the node itself stays perfectly healthy —
// so it deserves its own indicator rather than inheriting the node's.
//
// Disabled channels are excluded: switching off an unwired input must not make
// the board it sits on look degraded.
export function deriveModuleStatus({ portStatuses, lastSeen, now = Date.now() }) {
  if (lastSeen == null || now - lastSeen > STALE_MS) return 'offline';
  const considered = portStatuses.filter((s) => s !== 'Disabled');
  if (!considered.length) return 'offline';   // board present but nothing monitored

  let worst = 'Normal';
  for (const s of considered) {
    // A single dead sensor does not make the BOARD offline — the board is
    // plainly alive, it just has a channel that stopped responding. Roll a
    // stale port up as a warning so the distinction survives.
    const asNode = s === 'Offline' ? 'Warning' : s;
    if (RANK[asNode] > RANK[worst]) worst = asNode;
  }
  return UI_TO_DEVICE[worst] ?? 'offline';
}

// Node-level status = worst active port condition, folded in with a node-wide
// fault flag (st.f, if the node ever sends one) and staleness. Returns the
// frontend's device.status vocab (online | warning | fault | offline).

export function deriveNodeStatus({ portStatuses, systemFault, lastSeen, now = Date.now() }) {
  if (lastSeen == null || now - lastSeen > STALE_MS) return 'offline';

  // "Offline" at the NODE level means the node itself stopped reporting, which
  // the staleness check above already decided. Beyond that point the node is
  // demonstrably alive, so a stale port or a silent board is a WARNING about
  // part of the system — not a claim that the whole node is down. Without this,
  // one unplugged sensor turns a perfectly healthy node's card grey.
  let worst = 'Normal';
  for (const s of portStatuses) {
    const asNode = s === 'Offline' ? 'Warning' : s;
    if (RANK[asNode] > RANK[worst]) worst = asNode;
  }

  // Fold in an optional node-wide fault code (0 is healthy).
  if (systemFault != null && systemFault !== 0) {
    const { severity } = describeCode(systemFault);
    const asUi = SEVERITY_TO_UI[severity] ?? 'Warning';
    if (RANK[asUi] > RANK[worst]) worst = asUi;
  }

  return UI_TO_DEVICE[worst];
}
