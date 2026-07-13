// status.js ────────────────────────────────────────────────────────────────
// Single source of truth for the hardware status dictionary and for turning
// raw status codes + safe-range checks + staleness into the UI's four-state
// vocabulary (Normal | Warning | Fault | Offline).
//
// IMPORTANT — spec vs. firmware discrepancy:
//   The FROZEN protocol spec ("IoT Communication Protocol & Payload
//   Specification") defines these integer codes. Your current firmware
//   (blink_example_main.c :: evaluate_port_status) emits a DIFFERENT, smaller
//   scheme. Both are mapped below so ingest works no matter which the node
//   sends; flip STATUS_PROFILE to match whatever the hardware actually emits.

export const STATUS_PROFILE =
  process.env.STATUS_PROFILE === 'firmware' ? 'firmware' : 'spec';

// --- Frozen spec dictionary (node-wide st.f AND port health codes) ----------
export const SPEC_CODES = {
  0: { name: 'SYS_NORMAL',          severity: 'normal'  },
  1: { name: 'CELLULAR_ACTIVE',     severity: 'normal'  },
  2: { name: 'I2C_BUS_TIMEOUT',     severity: 'warning' },
  3: { name: 'DRV_THERMAL_SHUTDOWN',severity: 'fault'   },
  4: { name: 'ADC_CONV_TIMEOUT',    severity: 'fault'   },
  5: { name: 'PORT_OPEN_CIRCUIT',   severity: 'offline' }, // sensor unplugged
  6: { name: 'RAIL_VOLTAGE_FLUC',   severity: 'fault'   },
  7: { name: 'RSSI_CRITICAL',       severity: 'warning' },
  8: { name: 'HEAP_EXHAUST_WARN',   severity: 'warning' },
  9: { name: 'EEPROM_WRITE_FAIL',   severity: 'warning' },
};

// --- Firmware's evaluate_port_status() scheme (per-port only) ----------------
export const FIRMWARE_CODES = {
  0: { name: 'PORT_NORMAL',       severity: 'normal'  },
  1: { name: 'PORT_OPEN_CIRCUIT', severity: 'offline' },
  2: { name: 'PORT_SATURATION',   severity: 'fault'   },
  3: { name: 'PORT_I2C_TIMEOUT',  severity: 'fault'   },
};

export function describeCode(code) {
  const table = STATUS_PROFILE === 'firmware' ? FIRMWARE_CODES : SPEC_CODES;
  return table[code] ?? { name: `UNKNOWN_${code}`, severity: 'warning' };
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
// Evaluation order mirrors the spec: gray(offline) → red(fault) → amber(warn) → green.
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

// Node-level status = worst active port condition, folded in with node-wide
// fault flag (st.f) and staleness. Returns the frontend's device.status vocab
// (online | warning | fault | offline).
const RANK = { Offline: 3, Fault: 2, Warning: 1, Normal: 0 };
const UI_TO_DEVICE = { Offline: 'offline', Fault: 'fault', Warning: 'warning', Normal: 'online' };

export function deriveNodeStatus({ portStatuses, systemFault, lastSeen, now = Date.now() }) {
  if (lastSeen == null || now - lastSeen > STALE_MS) return 'offline';

  let worst = 'Normal';
  for (const s of portStatuses) {
    if (RANK[s] > RANK[worst]) worst = s;
  }

  // Fold in node-wide st.f code (0/1 are healthy).
  if (systemFault != null && systemFault !== 0 && systemFault !== 1) {
    const { severity } = describeCode(systemFault);
    const asUi = SEVERITY_TO_UI[severity] ?? 'Warning';
    if (RANK[asUi] > RANK[worst]) worst = asUi;
  }

  return UI_TO_DEVICE[worst];
}
