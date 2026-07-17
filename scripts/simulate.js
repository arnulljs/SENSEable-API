// simulate.js ───────────────────────────────────────────────────────────────
// Fakes the ESP32 against the FROZEN Payload-Schemas spec — posts telemetry
// (raw ADC counts), periodic discovery, and optional command acks to the
// running backend's broker-free HTTP ingest endpoints, so the dashboard shows
// moving values, history, discovery state, and fault paths with no broker or
// hardware. Run the server first (`npm start`), then `npm run simulate`.

const BASE = process.env.BACKEND_URL ?? 'http://localhost:4000';
const PERIOD_MS = Number(process.env.SIM_PERIOD_MS ?? 3000);
const TID = process.env.SIM_TID ?? 'tenant-123';   // matches tenants.mqtt_tid
const NID = process.env.SIM_NID ?? 'N001';
const ADDR = process.env.SIM_ADDR ?? '0x48';

// Raw-count bands chosen so the seeded linear calibrations land inside each
// port's safe range. Occasionally we push a channel out of band or inject a
// frozen-schema status code to exercise Warning/Fault/Offline.
const CHANNELS = [
  { ch: 0, lo: 10000, hi: 15500 }, // Dissolved Oxygen
  { ch: 1, lo: 14000, hi: 19000 }, // Salinity
  { ch: 2, lo: 12000, hi: 18000 }, // Temperature
];

const randInt = (lo, hi) => Math.floor(lo + Math.random() * (hi - lo));
let seq = 1000;

// Frozen status codes: 0 NORMAL, 1 OPEN (4500..5000), 2 FAULT_OOR (±32760),
// 3 HARDWARE_OFFLINE (-9999).
function buildTelemetry() {
  const ports = CHANNELS.map(({ ch, lo, hi }) => {
    const roll = Math.random();
    if (roll < 0.03) return [ch, randInt(4500, 5000), 1];      // OPEN
    if (roll < 0.05) return [ch, 32767, 2];                    // FAULT_OOR
    if (roll < 0.06) return [ch, -9999, 3];                    // HARDWARE_OFFLINE
    if (roll < 0.10) return [ch, randInt(hi, hi + 4000), 0];   // out of safe band
    return [ch, randInt(lo, hi), 0];                           // NORMAL
  });
  return {
    t: 'tlm', v: 1, tid: TID, nid: NID,
    ts: Date.now() / 1000,
    adc: [{ a: ADDR, p: ports }],
  };
}

// Discovery: a per-chip port connection map (CONNECTED/DISCONNECTED/DISABLED).
function buildDiscovery() {
  const states = ['CONNECTED', 'CONNECTED', 'CONNECTED', 'DISCONNECTED'];
  return {
    t: 'disco', v: 1, tid: TID, nid: NID,
    ts: Date.now() / 1000,
    detected_chips: 1,
    buses: [{ a: ADDR, ports: { p0: states[0], p1: states[1], p2: states[2], p3: states[3] } }],
  };
}

async function post(path, body, label) {
  try {
    const res = await fetch(`${BASE}/api${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    console.log(label, await res.json());
  } catch (e) {
    console.error(`post ${path} failed — is the server running?`, e.message);
  }
}

async function tick() {
  await post('/ingest/telemetry', buildTelemetry(), `tlm q=${seq++}`);
  if (seq % 10 === 0) await post('/ingest/discovery', buildDiscovery(), 'disco');
}

console.log(`[sim] posting tlm/disco to ${BASE} (tid=${TID} nid=${NID}) every ${PERIOD_MS}ms (Ctrl+C to stop)`);
post('/ingest/discovery', buildDiscovery(), 'disco(initial)');
tick();
setInterval(tick, PERIOD_MS);
