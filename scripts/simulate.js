// simulate.js ───────────────────────────────────────────────────────────────
// Fakes the ESP32: posts frozen-schema telemetry (raw ADC counts) to the
// running backend's HTTP ingest endpoint on an interval, so the dashboard
// shows moving values, history, and occasional fault states — no broker or
// hardware needed. Run the server first (`npm start`), then `npm run simulate`.

const BASE = process.env.BACKEND_URL ?? 'http://localhost:4000';
const PERIOD_MS = Number(process.env.SIM_PERIOD_MS ?? 3000);

// Raw-count bands chosen so the seeded linear calibrations land inside each
// port's safe range. Occasionally we push a channel out of band / inject a
// health code to exercise the Warning/Fault/Offline paths.
const CHANNELS = [
  { ch: 0, lo: 10000, hi: 15500 }, // Dissolved Oxygen
  { ch: 1, lo: 14000, hi: 19000 }, // Salinity
  { ch: 2, lo: 12000, hi: 18000 }, // Temperature
];

const randInt = (lo, hi) => Math.floor(lo + Math.random() * (hi - lo));
let seq = 1000;

function buildPacket() {
  const ports = CHANNELS.map(({ ch, lo, hi }) => {
    // ~8% of the time, misbehave: open circuit (code 5), or drift high (warn/fault).
    const roll = Math.random();
    if (roll < 0.04) return [ch, -1, 5];            // PORT_OPEN_CIRCUIT
    if (roll < 0.08) return [ch, randInt(hi, hi + 4000), 0]; // out of safe band
    return [ch, randInt(lo, hi), 0];
  });

  return {
    t: 'tlm', v: 1,
    tid: 't1', sid: 's1', nid: 'N001',
    ts: Math.floor(Date.now() / 1000),
    q: seq++,
    st: {
      up: 86400 + seq,
      rs: randInt(-70, -50),
      hp: randInt(120000, 150000),
      f: 0,
    },
    adc: [{ a: '0x48', p: ports }],
  };
}

async function tick() {
  const pkt = buildPacket();
  try {
    const res = await fetch(`${BASE}/api/ingest/telemetry`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(pkt),
    });
    const body = await res.json();
    console.log(`q=${pkt.q}`, body);
  } catch (e) {
    console.error('post failed — is the server running?', e.message);
  }
}

console.log(`[sim] posting telemetry to ${BASE} every ${PERIOD_MS}ms (Ctrl+C to stop)`);
tick();
setInterval(tick, PERIOD_MS);
