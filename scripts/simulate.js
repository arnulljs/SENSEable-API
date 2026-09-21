// simulate.js ───────────────────────────────────────────────────────────────
// Fakes the ESP32 against the FROZEN Payload-Schemas spec — posts telemetry
// (raw ADC counts), periodic discovery, and optional command acks to the
// running backend's broker-free HTTP ingest endpoints, so the dashboard shows
// moving values, history, discovery state, and fault paths with no broker or
// hardware. Run the server first (`npm start`), then `npm run simulate`.
//
// TRANSPORTS
//   default    HTTP POST to /api/ingest/*. Exercises the pipeline, not the
//              broker. Rows land as origin='cloud'.
//   --mqtt     Publish to a real broker instead, which is what you want when
//              testing the cloud-first path end to end: the packet crosses
//              HiveMQ, the edge's cloud subscriber tags it origin='cloud', and
//              (with EDGE_BRIDGES_CLOUD=true) the sync worker carries it to
//              Supabase and the Vercel dashboard.
//   --local    Publish to MQTT_LOCAL_URL instead — the failover path, so rows
//              land origin='local', synced=false.
//
// OPTIONS
//   --seconds N   stop after N seconds instead of running until Ctrl+C
//   --replay      after the run, re-send the last few samples flagged "r":1,
//                 the way the firmware replays its LittleFS spool. The
//                 dashboard must NOT move when these arrive.

import 'dotenv/config';

const args = new Set(process.argv.slice(2));
const argVal = (name, fallback) => {
  const i = process.argv.indexOf(name);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
};

const USE_MQTT = args.has('--mqtt') || args.has('--local');
const USE_LOCAL = args.has('--local');
const DURATION_S = Number(argVal('--seconds', 0));
const DO_REPLAY = args.has('--replay');

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

// ── MQTT transport ──────────────────────────────────────────────────────────
// Same payloads, same frozen topic namespace, published to a real broker so the
// whole path is exercised rather than just the ingest function.
let client = null;
const TOPIC = (kind) => `${process.env.MQTT_TOPIC_BASE ?? 'usc/thesis'}/${TID}/${NID}/${kind}`;

async function connectBroker() {
  const { default: mqtt } = await import('mqtt');
  const name = USE_LOCAL ? 'local' : 'cloud';
  const url = USE_LOCAL ? process.env.MQTT_LOCAL_URL : process.env.MQTT_URL;
  if (!url) { console.error(`no ${name} broker URL configured`); process.exit(1); }

  const pick = (k) => process.env[`MQTT_${name.toUpperCase()}_${k}`] ?? process.env[`MQTT_${k}`];
  const opts = {
    username: pick('USERNAME'),
    password: pick('PASSWORD'),
    clientId: `senseable-sim-${Math.random().toString(16).slice(2, 8)}`,
  };
  if (url.startsWith('mqtts://')) {
    const ca = pick('CA_CERT');
    if (ca) { const { readFile } = await import('node:fs/promises'); opts.ca = await readFile(ca); }
    opts.rejectUnauthorized = (pick('TLS_INSECURE') ?? process.env.MQTT_TLS_INSECURE) !== 'true';
  }

  return new Promise((resolve, reject) => {
    const c = mqtt.connect(url, opts);
    c.on('connect', () => { console.log(`[sim] connected to ${name} broker ${url}`); resolve(c); });
    c.on('error', (e) => reject(new Error(`${name} broker: ${e.message}`)));
  });
}

const sent = [];   // kept so --replay can re-send them flagged

async function send(kind, body, label) {
  if (USE_MQTT) {
    client.publish(TOPIC(kind === 'telemetry' ? 'tlm' : 'disco'), JSON.stringify(body), { qos: 1 });
    console.log(label, JSON.stringify(body).slice(0, 90));
  } else {
    await post(`/ingest/${kind}`, body, label);
  }
  if (kind === 'telemetry') { sent.push(body); if (sent.length > 20) sent.shift(); }
}

async function tick() {
  await send('telemetry', buildTelemetry(), `tlm q=${seq++}`);
  if (seq % 10 === 0) await send('discovery', buildDiscovery(), 'disco');
}

// A replayed sample is a real reading arriving late. The backend persists it and
// deliberately leaves live state alone, so the gauges must not twitch here.
async function replayBurst() {
  console.log(`\n[sim] replaying ${sent.length} buffered sample(s) with "r":1 — ` +
              'the dashboard should NOT move');
  for (const body of sent) {
    await send('telemetry', { ...body, r: 1 }, 'replay');
    await new Promise((r) => setTimeout(r, 150));
  }
}

async function main() {
  if (USE_MQTT) client = await connectBroker();

  const where = USE_MQTT ? `${USE_LOCAL ? 'local' : 'cloud'} broker` : BASE;
  console.log(`[sim] tlm/disco -> ${where} (tid=${TID} nid=${NID}) every ${PERIOD_MS}ms` +
              (DURATION_S ? ` for ${DURATION_S}s` : ' (Ctrl+C to stop)'));

  await send('discovery', buildDiscovery(), 'disco(initial)');
  await tick();
  const timer = setInterval(tick, PERIOD_MS);

  if (!DURATION_S) return;
  setTimeout(async () => {
    clearInterval(timer);
    if (DO_REPLAY) await replayBurst();
    console.log(`\n[sim] done — ${seq - 1000} telemetry packet(s) sent`);
    client?.end(true);
    process.exit(0);
  }, DURATION_S * 1000);
}

main().catch((e) => { console.error('[sim]', e.message); process.exit(1); });
