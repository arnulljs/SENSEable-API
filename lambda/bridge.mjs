// lambda/bridge.mjs ─────────────────────────────────────────────────────────
// The cloud bridge as an AWS Lambda: HiveMQ -> Supabase, no server to run.
//
// A Lambda cannot stay subscribed to a broker, so this uses an MQTT feature
// instead: a PERSISTENT SESSION. The function connects with a fixed client id
// and clean=false, which tells HiveMQ to keep this subscriber's session — and to
// QUEUE every QoS 1 message published to its topics while it is disconnected.
// The firmware publishes telemetry and discovery at QoS 1, so nothing published
// between runs is lost; it waits on the broker.
//
// Each invocation (EventBridge Scheduler, once a minute):
//   1. load the routing/device state it needs from Supabase (lite hydrate);
//   2. connect, receive everything queued since the last run, stop once the
//      stream goes quiet;
//   3. run each message through the SAME ingest code the edge server uses
//      (tenant/pin resolution, provisioning, calibration, replay handling),
//      writing to Supabase with TIER=cloud;
//   4. wait for every write to land, then disconnect WITHOUT clearing the
//      session, so the broker starts queuing again for the next run.
//
// Readings keep their device timestamp (`ts`), so a sample processed up to a
// minute late is still filed at the moment it was taken.
//
// Data-only (BRIDGE_SIDE_EFFECTS=false): notifications and command dispatch
// stay on the edge server, which keeps CLOUD_BRIDGE_RUNNING unset.

import mqtt from 'mqtt';
import { hydrate } from '../src/store.js';
import { ingestTelemetry, ingestDiscovery, ingestAck } from '../src/ingest.js';
import { drainWrites } from '../db/pool.js';

const IDLE_MS   = Number(process.env.COLLECT_IDLE_MS ?? 3000);    // stop after this long with no message
const MAX_MS    = Number(process.env.COLLECT_MAX_MS ?? 35000);    // hard cap per run
const CLIENT_ID = process.env.MQTT_CLIENT_ID ?? 'senseable-lambda-bridge';
const TOPICS    = ['tlm', 'disco', 'ack'].map((k) => `usc/thesis/+/+/${k}`);

function collect() {
  return new Promise((resolve, reject) => {
    const msgs = [];
    let idle = null;
    let hard = null;
    let settled = false;

    const client = mqtt.connect(process.env.MQTT_URL, {
      clientId: CLIENT_ID,
      username: process.env.MQTT_CLOUD_USERNAME ?? process.env.MQTT_USERNAME,
      password: process.env.MQTT_CLOUD_PASSWORD ?? process.env.MQTT_PASSWORD,
      protocolVersion: 5,
      clean: false,                                   // keep the session between runs
      properties: { sessionExpiryInterval: Number(process.env.MQTT_SESSION_EXPIRY_S ?? 86400) },
      reconnectPeriod: 0,                             // one connection per run
      connectTimeout: 10_000,
      keepalive: 30,
    });

    const finish = (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(idle); clearTimeout(hard);
      if (err) { client.end(true); return reject(err); }
      resolve({ client, msgs });
    };
    const armIdle = () => { clearTimeout(idle); idle = setTimeout(() => finish(), IDLE_MS); };

    client.on('connect', (connack) => {
      // On a resumed session the broker already holds the subscriptions and
      // starts delivering the queue immediately; subscribing again is harmless.
      console.log(`[bridge] connected (session ${connack.sessionPresent ? 'resumed' : 'NEW — nothing was queued'})`);
      client.subscribe(TOPICS, { qos: 1 }, (err) => {
        if (err) return finish(err);
        armIdle();
        hard = setTimeout(() => finish(), MAX_MS);
      });
    });
    client.on('message', (topic, payload, packet) => {
      msgs.push({ topic, payload, retain: packet?.retain === true });
      armIdle();
    });
    client.on('error', (err) => finish(err));
  });
}

async function ingestOne({ topic, payload, retain }) {
  // A retained message is a replay of the last one ever published, not a new
  // reading — the same rule mqtt.js applies on the edge.
  if (retain) return 'retained';
  let pkt;
  try { pkt = JSON.parse(payload.toString()); } catch { return 'bad-json'; }
  const kind = pkt.t ?? topic.split('/').pop();
  let r;
  if (kind === 'tlm')        r = await ingestTelemetry(pkt, { origin: 'cloud' });
  else if (kind === 'disco') r = await ingestDiscovery(pkt);
  else if (kind === 'ack')   r = await ingestAck(pkt);
  else return 'ignored';
  if (!r?.ok) { console.warn(`[bridge] ${topic}: ${r?.error}`); return 'rejected'; }
  return r.replay ? 'replay' : 'ok';
}

export async function handler() {
  const t0 = Date.now();
  // Fresh every run: a warm container would otherwise act on the previous
  // minute's pins and devices.
  await hydrate({ lite: true });

  const { client, msgs } = await collect();
  const counts = {};
  for (const m of msgs) {
    let outcome;
    try { outcome = await ingestOne(m); }
    catch (e) { outcome = 'error'; console.error(`[bridge] ${m.topic}: ${e.message}`); }
    counts[outcome] = (counts[outcome] ?? 0) + 1;
    // One message's writes land before the next starts. A batch of queued
    // samples otherwise races its own UPDATEs to ports.last_value, and the
    // gauge could end on an older reading than the newest one received.
    await drainWrites();
  }

  // end(false): graceful DISCONNECT that keeps the session, so the broker keeps
  // queuing for the next run. QoS 1 acks for everything above were sent as the
  // messages arrived.
  await new Promise((r) => client.end(false, {}, r));

  const summary = { messages: msgs.length, ...counts, ms: Date.now() - t0 };
  console.log('[bridge]', JSON.stringify(summary));
  return summary;
}
