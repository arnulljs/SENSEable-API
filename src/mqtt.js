// mqtt.js ───────────────────────────────────────────────────────────────────
// Broker bridge for the FROZEN topic namespace:
//
//   usc/thesis/{tid}/{nid}/tlm     ← telemetry   (subscribe)
//   usc/thesis/{tid}/{nid}/disco   ← discovery   (subscribe)
//   usc/thesis/{tid}/{nid}/ack     ← command ack (subscribe)
//   usc/thesis/{tid}/{nid}/cmd     → commands    (publish)
//
// ── CLOUD-FIRST: TWO BROKERS, ONE INGEST PIPELINE ───────────────────────────
// The node publishes to the CLOUD broker during normal operation and falls back
// to the LOCAL broker when the cloud is unreachable. It never uses both at once.
// The edge server must be attached to both at once, because it cannot know which
// one the node is using right now:
//
//   CLOUD  (MQTT_URL)        Phase 1, passive mirror. Every payload the node
//                            sends to the cloud is fanned out to us too, and we
//                            write it as origin='cloud', synced=true — Supabase
//                            already has it, so nothing is owed upward.
//
//   LOCAL  (MQTT_LOCAL_URL)  Phase 2, active ingestion. Only carries traffic
//                            once the node has failed over. Rows land as
//                            origin='local', synced=false, and scripts/sync.js
//                            drains them upward when the link returns.
//
// That origin tag is the entire echo-prevention mechanism. A mirrored row marked
// unsynced would be pushed back to the cloud by the sync worker, the cloud
// broker would fan it out to us again, and the two would feed each other
// forever. This is the loop the implementation guide warns about, in the shape
// it actually takes in this codebase.
//
// Both brokers are OPTIONAL and independent. With neither set the server still
// runs on the HTTP ingest endpoints, so `npm start` never blocks on a broker.
// A bench with no cloud account sets only MQTT_LOCAL_URL and behaves exactly
// like the previous edge-primary build.
//
// ── WHY THE DIAGNOSTICS ARE HERE ────────────────────────────────────────────
// A broker can accept a connection and then refuse the subscription. MQTT.js
// only reports `err` for network-level failures; a per-topic ACL denial arrives
// as code 128 inside the SUBACK's `granted` array. Ignoring that meant logging a
// cheerful "subscribed" for topics the broker had just denied, with a dashboard
// frozen at "last seen 22h ago" as the only symptom. Nothing logged on receipt
// either, so "connected but silent" and "connected and working" looked
// identical. Both are checked and surfaced below, per broker.

import { ingestTelemetry, ingestDiscovery, ingestAck } from './ingest.js';
import { broadcastDevices } from './realtime.js';
import { checkAllPresence } from './presence.js';
import { TOPIC_BASE } from './commands.js';

// name -> { name, origin, url, client, stats }
const brokers = new Map();

// Which broker last carried an outbound command, for /api/health. Kept separate
// from publishCommand's return value, which stays a plain boolean because
// routes.js puts it straight into a JSON response body.
let lastPublishVia = null;

function blankStats(url, origin) {
  return {
    url, origin,
    connected: false,
    subscribed: [],          // topics the broker actually GRANTED
    refused: [],             // topics the broker DENIED (almost always ACL)
    connects: 0,
    closes: 0,
    lastError: null,
    received: { tlm: 0, disco: 0, ack: 0, other: 0 },
    // Retained replays the broker handed us on subscribe. Counted, never
    // ingested — see route().
    retained: { tlm: 0, disco: 0, ack: 0, other: 0 },
    accepted: 0,
    rejected: 0,
    lastPacketAt: null,
    lastRejectReason: null,
  };
}

/**
 * Per-broker health, plus a derived `route` naming the path the hardware is
 * actually using. That is OBSERVED from traffic, not assumed from config, and it
 * is what the dashboard's failover banner reads. Flat aliases at the end keep
 * the old single-broker response shape working for /api/health.
 */
export function getMqttStats() {
  const out = { brokers: {}, route: 'unknown', lastPublishVia };
  let newest = null;

  for (const [name, b] of brokers) {
    out.brokers[name] = {
      ...b.stats,
      lastPacketAgeMs: b.stats.lastPacketAt ? Date.now() - b.stats.lastPacketAt : null,
    };
    if (b.stats.lastPacketAt && (!newest || b.stats.lastPacketAt > newest.at)) {
      newest = { at: b.stats.lastPacketAt, origin: b.stats.origin };
    }
  }

  if (newest) {
    out.route = newest.origin === 'local' ? 'ROUTE_LOCAL_FAILOVER' : 'ROUTE_CLOUD_FIRST';
  }

  const primary = brokers.get('cloud') ?? brokers.get('local');
  if (primary) {
    out.url = primary.stats.url;
    out.connected = primary.stats.connected;
    out.lastPacketAt = primary.stats.lastPacketAt;
    out.lastPacketAgeMs = primary.stats.lastPacketAt
      ? Date.now() - primary.stats.lastPacketAt : null;
    out.subscribed = primary.stats.subscribed;
    out.refused = primary.stats.refused;
  }
  return out;
}

/**
 * Publish a command envelope. The command has to go out on whichever broker the
 * node is currently listening to, so this prefers the path that most recently
 * delivered a packet and falls back to any connected broker.
 *
 * Returns a BOOLEAN: true if handed to a broker, false if nothing could carry it
 * (the caller then falls back to the HTTP command path). The broker actually
 * used is recorded in getMqttStats().lastPublishVia.
 */
export function publishCommand(topic, payloadObj, { qos = 1 } = {}) {
  const live = [...brokers.values()]
    .filter((b) => b.client?.connected)
    .sort((a, b) => (b.stats.lastPacketAt ?? 0) - (a.stats.lastPacketAt ?? 0));
  if (!live.length) { lastPublishVia = null; return false; }

  const chosen = live[0];
  chosen.client.publish(topic, JSON.stringify(payloadObj), { qos });
  lastPublishVia = chosen.name;
  return true;
}

// Ingest is async (it may provision new hardware on first sight), so this awaits
// rather than fire-and-forgetting — otherwise a burst of packets from an unknown
// node could each try to create it before the first insert lands.
// Topics whose retained replay has already been logged once, so a restart prints
// one line per node rather than one per packet.
const retainedLogged = new Set();

async function route(broker, topic, buf, retained = false) {
  const { stats, origin, name } = broker;
  let pkt;
  try { pkt = JSON.parse(buf.toString()); }
  catch { console.warn(`[mqtt:${name}] non-JSON payload on`, topic); return; }

  // Prefer the packet's own type; fall back to the topic suffix.
  const kind = pkt.t ?? topic.split('/').pop();
  const bucket = kind === 'tlm' || kind === 'disco' || kind === 'ack' ? kind : 'other';

  // RETAINED MESSAGES ARE NOT PRESENCE.
  // The firmware publishes discovery with retain=1, so the broker keeps the last
  // one and replays it to every new subscriber. Ingesting that replay marked the
  // node as just seen on every backend restart: a board that died an hour ago
  // showed Connected for one staleness window, then flipped Offline — and a
  // device an operator had deleted was re-provisioned from the replay. A
  // retained packet is, by definition, not evidence the node is alive now; the
  // node republishes discovery live on every (re)connect anyway. It is also kept
  // out of lastPacketAt, which publishCommand() and the observed route use to
  // decide which broker the node is on.
  if (retained) {
    stats.retained[bucket] += 1;
    if (!retainedLogged.has(topic)) {
      retainedLogged.add(topic);
      console.log(`[mqtt:${name}] ignored retained ${kind} on ${topic} (replayed by broker, not live)`);
    }
    return;
  }

  stats.lastPacketAt = Date.now();
  stats.received[bucket] += 1;

  let result;
  try {
    // `origin` is the ONLY difference between the two paths. Everything below
    // this line is identical whichever broker delivered the packet, which is
    // what keeps failover from being a second, divergent ingest implementation.
    if (kind === 'tlm')        result = await ingestTelemetry(pkt, { origin });
    else if (kind === 'disco') result = await ingestDiscovery(pkt);
    else if (kind === 'ack')   result = await ingestAck(pkt);
    else return; // ignore our own outbound cmd echoes and anything unknown
  } catch (e) {
    stats.rejected += 1;
    stats.lastRejectReason = e.message;
    console.error(`[mqtt:${name}] ${topic}: ingest threw —`, e.message);
    return;
  }

  if (result && !result.ok) {
    stats.rejected += 1;
    stats.lastRejectReason = result.error;
    console.warn(`[mqtt:${name}] ${topic}: ${result.error}`);
    return;
  }
  stats.accepted += 1;

  // A FIFO replay burst is dozens of packets in a few seconds, none of which
  // changes anything a viewer is looking at. Skipping the presence sweep and the
  // broadcast keeps the dashboard from being hammered with identical updates
  // while history backfills.
  if (result?.replay) return;

  // Check presence on the ingest path too, not only on the sweep. Recovery is
  // the case that matters: a node coming back is known the instant its first
  // packet lands. Going offline still comes from the sweep, since absence has no
  // packet to trigger on.
  checkAllPresence();

  // The packet changed the read model, so tell every subscribed dashboard now.
  // Coalesced inside broadcastDevices(), and a no-op when the socket server is
  // not running.
  broadcastDevices();

  if (result?.provisioned) {
    console.log(`[mqtt:${name}] ${topic}: provisioned ${result.provisioned} new item(s)`);
  }
}

// Per-broker credentials fall back to the shared ones, so a single-broker setup
// needs no new variables at all: MQTT_CLOUD_USERNAME overrides MQTT_USERNAME
// only if you actually set it.
const envFor = (name, key) =>
  process.env[`MQTT_${name.toUpperCase()}_${key}`] ?? process.env[`MQTT_${key}`];

async function connectBroker({ name, url, origin }) {
  const { default: mqtt } = await import('mqtt');

  const opts = {
    username: envFor(name, 'USERNAME'),
    password: envFor(name, 'PASSWORD'),
    reconnectPeriod: Number(process.env.MQTT_RECONNECT_MS ?? 5000),
    // Distinct client ids per broker. A shared id would make the two
    // connections evict each other on any broker that enforces uniqueness.
    clientId: `${process.env.MQTT_CLIENT_ID ?? 'senseable-backend'}-${name}-` +
              Math.random().toString(16).slice(2, 8),
    keepalive: Number(process.env.MQTT_KEEPALIVE ?? 20),
    connectTimeout: Number(process.env.MQTT_CONNECT_TIMEOUT_MS ?? 15000),
    clean: true,
    resubscribe: false,
  };

  // TLS for mqtts://. Point the CA at the broker's root (ISRG Root X1 for HiveMQ
  // Cloud, or your Mosquitto ca.crt). MQTT_TLS_INSECURE=true skips hostname and
  // chain checks, which the bench needs because the local broker's certificate
  // SAN is pinned to a DHCP address that changes on every lease renewal.
  if (url.startsWith('mqtts://') || url.startsWith('tls://')) {
    const caPath = envFor(name, 'CA_CERT');
    if (caPath) {
      const { readFile } = await import('node:fs/promises');
      try { opts.ca = await readFile(caPath); }
      catch (e) { console.warn(`[mqtt:${name}] could not read CA cert:`, e.message); }
    }
    // Scoped per broker: a public cloud broker must verify properly even while
    // the local bench broker is allowed to skip it.
    const insecure = envFor(name, 'TLS_INSECURE') ?? process.env.MQTT_TLS_INSECURE;
    opts.rejectUnauthorized = insecure !== 'true';
  }

  const stats = blankStats(url, origin);
  const broker = { name, origin, url, client: null, stats };
  brokers.set(name, broker);

  const client = mqtt.connect(url, opts);
  broker.client = client;

  const base = TOPIC_BASE;                  // usc/thesis
  const tid  = process.env.MQTT_TID ?? '+'; // '+' = any tenant
  const nid  = process.env.MQTT_NID ?? '+'; // '+' = any node
  const topics = [
    `${base}/${tid}/${nid}/tlm`,
    `${base}/${tid}/${nid}/disco`,
    `${base}/${tid}/${nid}/ack`,
  ];

  client.on('connect', () => {
    stats.connected = true;
    stats.connects += 1;
    console.log(`[mqtt:${name}] connected to ${url}`);

    client.subscribe(topics, { qos: 1 }, (err, granted) => {
      if (err) {
        stats.lastError = err.message;
        console.error(`[mqtt:${name}] subscribe failed:`, err.message);
        return;
      }
      // THE CHECK THAT IS EASY TO MISS. `granted` carries a per-topic QoS, and
      // 128 means the broker REFUSED that subscription — almost always an
      // aclfile that does not list the topic for this user.
      const ok = [];
      const denied = [];
      for (const g of granted ?? []) (g.qos === 128 ? denied : ok).push(g.topic);
      // A broker returning no `granted` array is treated as having granted what
      // we asked for, rather than silently reporting nothing.
      stats.subscribed = (granted && granted.length) ? ok : topics;
      stats.refused = denied;

      if (denied.length) {
        console.error(`[mqtt:${name}] BROKER REFUSED ${denied.length} subscription(s): ${denied.join(', ')}`);
        console.error(`[mqtt:${name}] this is an ACL problem — permit these topics for user ` +
                      `'${opts.username ?? '(none)'}' and restart the broker:`);
        for (const t of denied) console.error(`[mqtt:${name}]     topic read ${t}`);
      }
      if (stats.subscribed.length) {
        console.log(`[mqtt:${name}] subscribed:`, stats.subscribed.join(', '));
      } else {
        console.error(`[mqtt:${name}] NOTHING was subscribed — no telemetry can arrive.`);
      }
    });
  });

  // MQTT.js passes the raw packet third; `retain` is set on messages the broker
  // is replaying from its retained store rather than forwarding live.
  client.on('message', (topic, buf, packet) => route(broker, topic, buf, packet?.retain === true));

  client.on('error', (e) => {
    stats.lastError = e.message;
    console.error(`[mqtt:${name}] error:`, e.message);
  });

  client.on('close', () => {
    if (stats.connected) stats.closes += 1;
    stats.connected = false;
    // A dropped CLOUD connection is worth seeing in the log: it is the same
    // condition that pushes the hardware into failover.
    console.warn(`[mqtt:${name}] connection closed`);
  });

  client.on('reconnect', () => console.log(`[mqtt:${name}] reconnecting…`));
  client.on('offline',   () => console.warn(`[mqtt:${name}] offline`));

  return broker;
}

export function startMqtt() {
  // MQTT_URL is the CLOUD broker under cloud-first. MQTT_LOCAL_URL is the
  // on-site Mosquitto the node falls back to.
  const wanted = [
    { name: 'cloud', url: process.env.MQTT_URL,       origin: 'cloud' },
    { name: 'local', url: process.env.MQTT_LOCAL_URL, origin: 'local' },
  ].filter((b) => b.url);

  if (!wanted.length) {
    console.log('[mqtt] no broker configured — skipping (HTTP ingest still active)');
    return Promise.resolve([]);
  }
  if (!process.env.MQTT_LOCAL_URL) {
    console.warn('[mqtt] MQTT_LOCAL_URL is not set — failover telemetry cannot be ingested. ' +
                 'Set it to the on-site Mosquitto broker to complete the cloud-first topology.');
  }

  return Promise.all(wanted.map((b) =>
    connectBroker(b).catch((e) => {
      console.error(`[mqtt:${b.name}] failed to start:`, e.message);
      return null;
    })))
    .then((list) => {
      startHeartbeat();
      return list.filter(Boolean);
    });
}

// Heartbeat. "Connected but receiving nothing" and "connected and working" used
// to look identical in the console; this separates them and names the likely
// cause when a link is silent. It also prints the observed route, which is the
// fastest way to watch a failover happen during a demo.
let heartbeat = null;
function startHeartbeat() {
  const every = Number(process.env.MQTT_HEARTBEAT_MS ?? 60_000);
  if (every <= 0 || heartbeat) return;

  heartbeat = setInterval(() => {
    for (const [name, b] of brokers) {
      const s = b.stats;
      if (!s.connected) continue;
      const age = s.lastPacketAt ? Math.round((Date.now() - s.lastPacketAt) / 1000) : null;
      if (s.lastPacketAt == null) {
        console.warn(`[mqtt:${name}] connected but NO packets received yet — check the node is ` +
                     'publishing and that the broker ACL permits these topics');
      } else if (age > 120) {
        console.warn(`[mqtt:${name}] connected but last packet was ${age}s ago — node may be down`);
      } else {
        console.log(`[mqtt:${name}] ok — tlm=${s.received.tlm} disco=${s.received.disco} ` +
                    `ack=${s.received.ack} accepted=${s.accepted} rejected=${s.rejected} ` +
                    `last=${age}s ago`);
      }
    }
    console.log(`[mqtt] observed route: ${getMqttStats().route}`);
  }, every);
  heartbeat.unref();
}

export function stopMqtt() {
  if (heartbeat) { clearInterval(heartbeat); heartbeat = null; }
  for (const b of brokers.values()) b.client?.end(true);
  brokers.clear();
}
