// mqtt.js ───────────────────────────────────────────────────────────────────
// Live broker bridge for the FROZEN topic namespace:
//
//   usc/thesis/{tid}/{nid}/tlm     ← telemetry   (subscribe)
//   usc/thesis/{tid}/{nid}/disco   ← discovery   (subscribe)
//   usc/thesis/{tid}/{nid}/ack     ← command ack (subscribe)
//   usc/thesis/{tid}/{nid}/cmd     → commands    (publish)
//
// OPTIONAL: only connects when MQTT_URL is set. With no broker configured the
// server still runs fully on the HTTP ingest/command endpoints, so `npm start`
// never blocks. Routing is by the packet's `t` field (tlm|disco|ack), not by
// the topic suffix, so a broker that fans everything to `.../#` still works.
//
// ── WHY THIS FILE GREW DIAGNOSTICS ──────────────────────────────────────────
// The previous version could print
//
//     [mqtt] connected to mqtts://…
//     [mqtt] subscribed: usc/thesis/+/+/tlm, …
//
// and then receive nothing at all, with the console staying completely silent.
// A dashboard frozen at "last seen 22h ago" was the only symptom, and it is
// indistinguishable from a dead sensor. Two specific holes caused that:
//
//  1. SUBSCRIBE FAILURE WAS INVISIBLE. In MQTT.js the subscribe callback's
//     `err` argument only fires for a network-level failure. When a broker
//     REFUSES a subscription — which Mosquitto does per-topic via its ACL — it
//     returns success at the protocol level with a per-topic failure code of
//     128 in the SUBACK's `granted` array. The old code ignored `granted`
//     entirely, so an ACL that permits `tlm` but not `disco`/`ack` produced a
//     cheerful "subscribed" log for topics the broker had just denied. The
//     backend was, in effect, lying about being subscribed.
//
//  2. SUCCESS WAS ALSO INVISIBLE. Nothing logged on a received packet, so
//     "connected but receiving zero telemetry" looked identical to "connected
//     and working". There was no way to tell from the console which one you had.
//
// Both are fixed below: SUBACK codes are checked per topic and refusals are
// reported as errors, and packet flow is counted and surfaced through
// /api/health plus a periodic heartbeat.

import { ingestTelemetry, ingestDiscovery, ingestAck } from './ingest.js';
import { TOPIC_BASE } from './commands.js';

let client = null;

// Observability state. Exposed via getMqttStats() so /api/health can answer
// "is telemetry actually arriving?" without anyone reading the console.
const stats = {
  url: null,
  connected: false,
  subscribed: [],          // topics the broker actually GRANTED
  refused: [],             // topics the broker DENIED (almost always ACL)
  connects: 0,
  closes: 0,
  lastError: null,
  received: { tlm: 0, disco: 0, ack: 0, other: 0 },
  accepted: 0,
  rejected: 0,
  lastPacketAt: null,
  lastRejectReason: null,
};

export function getMqttClient() {
  return client;
}

export function getMqttStats() {
  return {
    ...stats,
    lastPacketAgeMs: stats.lastPacketAt ? Date.now() - stats.lastPacketAt : null,
  };
}

// Publish a pre-built command envelope to a topic. Returns true if handed to
// the broker, false if no broker is connected (caller falls back to HTTP-only).
export function publishCommand(topic, payloadObj, { qos = 1 } = {}) {
  if (!client || !client.connected) return false;
  client.publish(topic, JSON.stringify(payloadObj), { qos });
  return true;
}

// Ingest is async (it may provision new hardware on first sight), so this
// awaits rather than fire-and-forgetting — otherwise a burst of packets from an
// unknown node could each try to create it before the first insert lands.
async function route(topic, buf) {
  let pkt;
  try { pkt = JSON.parse(buf.toString()); }
  catch { console.warn('[mqtt] non-JSON payload on', topic); return; }

  // Prefer the packet's own type; fall back to the topic suffix.
  const kind = pkt.t ?? topic.split('/').pop();
  stats.lastPacketAt = Date.now();
  stats.received[kind === 'tlm' || kind === 'disco' || kind === 'ack' ? kind : 'other'] += 1;

  let result;
  try {
    if (kind === 'tlm')        result = await ingestTelemetry(pkt);
    else if (kind === 'disco') result = await ingestDiscovery(pkt);
    else if (kind === 'ack')   result = await ingestAck(pkt);
    else return; // ignore our own outbound cmd echoes and anything unknown
  } catch (e) {
    stats.rejected += 1;
    stats.lastRejectReason = e.message;
    console.error(`[mqtt] ${topic}: ingest threw —`, e.message);
    return;
  }

  if (result && !result.ok) {
    stats.rejected += 1;
    stats.lastRejectReason = result.error;
    console.warn(`[mqtt] ${topic}: ${result.error}`);
    return;
  }
  stats.accepted += 1;
  if (result?.provisioned) {
    console.log(`[mqtt] ${topic}: provisioned ${result.provisioned} new item(s)`);
  }
}

export function startMqtt() {
  const url = process.env.MQTT_URL;
  if (!url) {
    console.log('[mqtt] MQTT_URL not set — skipping broker (HTTP ingest still active)');
    return null;
  }
  stats.url = url;

  return import('mqtt')
    .then(async ({ default: mqtt }) => {
      const opts = {
        username: process.env.MQTT_USERNAME,
        password: process.env.MQTT_PASSWORD,
        reconnectPeriod: Number(process.env.MQTT_RECONNECT_MS ?? 5000),
        clientId: process.env.MQTT_CLIENT_ID ?? `senseable-backend-${Math.random().toString(16).slice(2, 8)}`,
        keepalive: Number(process.env.MQTT_KEEPALIVE ?? 20),
        connectTimeout: Number(process.env.MQTT_CONNECT_TIMEOUT_MS ?? 15000),
        clean: true,
        resubscribe: false,
      };

      // TLS: for mqtts://. Point MQTT_CA_CERT at the broker's CA (ISRG Root X1
      // for HiveMQ Cloud, or your Mosquitto ca.crt). MQTT_TLS_INSECURE=true
      // skips hostname/chain checks — needed on the bench because the server
      // cert's SAN is pinned to an IP that changes with DHCP.
      if (url.startsWith('mqtts://') || url.startsWith('tls://')) {
        if (process.env.MQTT_CA_CERT) {
          const { readFile } = await import('node:fs/promises');
          try { opts.ca = await readFile(process.env.MQTT_CA_CERT); }
          catch (e) { console.warn('[mqtt] could not read MQTT_CA_CERT:', e.message); }
        }
        opts.rejectUnauthorized = process.env.MQTT_TLS_INSECURE !== 'true';
      }

      client = mqtt.connect(url, opts);

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
        console.log(`[mqtt] connected to ${url} as ${opts.clientId ?? client.options.clientId}`);

        client.subscribe(topics, { qos: 1 }, (err, granted) => {
          if (err) {
            stats.lastError = err.message;
            console.error('[mqtt] subscribe failed:', err.message);
            return;
          }

          // THE CHECK THAT WAS MISSING. `granted` carries a per-topic QoS, and
          // 128 means the broker REFUSED that subscription — almost always an
          // aclfile that doesn't list the topic for this user. Without this,
          // a denied subscription logs as a success and the node goes silent
          // with no explanation anywhere.
          const ok = [];
          const denied = [];
          for (const g of granted ?? []) {
            (g.qos === 128 ? denied : ok).push(g.topic);
          }
          // A broker that returns no `granted` array at all is treated as having
          // granted what we asked for, rather than silently reporting nothing.
          stats.subscribed = (granted && granted.length) ? ok : topics;
          stats.refused = denied;

          if (denied.length) {
            console.error(
              `[mqtt] BROKER REFUSED ${denied.length} subscription(s): ${denied.join(', ')}`);
            console.error(
              '[mqtt] this is an ACL problem — add these to Mosquitto\'s aclfile for user ' +
              `'${process.env.MQTT_USERNAME ?? '(none)'}' and restart the broker:`);
            for (const t of denied) console.error(`[mqtt]     topic read ${t}`);
          }
          if (stats.subscribed.length) {
            console.log('[mqtt] subscribed:', stats.subscribed.join(', '));
          }
          if (!stats.subscribed.length) {
            console.error('[mqtt] NOTHING was subscribed — no telemetry can arrive.');
          }
        });
      });

      client.on('message', route);

      client.on('error', (e) => {
        stats.lastError = e.message;
        console.error('[mqtt] error:', e.message);
      });

      client.on('close', () => {
        if (stats.connected) stats.closes += 1;
        stats.connected = false;
        console.warn('[mqtt] connection closed');
      });

      client.on('reconnect', () => console.log('[mqtt] reconnecting…'));
      client.on('offline',   () => console.warn('[mqtt] offline'));

      // Heartbeat. "Connected but receiving nothing" and "connected and working"
      // used to look identical in the console; this makes them distinguishable
      // at a glance, and names the likely cause when the link is silent.
      const every = Number(process.env.MQTT_HEARTBEAT_MS ?? 60_000);
      if (every > 0) {
        setInterval(() => {
          if (!stats.connected) return;
          const age = stats.lastPacketAt ? Math.round((Date.now() - stats.lastPacketAt) / 1000) : null;
          if (stats.lastPacketAt == null) {
            console.warn(
              '[mqtt] connected but NO packets received yet — check the node is publishing ' +
              'and that the broker ACL permits these topics');
          } else if (age > 120) {
            console.warn(`[mqtt] connected but last packet was ${age}s ago — node may be down`);
          } else {
            console.log(
              `[mqtt] ok — tlm=${stats.received.tlm} disco=${stats.received.disco} ` +
              `ack=${stats.received.ack} accepted=${stats.accepted} rejected=${stats.rejected} ` +
              `last=${age}s ago`);
          }
        }, every).unref();
      }

      return client;
    })
    .catch((e) => {
      stats.lastError = e.message;
      console.error('[mqtt] failed to start:', e.message);
      return null;
    });
}
