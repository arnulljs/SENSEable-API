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

import { ingestTelemetry, ingestDiscovery, ingestAck } from './ingest.js';
import { TOPIC_BASE } from './commands.js';

let client = null;

export function getMqttClient() {
  return client;
}

// Publish a pre-built command envelope to a topic. Returns true if handed to
// the broker, false if no broker is connected (caller falls back to HTTP-only).
export function publishCommand(topic, payloadObj, { qos = 1 } = {}) {
  if (!client || !client.connected) return false;
  client.publish(topic, JSON.stringify(payloadObj), { qos });
  return true;
}

function route(topic, buf) {
  let pkt;
  try { pkt = JSON.parse(buf.toString()); }
  catch { console.warn('[mqtt] non-JSON payload on', topic); return; }

  // Prefer the packet's own type; fall back to the topic suffix.
  const kind = pkt.t ?? topic.split('/').pop();
  let result;
  if (kind === 'tlm')        result = ingestTelemetry(pkt);
  else if (kind === 'disco') result = ingestDiscovery(pkt);
  else if (kind === 'ack')   result = ingestAck(pkt);
  else return; // ignore our own outbound cmd echoes and anything unknown

  if (result && !result.ok) console.warn(`[mqtt] ${topic}: ${result.error}`);
}

export function startMqtt() {
  const url = process.env.MQTT_URL;
  if (!url) {
    console.log('[mqtt] MQTT_URL not set — skipping broker (HTTP ingest still active)');
    return null;
  }

  return import('mqtt')
    .then(async ({ default: mqtt }) => {
      const opts = {
        username: process.env.MQTT_USERNAME,
        password: process.env.MQTT_PASSWORD,
        reconnectPeriod: 5000,
        clientId: process.env.MQTT_CLIENT_ID ?? `senseable-backend-${Math.random().toString(16).slice(2, 8)}`,
      };

      // TLS: for mqtts://. Point MQTT_CA_CERT at the broker's CA (ISRG Root X1
      // for HiveMQ Cloud, or your Mosquitto ca.crt). MQTT_TLS_INSECURE=true
      // skips hostname/chain checks — bench only, never in production.
      if (url.startsWith('mqtts://') || url.startsWith('tls://')) {
        if (process.env.MQTT_CA_CERT) {
          const { readFile } = await import('node:fs/promises');
          try { opts.ca = await readFile(process.env.MQTT_CA_CERT); }
          catch (e) { console.warn('[mqtt] could not read MQTT_CA_CERT:', e.message); }
        }
        opts.rejectUnauthorized = process.env.MQTT_TLS_INSECURE !== 'true';
      }

      client = mqtt.connect(url, opts);

      const base = TOPIC_BASE;                 // usc/thesis
      const tid  = process.env.MQTT_TID ?? '+'; // '+' = any tenant
      const nid  = process.env.MQTT_NID ?? '+'; // '+' = any node
      const topics = [
        `${base}/${tid}/${nid}/tlm`,
        `${base}/${tid}/${nid}/disco`,
        `${base}/${tid}/${nid}/ack`,
      ];

      client.on('connect', () => {
        console.log(`[mqtt] connected to ${url}`);
        client.subscribe(topics, { qos: 1 }, (err) => {
          if (err) console.error('[mqtt] subscribe error:', err.message);
          else console.log('[mqtt] subscribed:', topics.join(', '));
        });
      });

      client.on('message', route);
      client.on('error', (e) => console.error('[mqtt] error:', e.message));
      client.on('close', () => console.warn('[mqtt] connection closed'));
      return client;
    })
    .catch((e) => {
      console.error('[mqtt] failed to start:', e.message);
      return null;
    });
}
