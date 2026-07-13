// mqtt.js ───────────────────────────────────────────────────────────────────
// OPTIONAL live ingest. Only connects when MQTT_URL is set — with no broker
// configured the server still runs fully on the HTTP ingest endpoints, so this
// never blocks `npm start`. Topic namespace follows the frozen spec:
//   discovery/{appId}/{nodeId}/init
//   telemetry/{appId}/{nodeId}         (merged data+status)
//   telemetry/{appId}/{nodeId}/data    (firmware's split variant — also handled)

import { ingestTelemetry, ingestDiscovery } from './ingest.js';

export function startMqtt() {
  const url = process.env.MQTT_URL;
  if (!url) {
    console.log('[mqtt] MQTT_URL not set — skipping broker (HTTP ingest still active)');
    return null;
  }

  // Lazy import so the dependency isn't required unless MQTT is actually used.
  return import('mqtt')
    .then(({ default: mqtt }) => {
      const client = mqtt.connect(url, {
        username: process.env.MQTT_USERNAME,
        password: process.env.MQTT_PASSWORD,
        reconnectPeriod: 5000,
      });

      const appId = process.env.APP_ID ?? '+';
      const topics = [
        `telemetry/${appId}/+`,
        `telemetry/${appId}/+/data`,
        `discovery/${appId}/+/init`,
      ];

      client.on('connect', () => {
        console.log(`[mqtt] connected to ${url}`);
        client.subscribe(topics, (err) => {
          if (err) console.error('[mqtt] subscribe error:', err.message);
          else console.log('[mqtt] subscribed:', topics.join(', '));
        });
      });

      client.on('message', (topic, buf) => {
        let pkt;
        try { pkt = JSON.parse(buf.toString()); }
        catch { console.warn('[mqtt] non-JSON payload on', topic); return; }

        const result = topic.startsWith('discovery/')
          ? ingestDiscovery(pkt)
          : ingestTelemetry(pkt);

        if (!result.ok) console.warn(`[mqtt] ${topic}: ${result.error}`);
      });

      client.on('error', (e) => console.error('[mqtt] error:', e.message));
      return client;
    })
    .catch((e) => {
      console.error('[mqtt] failed to start:', e.message);
      return null;
    });
}
