// server.js ─────────────────────────────────────────────────────────────────
// SENSEable backend entrypoint. Now PostgreSQL-backed: the store hydrates from
// the database BEFORE the HTTP server accepts traffic, so the first request
// already sees real data rather than an empty cache.

import express from 'express';
import cors from 'cors';
import path from 'node:path';
import { existsSync } from 'node:fs';
import { securityHeaders, corsOptions, apiKeyGate, readLimiter } from './security.js';
import { router } from './routes.js';
import { startMqtt } from './mqtt.js';
import { refreshAll } from './ingest.js';
import { hydrate, refreshRouting, refreshConfig } from './store.js';
import { TIER, CLOUD_BRIDGE_RUNNING } from './role.js';
import { startRealtime, stopRealtime, broadcastDevices } from './realtime.js';
import { checkAllPresence } from './presence.js';
import { dispatchPendingCommands } from './dispatch.js';
import { closePool } from '../db/pool.js';

const PORT = Number(process.env.PORT ?? 4000);

const app = express();

// Trust the proxy hop count in front of us so req.ip is the real client rather
// than the proxy — rate limiting keyed on the proxy's address would lump every
// caller into one bucket.
app.set('trust proxy', Number(process.env.TRUST_PROXY ?? 0));

// Don't advertise the framework.
app.disable('x-powered-by');

app.use(securityHeaders);

// Allowlisted origins instead of the wide-open cors(). See security.js: a
// bare cors() lets ANY website read this API using the visitor's network
// position, which on a LAN-deployed device is a real exposure.
app.use(cors(corsOptions()));

// Body cap. Already present, kept explicit: telemetry envelopes are small, and
// an unbounded parser is a trivial memory exhaustion vector.
app.use(express.json({ limit: '256kb' }));

// Coarse gate. No-op unless API_KEY is set, so bench work is unaffected.
app.use('/api', apiKeyGate);

// Baseline limiter across the whole API; tighter limits are applied per-route
// for writes and commands.
app.use('/api', readLimiter);

app.use('/api', router);

// ── Failover dashboard ──────────────────────────────────────────────────────
// Under cloud-first the operator normally uses the Vercel-hosted dashboard. That
// build CANNOT fall back to this server on its own: the page is served over
// https and a browser refuses to fetch http://192.168.x.x from an https origin
// (mixed content — no CSP change or flag gets around it).
//
// So the edge serves its own copy of the SPA. During an outage the operator
// opens http://<edge-host>:4000 on the LAN and gets a same-origin dashboard
// talking to this server, which is also what makes the failover demonstrable
// rather than merely described.
//
//   cd ../SENSEable && npm run build && cp -r dist ../SENSEable-API/public
//
// Set STATIC_DIR to point somewhere else. When the directory is absent the
// server behaves exactly as before and just answers the JSON banner.
const STATIC_DIR = process.env.STATIC_DIR ?? path.join(process.cwd(), 'public');

if (existsSync(path.join(STATIC_DIR, 'index.html'))) {
  app.use(express.static(STATIC_DIR, { index: false, maxAge: '1h' }));
  // SPA history fallback, but never for /api — a mistyped endpoint must stay a
  // 404 rather than silently returning the app shell with a 200.
  app.get(/^(?!\/api\/).*/, (_req, res) =>
    res.sendFile(path.join(STATIC_DIR, 'index.html')));
  console.log(`[http] serving failover dashboard from ${STATIC_DIR}`);
} else {
  app.get('/', (_req, res) =>
    res.json({ service: 'senseable-backend', ok: true, api: '/api' }));
}

const SWEEP_MS = Number(process.env.SWEEP_MS ?? 5000);

async function main() {
  try {
    await hydrate();                   // load the tenant tree from Postgres
  } catch (err) {
    console.error('[boot] hydrate failed — is PostgreSQL up and migrated?');
    console.error('       DATABASE_URL =', process.env.DATABASE_URL ? '(set)' : '(MISSING)');
    console.error('      ', err.message);
    process.exit(1);
  }

  // Keep statuses honest even when nothing is ingesting: a node with no recent
  // telemetry flips to Offline on its own. This transition has NO packet behind
  // it — it happens precisely because packets stopped — so the sweep has to push
  // it explicitly or a disconnected node would keep reading green on every open
  // dashboard until something else happened to trigger a broadcast.
  setInterval(() => {
    refreshAll();
    // AFTER refreshAll, so presence compares settled statuses rather than
    // racing the computation that produces them. This is where "the ESP32 went
    // offline" is detected: that transition has no packet behind it — it
    // happens precisely because packets stopped — so nothing else can notice it.
    checkAllPresence();
    broadcastDevices();
    // Drain the command outbox. Cloud-authored commands arrive here as rows via
    // the sync worker's downward pass, never as MQTT, so something on the edge
    // has to put them on a broker. Riding the existing sweep keeps it to one
    // timer and means a command is never more than SWEEP_MS from the wire.
    dispatchPendingCommands();
  }, SWEEP_MS).unref();

  // Pins and tid mappings can change underneath this process (the other tier
  // writes them and the sync worker carries them here), so re-read them.
  const ROUTING_REFRESH_MS = Number(process.env.ROUTING_REFRESH_MS ?? 15_000);
  setInterval(() => {
    refreshRouting().catch((e) => console.error('[store] routing refresh failed:', e.message));
    refreshConfig().catch((e) => console.error('[store] config refresh failed:', e.message));
  }, ROUTING_REFRESH_MS).unref();

  console.log(`[role] tier=${TIER}` + (TIER === 'edge'
    ? (CLOUD_BRIDGE_RUNNING ? ' — cloud bridge running: mirror in normal operation, owner during failover'
                            : ' — no cloud bridge declared: this server is the only writer')
    : ' — cloud bridge: writes straight into Supabase, owns notifications and dispatch'));

  const HOST = process.env.HOST ?? '0.0.0.0';
  const server = app.listen(PORT, HOST, () => {
    console.log(`[http] SENSEable backend on http://${HOST === '0.0.0.0' ? 'localhost' : HOST}:${PORT}`);
    console.log(`[http] devices: http://localhost:${PORT}/api/devices`);
    startMqtt();
  });

  // Share the HTTP server so the socket lives at ws://<same-host>/ws — no
  // second port to open, and it inherits whatever the deployment already does
  // for TLS termination.
  startRealtime(server);

  const shutdown = async (sig) => {
    console.log(`\n[${sig}] shutting down...`);
    await stopRealtime();
    server.close();
    await closePool();
    process.exit(0);
  };
  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main();
