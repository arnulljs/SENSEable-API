// server.js ─────────────────────────────────────────────────────────────────
// SENSEable backend entrypoint. Now PostgreSQL-backed: the store hydrates from
// the database BEFORE the HTTP server accepts traffic, so the first request
// already sees real data rather than an empty cache.

import express from 'express';
import cors from 'cors';
import { securityHeaders, corsOptions, apiKeyGate, readLimiter, logSecurityEvent } from './security.js';
import { router } from './routes.js';
import { startMqtt } from './mqtt.js';
import { refreshAll } from './ingest.js';
import { hydrate } from './store.js';
import { startRealtime, stopRealtime, broadcastDevices } from './realtime.js';
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

// CORS rejections arrive here as a thrown Error. Without this they surface as
// an unhandled stack trace on every blocked request — which buries the one line
// that matters ("origin X not allowed") and, during a demo, looks like a crash
// rather than a control doing its job.
app.use((err, req, res, next) => {
  if (err && /not allowed/.test(err.message)) {
    logSecurityEvent('cors_blocked', req, err.message);
    return res.status(403).json({ error: 'origin not allowed' });
  }
  return next(err);
});
app.get('/', (_req, res) =>
  res.json({ service: 'senseable-backend', ok: true, api: '/api' })
);

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
  setInterval(() => { refreshAll(); broadcastDevices(); }, SWEEP_MS).unref();

  const server = app.listen(PORT, () => {
    console.log(`[http] SENSEable backend on http://localhost:${PORT}`);
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
