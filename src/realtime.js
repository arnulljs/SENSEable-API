// realtime.js ────────────────────────────────────────────────────────────────
// WebSocket push layer for live telemetry (thesis Section III.B.3.c/d, Fig. 5).
//
// WHY THIS EXISTS
// The dashboard previously polled GET /api/devices every 3s. That works, but it
// is the wrong shape for live monitoring: the client asks "anything new?" on a
// timer whether or not anything changed, so most round trips return an
// unchanged payload, and a real hardware event waits up to a full poll interval
// before the browser hears about it. Here the SERVER speaks first — the same
// ingest path that writes a packet to the store also pushes the new projection
// to every subscriber, so the UI updates in the same tick the packet lands.
//
// WHAT IS AND IS NOT PUSHED
// Only the live read model (device/module/port/actuator state) moves over the
// socket. One-shot and mutating operations — config fetches, calibration
// writes, formula CRUD, command issuance — stay on REST, because they are
// request/response by nature and gain nothing from a persistent channel. This
// mirrors the split described in the manuscript: REST for configuration and
// history, WebSocket for the live status stream layered on top.
//
// TENANT SCOPING
// A socket declares its tenant once at subscribe time and only ever receives
// projectDevices(thatTenant). The slug is resolved against the store the same
// way the REST route resolves `x-tenant-id`, and an unknown slug is refused
// rather than silently upgraded to "all tenants" — the same fail-closed
// posture the cloud read tier takes.
//
// COALESCING
// A fully populated node publishes 16 channels; discovery and ack traffic
// interleaves with it. Broadcasting per packet would send the same whole-tree
// projection many times within a few milliseconds. Instead a broadcast request
// sets a dirty flag and the actual send happens on a short timer, so a burst
// collapses into one frame carrying the latest state.

import { WebSocketServer } from 'ws';
import { store, projectDevices } from './store.js';

// How long to wait before flushing a dirty broadcast. Long enough to collapse a
// multi-channel burst, short enough to stay imperceptible.
const COALESCE_MS = Number(process.env.WS_COALESCE_MS ?? 120);

// Liveness. A browser tab that goes to sleep or a laptop that closes its lid
// leaves a socket that looks open but will never respond; without this the
// server accumulates dead subscribers and broadcasts into the void.
const HEARTBEAT_MS = Number(process.env.WS_HEARTBEAT_MS ?? 30_000);

let wss = null;
let dirty = false;
let flushTimer = null;
let heartbeatTimer = null;

const stats = {
  connections: 0,      // total accepted since boot
  subscribes: 0,
  rejected: 0,         // bad subscribe (unknown/missing tenant)
  broadcasts: 0,       // frames actually sent (post-coalescing)
  coalesced: 0,        // requests that were absorbed into a pending flush
  lastBroadcastAt: null,
};

export function getRealtimeStats() {
  return {
    ...stats,
    clients: wss ? wss.clients.size : 0,
    enabled: wss != null,
  };
}

// Resolve a tenant slug to the id projectDevices() filters on, or null.
// store.tenants is keyed by tenant id, and each record carries its slug, so a
// slug lookup is a scan — cheap at the tenant counts this platform targets, and
// it keeps the socket path using exactly the same source of truth as REST.
function resolveTenantId(slug) {
  if (!slug) return null;
  if (store.tenants[slug]) return slug;          // already an id
  for (const [id, t] of Object.entries(store.tenants)) {
    if (t?.slug === slug) return id;
  }
  return null;
}

function send(ws, obj) {
  if (ws.readyState !== ws.OPEN) return false;
  try {
    ws.send(JSON.stringify(obj));
    return true;
  } catch {
    return false;                                 // socket died mid-send
  }
}

// Push the current projection to one subscribed socket.
function pushTo(ws) {
  if (!ws.tenantId) return false;
  return send(ws, {
    type: 'devices',
    tenant: ws.tenantSlug,
    devices: projectDevices(ws.tenantId),
    ts: Date.now(),
  });
}

function flush() {
  flushTimer = null;
  if (!dirty || !wss) return;
  dirty = false;

  let sent = 0;
  for (const ws of wss.clients) {
    if (pushTo(ws)) sent += 1;
  }
  if (sent) {
    stats.broadcasts += 1;
    stats.lastBroadcastAt = Date.now();
  }
}

/**
 * Mark the read model as changed. Safe to call on every ingested packet — the
 * actual send is coalesced, so a 16-channel burst produces one frame, not 16.
 * A no-op when the socket server was never started, so ingest code can call it
 * unconditionally without caring whether realtime is enabled.
 */
export function broadcastDevices() {
  if (!wss) return;
  if (flushTimer) { stats.coalesced += 1; return; }
  dirty = true;
  flushTimer = setTimeout(flush, COALESCE_MS);
}

function handleMessage(ws, raw) {
  let msg;
  try { msg = JSON.parse(raw.toString()); }
  catch { return send(ws, { type: 'error', error: 'malformed JSON' }); }

  if (msg?.type !== 'subscribe') {
    return send(ws, { type: 'error', error: `unknown message type '${msg?.type}'` });
  }

  const slug = msg.tenant;
  const tenantId = resolveTenantId(slug);

  // Fail closed, exactly as the REST/cloud tiers do: no tenant means no data,
  // never all data.
  if (!tenantId) {
    stats.rejected += 1;
    return send(ws, {
      type: 'error',
      error: slug ? `unknown tenant '${slug}'` : 'tenant is required',
    });
  }

  ws.tenantSlug = slug;
  ws.tenantId = tenantId;
  stats.subscribes += 1;

  send(ws, { type: 'subscribed', tenant: slug });
  pushTo(ws);                                     // immediate first paint
}

export function startRealtime(server) {
  if (process.env.WS_ENABLED === 'false') {
    console.log('[ws] disabled via WS_ENABLED=false — dashboard falls back to REST polling');
    return null;
  }

  wss = new WebSocketServer({ server, path: '/ws' });

  wss.on('connection', (ws) => {
    stats.connections += 1;
    ws.isAlive = true;
    ws.tenantId = null;
    ws.tenantSlug = null;

    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('message', (raw) => handleMessage(ws, raw));
    ws.on('error', () => { /* transport-level; close handler does cleanup */ });

    send(ws, { type: 'hello', coalesceMs: COALESCE_MS });
  });

  // Terminate sockets that stopped answering pings. `isAlive` is set false
  // before each ping and only restored by the pong, so a socket that misses a
  // full interval is gone.
  heartbeatTimer = setInterval(() => {
    if (!wss) return;
    for (const ws of wss.clients) {
      if (ws.isAlive === false) { ws.terminate(); continue; }
      ws.isAlive = false;
      try { ws.ping(); } catch { ws.terminate(); }
    }
  }, HEARTBEAT_MS);
  heartbeatTimer.unref();

  console.log(`[ws] realtime on /ws (coalesce ${COALESCE_MS}ms, heartbeat ${HEARTBEAT_MS}ms)`);
  return wss;
}

export async function stopRealtime() {
  if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
  if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
  if (!wss) return;
  const server = wss;
  wss = null;
  await new Promise((resolve) => server.close(resolve));
}
