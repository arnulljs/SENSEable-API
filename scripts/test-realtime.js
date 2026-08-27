// scripts/test-realtime.js ───────────────────────────────────────────────────
// Integration test for the WebSocket push layer. Runs against a real HTTP
// server and real ws clients — no mocking of the socket itself — with the store
// stubbed so the test doesn't need PostgreSQL.
//
//   node scripts/test-realtime.js
//
// Exits non-zero on the first failure.

import http from 'node:http';
import { WebSocket } from 'ws';
import { store } from '../src/store.js';
import { startRealtime, stopRealtime, broadcastDevices, getRealtimeStats } from '../src/realtime.js';

let passed = 0, failed = 0;
const results = [];

function check(name, cond, detail = '') {
  if (cond) { passed += 1; results.push(`  PASS  ${name}`); }
  else { failed += 1; results.push(`  FAIL  ${name}${detail ? ` — ${detail}` : ''}`); }
}

const wait = (ms) => new Promise((r) => setTimeout(r, ms));

// ── Stub the store with two tenants, mirroring the real seed ────────────────
function seedStore() {
  store.ready = true;
  store.tenants = {
    aquatech: { id: 'aquatech', slug: 'aquatech', name: 'AquaTech Hatchery Corp' },
    llba:     { id: 'llba',     slug: 'llba',     name: 'Lapu-Lapu Bay Aquafarms' },
  };
  store.devices = [
    {
      id: 'aquatech:N001', tenantId: 'aquatech', name: 'ESP32 N001', nodeId: 'N001',
      status: 'online', commMode: 'Wi-Fi', uptime: 100, rssi: -50, freeHeap: 100000,
      lastSeen: Date.now(), configured: true, actuators: [],
      modules: [{
        id: '0x48', address: '0x48', name: 'Expansion Board 0x48',
        lastSeen: Date.now(), configured: true,
        ports: [{
          id: 'A0', label: 'Dissolved Oxygen', unit: 'mg/L', value: 7.2,
          rangeMin: 0, rangeMax: 20, safeMin: 5, safeMax: 12,
          status: 'Normal', history: [], activeFlag: true, connState: 1,
          lastSeen: Date.now(), configured: true, enabled: true,
        }],
      }],
    },
    // Second tenant, deliberately with its own device — the isolation test
    // depends on each tenant having something the other must not see.
    {
      id: 'llba:N002', tenantId: 'llba', name: 'ESP32 N002', nodeId: 'N002',
      status: 'online', commMode: 'Wi-Fi', uptime: 50, rssi: -60, freeHeap: 90000,
      lastSeen: Date.now(), configured: true, actuators: [],
      modules: [],
    },
  ];
}

// Collect frames from a socket, with helpers to await a specific type.
function client(url) {
  const ws = new WebSocket(url);
  const frames = [];
  ws.on('message', (raw) => {
    try { frames.push(JSON.parse(raw.toString())); } catch { /* ignore */ }
  });
  return {
    ws, frames,
    open: () => new Promise((res, rej) => { ws.on('open', res); ws.on('error', rej); }),
    of: (type) => frames.filter((f) => f.type === type),
    waitFor: async (type, ms = 1500) => {
      const t0 = Date.now();
      while (Date.now() - t0 < ms) {
        const hit = frames.find((f) => f.type === type);
        if (hit) return hit;
        await wait(20);
      }
      return null;
    },
    send: (obj) => ws.send(JSON.stringify(obj)),
    close: () => { try { ws.close(); } catch { /* already gone */ } },
  };
}

async function main() {
  seedStore();

  const server = http.createServer((_req, res) => { res.writeHead(404); res.end(); });
  await new Promise((res) => server.listen(0, res));
  const port = server.address().port;
  const URL_ = `ws://127.0.0.1:${port}/ws`;

  startRealtime(server);

  // ── 1. Connection and hello ───────────────────────────────────────────────
  const a = client(URL_);
  await a.open();
  const hello = await a.waitFor('hello');
  check('server sends hello on connect', hello != null);
  check('hello advertises coalesce window', hello?.coalesceMs > 0);

  // ── 2. Subscribe returns an immediate first payload ──────────────────────
  a.send({ type: 'subscribe', tenant: 'aquatech' });
  const sub = await a.waitFor('subscribed');
  check('subscribe is acknowledged', sub?.tenant === 'aquatech');

  const first = await a.waitFor('devices');
  check('subscribe triggers immediate device push', first != null);
  check('pushed payload is an array', Array.isArray(first?.devices));
  check('payload carries the subscribed tenant', first?.tenant === 'aquatech');

  // ── 3. Shape parity with the REST read model ─────────────────────────────
  const dev = first?.devices?.[0];
  check('device has REST-identical id', dev?.id === 'aquatech:N001');
  check('device carries tenantId', dev?.tenantId === 'aquatech');
  check('device exposes modules array', Array.isArray(dev?.modules));
  check('module exposes ports array', Array.isArray(dev?.modules?.[0]?.ports));
  check('port carries label + unit', dev?.modules?.[0]?.ports?.[0]?.label === 'Dissolved Oxygen');
  check('device has actuators field (Control page needs it)', Array.isArray(dev?.actuators));
  check('derived active flag present', typeof dev?.active === 'boolean');
  check('derived module status present', typeof dev?.modules?.[0]?.status === 'string');

  // ── 4. TENANT ISOLATION — the security-critical case ─────────────────────
  const ids = (first?.devices ?? []).map((d) => d.id);
  check('aquatech subscriber sees own device', ids.includes('aquatech:N001'));
  check('aquatech subscriber CANNOT see llba device', !ids.includes('llba:N002'),
        `leaked: ${ids.join(', ')}`);

  const b = client(URL_);
  await b.open();
  await b.waitFor('hello');
  b.send({ type: 'subscribe', tenant: 'llba' });
  const bFirst = await b.waitFor('devices');
  const bIds = (bFirst?.devices ?? []).map((d) => d.id);
  check('llba subscriber sees own device', bIds.includes('llba:N002'));
  check('llba subscriber CANNOT see aquatech device', !bIds.includes('aquatech:N001'),
        `leaked: ${bIds.join(', ')}`);

  // ── 5. Fail closed on bad/missing tenant ─────────────────────────────────
  const c = client(URL_);
  await c.open();
  await c.waitFor('hello');
  c.send({ type: 'subscribe', tenant: 'does-not-exist' });
  const err = await c.waitFor('error');
  check('unknown tenant is refused', err != null && /unknown tenant/.test(err.error ?? ''));
  check('refused socket got NO device data', c.of('devices').length === 0);

  const d = client(URL_);
  await d.open();
  await d.waitFor('hello');
  d.send({ type: 'subscribe' });                  // no tenant at all
  const err2 = await d.waitFor('error');
  check('missing tenant is refused (fails closed)', err2 != null && /required/.test(err2.error ?? ''));
  check('unscoped socket got NO device data', d.of('devices').length === 0);

  // ── 6. Malformed input doesn't kill the server ───────────────────────────
  const e = client(URL_);
  await e.open();
  await e.waitFor('hello');
  e.ws.send('not json at all');
  const err3 = await e.waitFor('error');
  check('malformed JSON is rejected gracefully', err3 != null && /malformed/.test(err3.error ?? ''));
  e.send({ type: 'nonsense' });
  await wait(100);
  check('unknown message type is rejected', e.of('error').length >= 2);
  check('server still alive after bad input', getRealtimeStats().enabled === true);

  // ── 7. Broadcast on state change ─────────────────────────────────────────
  const beforeCount = a.of('devices').length;
  store.devices[0].modules[0].ports[0].value = 9.9;
  broadcastDevices();
  await wait(300);
  const afterFrames = a.of('devices');
  check('state change pushes a new frame', afterFrames.length > beforeCount);
  const latest = afterFrames[afterFrames.length - 1];
  check('pushed frame carries the NEW value',
        latest?.devices?.[0]?.modules?.[0]?.ports?.[0]?.value === 9.9,
        `got ${latest?.devices?.[0]?.modules?.[0]?.ports?.[0]?.value}`);

  // ── 8. COALESCING — a 16-channel burst must not send 16 frames ───────────
  const preBurst = a.of('devices').length;
  const preStats = getRealtimeStats().broadcasts;
  for (let i = 0; i < 16; i++) broadcastDevices();   // simulate full-node burst
  await wait(400);
  const burstFrames = a.of('devices').length - preBurst;
  check('16 rapid changes collapse to 1 frame', burstFrames === 1, `sent ${burstFrames}`);
  check('coalesce counter recorded the absorbed calls', getRealtimeStats().coalesced >= 15);
  check('broadcast counter incremented once', getRealtimeStats().broadcasts === preStats + 1);

  // ── 9. Isolation holds on broadcast, not just on subscribe ───────────────
  const bBefore = b.of('devices').length;
  broadcastDevices();
  await wait(300);
  const bLatest = b.of('devices')[b.of('devices').length - 1];
  check('llba got its own broadcast', b.of('devices').length > bBefore);
  check('broadcast to llba still excludes aquatech',
        !(bLatest?.devices ?? []).some((x) => x.id === 'aquatech:N001'));

  // ── 10. Unsubscribed sockets receive nothing ─────────────────────────────
  const f = client(URL_);
  await f.open();
  await f.waitFor('hello');
  broadcastDevices();
  await wait(300);
  check('never-subscribed socket gets no data', f.of('devices').length === 0);

  // ── 11. Disconnect cleanup ───────────────────────────────────────────────
  const liveBefore = getRealtimeStats().clients;
  f.close(); c.close(); d.close(); e.close();
  await wait(300);
  check('client count drops after disconnects', getRealtimeStats().clients < liveBefore,
        `${liveBefore} -> ${getRealtimeStats().clients}`);

  // Surviving sockets must still work after others drop.
  const survivorBefore = a.of('devices').length;
  broadcastDevices();
  await wait(300);
  check('surviving socket still receives after peers disconnect',
        a.of('devices').length > survivorBefore);

  // ── 12. Shutdown ─────────────────────────────────────────────────────────
  a.close(); b.close();
  await wait(100);
  await stopRealtime();
  check('stopRealtime disables the hub', getRealtimeStats().enabled === false);
  check('broadcast after shutdown is a safe no-op',
        (() => { try { broadcastDevices(); return true; } catch { return false; } })());

  await new Promise((res) => server.close(res));

  console.log('\n' + results.join('\n'));
  console.log(`\n${passed} passed, ${failed} failed\n`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error('test harness crashed:', e); process.exit(1); });
