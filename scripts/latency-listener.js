// latency-listener.js ────────────────────────────────────────────────────────
// Latency recorder for the thesis (testing guide TC-2.5, TC-2.11, TC-3.4).
// Runs on the on-site laptop, apart from the backend. Writes a report you can
// open: report-<start>.xlsx (one Summary sheet + raw sheets) and the same data
// as report-<start>.json, refreshed every 5 minutes and when it stops.
//
//   node scripts/latency-listener.js                          all nodes it hears
//   node scripts/latency-listener.js --nodes NODE-1BC478 --duration-min 180
//   node scripts/latency-listener.js --nodes NODE-1BC478 --cmd 50 --cmd-actuator out1
//   node scripts/latency-listener.js --nodes NODE-1BC478 --cmd 10 --cmd-dur-ms 10000 --cmd-gap-s 20
//
// WHAT IT MEASURES (every hop, on the cloud-first path)
//
//   node ──① ──▶ HiveMQ ──② ──▶ Lambda ──▶ Supabase ──③ ──▶ Vercel API ──▶ dashboard
//     ◀──────── ping ④ ────────▶ (this laptop)
//   dashboard click ──⑤ ──▶ Vercel ──▶ HiveMQ ──▶ node ──▶ ACK ──▶ Lambda ──▶ Supabase ──▶ Vercel
//
//   ① node → broker      this listener's receipt − the node's ts_ms (needs the
//                        node's SNTP; rows without ts_ms skip only this hop)
//   ④ ping               round trip on this laptop's clock alone, split into
//                        downlink / time on the node / uplink with rx_ms, ts_ms
//   ② broker → database  the row's readings.received_at (API migration 026)
//                        − this listener's receipt. received_at is Supabase's
//                        clock: corrected by the offset measured against
//                        /api/health once a minute (Clock sheet)
//   ③ database → API     first /api/devices poll that shows the row − received_at
//      broker → API      the same poll − this listener's receipt (laptop clock
//                        only, so it needs no correction)
//      node → API        end to end, the same poll − ts_ms
//   API response time    every /api/devices poll's duration (Vercel function)
//   ⑤ dashboard command  opt-in with --cmd N: N binary actuate commands sent
//                        through the Vercel API like a dashboard click (ON, OFF,
//                        ON, … on --cmd-actuator, --cmd-gap-s apart, OFF last),
//                        timed at each step: Vercel response, command on the
//                        broker, first ACK, ACK visible through /api/commands.
//                        With --cmd-dur-ms D every command is instead a timed ON
//                        of D ms, and the node's auto-off is timed too (TC-3.4
//                        shutoff accuracy). The node's own ACK timing (firmware
//                        2026-11-04: rx_ms arrival, act_ms output switched,
//                        ts_ms ACK sent, t0 echoed) splits the path further.
//                        It switches a real output: leave it unwired or on a
//                        safe load.
//
//   The dashboard itself re-reads /api/devices every 3 s, so what a user sees
//   is "→ API" plus 0–3 s (mean 1.5 s). The poll here runs every --api-poll-s
//   (default 1 s): a row's API time is known to within that.
//
// Statistics use first copies outside reconnect bursts; database hops also
// only rows that went live to the cloud (not edge failover rows synced later).
// Raw CSVs (tlm_latency.csv, ping_rtt.csv, mqtt_events.csv) are still
// appended row by row, so a crash loses nothing; chrony_tracking.txt records
// the laptop clock at start and stop.
//
// SETTINGS (.env.latency, never committed)
//   LATENCY_MQTT_URL / _USERNAME / _PASSWORD   its own HiveMQ login (read
//                        usc/thesis/#, publish commands); own client ID, clean
//                        session, so it never takes over another client
//   LATENCY_API_BASE     https://senseable-iot.vercel.app      ┐ all five turn
//   LATENCY_SUPABASE_URL / LATENCY_SUPABASE_ANON  (the VITE_ ones)│ on hops ②③⑤;
//   LATENCY_EMAIL / LATENCY_PASSWORD  a member of the nodes' org ┘ without them
//                        only ① and ④ run

import dotenv from 'dotenv';
import mqtt from 'mqtt';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';

dotenv.config({ path: process.env.LATENCY_ENV ?? '.env.latency', override: true });
const E = process.env;

const argv = process.argv.slice(2);
const arg = (name, dflt) => { const i = argv.indexOf(`--${name}`); return i >= 0 ? argv[i + 1] : dflt; };
const ONLY = arg('nodes', '') ? new Set(arg('nodes').split(',').map((s) => s.trim()).filter(Boolean)) : null;
const PING_MS = Number(arg('ping-s', '10')) * 1000;
const POLL_MS = Number(arg('api-poll-s', '1')) * 1000;
const DURATION_MS = Number(arg('duration-min', '0')) * 60_000;
const CMD_N = Number(arg('cmd', '0'));
const CMD_ACT = arg('cmd-actuator', 'out1');
const CMD_GAP_MS = Number(arg('cmd-gap-s', '10')) * 1000;
const CMD_DUR_MS = Number(arg('cmd-dur-ms', '0'));
const STALE_PING_MS = 5000;                 // a reply later than this is a queued burst, not latency
const PING_TIMEOUT_MS = 30_000;
const DB_WAIT_MS = 10 * 60_000;             // a row not in the database after this counts as missing
const NODE_IDLE_MS = 5 * 60_000;            // stop pinging a node not heard from in 5 min
const REPORT_EVERY_MS = 5 * 60_000;
const OUT = path.resolve(arg('out', './latency-out'));
const URL = E.LATENCY_MQTT_URL;
const BASE = E.MQTT_TOPIC_BASE ?? 'usc/thesis';
const API = E.LATENCY_API_BASE?.replace(/\/$/, '');
const BACKEND = Boolean(API && E.LATENCY_SUPABASE_URL && E.LATENCY_SUPABASE_ANON && E.LATENCY_EMAIL && E.LATENCY_PASSWORD);

if (!URL || !E.LATENCY_MQTT_USERNAME || !E.LATENCY_MQTT_PASSWORD) {
  console.error('Set LATENCY_MQTT_URL, LATENCY_MQTT_USERNAME, LATENCY_MQTT_PASSWORD in .env.latency');
  process.exit(2);
}
if (!(PING_MS >= 1000) || !(POLL_MS >= 500)) { console.error('--ping-s must be ≥ 1 and --api-poll-s ≥ 0.5'); process.exit(2); }
if (CMD_DUR_MS && !(CMD_DUR_MS >= 100 && CMD_GAP_MS >= CMD_DUR_MS + 5000)) {
  console.error('--cmd-dur-ms must be ≥ 100, and --cmd-gap-s must leave ≥ 5 s after the auto-off'); process.exit(2);
}
if (CMD_N && (!BACKEND || !ONLY || ONLY.size !== 1)) {
  console.error('--cmd needs the LATENCY_API_* settings and exactly one --nodes NODE-ID'); process.exit(2);
}

const STARTED = new Date();
const REPORT = path.join(OUT, `report-${STARTED.toISOString().replace(/[:.]/g, '-')}`);

// ── CSV files (appended to across runs; header written once) ─────────────────
fs.mkdirSync(OUT, { recursive: true });
function csv(name, header) {
  const file = path.join(OUT, name);
  const fresh = !fs.existsSync(file) || fs.statSync(file).size === 0;
  // Synchronous appends: one small write per message, and a Ctrl-C or a crash
  // can never lose rows still sitting in a stream buffer.
  if (fresh) fs.appendFileSync(file, header.join(',') + '\n');
  return (row) => fs.appendFileSync(file, header.map((k) => cell(row[k])).join(',') + '\n');
}
const cell = (v) => (v == null ? '' : /[",\n]/.test(String(v)) ? `"${String(v).replace(/"/g, '""')}"` : String(v));
const num = (v) => (Number.isFinite(Number(v)) && v !== null && v !== '' ? Number(v) : null);

const writeTlm = csv('tlm_latency.csv', ['recv_ms', 'nid', 'net', 'rt', 'ts_ms', 'latency_ms', 'sync_age_s',
  'dup', 'first_copy', 'after_reconnect', 'tid', 'win_ms', 'recv_iso']);
const writePing = csv('ping_rtt.csv', ['t0', 'recv_ms', 'rtt_ms', 'down_ms', 'up_ms', 'proc_ms', 'sync_age_s',
  'net', 'rt', 'nid', 'tid', 'cid', 'outcome', 'rx_ms', 'ts_ms', 'stale']);
const writeEvent = csv('mqtt_events.csv', ['at_ms', 'at_iso', 'event', 'detail']);
const events = [];
const event = (e, detail = '') => {
  const at = Date.now();
  writeEvent({ at_ms: at, at_iso: new Date(at).toISOString(), event: e, detail });
  events.push({ at_ms: at, event: e, detail });
  console.log(`[${new Date(at).toISOString()}] ${e}${detail ? ` — ${detail}` : ''}`);
};

// ── Clock record: chronyc output at start and stop (handover 5) ──────────────
function chronySnapshot(when) {
  let text = `===== ${when} ${new Date().toISOString()} =====\n`;
  for (const args of [['tracking'], ['clients']]) {
    try {
      text += `$ chronyc ${args.join(' ')}\n` + execFileSync('chronyc', args, { encoding: 'utf8', timeout: 5000 });
    } catch (e) {
      text += `$ chronyc ${args.join(' ')}\n(failed: ${String(e.stderr || e.message).trim().split('\n')[0]}` +
              `${args[0] === 'clients' ? ' — run the listener with sudo, or record `sudo chronyc clients` by hand' : ''})\n`;
    }
  }
  fs.appendFileSync(path.join(OUT, 'chrony_tracking.txt'), text + '\n');
}
chronySnapshot('start');

// ── Laptop clock check: every latency row depends on it ─────────────────────
let ntpSynced = 'unknown';
try {
  ntpSynced = execFileSync('timedatectl', ['show', '-p', 'NTPSynchronized', '--value'], { encoding: 'utf8' }).trim();
  if (ntpSynced !== 'yes') {
    console.warn('WARNING: this laptop\'s clock is not NTP-synchronized (timedatectl NTPSynchronized=' +
                 `${ntpSynced}). tlm latency will be off by the clock error. Fix: sudo timedatectl set-ntp true`);
  }
  event('clock_check', `NTPSynchronized=${ntpSynced}`);
} catch {
  event('clock_check', 'timedatectl unavailable — confirm NTP sync manually');
}

// ── State ────────────────────────────────────────────────────────────────────
const nodes = new Map();      // nid → { tid, lastHeard }
const pending = new Map();    // ping cid → { nid, tid, t0 }
const seenTs = new Set();     // nid|ts_ms already logged → later copies are first_copy=false
let lastConnectAt = 0;        // rows within 10 s of a (re)connect are flagged after_reconnect
const AFTER_RECONNECT_MS = 10_000;
const stats = { tlm: 0, tlmSkippedNoTs: 0, tlmSkippedOld: 0, pings: 0, pongs: 0, timeouts: 0 };
const tlmRows = [];           // live first copies, one per telemetry message
const byWindow = new Map();   // nid|ts (s) → tlm row, for matching database rows
const pingRows = [];
const cmdSeen = new Map();    // cid → when the command crossed the broker
const ackSeen = new Map();    // cid → { first, firstStatus, last, lastStatus, byStatus: {status → timing} }

const clientId = `senseable-latency-${os.hostname()}-${randomUUID().slice(0, 6)}`;
const client = mqtt.connect(URL, {
  username: E.LATENCY_MQTT_USERNAME,
  password: E.LATENCY_MQTT_PASSWORD,
  clientId, clean: true, protocolVersion: 5,
  reconnectPeriod: 2000, connectTimeout: 15_000, keepalive: 30,
});

let connects = 0;
client.on('connect', () => {
  connects++;
  // The very first connect is not a reconnect; every later one is.
  if (connects > 1) lastConnectAt = Date.now();
  event(connects > 1 ? 'reconnected' : 'connected', clientId);
  const topics = [`${BASE}/+/+/tlm`, `${BASE}/+/+/ack`, `${BASE}/+/+/cmd`];
  client.subscribe(topics, { qos: 1 }, (err, granted) => {
    const bad = err ? err.message : granted?.filter((g) => g.qos >= 128).map((g) => g.topic).join(' ');
    event(bad ? 'subscribe_failed' : 'subscribed', bad || topics.join(', '));
  });
});
client.on('reconnect', () => event('reconnecting'));
client.on('close', () => event('closed'));
client.on('offline', () => event('offline'));
client.on('error', (e) => event('error', e.message));

client.on('message', (topic, payload, packet) => {
  const received = Date.now();                     // first: before any parsing
  if (packet.retain) return;                       // broker replay, not live
  const parts = topic.split('/');
  const kind = parts.at(-1), nid = parts.at(-2), tid = parts.at(-3);
  if (ONLY && !ONLY.has(nid)) return;
  let pkt;
  try { pkt = JSON.parse(payload.toString()); } catch { return; }

  if (kind === 'cmd') {                            // includes the dashboard's (⑤)
    if (pkt.cid && !cmdSeen.has(pkt.cid)) cmdSeen.set(pkt.cid, received);
    return;
  }

  if (kind === 'tlm') {
    nodes.set(nid, { tid, lastHeard: received });
    if (pkt.r === 1 || pkt.r === true) return;     // spool replay: old by design
    let tsMs = num(pkt.ts_ms);
    if (tsMs == null) stats.tlmSkippedNoTs++;      // node clock not SNTP-synced: ① unknown
    else if (tsMs < Date.UTC(2026, 0, 1)) { stats.tlmSkippedOld++; tsMs = null; }
    const key = `${nid}|${tsMs ?? `r${received}`}`;
    const firstCopy = !seenTs.has(key);
    seenTs.add(key);
    const afterReconnect = lastConnectAt > 0 && received - lastConnectAt <= AFTER_RECONNECT_MS;
    stats.tlm++;
    writeTlm({ recv_ms: received, recv_iso: new Date(received).toISOString(), nid, tid, net: pkt.net, rt: pkt.rt,
               ts_ms: tsMs, latency_ms: tsMs != null ? received - tsMs : null, sync_age_s: num(pkt.sync_age_s),
               dup: packet.dup ? 'true' : 'false', first_copy: String(firstCopy),
               after_reconnect: String(afterReconnect), win_ms: num(pkt.win_ms) });
    if (!firstCopy) return;
    const row = { recv_ms: received, nid, net: pkt.net ?? null, rt: pkt.rt ?? null, ts_s: num(pkt.ts), ts_ms: tsMs,
                  sync_age_s: num(pkt.sync_age_s), after_reconnect: afterReconnect,
                  received_at_ms: null, api_seen_ms: null, source: null, db: BACKEND ? 'waiting' : 'off' };
    tlmRows.push(row);
    // The database stores the window under the node's ts (seconds); that is
    // the key a reading comes back with from /api/devices.
    if (BACKEND && row.ts_s != null) byWindow.set(`${nid}|${row.ts_s}`, row);
    else if (BACKEND) row.db = 'no ts';
    return;
  }

  if (kind === 'ack') {
    if (pkt.cid && pkt.action !== 'ping') {        // ⑤: any command's ACKs, by cid
      let a = ackSeen.get(pkt.cid);
      if (!a) ackSeen.set(pkt.cid, a = { first: received, firstStatus: pkt.status, byStatus: {} });
      a.last = received; a.lastStatus = pkt.status;
      // Firmware 2026-11-04: started/stopped/completed carry port, the t0 echo
      // and, once the node's clock is synced, rx_ms / act_ms / ts_ms.
      a.byStatus[pkt.status] ??= { recv: received, port: num(pkt.port), t0: num(pkt.t0), rx_ms: num(pkt.rx_ms),
                                   act_ms: num(pkt.act_ms), ts_ms: num(pkt.ts_ms), sync_age_s: num(pkt.sync_age_s) };
    }
    if (pkt.action !== 'ping') return;
    const p = pending.get(pkt.cid);
    if (!p) return;                                // not ours (another listener)
    pending.delete(pkt.cid);
    nodes.set(nid, { tid, lastHeard: received });
    const t0 = num(pkt.t0) ?? p.t0;
    const rx = num(pkt.rx_ms), ts = num(pkt.ts_ms);
    stats.pongs++;
    // A reply this late was queued somewhere (node offline, reconnect burst):
    // kept in the file, left out of the statistics. A reply without rx_ms is
    // NOT stale: the node's clock just isn't synced yet, and its round trip,
    // on this laptop's clock alone, is still valid.
    const row = { t0, recv_ms: received, nid, tid, cid: pkt.cid, outcome: 'reply', rtt_ms: received - t0,
                  stale: received - t0 > STALE_PING_MS,
                  down_ms: rx != null ? rx - t0 : null, proc_ms: rx != null && ts != null ? ts - rx : null,
                  up_ms: ts != null ? received - ts : null, rx_ms: rx, ts_ms: ts,
                  sync_age_s: num(pkt.sync_age_s), net: pkt.net, rt: pkt.rt };
    // NTP's offset formula (guide TC-2.11): positive = node clock ahead.
    row.offset_ms = rx != null && ts != null ? ((rx - t0) - (received - ts)) / 2 : null;
    pingRows.push(row);
    writePing(row);
  }
});

// ── Pinging (④) ──────────────────────────────────────────────────────────────
// The command envelope the firmware validates: its own tid and nid (learned
// from the topic of its telemetry), a cid, and a current ts (commands older
// than 5 min are refused). t0 is echoed back unchanged.
function pingAll() {
  if (!client.connected) return;
  const now = Date.now();
  for (const [nid, { tid, lastHeard }] of nodes) {
    if (now - lastHeard > NODE_IDLE_MS) continue;
    const cid = `ping-${randomUUID()}`;
    const t0 = Date.now();
    pending.set(cid, { nid, tid, t0 });
    stats.pings++;
    client.publish(`${BASE}/${tid}/${nid}/cmd`,
      JSON.stringify({ t: 'cmd', v: 1, tid, nid, cid, ts: Math.floor(t0 / 1000), action: 'ping', t0 }),
      // QoS 0: HiveMQ must not queue pings for an offline node and deliver them
      // as a burst on reconnect. A ping sent while the node is away is lost,
      // which is the truth. (The node still replies at QoS 1.)
      { qos: 0 });
  }
  for (const [cid, p] of pending) {
    if (now - p.t0 > PING_TIMEOUT_MS) {
      pending.delete(cid);
      stats.timeouts++;
      const row = { t0: p.t0, nid: p.nid, tid: p.tid, cid, outcome: 'timeout' };
      pingRows.push(row);
      writePing(row);
    }
  }
}
setInterval(pingAll, PING_MS);

// ── Backend: Vercel API and Supabase (②③⑤) ──────────────────────────────────
const apiPolls = [];          // { t1, ms, status, kind }
const clockRows = [];         // /api/health samples
const cmdRows = [];
const deviceIds = new Map();  // nid → the dashboard's device id ('slug:NODE-…')
let token = null;
let running = true;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function signIn() {
  const r = await fetch(`${E.LATENCY_SUPABASE_URL}/auth/v1/token?grant_type=password`, {
    method: 'POST',
    headers: { apikey: E.LATENCY_SUPABASE_ANON, 'content-type': 'application/json' },
    body: JSON.stringify({ email: E.LATENCY_EMAIL, password: E.LATENCY_PASSWORD }),
  });
  const j = await r.json().catch(() => ({}));
  if (!j.access_token) throw new Error(`sign-in failed: ${j.error_description ?? j.msg ?? r.status}`);
  token = j.access_token;
  event('api_signed_in', E.LATENCY_EMAIL);
}

// One timed request. Supabase tokens last an hour: a 401 signs in again once.
async function api(method, p, body, kind) {
  for (let attempt = 0; attempt < 2; attempt++) {
    if (!token) await signIn();
    const t1 = Date.now();
    let status = 0, json = null;
    try {
      const r = await fetch(`${API}/api/${p}`, {
        method, body: body ? JSON.stringify(body) : undefined,
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      });
      status = r.status;
      json = await r.json().catch(() => null);
    } catch (e) { status = 0; json = { error: e.message }; }
    const t4 = Date.now();
    if (kind) apiPolls.push({ t1, ms: t4 - t1, status, kind });
    if (status === 401 && attempt === 0) { token = null; continue; }
    return { status, json, t1, t4 };
  }
}

// ③: poll the same read the dashboard uses; the first poll that shows a
// window's reading is when it became visible through Vercel.
async function pollDevices() {
  const { status, json, t4 } = await api('GET', 'devices', null, 'GET /api/devices');
  if (status !== 200 || !Array.isArray(json)) return;
  for (const d of json) {
    deviceIds.set(d.nodeId, d.id);
    for (const m of d.modules ?? []) for (const p of m.ports ?? []) for (const h of p.history ?? []) {
      if (!h.ts) continue;
      const row = byWindow.get(`${d.nodeId}|${Math.round(Date.parse(h.ts) / 1000)}`);
      if (!row || row.api_seen_ms != null) continue;
      row.api_seen_ms = t4;
      row.received_at_ms = h.receivedAt ? Date.parse(h.receivedAt) : null;
      row.source = h.source ?? null;
      row.db = row.received_at_ms != null ? 'found' : 'found (no received_at: API migration 026 missing?)';
    }
  }
  const now = Date.now();
  for (const [k, row] of byWindow) {
    if (row.api_seen_ms != null) { byWindow.delete(k); continue; }
    if (now - row.recv_ms > DB_WAIT_MS) {
      row.db = deviceIds.has(row.nid) ? 'not in database after 10 min (no enabled port?)' : 'node not in this org';
      byWindow.delete(k);
    }
  }
}

// Supabase's clock vs this laptop's, NTP-style: offset = db_now − midpoint.
// The sample with the smallest round trip has the smallest error (± rtt/2).
async function clockSample() {
  const t1 = Date.now();
  try {
    const r = await fetch(`${API}/api/health`);
    const j = await r.json();
    const t4 = Date.now();
    const db = Date.parse(j.now);
    if (Number.isFinite(db)) clockRows.push({ t1, t4, rtt_ms: t4 - t1, db_ms: db, offset_ms: db - (t1 + t4) / 2 });
  } catch (e) { event('clock_sample_failed', e.message); }
}
const dbOffset = () => {
  if (!clockRows.length) return null;
  return clockRows.reduce((b, r) => (r.rtt_ms < b.rtt_ms ? r : b)).offset_ms;
};

async function backendLoop() {
  try { await signIn(); } catch (e) { event('api_disabled', e.message); return; }
  let lastClock = 0;
  while (running) {
    const t = Date.now();
    if (t - lastClock >= 60_000) { lastClock = t; await clockSample(); }
    await pollDevices().catch((e) => event('poll_failed', e.message));
    await sleep(Math.max(0, POLL_MS - (Date.now() - t)));
  }
}

// ⑤: dashboard-path commands. Exactly what the Actuators panel sends.
async function commandTest() {
  const nid = [...ONLY][0];
  for (let i = 0; i < 60 && !deviceIds.has(nid) && running; i++) await sleep(1000);
  const deviceId = deviceIds.get(nid);
  if (!deviceId) { event('cmd_test_skipped', `${nid} not found in /api/devices`); return; }
  event('cmd_test_start', `${CMD_N} commands to ${deviceId} ${CMD_ACT}, ${CMD_GAP_MS / 1000} s apart` +
        (CMD_DUR_MS ? `, each a timed ON of ${CMD_DUR_MS} ms` : ', alternating ON/OFF'));
  const send = (state, dur = 0) => api('POST', 'commands',
    { deviceId, action: 'actuate', actuatorId: CMD_ACT, mode: 'bin', state, dur }, 'POST /api/commands');
  let last = 0;
  for (let i = 0; i < CMD_N && running; i++) {
    const state = CMD_DUR_MS ? 1 : i % 2 === 0 ? 1 : 0;
    last = CMD_DUR_MS ? 0 : state;                 // a timed ON switches itself off
    const r = await send(state, CMD_DUR_MS);
    const row = { i: i + 1, state, dur_ms: CMD_DUR_MS, click_ms: r.t1, post_status: r.status, post_ms: r.t4 - r.t1,
                  cid: r.json?.cid ?? null, published: r.json?.published ?? null };
    cmdRows.push(row);
    if (!row.cid) { row.error = r.json?.error ?? `HTTP ${r.status}`; await sleep(CMD_GAP_MS); continue; }
    // Wait for the ACK to show up through Vercel (Lambda → Supabase → API).
    const deadline = Date.now() + Math.max(CMD_GAP_MS, 90_000);
    while (Date.now() < deadline && running) {
      await sleep(1000);
      const res = await api('GET', `commands?device=${encodeURIComponent(deviceId)}`, null, 'GET /api/commands');
      const c = Array.isArray(res.json) ? res.json.find((x) => x.cid === row.cid) : null;
      if (c?.ackedAt) { row.api_ack_seen_ms = res.t4; row.db_acked_at_ms = c.ackedAt; row.final_status = c.status; break; }
    }
    const wait = row.click_ms + CMD_GAP_MS - Date.now();
    if (wait > 0) await sleep(wait);
  }
  if (last === 1) { await send(0); event('cmd_test_off', `${CMD_ACT} switched OFF after the test (not timed)`); }
  event('cmd_test_done', `${cmdRows.length} commands`);
}

// ── Report: Summary + raw sheets, as .xlsx and .json ────────────────────────
const sortN = (a) => [...a].sort((x, y) => x - y);
const q = (s, p) => (s.length ? s[Math.min(s.length - 1, Math.max(0, Math.ceil(p * s.length) - 1))] : null);
const r1 = (v) => (v == null || !Number.isFinite(v) ? null : Math.round(v * 10) / 10);
function describe(values) {
  const v = values.filter((x) => x != null && Number.isFinite(x));
  if (!v.length) return { n: 0 };
  const s = sortN(v), n = s.length, mean = s.reduce((a, b) => a + b, 0) / n;
  const sd = n > 1 ? Math.sqrt(s.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1)) : 0;
  return { n, median: r1(q(s, 0.5)), mean: r1(mean), sd: r1(sd), p95: r1(q(s, 0.95)), p99: r1(q(s, 0.99)),
           min: r1(s[0]), max: r1(s[n - 1]) };
}

function buildReport() {
  const off = dbOffset();
  const dbAt = (ms) => (ms == null || off == null ? null : ms - off);   // Supabase time → laptop time
  const clean = tlmRows.filter((r) => !r.after_reconnect);
  const live = clean.filter((r) => (r.rt ?? 'cloud') === 'cloud' && (r.source ?? 'cloud') === 'cloud');
  const tlm = tlmRows.map((r) => ({
    recv_iso: new Date(r.recv_ms).toISOString(), nid: r.nid, net: r.net, rt: r.rt, source: r.source,
    after_reconnect: r.after_reconnect, sync_age_s: r.sync_age_s,
    node_to_broker_ms: r.ts_ms != null ? r.recv_ms - r.ts_ms : null,
    broker_to_db_ms: r.received_at_ms != null && off != null ? r1(dbAt(r.received_at_ms) - r.recv_ms) : null,
    db_to_api_ms: r.api_seen_ms != null && r.received_at_ms != null && off != null ? r1(r.api_seen_ms - dbAt(r.received_at_ms)) : null,
    broker_to_api_ms: r.api_seen_ms != null ? r.api_seen_ms - r.recv_ms : null,
    node_to_api_ms: r.api_seen_ms != null && r.ts_ms != null ? r.api_seen_ms - r.ts_ms : null,
    database: r.db, ts_ms: r.ts_ms, recv_ms: r.recv_ms, received_at_ms: r.received_at_ms, api_seen_ms: r.api_seen_ms,
  }));
  const at = new Map(tlmRows.map((r, i) => [r, i]));
  const T = (rows, k) => rows.map((r) => tlm[at.get(r)][k]);
  const replies = pingRows.filter((p) => p.outcome === 'reply' && !p.stale);
  let lastStarted = null;   // act_ms of the previous 'started' on the test port (manual on-time)
  const cmds = cmdRows.map((c) => {
    const broker = c.cid ? cmdSeen.get(c.cid) ?? null : null;
    const ack = c.cid ? ackSeen.get(c.cid) : null;
    // The ACK that answers this command: started (ON) or stopped (OFF). Its
    // node timing exists only once the node's clock is synced this boot.
    const a = ack?.byStatus.started ?? ack?.byStatus.stopped ?? null;
    const done = ack?.byStatus.completed ?? null;
    let onTime = null;
    if (ack?.byStatus.started?.act_ms != null) lastStarted = ack.byStatus.started.act_ms;
    else if (ack?.byStatus.stopped?.act_ms != null && lastStarted != null) {
      onTime = ack.byStatus.stopped.act_ms - lastStarted; lastStarted = null;
    }
    return {
      i: c.i, state: c.state, cid: c.cid, http: c.post_status, published_immediately: c.published, error: c.error ?? null,
      vercel_response_ms: c.post_ms,
      click_to_broker_ms: broker != null ? broker - c.click_ms : null,
      broker_to_ack_ms: broker != null && ack ? ack.first - broker : null,
      first_ack_status: ack?.firstStatus ?? null,
      ack_to_db_ms: ack && c.db_acked_at_ms != null && off != null ? r1(dbAt(c.db_acked_at_ms) - ack.first) : null,
      click_to_ack_ms: ack ? ack.first - c.click_ms : null,
      click_to_ack_in_api_ms: c.api_ack_seen_ms != null ? c.api_ack_seen_ms - c.click_ms : null,
      final_status: c.final_status ?? null,
      // Node timing (firmware 2026-11-04). t0 is Vercel's clock, rx/act/ts the
      // node's (SNTP), recv this laptop's (NTP).
      vercel_to_node_ms: a?.rx_ms != null && a?.t0 != null ? a.rx_ms - a.t0 : null,
      node_rx_to_output_ms: a?.rx_ms != null && a?.act_ms != null ? a.act_ms - a.rx_ms : null,
      ack_send_to_broker_ms: a?.ts_ms != null ? a.recv - a.ts_ms : null,
      click_to_output_ms: a?.act_ms != null ? a.act_ms - c.click_ms : null,
      on_time_ms: onTime,
      auto_off_ms: done?.act_ms != null && ack?.byStatus.started?.act_ms != null ? done.act_ms - ack.byStatus.started.act_ms : null,
      auto_off_error_ms: c.dur_ms && done?.act_ms != null && ack?.byStatus.started?.act_ms != null
        ? done.act_ms - ack.byStatus.started.act_ms - c.dur_ms : null,
      dur_ms: c.dur_ms, node_port: a?.port ?? null, t0: a?.t0 ?? null, rx_ms: a?.rx_ms ?? null, act_ms: a?.act_ms ?? null,
      ack_ts_ms: a?.ts_ms ?? null, completed_act_ms: done?.act_ms ?? null, node_sync_age_s: a?.sync_age_s ?? null,
      click_ms: c.click_ms, broker_ms: broker, ack_ms: ack?.first ?? null, db_acked_at_ms: c.db_acked_at_ms ?? null,
    };
  });
  const C = (k) => cmds.map((c) => c[k]);
  const devPolls = apiPolls.filter((p) => p.kind === 'GET /api/devices' && p.status === 200).map((p) => p.ms);
  const postPolls = apiPolls.filter((p) => p.kind === 'POST /api/commands').map((p) => p.ms);
  const offNote = off == null ? 'no clock sample' : `Supabase clock offset ${r1(off)} ms applied (± ${r1(Math.min(...clockRows.map((r) => r.rtt_ms)) / 2)} ms)`;

  const metric = (id, name, clock, values, note = '') => ({ id, metric: name, clock, ...describe(values), note });
  const summary = [
    metric('1', 'Node → broker (one-way, telemetry)', 'node + laptop', T(clean, 'node_to_broker_ms'),
      'recv − ts_ms; valid to the node clock offset (row 4d)'),
    metric('2', 'Broker → database (Lambda ingest)', 'laptop + Supabase', T(live, 'broker_to_db_ms'),
      `received_at − recv; expect a 0–60 s sawtooth from the 1-min Lambda schedule. ${offNote}`),
    metric('3a', 'Database → Vercel API visible', 'laptop + Supabase', T(live, 'db_to_api_ms'),
      `first /api/devices poll showing the row − received_at; ± ${POLL_MS / 1000} s poll resolution`),
    metric('3b', 'Broker → Vercel API visible', 'laptop only', T(live, 'broker_to_api_ms'),
      'needs no clock correction; = row 2 + row 3a'),
    metric('3c', 'Node → Vercel API visible (end to end)', 'node + laptop', T(live, 'node_to_api_ms'),
      'the dashboard re-reads every 3 s: add 0–3 s (mean 1.5 s) for what a user sees'),
    metric('3d', 'Vercel API response time (GET /api/devices)', 'laptop only', devPolls,
      'one request = Vercel function + Supabase query + transfer'),
    metric('4a', 'Ping round trip (laptop → broker → node → broker → laptop)', 'laptop only', replies.map((p) => p.rtt_ms)),
    metric('4b', 'Ping downlink (broker → node)', 'node + laptop', replies.map((p) => p.down_ms)),
    metric('4c', 'Ping time on the node', 'node only', replies.map((p) => p.proc_ms)),
    metric('4d', 'Node clock offset vs laptop (TC-2.11)', 'node + laptop', replies.map((p) => p.offset_ms),
      'NTP formula; positive = node ahead. Small vs row 1 means the one-way figures hold'),
    metric('4e', 'Ping uplink (node → broker)', 'node + laptop', replies.map((p) => p.up_ms)),
    metric('5a', 'Dashboard command: Vercel response (POST /api/commands)', 'laptop only', postPolls),
    metric('5b', 'Dashboard command: click → command on broker', 'laptop only', C('click_to_broker_ms')),
    metric('5c', 'Dashboard command: broker → node ACK', 'laptop only', C('broker_to_ack_ms'),
      'round trip broker → node → broker; row 5h is the time on the node alone'),
    metric('5d', 'Dashboard command: ACK → database (Lambda)', 'laptop + Supabase', C('ack_to_db_ms')),
    metric('5e', 'Dashboard command: click → ACK at broker', 'laptop only', C('click_to_ack_ms')),
    metric('5f', 'Dashboard command: click → ACK visible in Vercel', 'laptop only', C('click_to_ack_in_api_ms'),
      'what the dashboard can show as "done"; ± 1 s poll'),
    metric('5g', 'Dashboard command: Vercel → node (rx_ms − t0)', 'Vercel + node', C('vercel_to_node_ms'),
      't0 is stamped by Vercel when the command is built; needs the node clock synced'),
    metric('5h', 'Node: command received → output switched (act_ms − rx_ms)', 'node only', C('node_rx_to_output_ms'),
      'the real actuation latency on the node'),
    metric('5i', 'Node ACK sent → broker (recv − ts_ms)', 'node + laptop', C('ack_send_to_broker_ms')),
    metric('5j', 'Dashboard command: click → output switched (act_ms − click)', 'laptop + node', C('click_to_output_ms'),
      'end to end, button press to the output physically changing'),
    metric('5k', 'Auto-off error (completed act_ms − started act_ms − dur)', 'node only', C('auto_off_error_ms'),
      'TC-3.4 shutoff accuracy; only with --cmd-dur-ms'),
  ];
  const counts = {
    telemetry_messages: stats.tlm, live_first_copies: tlmRows.length, after_reconnect: tlmRows.length - clean.length,
    without_ts_ms: stats.tlmSkippedNoTs, unset_node_clock: stats.tlmSkippedOld,
    found_in_database: tlmRows.filter((r) => r.api_seen_ms != null).length,
    not_in_database: tlmRows.filter((r) => /^not in|^node not/.test(r.db)).length,
    still_waiting: tlmRows.filter((r) => r.db === 'waiting').length,
    pings_sent: stats.pings, ping_replies: stats.pongs, ping_timeouts: stats.timeouts,
    ping_replies_stale: pingRows.filter((p) => p.stale).length,
    ping_loss_pct: stats.pings ? r1((100 * stats.timeouts) / stats.pings) : null,
    api_requests: apiPolls.length, api_errors: apiPolls.filter((p) => p.status !== 200 && p.status !== 201).length,
    commands_sent: cmds.length, commands_acked: cmds.filter((c) => c.ack_ms != null).length,
  };
  const meta = {
    started: STARTED.toISOString(), updated: new Date().toISOString(), host: os.hostname(), client_id: clientId,
    nodes: ONLY ? [...ONLY] : [...nodes.keys()], ping_s: PING_MS / 1000, ping_qos: 0,
    ping_stale_after_ms: STALE_PING_MS, api_poll_s: POLL_MS / 1000,
    cmd_test: CMD_N ? `${CMD_N} × ${CMD_ACT}, ${CMD_GAP_MS / 1000} s apart${CMD_DUR_MS ? `, timed ${CMD_DUR_MS} ms` : ', ON/OFF'}` : 'off',
    api: BACKEND ? API : 'off (no LATENCY_API_* settings)', laptop_ntp_synchronized: ntpSynced,
    supabase_clock_offset_ms: r1(off), units: 'milliseconds unless the column says otherwise',
  };
  return { meta, counts, summary, telemetry: tlm, pings: pingRows, commands: cmds, clock: clockRows,
           api_requests: apiPolls.slice(-50_000), events };
}

function writeReport() {
  const rep = buildReport();
  fs.writeFileSync(`${REPORT}.json`, JSON.stringify(rep, null, 2));
  const table = (rows) => {
    if (!rows.length) return [['(none)']];
    const keys = Object.keys(rows.reduce((a, r) => Object.assign(a, r), {}));
    return [keys, ...rows.map((r) => keys.map((k) => r[k] ?? null))];
  };
  const cols = ['id', 'metric', 'n', 'median', 'mean', 'sd', 'p95', 'p99', 'min', 'max', 'clock', 'note'];
  const summarySheet = [
    ['SENSEable latency test — all times in ms'], [],
    ...Object.entries(rep.meta).map(([k, v]) => [k, Array.isArray(v) ? v.join(', ') : v]), [],
    cols, ...rep.summary.map((m) => cols.map((c) => m[c] ?? null)), [],
    ['Counts'], ...Object.entries(rep.counts),
  ];
  fs.writeFileSync(`${REPORT}.xlsx`, xlsx([
    { name: 'Summary', rows: summarySheet, widths: [8, 52, 8, 10, 10, 10, 10, 10, 10, 10, 18, 70] },
    { name: 'Telemetry', rows: table(rep.telemetry) },
    { name: 'Pings', rows: table(rep.pings) },
    { name: 'Commands', rows: table(rep.commands) },
    { name: 'API requests', rows: table(rep.api_requests.map((p) => ({ at: new Date(p.t1).toISOString(), ...p }))) },
    { name: 'Clock', rows: table(rep.clock) },
    { name: 'Events', rows: table(rep.events.map((e) => ({ at: new Date(e.at_ms).toISOString(), ...e }))) },
  ]));
  return rep;
}

// Minimal .xlsx writer (Office Open XML in a zip): inline strings, numbers,
// column widths. Enough for Excel, LibreOffice and Google Sheets to open.
function xlsx(sheets) {
  const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '');
  const col = (i) => { let s = ''; for (i++; i; i = Math.floor((i - 1) / 26)) s = String.fromCharCode(65 + ((i - 1) % 26)) + s; return s; };
  const c = (v, ref) => (v == null || v === '' ? ''
    : typeof v === 'number' && Number.isFinite(v) ? `<c r="${ref}"><v>${v}</v></c>`
    : `<c r="${ref}" t="inlineStr"><is><t xml:space="preserve">${esc(v)}</t></is></c>`);
  const sheetXml = ({ rows, widths }) =>
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">' +
    `<cols>${(widths ?? (rows[0] ?? []).map(() => 16)).map((w, i) => `<col min="${i + 1}" max="${i + 1}" width="${w}" customWidth="1"/>`).join('')}</cols>` +
    `<sheetData>${rows.map((r, ri) => `<row r="${ri + 1}">${r.map((v, ci) => c(v, col(ci) + (ri + 1))).join('')}</row>`).join('')}</sheetData></worksheet>`;
  const ns = 'http://schemas.openxmlformats.org';
  const files = [
    ['[Content_Types].xml', `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="${ns}/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>${sheets.map((_, i) => `<Override PartName="/xl/worksheets/sheet${i + 1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`).join('')}</Types>`],
    ['_rels/.rels', `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="${ns}/package/2006/relationships"><Relationship Id="rId1" Type="${ns}/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`],
    ['xl/workbook.xml', `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="${ns}/spreadsheetml/2006/main" xmlns:r="${ns}/officeDocument/2006/relationships"><sheets>${sheets.map((s, i) => `<sheet name="${esc(s.name)}" sheetId="${i + 1}" r:id="rId${i + 1}"/>`).join('')}</sheets></workbook>`],
    ['xl/_rels/workbook.xml.rels', `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="${ns}/package/2006/relationships">${sheets.map((_, i) => `<Relationship Id="rId${i + 1}" Type="${ns}/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet${i + 1}.xml"/>`).join('')}</Relationships>`],
    ...sheets.map((s, i) => [`xl/worksheets/sheet${i + 1}.xml`, sheetXml(s)]),
  ];
  return zip(files.map(([name, text]) => [name, Buffer.from(text, 'utf8')]));
}

// Zip (deflate) with the central directory, written by hand: Node has deflate
// and CRC-32 built in, so this needs no dependency.
function zip(entries) {
  const crc32 = zlib.crc32 ?? ((buf) => {
    let c = ~0;
    for (const b of buf) { c ^= b; for (let k = 0; k < 8; k++) c = (c >>> 1) ^ (0xedb88320 & -(c & 1)); }
    return ~c >>> 0;
  });
  const local = [], central = [];
  let offset = 0;
  for (const [name, data] of entries) {
    const comp = zlib.deflateRawSync(data), nm = Buffer.from(name), crc = crc32(data);
    const h = Buffer.alloc(30);
    h.writeUInt32LE(0x04034b50, 0); h.writeUInt16LE(20, 4); h.writeUInt16LE(8, 8); h.writeUInt16LE(0x21, 12);
    h.writeUInt32LE(crc, 14); h.writeUInt32LE(comp.length, 18); h.writeUInt32LE(data.length, 22); h.writeUInt16LE(nm.length, 26);
    const d = Buffer.alloc(46);
    d.writeUInt32LE(0x02014b50, 0); d.writeUInt16LE(20, 4); d.writeUInt16LE(20, 6); d.writeUInt16LE(8, 10); d.writeUInt16LE(0x21, 14);
    d.writeUInt32LE(crc, 16); d.writeUInt32LE(comp.length, 20); d.writeUInt32LE(data.length, 24); d.writeUInt16LE(nm.length, 28);
    d.writeUInt32LE(offset, 42);
    local.push(h, nm, comp); central.push(d, nm);
    offset += 30 + nm.length + comp.length;
  }
  const cd = Buffer.concat(central), end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0); end.writeUInt16LE(entries.length, 8); end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(cd.length, 12); end.writeUInt32LE(offset, 16);
  return Buffer.concat([...local, cd, end]);
}

// ── Console summary every minute; report every 5 min and on exit ─────────────
function summary() {
  const rep = buildReport();
  const show = (id) => { const m = rep.summary.find((x) => x.id === id); return m.n ? `${m.median} ms median, p95 ${m.p95} (n=${m.n})` : '—'; };
  console.log(`\n${new Date().toISOString()}  tlm ${stats.tlm}  pings ${stats.pings} → ${stats.pongs} replies, ${stats.timeouts} timeouts`);
  console.log(`  ① node → broker        ${show('1')}`);
  console.log(`  ④ ping round trip      ${show('4a')}`);
  if (BACKEND) {
    console.log(`  ② broker → database    ${show('2')}`);
    console.log(`  ③ broker → API visible ${show('3b')}`);
    console.log(`    API response         ${show('3d')}`);
    if (CMD_N) {
      console.log(`  ⑤ click → ACK in API   ${show('5f')}`);
      console.log(`    click → output       ${show('5j')}`);
      if (CMD_DUR_MS) console.log(`    auto-off error       ${show('5k')}`);
    }
  }
  console.log(`  report: ${REPORT}.xlsx\n`);
}
setInterval(summary, 60_000);
setInterval(() => { try { writeReport(); } catch (e) { event('report_failed', e.message); } }, REPORT_EVERY_MS);

let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  running = false;
  // Pings still in flight are written too, so every ping sent is in the file.
  for (const [cid, p] of pending) {
    const row = { t0: p.t0, nid: p.nid, tid: p.tid, cid, outcome: 'no_reply_at_stop' };
    pingRows.push(row); writePing(row);
  }
  chronySnapshot('stop');
  event('stopped');
  writeReport();
  summary();
  client.end(false, () => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
if (DURATION_MS > 0) setTimeout(stop, DURATION_MS);

console.log(`Listening as ${clientId}; writing to ${OUT}. Ctrl-C to stop.`);
console.log(BACKEND ? `Backend hops on: ${API}, polling every ${POLL_MS / 1000} s.`
                    : 'Backend hops off: set LATENCY_API_BASE, LATENCY_SUPABASE_URL, LATENCY_SUPABASE_ANON, LATENCY_EMAIL, LATENCY_PASSWORD to measure Lambda, database and Vercel.');
if (BACKEND) {
  backendLoop().catch((e) => event('backend_failed', e.message));
  if (CMD_N) commandTest().catch((e) => event('cmd_test_failed', e.message));
}
