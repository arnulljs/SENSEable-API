// latency-listener.js ────────────────────────────────────────────────────────
// Standalone latency recorder for the thesis measurements. Runs on the on-site
// laptop, apart from the backend, and changes nothing in the database.
//
//   node scripts/latency-listener.js                     all nodes it hears
//   node scripts/latency-listener.js --nodes NODE-C045FC --ping-s 10 --out ./latency-out
//
// WHAT IT MEASURES
//   tlm_latency.csv  node send → this listener's receipt, one row per telemetry
//                    message: received_ms − ts_ms. Needs both clocks right: the
//                    node's from SNTP (rows without ts_ms are skipped and
//                    counted), this laptop's from NTP (checked at start).
//   ping_rtt.csv     every --ping-s seconds, per node: a `ping` command carrying
//                    t0; the node echoes t0 in its ack. Round trip =
//                    receipt − t0 on ONE clock, so it needs no sync at all. With
//                    the node's rx_ms/ts_ms the trip splits into downlink, time
//                    on the node, and uplink. A ping with no reply in 30 s is
//                    logged as a timeout.
//   mqtt_events.csv  connects, drops, reconnects and errors, so a gap in the
//                    other two files can be explained.
//
// The receipt time is taken FIRST in the message handler, before parsing, so
// nothing this script does is counted as network time. Retained and replayed
// (r:1) messages are not live and are skipped.
//
// It uses its own login and client ID with a clean session, so it never
// receives a backlog queued for anyone else and never takes over another
// client's session. Login from .env.latency:
//   LATENCY_MQTT_URL=mqtts://<cluster>.s1.eu.hivemq.cloud:8883
//   LATENCY_MQTT_USERNAME=latency-listener
//   LATENCY_MQTT_PASSWORD=...

import dotenv from 'dotenv';
import mqtt from 'mqtt';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';

dotenv.config({ path: process.env.LATENCY_ENV ?? '.env.latency', override: true });

const argv = process.argv.slice(2);
const arg = (name, dflt) => { const i = argv.indexOf(`--${name}`); return i >= 0 ? argv[i + 1] : dflt; };
const ONLY = arg('nodes', '') ? new Set(arg('nodes').split(',').map((s) => s.trim()).filter(Boolean)) : null;
const PING_MS = Number(arg('ping-s', '10')) * 1000;
const PING_TIMEOUT_MS = 30_000;
const NODE_IDLE_MS = 5 * 60_000;            // stop pinging a node not heard from in 5 min
const OUT = path.resolve(arg('out', './latency-out'));
const URL = process.env.LATENCY_MQTT_URL;
const BASE = process.env.MQTT_TOPIC_BASE ?? 'usc/thesis';

if (!URL || !process.env.LATENCY_MQTT_USERNAME || !process.env.LATENCY_MQTT_PASSWORD) {
  console.error('Set LATENCY_MQTT_URL, LATENCY_MQTT_USERNAME, LATENCY_MQTT_PASSWORD in .env.latency');
  process.exit(2);
}
if (!(PING_MS >= 1000)) { console.error('--ping-s must be at least 1'); process.exit(2); }

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

const writeTlm = csv('tlm_latency.csv', ['received_ms', 'received_iso', 'nid', 'tid', 'ts_ms', 'latency_ms',
  'sync_age_s', 'net', 'rt', 'win_ms']);
const writePing = csv('ping_rtt.csv', ['t0_ms', 'received_ms', 'nid', 'tid', 'cid', 'outcome', 'rtt_ms',
  'downlink_ms', 'on_node_ms', 'uplink_ms', 'rx_ms', 'ts_ms', 'sync_age_s', 'net', 'rt']);
const writeEvent = csv('mqtt_events.csv', ['at_ms', 'at_iso', 'event', 'detail']);
const event = (e, detail = '') => {
  const at = Date.now();
  writeEvent({ at_ms: at, at_iso: new Date(at).toISOString(), event: e, detail });
  console.log(`[${new Date(at).toISOString()}] ${e}${detail ? ` — ${detail}` : ''}`);
};

// ── Laptop clock check: every latency row depends on it ─────────────────────
try {
  const synced = execFileSync('timedatectl', ['show', '-p', 'NTPSynchronized', '--value'], { encoding: 'utf8' }).trim();
  if (synced !== 'yes') {
    console.warn('WARNING: this laptop\'s clock is not NTP-synchronized (timedatectl NTPSynchronized=' +
                 `${synced}). tlm latency will be off by the clock error. Fix: sudo timedatectl set-ntp true`);
  }
  event('clock_check', `NTPSynchronized=${synced}`);
} catch {
  event('clock_check', 'timedatectl unavailable — confirm NTP sync manually');
}

// ── State ────────────────────────────────────────────────────────────────────
const nodes = new Map();      // nid → { tid, lastHeard }
const pending = new Map();    // cid → { nid, tid, t0 }
const stats = { tlm: 0, tlmSkippedNoTs: 0, tlmSkippedOld: 0, pings: 0, pongs: 0, timeouts: 0 };
const latencies = new Map();  // nid → [latency_ms]
const rtts = new Map();       // nid → [rtt_ms]
const push = (m, k, v) => { if (!m.has(k)) m.set(k, []); m.get(k).push(v); };

const clientId = `senseable-latency-${os.hostname()}-${randomUUID().slice(0, 6)}`;
const client = mqtt.connect(URL, {
  username: process.env.LATENCY_MQTT_USERNAME,
  password: process.env.LATENCY_MQTT_PASSWORD,
  clientId, clean: true, protocolVersion: 5,
  reconnectPeriod: 2000, connectTimeout: 15_000, keepalive: 30,
});

client.on('connect', () => {
  event('connected', clientId);
  client.subscribe([`${BASE}/+/+/tlm`, `${BASE}/+/+/ack`], { qos: 1 }, (err, granted) => {
    const bad = err ? err.message : granted?.filter((g) => g.qos >= 128).map((g) => g.topic).join(' ');
    event(bad ? 'subscribe_failed' : 'subscribed', bad || `${BASE}/+/+/tlm, ${BASE}/+/+/ack`);
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

  if (kind === 'tlm') {
    nodes.set(nid, { tid, lastHeard: received });
    if (pkt.r === 1 || pkt.r === true) return;     // spool replay: old by design
    const tsMs = num(pkt.ts_ms);
    if (tsMs == null) { stats.tlmSkippedNoTs++; return; }   // node clock not SNTP-synced
    if (tsMs < Date.UTC(2026, 0, 1)) { stats.tlmSkippedOld++; return; }
    const latency = received - tsMs;
    stats.tlm++;
    push(latencies, nid, latency);
    writeTlm({ received_ms: received, received_iso: new Date(received).toISOString(), nid, tid,
               ts_ms: tsMs, latency_ms: latency, sync_age_s: num(pkt.sync_age_s), net: pkt.net,
               rt: pkt.rt, win_ms: num(pkt.win_ms) });
    return;
  }

  if (kind === 'ack' && pkt.action === 'ping') {
    const p = pending.get(pkt.cid);
    if (!p) return;                                // not ours (another listener)
    pending.delete(pkt.cid);
    nodes.set(nid, { tid, lastHeard: received });
    const t0 = num(pkt.t0) ?? p.t0;
    const rx = num(pkt.rx_ms), ts = num(pkt.ts_ms);
    const rtt = received - t0;
    stats.pongs++;
    push(rtts, nid, rtt);
    writePing({ t0_ms: t0, received_ms: received, nid, tid, cid: pkt.cid, outcome: 'reply', rtt_ms: rtt,
                downlink_ms: rx != null ? rx - t0 : null, on_node_ms: rx != null && ts != null ? ts - rx : null,
                uplink_ms: ts != null ? received - ts : null, rx_ms: rx, ts_ms: ts,
                sync_age_s: num(pkt.sync_age_s), net: pkt.net, rt: pkt.rt });
  }
});

// ── Pinging ──────────────────────────────────────────────────────────────────
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
      { qos: 1 });
  }
  for (const [cid, p] of pending) {
    if (now - p.t0 > PING_TIMEOUT_MS) {
      pending.delete(cid);
      stats.timeouts++;
      writePing({ t0_ms: p.t0, nid: p.nid, tid: p.tid, cid, outcome: 'timeout' });
    }
  }
}
setInterval(pingAll, PING_MS);

// ── Summary every minute and on exit ─────────────────────────────────────────
const pct = (arr, q) => { const s = [...arr].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(q * s.length))]; };
function summary() {
  console.log(`\n${new Date().toISOString()}  tlm rows ${stats.tlm} (skipped: ${stats.tlmSkippedNoTs} without ts_ms, ` +
              `${stats.tlmSkippedOld} with an unset clock)  pings ${stats.pings} → ${stats.pongs} replies, ${stats.timeouts} timeouts`);
  for (const nid of new Set([...latencies.keys(), ...rtts.keys()])) {
    const l = latencies.get(nid) ?? [], r = rtts.get(nid) ?? [];
    console.log(`  ${nid}: tlm latency median ${l.length ? pct(l, 0.5) : '—'} ms, p95 ${l.length ? pct(l, 0.95) : '—'} ms (n=${l.length})` +
                ` | ping RTT median ${r.length ? pct(r, 0.5) : '—'} ms, p95 ${r.length ? pct(r, 0.95) : '—'} ms (n=${r.length})`);
  }
  console.log(`  files: ${OUT}\n`);
}
setInterval(summary, 60_000);
function stop() {
  // Pings still in flight are written too, so every ping sent is in the file.
  for (const [cid, p] of pending) writePing({ t0_ms: p.t0, nid: p.nid, tid: p.tid, cid, outcome: 'no_reply_at_stop' });
  summary(); event('stopped'); client.end(false, () => process.exit(0));
}
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
console.log(`Listening as ${clientId}; writing to ${OUT}. Ctrl-C to stop.`);
