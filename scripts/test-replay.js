// scripts/test-replay.js ────────────────────────────────────────────────────
// Proves a FIFO replay burst backfills history WITHOUT rewriting the present.
//
// The firmware buffers samples to LittleFS during a WAN outage and pushes them
// on recovery, tagged "r":1. Those readings are real and belong in the time
// series, but they are not the current state of the tank. Without the guard, a
// replay walks the dashboard through history: the gauge shows an outage-era
// value, and alerts fire for conditions that already resolved.
import 'dotenv/config';
import { hydrate, findNodeScoped, findPortByChannel } from '../src/store.js';
import { ingestTelemetry } from '../src/ingest.js';
import { adminPool, closePool } from '../db/pool.js';

let pass = 0, fail = 0;
const is = (n, g, w) => { const ok = String(g) === String(w);
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${n}${ok ? '' : ` (got ${g}, want ${w})`}`); ok ? pass++ : fail++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// The tid must be one tenants.mqtt_tid actually maps, or strict ingest rejects
// every packet. Hardcoding 'tenant-123' broke the moment the bench tenant was
// re-mapped, so it is taken from the database after hydrate() unless
// REPLAY_TID overrides it.
let TID = process.env.REPLAY_TID ?? null;
const NID = 'N-REPLAY';
const pkt = (tsSec, raw, replay) => ({
  t: 'tlm', v: 1, tid: TID, nid: NID, ts: tsSec,
  adc: [{ a: '0x48', p: [[0, raw, 0]] }],
  ...(replay ? { r: 1 } : {}),
});

await hydrate();
const { store } = await import('../src/store.js');
TID ??= Object.keys(store.tenantByMqttTid)[0] ?? null;
if (!TID || !store.tenantByMqttTid[TID]) {
  console.error(`  no usable tid: tenants.mqtt_tid maps [${Object.keys(store.tenantByMqttTid).join(', ')}]` +
                (process.env.REPLAY_TID ? `, REPLAY_TID='${process.env.REPLAY_TID}' is not among them` : ''));
  await closePool();
  process.exit(1);
}
console.log(`  using tid '${TID}' (tenant ${store.tenantByMqttTid[TID].slug})`);
const nowSec = Math.floor(Date.now() / 1000);

// Live sample: the tank is fine right now.
await ingestTelemetry(pkt(nowSec, 9000), { origin: 'cloud' });
await sleep(400);

// Read the live state straight out of the in-memory store, which is what the
// dashboard projection is built from.
const portOf = () => findPortByChannel(findNodeScoped(TID, NID), '0x48', 0);
const live = portOf();
if (!live) {
  console.error(`  FAIL  live sample was not ingested for ${TID}/${NID} — nothing else can be checked`);
  await closePool();
  process.exit(1);
}
is('live sample sets the current value', live.value != null, true);
const current = live.value;
const historyLen = live.history.length;

// Replay burst: three hours of buffered samples at a very different raw value.
for (let i = 0; i < 5; i += 1) {
  const r = await ingestTelemetry(pkt(nowSec - 10800 + i * 600, 1500, true), { origin: 'cloud' });
  is(`replay packet ${i + 1} flagged`, r.replay, true);
}
await sleep(600);

const after = portOf();
is('gauge still shows the LIVE value', after.value, current);
is('live history ring untouched', after.history.length, historyLen);

const { rows } = await adminPool.query(
  `SELECT count(*)::int n FROM readings r
     JOIN ports p ON p.port_id = r.port_id
     JOIN modules m ON m.module_id = p.module_id
     JOIN devices d ON d.device_id = m.device_id
    WHERE d.node_id = $1 AND r.raw_adc = 1500`, [NID]);
is('all 5 replayed readings persisted', rows[0].n, 5);

const { rows: last } = await adminPool.query(
  `SELECT p.last_value FROM ports p
     JOIN modules m ON m.module_id = p.module_id
     JOIN devices d ON d.device_id = m.device_id
    WHERE d.node_id = $1 AND p.port_code = 'A0'`, [NID]);
is('ports.last_value not overwritten by history', Number(last[0].last_value), Number(current));

await adminPool.query(
  `DELETE FROM devices WHERE node_id = $1`, [NID]);
console.log(`\n  ${pass} passed, ${fail} failed`);
await closePool();
process.exit(fail ? 1 : 0);
