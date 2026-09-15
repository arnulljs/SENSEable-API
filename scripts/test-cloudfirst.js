// scripts/test-cloudfirst.js ────────────────────────────────────────────────
// End-to-end proof for the cloud-first topology, run against two REAL Postgres
// databases (edge + cloud). No mocks: it drives the actual ingest pipeline, the
// actual provisioning code, and the actual sync worker.
//
//   TIER=cloud DATABASE_URL_OWNER=<cloud> node scripts/test-cloudfirst.js --cloud-ingest
//   node scripts/test-cloudfirst.js --edge-failover
//   node scripts/test-cloudfirst.js --edge-mirror
//
// Each mode runs in its own process because store.js hydrates one database per
// process. scripts/run-cloudfirst-tests.sh drives all of them and asserts.

import 'dotenv/config';
import { hydrate, store, projectDevices } from '../src/store.js';
import { ingestTelemetry } from '../src/ingest.js';
import { closePool } from '../db/pool.js';

const args = new Set(process.argv.slice(2));
const TID = process.env.TEST_TID ?? 'tenant-123';
const NID = process.env.TEST_NID ?? 'N001';

// A frozen-protocol telemetry packet: raw ADC counts only, two boards.
// `ts` is supplied so both tiers agree on the sample instant — that is what
// makes (port_id, ts) a usable dedupe key.
function packet(tsMs, counts = [1200, 8400, 15000]) {
  return {
    t: 'tlm', v: 1, tid: TID, nid: NID, ts: tsMs,
    adc: [
      { a: '0x48', p: [[0, counts[0], 0], [1, counts[1], 0], [2, counts[2], 0]] },
      { a: '0x4a', p: [[0, counts[0] + 7, 0]] },
    ],
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  await hydrate();

  const tenant = store.tenantByMqttTid?.[TID];
  if (!tenant) {
    console.error(`FAIL: tid '${TID}' is not mapped to a tenant in this database`);
    process.exit(1);
  }

  const base = Number(process.env.TEST_TS_BASE ?? Date.parse('2026-09-15T00:00:00Z'));
  const origin = args.has('--edge-failover') ? 'local' : 'cloud';
  const n = Number(process.env.TEST_PACKETS ?? 3);

  for (let i = 0; i < n; i += 1) {
    const res = await ingestTelemetry(packet(base + i * 1000, [1200 + i, 8400 + i, 15000 + i]),
      { origin });
    if (!res.ok) { console.error('FAIL: ingest rejected —', res.error); process.exit(1); }
    console.log(`ingested ts=${new Date(base + i * 1000).toISOString()} origin=${origin} ` +
                `matched=${res.matched} provisioned=${res.provisioned}`);
  }

  // Writes inside ingest are fire-and-forget by design (the hot path must not
  // block on the database). Give them a moment before the process exits.
  await sleep(600);

  const devs = projectDevices(tenant.id);
  console.log(`projected devices=${devs.length} modules=${devs[0]?.modules?.length ?? 0} ` +
              `ports=${devs[0]?.modules?.reduce((a, m) => a + m.ports.length, 0) ?? 0}`);

  await closePool();
}

main().catch((e) => { console.error('FAIL:', e); process.exit(1); });
