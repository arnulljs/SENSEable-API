// scripts/assign-tid.js ─────────────────────────────────────────────────────
// Tier 1 tool: give an organization its Device Tenant ID in the standard scheme,
// and retire a board's pin once its setup portal carries that tid. No SQL.
//
//   node scripts/assign-tid.js                       list organizations and their tids
//   node scripts/assign-tid.js assign aquatech llba  give each its tid, print the hand-over
//   node scripts/assign-tid.js assign --missing      every organization that has none
//   node scripts/assign-tid.js unpin NODE-C045FC     drop a pin once the board uses its org's tid
//
// SCHEME: the Tenant ID Naming Scheme Specification, derived from the
// organization's NAME, e.g. "Laguna Lake Development Authority" → llda-add4.
// Same code as api/_tid.js in the web repo (which registration uses); keep the
// two in step. To get a different tid, change the organization's name first.
//
// WHERE IT WRITES: the CLOUD database only. tenants and pins replicate, so the
// sync worker carries the change to the edge (within ~30 s) and the edge's
// routing refresh applies it (within ~15 s); the Lambda reads it on its next
// run. Writing both tiers separately would make them fight over the row.
//
// CONTINUITY: changing a tid would cut off every board still publishing on the
// old one. Such boards are pinned to their organization first, so they keep
// working until someone updates their portal; `unpin` then refuses until the
// board is actually publishing on the new tid.
import 'dotenv/config';
import dotenv from 'dotenv';
import pg from 'pg';
import { createHash } from 'node:crypto';

dotenv.config({ path: '.env.cloud', override: true });
if (!process.env.CLOUD_DATABASE_URL_OWNER) {
  console.error('CLOUD_DATABASE_URL_OWNER is not set — run this from senseable-api with .env.cloud present');
  process.exit(1);
}
const db = new pg.Pool({
  connectionString: process.env.CLOUD_DATABASE_URL_OWNER,
  ssl: process.env.CLOUD_DB_NO_SSL === 'true' ? false : { rejectUnauthorized: false },
  max: 2, connectionTimeoutMillis: 15_000,
});

const NOISE = new Set(['inc', 'corp', 'corporation', 'llc', 'ltd', 'co', 'company', 'systems',
  'group', 'the', 'of', 'and', 'solutions', 'services']);
export function generateTid(orgName) {
  const raw = String(orgName ?? '').toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
  const all = raw.split(/\s+/).filter(Boolean);
  const kept = all.filter((w) => !NOISE.has(w));
  const words = kept.length ? kept : all;
  let prefix = (words.length >= 3 ? words.map((w) => w[0]).join('') : words[0] ?? '').slice(0, 12);
  if (prefix.length < 2) prefix = all.join('').slice(0, 12);
  if (prefix.length < 2) return null;
  return `${prefix}-${createHash('sha256').update(raw).digest('hex').slice(0, 4)}`;
}

async function list() {
  const { rows } = await db.query(`
    SELECT t.slug, t.name, t.mqtt_tid,
           (SELECT count(*)::int FROM devices d WHERE d.tenant_id = t.tenant_id) AS boards,
           (SELECT count(*)::int FROM node_tenant_assignments a WHERE a.tenant_id = t.tenant_id) AS pinned
      FROM tenants t ORDER BY t.slug`);
  console.log('organization'.padEnd(28), 'tenant id'.padEnd(24), 'scheme', ' boards  pinned  name → tid');
  for (const r of rows) {
    const want = generateTid(r.name);
    const ok = !r.mqtt_tid ? 'NONE' : r.mqtt_tid === want ? 'ok' : 'old';
    console.log(r.slug.padEnd(28), String(r.mqtt_tid ?? '—').padEnd(24), ok.padEnd(6), String(r.boards).padStart(7),
                String(r.pinned).padStart(7), ` "${r.name}" → ${want ?? 'name too short'}`);
  }
}

async function assign(slugs) {
  for (const slug of slugs) {
    const { rows: [t] } = await db.query('SELECT tenant_id, slug, name, mqtt_tid FROM tenants WHERE slug = $1', [slug]);
    if (!t) { console.error(`\n✗ no organization '${slug}'`); process.exitCode = 1; continue; }
    const tid = generateTid(t.name);
    if (!tid) { console.error(`\n✗ ${slug}: name "${t.name}" has too few letters/digits for a tid`); process.exitCode = 1; continue; }
    if (tid === t.mqtt_tid) { console.log(`\n· ${t.name} (${slug}) already has ${tid}`); continue; }
    const { rows: [clash] } = await db.query('SELECT slug FROM tenants WHERE mqtt_tid = $1', [tid]);
    if (clash) {
      console.error(`\n✗ ${slug}: "${t.name}" gives ${tid}, already used by '${clash.slug}'. Rename one of them first.`);
      process.exitCode = 1; continue;
    }

    const c = await db.connect();
    let pinnedNow = [];
    try {
      await c.query('BEGIN');
      // Boards on the old tid that only reach this org through that tid: pin
      // them first so the change can't cut them off.
      if (t.mqtt_tid) {
        const { rows } = await c.query(`
          INSERT INTO node_tenant_assignments (node_id, tenant_id, note)
          SELECT d.node_id, d.tenant_id, 'kept routing while its portal moves to the new tenant id'
            FROM devices d
           WHERE d.tenant_id = $1 AND d.wire_tid = $2
          ON CONFLICT (node_id) DO NOTHING
          RETURNING node_id`, [t.tenant_id, t.mqtt_tid]);
        pinnedNow = rows.map((r) => r.node_id);
      }
      await c.query('UPDATE tenants SET mqtt_tid = $2 WHERE tenant_id = $1', [t.tenant_id, tid]);
      await c.query('COMMIT');
    } catch (e) {
      await c.query('ROLLBACK').catch(() => {});
      throw e;
    } finally { c.release(); }

    console.log(`\n✓ ${t.name} (${slug})`);
    console.log(`    Device Tenant ID: ${tid}${t.mqtt_tid ? `   (was ${t.mqtt_tid})` : ''}`);
    const { rows: boards } = await db.query(`
      SELECT d.node_id, d.wire_tid, (a.node_id IS NOT NULL) AS pinned
        FROM devices d LEFT JOIN node_tenant_assignments a
          ON a.node_id = d.node_id AND a.tenant_id = d.tenant_id
       WHERE d.tenant_id = $1 ORDER BY d.node_id`, [t.tenant_id]);
    for (const b of boards) {
      const why = pinnedNow.includes(b.node_id) ? 'pinned just now so it keeps working'
        : b.pinned ? 'pinned, keeps working' : 'not pinned';
      console.log(`    ${b.node_id}: publishes on '${b.wire_tid ?? 'not heard yet'}' (${why}).` +
                  ` Enter '${tid}' in its setup portal, then: node scripts/assign-tid.js unpin ${b.node_id}`);
    }
  }
}

async function unpin(nodes, force) {
  for (const nid of nodes) {
    const { rows: [p] } = await db.query(`
      SELECT a.node_id, t.slug, t.mqtt_tid, d.wire_tid
        FROM node_tenant_assignments a
        JOIN tenants t ON t.tenant_id = a.tenant_id
        LEFT JOIN devices d ON d.node_id = a.node_id AND d.tenant_id = a.tenant_id
       WHERE a.node_id = $1`, [nid]);
    if (!p) { console.log(`· ${nid}: not pinned, nothing to do`); continue; }
    if (p.wire_tid !== p.mqtt_tid && !force) {
      console.error(`✗ ${nid}: still publishes on '${p.wire_tid ?? 'not heard yet'}', not ${p.slug}'s ` +
                    `'${p.mqtt_tid}'. Without the pin its data would be rejected. Enter '${p.mqtt_tid}' in ` +
                    'its setup portal and wait a minute for it to report in (or pass --force).');
      process.exitCode = 1;
      continue;
    }
    await db.query('DELETE FROM node_tenant_assignments WHERE node_id = $1', [nid]);
    console.log(`✓ ${nid}: unpinned — it reaches ${p.slug} through its tenant id '${p.mqtt_tid}' now`);
  }
}

const [cmd, ...rest] = process.argv.slice(2);
const args = rest.filter((a) => !a.startsWith('--'));
try {
  if (!cmd || cmd === 'list') await list();
  else if (cmd === 'assign') {
    const slugs = rest.includes('--missing')
      ? (await db.query('SELECT slug FROM tenants WHERE mqtt_tid IS NULL ORDER BY slug')).rows.map((r) => r.slug)
      : args;
    if (!slugs.length) console.log('nothing to assign: name organizations by slug, or pass --missing');
    await assign(slugs);
    console.log('\nThe edge picks this up within about a minute (sync, then routing refresh).');
  } else if (cmd === 'unpin') await unpin(args, rest.includes('--force'));
  else { console.error(`unknown command '${cmd}' — use list, assign or unpin`); process.exitCode = 1; }
} finally {
  await db.end();
}
