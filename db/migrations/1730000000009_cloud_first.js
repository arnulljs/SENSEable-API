// 1730000000009_cloud_first.js
// Schema groundwork for the CLOUD-FIRST topology.
//
// Until now the edge server was the only tier that ever *created* a row, so a
// surrogate uuid minted with gen_random_uuid() was globally unique by
// construction: sync.js copied it upward and the two databases agreed.
//
// Cloud-first breaks that assumption. Telemetry now lands in the cloud during
// normal operation and in the edge only during failover, which means EITHER
// tier can be the first to see a new node, board or channel. Two tiers calling
// gen_random_uuid() for the same physical port produce two different ids, and
// then:
//
//   - sync.js pushes `devices` with ON CONFLICT (device_id). The uuid does not
//     collide, so it INSERTs — and trips UNIQUE (tenant_id, node_id) instead.
//     That table then fails on every pass, forever.
//   - `readings` carry a port_id that does not exist on the other side, so the
//     foreign key rejects the whole batch.
//
// This migration removes the possibility rather than detecting it after the
// fact. Every identity becomes a pure function of its natural key, so two tiers
// that have never spoken to each other still derive the same uuid for the same
// physical hardware.
//
//   tenant   = f(slug)
//   device   = f(tenant_id, node_id)
//   module   = f(device_id, i2c_address)
//   port     = f(module_id, port_code)
//   actuator = f(device_id, port)
//
// Existing rows are realigned in place. That is only safe because every foreign
// key is rebuilt with ON UPDATE CASCADE first, so rewriting a parent's primary
// key carries its children along automatically. Run this on BOTH tiers and they
// converge on identical ids with no data movement.
//
// It also adds the bookkeeping the failover path needs:
//
//   readings.origin   where a row was ingested ('cloud' mirror | 'local' failover)
//   readings.synced   false = still owed to the cloud; the backlog queue
//   readings (port_id, ts) UNIQUE   the dedupe key for reconciliation upserts
//   notifications.event_uid         a portable identity so notifications can be
//                                   pushed without carrying a bigint that the
//                                   cloud mints for itself

const CASCADE_FKS = [
  // [table, column, referenced table, referenced column, ON DELETE action]
  ['devices', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['users', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['sensor_profiles', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['calibration_formulas', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['modules', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['ports', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['actuators', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['readings', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['notifications', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['map_profiles', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['map_sensors', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],
  ['commands', 'tenant_id', 'tenants', 'tenant_id', 'CASCADE'],

  ['modules', 'device_id', 'devices', 'device_id', 'CASCADE'],
  ['actuators', 'device_id', 'devices', 'device_id', 'CASCADE'],
  ['commands', 'device_id', 'devices', 'device_id', 'CASCADE'],

  ['ports', 'module_id', 'modules', 'module_id', 'CASCADE'],

  ['readings', 'port_id', 'ports', 'port_id', 'CASCADE'],
  ['map_sensors', 'port_id', 'ports', 'port_id', 'CASCADE'],
  ['notifications', 'port_id', 'ports', 'port_id', 'SET NULL'],

  ['ports', 'formula_id', 'calibration_formulas', 'formula_id', 'SET NULL'],
  ['map_profiles', 'created_by', 'users', 'user_id', 'SET NULL'],
];

// ── Realignment is OPT-IN ────────────────────────────────────────────────────
// Rewriting an existing row's primary key cascades. Changing tenants.tenant_id
// carries readings.tenant_id on EVERY row; changing ports.port_id carries
// readings.port_id on every row again. On an empty database that is free. On a
// deployment with real telemetry history it is a full rewrite of the largest
// table in the database, several times over, inside one transaction — which is
// exactly the kind of statement a managed platform kills mid-flight.
//
// And it is usually unnecessary. If the two tiers have been running the old
// one-way sync, the cloud already holds the ids the edge minted, so they ALREADY
// agree. Determinism is only needed for rows created from here on, and the
// derive_pk() triggers below handle those. Old random ids and new derived ids
// coexist perfectly well: what matters is that both tiers agree on each row, not
// that every row was generated the same way.
//
// Run the realignment only when the two tiers genuinely disagree — a tier seeded
// independently, or one restored from a different backup. Check first:
//
//   SELECT slug, tenant_id FROM tenants ORDER BY slug;   -- on BOTH tiers
//
// If those match, skip it. If they differ, re-run with:
//
//   REALIGN_IDS=true npm run migrate:cloud up
//
// on a maintenance window, with nothing else connected.
const REALIGN = process.env.REALIGN_IDS === 'true';

const SKIP_NOTICE = `
DO $$ BEGIN RAISE NOTICE
  'skipping id realignment (REALIGN_IDS is not true) — existing rows keep their '
  'ids, new rows get derived ones from the triggers below'; END $$;`;

const REALIGN_SQL = `
-- ── 3. Realign existing rows, parents before children ───────────────────────
-- Idempotent: rows already carrying their derived id are skipped. Safe to
-- re-run, and safe to run on a tier that has never held any data.
UPDATE tenants SET tenant_id = senseable_uuid('tenant', slug)
 WHERE tenant_id IS DISTINCT FROM senseable_uuid('tenant', slug);

-- users are seeded independently on each tier, so their random ids diverge and
-- the first replication attempt trips UNIQUE (email). Email is the natural key.
UPDATE users SET user_id = senseable_uuid('user', lower(email::text))
 WHERE user_id IS DISTINCT FROM senseable_uuid('user', lower(email::text));

UPDATE devices SET device_id = senseable_uuid('device', tenant_id::text, node_id)
 WHERE device_id IS DISTINCT FROM senseable_uuid('device', tenant_id::text, node_id);

UPDATE modules SET module_id = senseable_uuid('module', device_id::text, lower(i2c_address))
 WHERE module_id IS DISTINCT FROM senseable_uuid('module', device_id::text, lower(i2c_address));

UPDATE ports SET port_id = senseable_uuid('port', module_id::text, port_code)
 WHERE port_id IS DISTINCT FROM senseable_uuid('port', module_id::text, port_code);

UPDATE actuators SET actuator_id = senseable_uuid('actuator', device_id::text, port)
 WHERE actuator_id IS DISTINCT FROM senseable_uuid('actuator', device_id::text, port);

UPDATE calibration_formulas
   SET formula_id = senseable_uuid('formula', tenant_id::text, label)
 WHERE formula_id IS DISTINCT FROM senseable_uuid('formula', tenant_id::text, label);

UPDATE sensor_profiles
   SET profile_id = senseable_uuid('profile', tenant_id::text, name)
 WHERE profile_id IS DISTINCT FROM senseable_uuid('profile', tenant_id::text, name);

UPDATE map_sensors
   SET map_sensor_id = senseable_uuid('mapsensor', tenant_id::text, port_id::text)
 WHERE map_sensor_id IS DISTINCT FROM senseable_uuid('mapsensor', tenant_id::text, port_id::text);

UPDATE map_profiles
   SET map_profile_id = senseable_uuid('mapprofile', tenant_id::text, name)
 WHERE map_profile_id IS DISTINCT FROM senseable_uuid('mapprofile', tenant_id::text, name);

`;

// NOT VALID is the whole trick here.
//
// ADD CONSTRAINT ... FOREIGN KEY normally validates the constraint by scanning
// every existing row and looking up its parent, while holding a lock that blocks
// the table. Two of these are on `readings`, the largest table in the database,
// and doing twenty-one of them inside one transaction over a remote link is what
// makes this migration appear to hang.
//
// The scan is also pointless. These are the SAME constraints that were already
// in place a moment ago; the only thing changing is the addition of ON UPDATE
// CASCADE. Every existing row was already validated by the constraint being
// replaced, so re-proving it buys nothing.
//
// NOT VALID skips only the check of PRE-EXISTING rows. New and modified rows are
// enforced exactly as before, so the database is no less protected from here on.
// If you want the formal validation anyway, run the migration with
// VALIDATE_FKS=true, or do it later without blocking writes:
//
//   ALTER TABLE readings VALIDATE CONSTRAINT readings_port_id_fkey;
const VALIDATE_FKS = process.env.VALIDATE_FKS === 'true';

const rebuildFk = ([table, col, refTable, refCol, onDelete]) => `
ALTER TABLE ${table} DROP CONSTRAINT IF EXISTS ${table}_${col}_fkey;
ALTER TABLE ${table} ADD CONSTRAINT ${table}_${col}_fkey
  FOREIGN KEY (${col}) REFERENCES ${refTable}(${refCol})
  ON DELETE ${onDelete} ON UPDATE CASCADE NOT VALID;${
  VALIDATE_FKS ? `
ALTER TABLE ${table} VALIDATE CONSTRAINT ${table}_${col}_fkey;` : ''}`;

export const up = (pgm) => {
  pgm.sql(`
-- ── 0. Do not let a managed-platform default kill this mid-flight ───────────
-- Supabase sets a statement_timeout on its roles and its pooler drops a
-- connection that goes quiet. This migration rewrites indexes on the telemetry
-- table, which on a real deployment is the largest thing in the database, so a
-- default timeout aborts it halfway and reports a TCP error rather than a SQL
-- one. SET LOCAL scopes both to this transaction.
--
-- Run this against the DIRECT connection (port 5432), not the transaction
-- pooler (6543). The pooler is for short application queries and will hang up
-- on a long DDL transaction no matter what these settings say.
SET LOCAL statement_timeout = 0;
SET LOCAL idle_in_transaction_session_timeout = 0;
SET LOCAL lock_timeout = '30s';

-- ── 1. Deterministic identity ───────────────────────────────────────────────
-- RFC 4122 v5 (SHA-1, name-based) over a project-private namespace. IMMUTABLE
-- so it can be used in index expressions and constant-folded in bulk updates.
--
-- search_path is pinned because pgcrypto lives in "public" on a self-hosted
-- edge database and in "extensions" on Supabase. Without this the function
-- resolves digest() differently on the two tiers, which would defeat the entire
-- point of the migration.
CREATE OR REPLACE FUNCTION senseable_uuid(kind text, VARIADIC parts text[])
RETURNS uuid
LANGUAGE plpgsql IMMUTABLE STRICT
SET search_path = public, extensions, pg_temp
AS $fn$
DECLARE
  ns   bytea := decode('a7c9e6d21f4b5c8a9e3d6b2f4a8c1d70', 'hex');
  name text  := kind || ':' || array_to_string(parts, '/');
  h    bytea;
BEGIN
  h := digest(ns || convert_to(lower(name), 'utf8'), 'sha1');
  h := set_byte(h, 6, (get_byte(h, 6) & 15) | 80);    -- version 5
  h := set_byte(h, 8, (get_byte(h, 8) & 63) | 128);   -- RFC 4122 variant
  RETURN encode(substring(h from 1 for 16), 'hex')::uuid;
END
$fn$;

COMMENT ON FUNCTION senseable_uuid(text, text[]) IS
  'Name-based UUIDv5 over a fixed namespace. Lets the edge and cloud tiers '
  'independently derive the SAME id for the same physical hardware, which is '
  'what makes cloud-first provisioning collision-free.';

-- ── 2. Foreign keys become ON UPDATE CASCADE ────────────────────────────────
-- Required before step 3: realigning a parent primary key must carry its
-- children. It is also correct permanently — an id that is a function of a
-- natural key changes if the natural key is ever corrected, and the children
-- should follow rather than orphan.
${CASCADE_FKS.map(rebuildFk).join('\n')}

${REALIGN ? REALIGN_SQL : SKIP_NOTICE}

-- ── 3b. Keep new rows deterministic, wherever they are created ──────────────
-- Step 3 realigns rows that already exist. This keeps every FUTURE row aligned
-- without having to find and patch every INSERT site — seed files, migrations,
-- the provisioning path, the dashboard's own writes. A trigger is the only
-- place that covers all of them, because a column DEFAULT cannot reference the
-- other columns of its own row.
--
-- BEFORE INSERT only, deliberately. Deriving on UPDATE too would mean renaming
-- a calibration formula silently changes its identity, which would appear on
-- the other tier as a brand new formula alongside the old one. Identity is
-- agreed once, at creation, and then left alone.
CREATE OR REPLACE FUNCTION derive_pk() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  CASE TG_TABLE_NAME
    WHEN 'tenants' THEN
      NEW.tenant_id := senseable_uuid('tenant', NEW.slug);
    WHEN 'users' THEN
      NEW.user_id := senseable_uuid('user', lower(NEW.email::text));
    WHEN 'devices' THEN
      NEW.device_id := senseable_uuid('device', NEW.tenant_id::text, NEW.node_id);
    WHEN 'modules' THEN
      NEW.module_id := senseable_uuid('module', NEW.device_id::text, lower(NEW.i2c_address));
    WHEN 'ports' THEN
      NEW.port_id := senseable_uuid('port', NEW.module_id::text, NEW.port_code);
    WHEN 'actuators' THEN
      NEW.actuator_id := senseable_uuid('actuator', NEW.device_id::text, NEW.port);
    WHEN 'calibration_formulas' THEN
      NEW.formula_id := senseable_uuid('formula', NEW.tenant_id::text, NEW.label);
    WHEN 'sensor_profiles' THEN
      NEW.profile_id := senseable_uuid('profile', NEW.tenant_id::text, NEW.name);
    WHEN 'map_sensors' THEN
      NEW.map_sensor_id := senseable_uuid('mapsensor', NEW.tenant_id::text, NEW.port_id::text);
    WHEN 'map_profiles' THEN
      NEW.map_profile_id := senseable_uuid('mapprofile', NEW.tenant_id::text, NEW.name);
    WHEN 'commands' THEN
      NEW.command_id := senseable_uuid('command', NEW.tenant_id::text, NEW.cid);
    ELSE
      NULL;
  END CASE;
  RETURN NEW;
END
$fn$;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['tenants','users','devices','modules','ports','actuators',
                           'calibration_formulas','sensor_profiles','map_sensors',
                           'map_profiles','commands'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_derive_pk ON %I;', t, t);
    EXECUTE format(
      'CREATE TRIGGER trg_%s_derive_pk BEFORE INSERT ON %I
         FOR EACH ROW EXECUTE FUNCTION derive_pk();', t, t);
  END LOOP;
END $$;

-- ── 4. Telemetry origin + backlog queue ─────────────────────────────────────
-- origin says WHERE a row entered the system, which the dashboard surfaces so
-- an operator can tell live cloud data from data recovered after an outage.
-- synced is the reconciliation queue: false means the cloud has not got it yet.
-- The column is added DEFAULT TRUE and then flipped to FALSE only for the tail
-- above the old watermark. The obvious way round — default false, then UPDATE
-- every already-replicated row to true — touches every row in the largest table
-- in the database and rewrites it. On a real deployment that is minutes of
-- exclusive work and the first thing a connection timeout kills. Postgres 11+
-- stores a non-volatile column default in the catalogue, so adding the column
-- this way is instant regardless of table size, and only the handful of rows
-- that genuinely still owe the cloud get written.
ALTER TABLE readings
  ADD COLUMN IF NOT EXISTS origin text NOT NULL DEFAULT 'cloud',
  ADD COLUMN IF NOT EXISTS synced boolean NOT NULL DEFAULT true;

-- Future rows are owed to the cloud until proven otherwise, so the RUNTIME
-- default is the opposite of the backfill default.
ALTER TABLE readings ALTER COLUMN synced SET DEFAULT false;

ALTER TABLE readings DROP CONSTRAINT IF EXISTS readings_origin_check;
ALTER TABLE readings ADD CONSTRAINT readings_origin_check
  CHECK (origin IN ('cloud', 'local'));

-- Anything past the old watermark was never replicated, so it is the backlog.
-- If no watermark row exists this tier has never synced and every row is left
-- marked synced; run "npm run sync:backfill" to force a full re-push rather
-- than having a migration decide to move a whole history unasked.
UPDATE readings r SET synced = false
  FROM sync_state s
 WHERE s.table_name = 'readings'
   AND s.last_synced_id IS NOT NULL
   AND r.reading_id > s.last_synced_id
   AND r.synced;

-- The hot query is "what is still owed", so index only those rows. The index
-- shrinks back to nothing as the backlog drains.
CREATE INDEX IF NOT EXISTS ix_readings_unsynced
  ON readings (reading_id) WHERE NOT synced;

-- ── 5. Portable dedupe key for readings ─────────────────────────────────────
-- reading_id is GENERATED ALWAYS AS IDENTITY and is now minted independently on
-- both tiers, so it can no longer be the conflict target for replication. A
-- reading is uniquely identified by its channel and its instant.
--
-- Deduplicate first: a pre-existing pair would make the index creation fail.
--
-- This is a window function over one pass, NOT a self-join. The self-join
-- version (DELETE FROM readings a USING readings b WHERE a.port_id = b.port_id
-- AND a.ts = b.ts AND a.reading_id > b.reading_id) has no index to work with,
-- because the index it would want is the one created on the next line. Postgres
-- plans it as a nested loop over every row pair, which is fine on an empty
-- database and effectively never finishes on a real telemetry table.
DELETE FROM readings
 WHERE ctid IN (
   SELECT ctid FROM (
     SELECT ctid,
            row_number() OVER (PARTITION BY port_id, ts ORDER BY reading_id) AS rn
       FROM readings) ranked
    WHERE rn > 1);

CREATE UNIQUE INDEX IF NOT EXISTS uq_readings_port_ts ON readings (port_id, ts);

-- ── 6. Portable identity for notifications ──────────────────────────────────
-- Same problem, same shape of fix. notification_id is a bigint identity with no
-- natural key, so replication needs an id that travels.
-- gen_random_uuid() is VOLATILE, so this column genuinely does have to be
-- written per row — there is no catalogue shortcut for a value that differs
-- every time. notifications is small (one row per alert, not per sample), so
-- that is acceptable here in a way it would not be on readings.
ALTER TABLE notifications
  ADD COLUMN IF NOT EXISTS event_uid uuid NOT NULL DEFAULT gen_random_uuid(),
  ADD COLUMN IF NOT EXISTS synced boolean NOT NULL DEFAULT true;

ALTER TABLE notifications ALTER COLUMN synced SET DEFAULT false;

CREATE UNIQUE INDEX IF NOT EXISTS uq_notifications_event_uid
  ON notifications (event_uid);

CREATE INDEX IF NOT EXISTS ix_notifications_unsynced
  ON notifications (notification_id) WHERE NOT synced;

UPDATE notifications n SET synced = false
  FROM sync_state s
 WHERE s.table_name = 'notifications'
   AND s.last_synced_id IS NOT NULL
   AND n.notification_id > s.last_synced_id
   AND n.synced;

-- ── 6b. Replication-aware updated_at ────────────────────────────────────────
-- set_updated_at() stamps now() on every UPDATE, which is exactly right for a
-- human edit and exactly wrong for a replicated one. Cloud-first makes sync
-- BI-DIRECTIONAL for configuration (the dashboard writes to the cloud, the edge
-- needs the result locally), and updated_at is the watermark BOTH directions
-- chase. A replayed row that gets a fresh now() looks locally modified, so the
-- next pass ships it straight back and the two tiers ping-pong the same row
-- forever.
--
-- The replay path sets app.sync_replay and the trigger then preserves whatever
-- updated_at the source row carried, so a replicated row is identical on both
-- sides and neither watermark sees a change that did not happen.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  IF coalesce(current_setting('app.sync_replay', true), '') = 'on' THEN
    RETURN NEW;                          -- carry the source timestamp verbatim
  END IF;
  NEW.updated_at = now();
  RETURN NEW;
END
$fn$;

-- ── 6c. Command outbox marker ───────────────────────────────────────────────
-- The downlink half of cloud-first. A Vercel function cannot hold an MQTT
-- connection, so the cloud dashboard WRITES a command row instead of publishing
-- it; the edge server pulls that row down and puts it on whichever broker the
-- hardware is currently using.
--
-- published_at is what separates "queued in the cloud, not yet on the wire" from
-- "already sent". Without it the dispatcher cannot tell a fresh cloud command
-- from one the edge itself published a second ago, and would send everything
-- twice.
ALTER TABLE commands ADD COLUMN IF NOT EXISTS published_at timestamptz;

CREATE INDEX IF NOT EXISTS ix_commands_undispatched
  ON commands (created_at) WHERE published_at IS NULL;

-- ── 7. Live push signal for the cloud read tier ─────────────────────────────
-- api/ws.js listens on 'senseable_sync' and re-reads. Under the old topology
-- the sync worker was the only thing that ever moved cloud rows, so it was also
-- the only thing that had to signal. Cloud-first ingests straight into the
-- cloud, which the sync worker never sees, so the signal has to come from the
-- table itself.
--
-- Statement-level, not row-level: a 16-channel telemetry packet is one INSERT
-- and must cost one NOTIFY, not sixteen. api/ws.js already coalesces further.
CREATE OR REPLACE FUNCTION notify_readings_changed() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  PERFORM pg_notify('senseable_sync', json_build_object(
    'src', 'ingest', 'at', (extract(epoch from now()) * 1000)::bigint)::text);
  RETURN NULL;
END
$fn$;

DROP TRIGGER IF EXISTS trg_readings_notify ON readings;
CREATE TRIGGER trg_readings_notify
  AFTER INSERT ON readings
  FOR EACH STATEMENT EXECUTE FUNCTION notify_readings_changed();
`);
};

export const down = (pgm) => {
  pgm.sql(`
DROP TRIGGER IF EXISTS trg_readings_notify ON readings;
DROP FUNCTION IF EXISTS notify_readings_changed();
DROP INDEX IF EXISTS uq_readings_port_ts;
DROP INDEX IF EXISTS ix_readings_unsynced;
DROP INDEX IF EXISTS uq_notifications_event_uid;
DROP INDEX IF EXISTS ix_notifications_unsynced;
ALTER TABLE readings      DROP COLUMN IF EXISTS origin, DROP COLUMN IF EXISTS synced;
ALTER TABLE notifications DROP COLUMN IF EXISTS event_uid, DROP COLUMN IF EXISTS synced;
DROP FUNCTION IF EXISTS senseable_uuid(text, text[]);
`);
  // Foreign keys are deliberately left with ON UPDATE CASCADE: reverting them
  // would break nothing but would also protect nothing, and the ids stay
  // deterministic either way.
};
