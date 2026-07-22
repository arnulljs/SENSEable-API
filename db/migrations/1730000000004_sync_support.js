// 1730000000004_sync_support.js
// Groundwork for the local→cloud synchronization tier (thesis two-tier
// redundancy model): the edge server is the primary store, and rows replicate
// asynchronously up to the cloud repository once WAN connectivity allows.
//
// Two things are needed for incremental sync, and neither existed yet:
//
//   1. A CHANGE MARKER on every mutable table. `readings` and `notifications`
//      are append-only with a monotonic bigint identity PK, so they can be
//      chased with `id > watermark`. Everything else is mutable — a rename or a
//      status flip leaves the PK untouched — so those need `updated_at`, which
//      only `actuators` and `map_profiles` had.
//
//   2. A WATERMARK STORE so a sync run resumes where the last one stopped,
//      which is what makes an outage recoverable rather than a full re-push.
//
// Runs on BOTH tiers: the columns must exist cloud-side for the upserts to
// land, and keeping one migration set means the two schemas never drift.
// `sync_state` is only *written* on the edge server.

const STAMPED = [
  'tenants', 'users', 'sensor_profiles', 'devices', 'modules',
  'calibration_formulas', 'ports', 'notifications', 'map_sensors', 'commands',
];

export const up = (pgm) => {
  pgm.sql(`
-- ── 1. updated_at on every mutable table that lacked one ────────────────────
-- set_updated_at() already exists (created in the init migration); we only add
-- the column + trigger where they're missing. DEFAULT now() backfills existing
-- rows to "changed at migration time", which is correct: the first sync run
-- must consider them dirty and push them up.
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'tenants','users','sensor_profiles','devices','modules',
    'calibration_formulas','ports','notifications','map_sensors','commands'
  ] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = t AND column_name = 'updated_at'
    ) THEN
      EXECUTE format(
        'ALTER TABLE %I ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();', t);
    END IF;

    EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_updated ON %I;', t, t);
    EXECUTE format(
      'CREATE TRIGGER trg_%s_updated BEFORE UPDATE ON %I
         FOR EACH ROW EXECUTE FUNCTION set_updated_at();', t, t);

    -- The sync worker's hot path is "give me rows changed since X", so the
    -- watermark column needs an index on every table it scans.
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS ix_%s_updated ON %I (updated_at);', t, t);
  END LOOP;
END $$;

-- actuators and map_profiles already carry updated_at + a trigger from the init
-- migration; they only need the index to make watermark scans cheap.
CREATE INDEX IF NOT EXISTS ix_actuators_updated    ON actuators (updated_at);
CREATE INDEX IF NOT EXISTS ix_map_profiles_updated ON map_profiles (updated_at);

-- ── 2. Sync watermark store (written on the EDGE tier only) ─────────────────
-- One row per replicated table. Deliberately NOT tenant-scoped and NOT under
-- RLS: this is operational metadata about the replication process itself, which
-- legitimately spans every tenant, exactly like the owner-role hydration pool.
CREATE TABLE IF NOT EXISTS sync_state (
  table_name      text PRIMARY KEY,
  -- watermark for append-only tables chased by identity PK (readings,
  -- notifications). NULL for tables tracked by timestamp.
  last_synced_id  bigint,
  -- watermark for mutable tables chased by updated_at. NULL for id-tracked.
  --
  -- Stored as TEXT, not timestamptz-via-JS: a JS Date holds only milliseconds,
  -- so round-tripping a microsecond-precision timestamp through the driver
  -- truncates it and every row looks perpetually dirty.
  last_synced_at  timestamptz,
  -- Tie-breaker half of the cursor. set_updated_at() uses now(), which is the
  -- TRANSACTION timestamp, so a bulk UPDATE stamps every affected row with the
  -- SAME updated_at. Paging on the timestamp alone would then either loop or
  -- skip rows once a batch boundary landed inside such a group. Keying on
  -- (updated_at, pk) makes the cursor total rather than ambiguous.
  last_synced_key text,
  -- observability: enough to answer "is replication healthy?" without logs.
  rows_synced     bigint      NOT NULL DEFAULT 0,
  last_run_at     timestamptz,
  last_error      text,
  updated_at      timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_sync_state_updated ON sync_state;
CREATE TRIGGER trg_sync_state_updated BEFORE UPDATE ON sync_state
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
`);
};

export const down = (pgm) => {
  pgm.sql(`
DROP TABLE IF EXISTS sync_state CASCADE;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'tenants','users','sensor_profiles','devices','modules',
    'calibration_formulas','ports','notifications','map_sensors','commands'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_updated ON %I;', t, t);
    EXECUTE format('DROP INDEX IF EXISTS ix_%s_updated;', t);
    EXECUTE format('ALTER TABLE %I DROP COLUMN IF EXISTS updated_at;', t);
  END LOOP;
END $$;

DROP INDEX IF EXISTS ix_actuators_updated;
DROP INDEX IF EXISTS ix_map_profiles_updated;
`);
};
