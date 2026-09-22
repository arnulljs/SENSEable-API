// 1730000000013_pins_replicate.js
//
// Node pins (node_tenant_assignments) now replicate between tiers, in both
// directions, including deletes.
//
// WHY: with a cloud bridge ingesting alongside the edge, both tiers resolve
// every packet's tenant independently. If a pin existed on only one of them,
// the same board would be filed under one organization in Supabase and another
// in local Postgres. Pins therefore sync like any other configuration row:
//   * updated_at + the replay-aware set_updated_at() trigger (migration 009), so
//     scripts/sync.js can chase them with its timestamp strategy;
//   * a delete tombstone (migration 012) — which needs sync_deletions.pk to hold
//     a text key, since node_id is not a uuid.
//
// Tombstones are now applied in BOTH directions by sync.js: edge deletes go up,
// and deletes made on the cloud tier (bridge, or the Vercel dashboard) come down.

export const up = (pgm) => {
  pgm.sql(`
ALTER TABLE node_tenant_assignments
  ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

DROP TRIGGER IF EXISTS node_tenant_assignments_updated_at ON node_tenant_assignments;
CREATE TRIGGER node_tenant_assignments_updated_at
  BEFORE UPDATE ON node_tenant_assignments
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

ALTER TABLE sync_deletions ALTER COLUMN pk TYPE text USING pk::text;

CREATE OR REPLACE FUNCTION record_sync_deletion() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF coalesce(current_setting('app.sync_replay', true), '') = 'on' THEN
    RETURN OLD;
  END IF;
  INSERT INTO sync_deletions (table_name, pk_col, pk)
  VALUES (TG_TABLE_NAME, TG_ARGV[0], to_jsonb(OLD) ->> TG_ARGV[0]);
  RETURN OLD;
END $$;

DROP TRIGGER IF EXISTS node_tenant_assignments_sync_delete ON node_tenant_assignments;
CREATE TRIGGER node_tenant_assignments_sync_delete
  AFTER DELETE ON node_tenant_assignments
  FOR EACH ROW EXECUTE FUNCTION record_sync_deletion('node_id');
`);
};

export const down = (pgm) => {
  pgm.sql(`
DROP TRIGGER IF EXISTS node_tenant_assignments_sync_delete ON node_tenant_assignments;
DROP TRIGGER IF EXISTS node_tenant_assignments_updated_at ON node_tenant_assignments;
ALTER TABLE node_tenant_assignments DROP COLUMN IF EXISTS updated_at;
DELETE FROM sync_deletions WHERE pk !~ '^[0-9a-f-]{36}$';
ALTER TABLE sync_deletions ALTER COLUMN pk TYPE uuid USING pk::uuid;
`);
};
