// 1730000000012_sync_deletions.js
//
// Deletions now replicate edge -> cloud.
//
// THE BUG
// scripts/sync.js replicates by UPSERT: rows chased by updated_at, inserted or
// updated on the other side. A DELETE leaves nothing to chase, so a device an
// operator removed on the edge dashboard stayed in Supabase forever. The cloud
// dashboard kept showing it — in the tenant it was removed from, next to its
// replacement in the tenant it moved to — which read as node assignment being
// broken on the deployed site while the on-site server was correct.
//
// THE FIX
// An AFTER DELETE trigger writes a tombstone (table, pk) for devices, modules,
// ports and actuators. The sync worker's up-pass applies each tombstone to the
// cloud as a DELETE, then clears it. Primary keys are the deterministic UUIDv5
// ids from migration 009, identical on both tiers, so the tombstone names the
// same row in Supabase.
//
// Cascades fire the trigger for every child row too; that is harmless (the
// cloud cascade has already removed them by the time their tombstone is
// applied, and the DELETE simply matches nothing).
//
// Rows deleted BY the sync worker itself run with app.sync_replay = 'on' and are
// not tombstoned, so a replicated delete can never echo back.
//
// SECURITY DEFINER: deletes are issued by the RLS-bound app role, which has no
// grant on this bookkeeping table and should not get one.

export const up = (pgm) => {
  pgm.sql(`
CREATE TABLE IF NOT EXISTS sync_deletions (
  id          bigserial PRIMARY KEY,
  table_name  text        NOT NULL,
  pk_col      text        NOT NULL,
  pk          uuid        NOT NULL,
  deleted_at  timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION record_sync_deletion() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF coalesce(current_setting('app.sync_replay', true), '') = 'on' THEN
    RETURN OLD;
  END IF;
  INSERT INTO sync_deletions (table_name, pk_col, pk)
  VALUES (TG_TABLE_NAME, TG_ARGV[0], (to_jsonb(OLD) ->> TG_ARGV[0])::uuid);
  RETURN OLD;
END $$;

DROP TRIGGER IF EXISTS devices_sync_delete   ON devices;
DROP TRIGGER IF EXISTS modules_sync_delete   ON modules;
DROP TRIGGER IF EXISTS ports_sync_delete     ON ports;
DROP TRIGGER IF EXISTS actuators_sync_delete ON actuators;
CREATE TRIGGER devices_sync_delete   AFTER DELETE ON devices   FOR EACH ROW EXECUTE FUNCTION record_sync_deletion('device_id');
CREATE TRIGGER modules_sync_delete   AFTER DELETE ON modules   FOR EACH ROW EXECUTE FUNCTION record_sync_deletion('module_id');
CREATE TRIGGER ports_sync_delete     AFTER DELETE ON ports     FOR EACH ROW EXECUTE FUNCTION record_sync_deletion('port_id');
CREATE TRIGGER actuators_sync_delete AFTER DELETE ON actuators FOR EACH ROW EXECUTE FUNCTION record_sync_deletion('actuator_id');

COMMENT ON TABLE sync_deletions IS
  'Tombstones for edge-side deletes; scripts/sync.js applies them to the cloud, then clears them.';
`);
};

export const down = (pgm) => {
  pgm.sql(`
DROP TRIGGER IF EXISTS devices_sync_delete   ON devices;
DROP TRIGGER IF EXISTS modules_sync_delete   ON modules;
DROP TRIGGER IF EXISTS ports_sync_delete     ON ports;
DROP TRIGGER IF EXISTS actuators_sync_delete ON actuators;
DROP FUNCTION IF EXISTS record_sync_deletion();
DROP TABLE IF EXISTS sync_deletions;
`);
};
