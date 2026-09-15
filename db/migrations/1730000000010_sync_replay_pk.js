// 1730000000010_sync_replay_pk.js
// Stop derive_pk() from rewriting primary keys on REPLICATED inserts.
//
// THE BUG THIS FIXES
// Migration 009 added BEFORE INSERT triggers so that any row created anywhere
// gets a deterministic id. That is right for a row a tier genuinely creates. It
// is wrong for a row the sync worker is copying from the other tier, because
// the copy already HAS an agreed id and the trigger silently replaces it.
//
// The damage is invisible until the next pass in the opposite direction:
//
//   1. Cloud holds an old command with a random command_id (created before 009).
//   2. The downward pass copies it to the edge. The trigger overwrites the id
//      with the derived one.
//   3. The upward pass now sees a row whose primary key does not exist in the
//      cloud, so it INSERTs — and trips UNIQUE (tenant_id, cid) instead.
//   4. `commands` fails on that pass, and on every pass after it.
//
// The same trap applies to every table in the downward path: devices, modules,
// ports, formulas, profiles, map rows, tenants, users. `commands` just happened
// to be the first one carrying rows old enough to still hold random ids.
//
// THE FIX
// The replay path already sets app.sync_replay for exactly this reason — 009
// taught set_updated_at() to honour it so replicated rows keep their source
// timestamp. derive_pk() needs the same exemption: during replay, take the row
// as given and change nothing.
//
// Local writes are unaffected. app.sync_replay is only ever set inside the sync
// worker's push transaction, scoped with SET LOCAL, so nothing else can see it.

export const up = (pgm) => {
  pgm.sql(`
CREATE OR REPLACE FUNCTION derive_pk() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  -- Replication carries an id the two tiers have already agreed on. Deriving a
  -- new one here would fork that agreement, and the fork only surfaces later as
  -- a unique-constraint failure on the natural key.
  IF coalesce(current_setting('app.sync_replay', true), '') = 'on' THEN
    RETURN NEW;
  END IF;

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
`);
};

export const down = (pgm) => {
  // Deliberately a no-op. Reverting would restore a function that corrupts
  // replicated ids, and nothing else in the schema depends on the old shape.
  pgm.sql(`DO $$ BEGIN RAISE NOTICE
    'derive_pk() keeps its sync_replay exemption — reverting it would break '
    'replication, and nothing depends on the old behaviour'; END $$;`);
};
