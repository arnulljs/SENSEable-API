-- seed.sql
-- Idempotent bootstrap seed. Run as the OWNER (owner bypasses RLS, so it can
-- write across every tenant):
--
--   psql "postgres://senseable_owner:PASS@localhost:5432/senseable" -f db/seed.sql
--
-- WHAT THIS DOES *NOT* DO ANYMORE
-- It no longer invents hardware. Earlier versions seeded a node, an expansion
-- board, three ports and three actuators so the UI had something to render
-- before any hardware existed. That made the dashboard a picture of this file
-- rather than of the pond.
--
-- Inventory is now DISCOVERED: the first telemetry or discovery packet from a
-- real node creates its device row, the first chip address creates the board,
-- and the first reading on a channel creates the port (src/provision.js). So a
-- fresh install shows an empty dashboard until hardware actually speaks — which
-- is the honest state — and everything that appears afterwards corresponds to
-- something physically wired up.
--
-- WHAT REMAINS, AND WHY
--   roles      global lookup; the app can't authorise anyone without it
--   tenants    RLS is keyed on tenant_id; nothing works without at least one,
--              and mqtt_tid is what maps an incoming packet to an organisation
--   users      you must be able to log in to see anything
--   formulas   calibration library — operator knowledge, not hardware state.
--              A newly provisioned channel starts on identity calibration and
--              shows raw counts until one of these is assigned to it.
--   profiles   reusable sensor templates, applied by hand when configuring a
--              freshly discovered channel
--
-- Passwords are hashed with bcrypt via pgcrypto's crypt()/gen_salt('bf') — no
-- plaintext ever lands in a column.

DO $$
DECLARE
  v_aqua uuid; v_llba uuid;
BEGIN
  -- roles (global lookup table, no tenant)
  INSERT INTO roles(role_id, role_name, tier, description) VALUES
    ('designer','Designer',2,'Client admin. Full operational access plus interactive-map editing.'),
    ('operator','Operator',3,'Operational access. Views map read-only.')
  ON CONFLICT (role_id) DO NOTHING;

  -- tenants
  INSERT INTO tenants(slug, org_code, name, status, plan, max_users) VALUES
    ('aquatech','AQUA-7421','AquaTech Hatchery Corp','active','Pilot',6)
  ON CONFLICT (slug) DO NOTHING;
  INSERT INTO tenants(slug, org_code, name, status, plan, max_users) VALUES
    ('llba','LLBA-3309','Lapu-Lapu Bay Aquafarms','active','Pilot',4)
  ON CONFLICT (slug) DO NOTHING;
  SELECT tenant_id INTO v_aqua FROM tenants WHERE slug='aquatech';
  SELECT tenant_id INTO v_llba FROM tenants WHERE slug='llba';

  -- users (bcrypt-hashed)
  INSERT INTO users(tenant_id, role_id, full_name, email, password_hash, status) VALUES
    (v_aqua,'designer','Mariz Santos','mariz@aquatech.ph', crypt('designer123', gen_salt('bf')),'active'),
    (v_aqua,'operator','Jay Bautista','jay@aquatech.ph',   crypt('operator123', gen_salt('bf')),'active'),
    (v_llba,'designer','Dane Lim','dane@llba.ph',          crypt('designer123', gen_salt('bf')),'active')
  ON CONFLICT (email) DO NOTHING;

  -- calibration formulas (AquaTech) — the conversion library an operator
  -- assigns to a channel once they know what sensor is plugged into it.
  INSERT INTO calibration_formulas(tenant_id, label, expression) VALUES
    (v_aqua,'DO','x * 0.0021 - 0.38'),
    (v_aqua,'salinity','x * 0.05182 + 0.0'),
    (v_aqua,'temperaTURE','(x * 0.003906) - 40.0')
  ON CONFLICT (tenant_id, label) DO NOTHING;

  -- sensor profiles (reusable templates applied when configuring a channel)
  INSERT INTO sensor_profiles(tenant_id, name, label, unit, range_min, range_max, safe_min, safe_max) VALUES
    (v_aqua,'Dissolved Oxygen (mg/L)','Dissolved Oxygen','mg/L',0,20,6,9),
    (v_aqua,'Salinity (PSU)','Salinity','PSU',0,70,35,50),
    (v_aqua,'Temperature (C)','Temperature','C',0,50,25,32)
  ON CONFLICT (tenant_id, name) DO NOTHING;

  RAISE NOTICE 'Seed complete: % tenants, no hardware. Devices appear when real nodes publish.',
    (SELECT count(*) FROM tenants);
END $$;
