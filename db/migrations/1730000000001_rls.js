// 1730000000001_rls.js
// Row-Level Security: one tenant-isolation policy per tenant-scoped table,
// keyed on the app.current_tenant GUC the backend sets per request.

export const up = (pgm) => {
  pgm.sql(`
-- Enable Row-Level Security on every tenant-scoped table and attach a single
-- isolation policy: a session may only see/write rows whose tenant_id matches
-- the app.current_tenant GUC that the backend sets per request.
--
-- Fail-closed: if the backend forgets to set app.current_tenant, current_setting
-- returns NULL, tenant_id = NULL is NULL, and ZERO rows are visible.
--
-- The owner role (which runs migrations + seed) is exempt from RLS because we do
-- NOT use FORCE — that's deliberate, so seeding across all tenants still works.
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'tenants','users','sensor_profiles','devices','modules',
    'calibration_formulas','ports','actuators','readings',
    'notifications','map_profiles'
  ] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY;', t);
    EXECUTE format($f$
      CREATE POLICY tenant_isolation ON %I
        USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
        WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);
    $f$, t);
  END LOOP;
END $$;
`);
};

export const down = (pgm) => {
  pgm.sql(`
    DO \$\$
    DECLARE t text;
    BEGIN
      FOREACH t IN ARRAY ARRAY[
        'tenants','users','sensor_profiles','devices','modules',
        'calibration_formulas','ports','actuators','readings',
        'notifications','map_profiles'
      ] LOOP
        EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I;', t);
        EXECUTE format('ALTER TABLE %I DISABLE ROW LEVEL SECURITY;', t);
      END LOOP;
    END \$\$;
  `);
};
