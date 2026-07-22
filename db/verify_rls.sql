-- verify_rls.sql
-- Proves tenant isolation is enforced by PostgreSQL itself, not by application
-- code. Run this as senseable_app (the RLS-CONSTRAINED runtime role) — running
-- it as senseable_owner proves nothing, because the owner is RLS-exempt.
--
--   psql "$CLOUD_APP_URL" -f db/verify_rls.sql
--
-- Run it against BOTH the local and the Supabase database: the whole point of
-- Option B is that the same policies protect both tiers.

\set ON_ERROR_STOP off
\pset pager off

\echo '════════════════════════════════════════════════════════════════'
\echo ' RLS VERIFICATION  (must be run as senseable_app)'
\echo '════════════════════════════════════════════════════════════════'

SELECT current_user AS connected_as,
       CASE WHEN current_user = 'senseable_app'
            THEN 'OK — RLS-constrained role'
            ELSE 'WRONG ROLE — results below are meaningless'
       END AS role_check;

\echo ''
\echo '── TEST 1: fail-closed. No tenant set ⇒ ZERO rows everywhere. ──'
-- current_setting('app.current_tenant', true) returns NULL, so the policy
-- predicate `tenant_id = NULL` is NULL (never true) and nothing is visible.
SELECT 'devices'       AS table_name, count(*) AS visible_rows FROM devices
UNION ALL SELECT 'ports',        count(*) FROM ports
UNION ALL SELECT 'readings',     count(*) FROM readings
UNION ALL SELECT 'actuators',    count(*) FROM actuators
UNION ALL SELECT 'notifications',count(*) FROM notifications
ORDER BY table_name;
\echo '   EXPECT: every count = 0'

\echo ''
\echo '── TEST 2: scoped read. Set tenant A ⇒ only tenant A rows. ──'
BEGIN;
-- Resolve a real tenant uuid as the owner would have seeded it. We cannot read
-- `tenants` without a tenant set, so this value is passed in by the harness in
-- normal use; here we demonstrate with a set_config round-trip.
SELECT set_config('app.current_tenant',
                  (SELECT tenant_id::text FROM tenants LIMIT 1), true) AS tenant_set;
\echo '   (if tenant_set is blank, TEST 1 already proved tenants is closed —'
\echo '    re-run this file with -v tenant=<uuid> for a fully scoped check)'
SELECT count(*) AS devices_visible FROM devices;
COMMIT;

\echo ''
\echo '── TEST 3: cross-tenant write is REJECTED by the database. ──'
BEGIN;
-- Claim tenant A, then attempt to insert a row belonging to a different tenant.
-- The policy WITH CHECK clause must reject this regardless of app-layer logic.
SELECT set_config('app.current_tenant',
                  '00000000-0000-0000-0000-000000000000', true);
INSERT INTO devices (tenant_id, node_id, name)
VALUES ('11111111-1111-1111-1111-111111111111', 'EVIL', 'cross-tenant write');
\echo '   EXPECT: ERROR — new row violates row-level security policy'
ROLLBACK;

\echo ''
\echo '── TEST 4: RLS is actually ENABLED on every tenant-scoped table. ──'
SELECT c.relname AS table_name,
       c.relrowsecurity AS rls_enabled,
       count(p.polname) AS policies
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_policy p ON p.polrelid = c.oid
WHERE n.nspname = 'public'
  AND c.relkind = 'r'
  AND c.relname IN ('tenants','users','sensor_profiles','devices','modules',
                    'calibration_formulas','ports','actuators','readings',
                    'notifications','map_profiles','commands')
GROUP BY c.relname, c.relrowsecurity
ORDER BY c.relname;
\echo '   EXPECT: rls_enabled = t and policies >= 1 for every row'

\echo ''
\echo '════════════════════════════════════════════════════════════════'
\echo ' Interpretation:'
\echo '   TEST 1 all zeros      → fail-closed confirmed'
\echo '   TEST 3 raised ERROR   → cross-tenant write blocked at the DB'
\echo '   TEST 4 all t / >=1    → no table left unprotected'
\echo '════════════════════════════════════════════════════════════════'
