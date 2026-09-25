// 1730000000016_supabase_auth.js
//
// Identity moves to Supabase Auth (auth.users). public.users stops being a
// credential store and becomes a PROFILE linking a Supabase auth user to a
// tenant + role. Keeps the table (map_profiles.created_by → users, and the
// migration-009 user_id-from-email trigger both rely on it) and just drops the
// now-redundant password column.
//
// PORTABILITY: this runs on BOTH the Supabase cloud DB and the edge laptop's
// plain Postgres. auth.users exists ONLY on Supabase, so the FK to it is added
// conditionally — skipped where the auth schema is absent. The edge never
// authenticates anyone (it trusts the tenant the request resolved to), so it
// needs the auth_id COLUMN for sync parity but not the FK.
//
// disable_transaction: ALTER TYPE ... ADD VALUE cannot run inside the
// transaction node-pg-migrate wraps migrations in.

export const up = (pgm) => {
  pgm.noTransaction();
  pgm.sql(`ALTER TYPE tenant_status ADD VALUE IF NOT EXISTS 'pending';`);
  pgm.sql(`
ALTER TABLE users ADD COLUMN IF NOT EXISTS auth_id uuid;
ALTER TABLE users DROP COLUMN IF EXISTS password_hash;
CREATE UNIQUE INDEX IF NOT EXISTS ux_users_auth_id ON users(auth_id) WHERE auth_id IS NOT NULL;

-- FK to Supabase's auth.users, only where that schema exists (cloud).
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='auth' AND table_name='users') THEN
    BEGIN
      ALTER TABLE users ADD CONSTRAINT users_auth_id_fkey
        FOREIGN KEY (auth_id) REFERENCES auth.users(id) ON DELETE CASCADE;
    EXCEPTION WHEN duplicate_object THEN NULL; END;
  END IF;
END $$;

-- TEST ACCOUNTS. Tenants created from the register form's "test account" box get
-- is_test=true and behave identically, but can be purged in one shot (manually,
-- or by the daily scheduled sweep in api/orgs.js). This is the tidy-on-demand
-- pattern — never auto-delete on session expiry, which has no reliable trigger.
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS is_test boolean NOT NULL DEFAULT false;

-- purge_test_tenants(older_than): delete test tenants (cascading to their users,
-- devices, readings, everything tenant-scoped) and return the auth.users ids that
-- were attached, so the caller can delete the matching Supabase Auth accounts
-- (SQL can't reach the auth admin API). older_than filters by tenant age; pass
-- interval '0' to wipe them all. SECURITY DEFINER: runs as owner, callable only
-- by the service role.
CREATE OR REPLACE FUNCTION purge_test_tenants(older_than interval DEFAULT interval '0')
RETURNS TABLE (auth_id uuid) LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE ids uuid[];
BEGIN
  SELECT array_agg(u.auth_id) INTO ids
  FROM users u JOIN tenants t ON t.tenant_id = u.tenant_id
  WHERE t.is_test AND u.auth_id IS NOT NULL AND t.created_at < now() - older_than;

  DELETE FROM tenants t
  WHERE t.is_test AND t.created_at < now() - older_than;

  RETURN QUERY SELECT x FROM unnest(coalesce(ids, ARRAY[]::uuid[])) AS x;
END $$;
REVOKE ALL ON FUNCTION purge_test_tenants(interval) FROM public;
DO $$ BEGIN
  GRANT EXECUTE ON FUNCTION purge_test_tenants(interval) TO service_role;
EXCEPTION WHEN undefined_object THEN NULL; END $$;

-- Map an authenticated auth user → their tenant. SECURITY DEFINER so callers
-- (Vercel functions using the anon/publishable key) never read users directly.
CREATE OR REPLACE FUNCTION tenant_for_auth(p_auth_id uuid)
RETURNS TABLE (tenant_id uuid, slug text, role_id text, full_name text, status text)
LANGUAGE sql SECURITY DEFINER SET search_path = public AS $$
  SELECT u.tenant_id, t.slug, u.role_id, u.full_name, u.status::text
  FROM users u JOIN tenants t ON t.tenant_id = u.tenant_id
  WHERE u.auth_id = p_auth_id;
$$;
REVOKE ALL ON FUNCTION tenant_for_auth(uuid) FROM public;
DO $$ BEGIN
  GRANT EXECUTE ON FUNCTION tenant_for_auth(uuid) TO authenticated, anon, service_role;
EXCEPTION WHEN undefined_object THEN
  -- edge Postgres has no Supabase roles; skip.
  NULL;
END $$;
`);
};

export const down = (pgm) => {
  pgm.noTransaction();
  pgm.sql(`
DROP FUNCTION IF EXISTS tenant_for_auth(uuid);
DROP FUNCTION IF EXISTS purge_test_tenants(interval);
ALTER TABLE tenants DROP COLUMN IF EXISTS is_test;
DROP INDEX IF EXISTS ux_users_auth_id;
ALTER TABLE users DROP CONSTRAINT IF EXISTS users_auth_id_fkey;
ALTER TABLE users DROP COLUMN IF EXISTS auth_id;
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash text NOT NULL DEFAULT '!';
`);
};
