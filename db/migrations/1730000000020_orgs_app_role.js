// 1730000000020_orgs_app_role.js
// The website's organization endpoints (web repo api/orgs.js: register, join,
// me, tid-check, test-account wipe) connect as senseable_app, which row-level
// security confines to the tenant named in app.current_tenant. Tenant-bound
// steps now set that per transaction; the lookups that must see across tenants
// before any tenant is known go through these narrow SECURITY DEFINER
// functions, so the app role itself gets no wider read.
export const up = (pgm) => {
  pgm.sql(`
-- Join: which organization does this operator code belong to?
CREATE OR REPLACE FUNCTION tenant_by_org_code(p_code text)
RETURNS TABLE (tenant_id uuid, slug text, name text, max_users int)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT t.tenant_id, t.slug, t.name, t.max_users FROM tenants t WHERE t.org_code = p_code;
$$;

-- Registration form: is this Tenant ID already used? Answers yes/no only.
CREATE OR REPLACE FUNCTION tid_taken(p_tid text)
RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT EXISTS (SELECT 1 FROM tenants WHERE mqtt_tid = p_tid);
$$;

REVOKE ALL ON FUNCTION tenant_by_org_code(text) FROM public;
REVOKE ALL ON FUNCTION tid_taken(text) FROM public;
GRANT EXECUTE ON FUNCTION
  tenant_for_auth(uuid), tenant_by_org_code(text), tid_taken(text), purge_test_tenants(interval)
  TO senseable_app;
`);
};

export const down = (pgm) => {
  pgm.sql(`
REVOKE EXECUTE ON FUNCTION tenant_for_auth(uuid), purge_test_tenants(interval) FROM senseable_app;
DROP FUNCTION IF EXISTS tenant_by_org_code(text);
DROP FUNCTION IF EXISTS tid_taken(text);
`);
};
