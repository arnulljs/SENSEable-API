// 1730000000005_tenant_resolver.js
// A narrow, auditable escape hatch for the RLS bootstrap problem.
//
// THE PROBLEM
// RLS policies compare `tenant_id = current_setting('app.current_tenant')::uuid`.
// To set that GUC for a request, the API must first translate the tenant SLUG
// the client sends ("aquatech") into a uuid — which means SELECTing from
// `tenants`. But `tenants` is itself RLS-protected, and with no tenant set yet
// the policy is fail-closed: zero rows. The lookup needed to establish scope is
// blocked by the scope it's trying to establish.
//
// On the edge tier this never came up: hydration runs on adminPool as
// senseable_owner, which is RLS-exempt. The cloud read tier has no such luxury —
// serverless functions connect as senseable_app precisely so that a bug in a
// handler cannot reach across tenants.
//
// THE FIX
// One SECURITY DEFINER function that executes with the OWNER's privileges (and
// therefore RLS-exempt) but does exactly one thing: slug → uuid. It cannot list
// tenants, cannot read any other column, and returns NULL for anything it
// doesn't recognise. That's a far smaller grant than "let the API role bypass
// RLS", which is the alternative.
//
// search_path is pinned inside the function: a SECURITY DEFINER routine that
// resolves objects through a caller-controlled search_path is the classic
// privilege-escalation vector in Postgres.

export const up = (pgm) => {
  pgm.sql(`
CREATE OR REPLACE FUNCTION resolve_tenant(p_slug text)
RETURNS uuid
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
  SELECT tenant_id FROM tenants WHERE slug = p_slug;
$$;

-- Executable by the constrained runtime roles; ownership (and thus the
-- RLS-exempt privileges it runs with) stays with senseable_owner.
REVOKE ALL   ON FUNCTION resolve_tenant(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_tenant(text) TO senseable_app, senseable_ro;

COMMENT ON FUNCTION resolve_tenant(text) IS
  'RLS bootstrap: maps a tenant slug to its uuid so a caller can set '
  'app.current_tenant. SECURITY DEFINER by necessity; deliberately returns '
  'nothing but the uuid.';
`);
};

export const down = (pgm) => {
  pgm.sql(`DROP FUNCTION IF EXISTS resolve_tenant(text);`);
};
