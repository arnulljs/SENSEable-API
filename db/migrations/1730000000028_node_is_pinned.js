// 1730000000028_node_is_pinned.js
// Migration 027 took node_tenant_assignments away from the app role, but the
// Vercel command path (api/_write.js queueCommand) asks one yes/no question of
// it as that role: "is this node pinned?", to avoid publishing to a topic the
// node does not listen on. This answers exactly that and nothing else, the
// same pattern as resolve_tenant() (migration 005): no rows, no tenant ids.
// Run on the cloud database FIRST, then the on-site one.
export const up = (pgm) => {
  pgm.sql(`
CREATE OR REPLACE FUNCTION node_is_pinned(p_node_id text)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
  SELECT EXISTS (SELECT 1 FROM node_tenant_assignments WHERE node_id = p_node_id);
$$;
REVOKE ALL    ON FUNCTION node_is_pinned(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION node_is_pinned(text) TO senseable_app;
COMMENT ON FUNCTION node_is_pinned(text) IS
  'Command routing: whether a node is pinned to a tenant. SECURITY DEFINER so the '
  'app role can ask without reading node_tenant_assignments (migration 027).';
`);
};
export const down = (pgm) => {
  pgm.sql('DROP FUNCTION IF EXISTS node_is_pinned(text);');
};
