// 1730000000011_node_tenant_assignments.js
// NODE-TENANT-OVERRIDE
//
// Pins a physical node (by its MAC-derived node_id) to a tenant from the
// database alone, overriding the tenant its packets' `tid` would resolve to.
//
// WHY: tid is compiled into the firmware and the web/backend team cannot change
// it without a reflash. For bench testing, boards need to move between tenants
// with zero firmware involvement. ingest.js checks this table (hydrated into
// store.tenantByNodeId) before the normal tid -> tenants.mqtt_tid path.
//
// NOT tenant-scoped and NOT under RLS, like sync_state: it is routing metadata
// that spans tenants, touched only by the owner-role admin pool. Text primary
// key, so migration 009's derive_pk() triggers do not apply. Not replicated by
// sync.js: only the tier running ingest.js reads it.

export const up = (pgm) => {
  pgm.sql(`
CREATE TABLE IF NOT EXISTS node_tenant_assignments (
  node_id     text PRIMARY KEY,
  tenant_id   uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  assigned_at timestamptz NOT NULL DEFAULT now(),
  note        text
);
COMMENT ON TABLE node_tenant_assignments IS
  'Bench-testing override: pins a node_id to a tenant ahead of tid resolution in ingest.js.';
`);
};

export const down = (pgm) => {
  pgm.sql('DROP TABLE IF EXISTS node_tenant_assignments;');
};
