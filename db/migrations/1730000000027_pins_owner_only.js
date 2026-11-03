// 1730000000027_pins_owner_only.js
// node_tenant_assignments is deliberately outside RLS (migration 011: routing
// metadata spanning tenants). But the bootstrap's default privileges still
// granted the app role (senseable_app) full access to it, so app-role code
// could read every org's pins, or re-pin another org's node to itself and
// receive its telemetry. Found by TC-3.2's RLS check. Every reader and writer
// of the table (store.js, sync.js, the Lambda's hydrate) uses the owner pool,
// so the app role loses nothing it uses.
// Run on the cloud database FIRST, then the on-site one.
export const up = (pgm) => {
  pgm.sql('REVOKE ALL ON node_tenant_assignments FROM senseable_app;');
};
export const down = (pgm) => {
  pgm.sql('GRANT SELECT, INSERT, UPDATE, DELETE ON node_tenant_assignments TO senseable_app;');
};
