// 1730000000018_wire_tid.js
// Persist the tid each node ACTUALLY publishes and subscribes under. For a node
// pinned to another tenant (node_tenant_assignments) it differs from that
// tenant's mqtt_tid, and commands must go to the node's real topic. It lived
// only in edge memory (dev.wireTid), so the cloud could not address a pinned
// node directly and every Vercel press fell back to a slower path. Learned from
// the node's own packets by whichever tier ingests them. NULL = not heard yet.
export const up = (pgm) => { pgm.sql(`ALTER TABLE devices ADD COLUMN IF NOT EXISTS wire_tid text;`); };
export const down = (pgm) => { pgm.sql(`ALTER TABLE devices DROP COLUMN IF EXISTS wire_tid;`); };
