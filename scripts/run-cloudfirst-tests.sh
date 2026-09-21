#!/usr/bin/env bash
# run-cloudfirst-tests.sh ────────────────────────────────────────────────────
# Clean-room proof of the cloud-first topology against two real PostgreSQL
# databases. Drops both, rebuilds from migrations, then exercises every path the
# architecture depends on and ASSERTS the outcome.
#
#   1. two tiers that have never spoken derive identical hardware ids
#   2. a failover backlog reaches the cloud with no id collision and no loss
#   3. mirrored rows do NOT echo back up (the loop the docs warn about)
#   4. a second pass moves nothing (no ping-pong on updated_at)
#   5. the mirror gap closes: cloud-only telemetry reaches the edge
#   6. (port_id, ts) stays unique on both tiers
#
# Usage:  bash scripts/run-cloudfirst-tests.sh
set -uo pipefail
cd "$(dirname "$0")/.."

EDGE_OWNER="postgres://senseable_owner:admin@127.0.0.1:5432/senseable"
EDGE_APP="postgres://senseable_app:app@127.0.0.1:5432/senseable"
CLOUD_OWNER="postgres://senseable_owner:admin@127.0.0.1:5432/senseable_cloud"
CLOUD_APP="postgres://senseable_app:app@127.0.0.1:5432/senseable_cloud"

# Bridge mode deliberately disables the readings pull-down (the edge is the
# SOURCE of cloud telemetry there, not a mirror of it), so the mirror assertions
# below expect different numbers.
BRIDGE="${EDGE_BRIDGES_CLOUD:-false}"
pass=0; fail=0
# SQL is passed through a file, never re-quoted into another shell, so a literal
# $$ or ' inside a query cannot be mangled by an intermediate expansion.
q() { printf '%s\n' "$2" > /tmp/q.sql; su postgres -c "psql -d $1 -At -f /tmp/q.sql"; }
ok()   { echo "  PASS  $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL  $1 (got: $2, want: $3)"; fail=$((fail+1)); }
is()   { [ "$2" = "$3" ] && ok "$1" || bad "$1" "$2" "$3"; }

echo "=== rebuild both tiers from zero ==="
su postgres -c "psql -q -c 'DROP DATABASE IF EXISTS senseable'"        >/dev/null 2>&1
su postgres -c "psql -q -c 'DROP DATABASE IF EXISTS senseable_cloud'"  >/dev/null 2>&1
su postgres -c "psql -q -f $PWD/db/00_bootstrap.sql"                   >/dev/null 2>&1
sed 's/senseable/senseable_cloud/g; s/senseable_cloud_owner/senseable_owner/g; s/senseable_cloud_app/senseable_app/g; s/senseable_cloud_ro/senseable_ro/g' \
  db/00_bootstrap.sql > /tmp/bootstrap_cloud.sql
su postgres -c "psql -q -f /tmp/bootstrap_cloud.sql"                   >/dev/null 2>&1

npm run migrate     up >/dev/null 2>&1 || { echo "edge migration FAILED";  exit 1; }
npm run migrate:cloud up >/dev/null 2>&1 || { echo "cloud migration FAILED"; exit 1; }
for db in senseable senseable_cloud; do
  su postgres -c "psql -q -d $db -f $PWD/db/seed.sql"  >/dev/null 2>&1
  su postgres -c "psql -q -d $db -c \"UPDATE tenants SET mqtt_tid='tenant-123' WHERE slug='aquatech'\"" >/dev/null
done
echo "  both tiers migrated and seeded independently"

echo
echo "=== 1. cloud-first normal operation: node publishes to the CLOUD broker ==="
TIER=cloud DATABASE_URL="$CLOUD_APP" DATABASE_URL_OWNER="$CLOUD_OWNER" \
  TEST_TS_BASE=$(node -e "console.log(Date.parse('2026-09-15T00:00:00Z'))") \
  node scripts/test-cloudfirst.js >/dev/null 2>&1 || { echo "cloud ingest FAILED"; exit 1; }
is "cloud ingested 12 readings"        "$(q senseable_cloud 'select count(*) from readings')" "12"
is "cloud rows need no upward sync"    "$(q senseable_cloud 'select count(*) from readings where not synced')" "0"

echo
echo "=== 2. WAN drop: node fails over to the LOCAL broker, edge ingests alone ==="
DATABASE_URL="$EDGE_APP" DATABASE_URL_OWNER="$EDGE_OWNER" \
  TEST_TS_BASE=$(node -e "console.log(Date.parse('2026-09-15T00:10:00Z'))") \
  node scripts/test-cloudfirst.js --edge-failover >/dev/null 2>&1 || { echo "edge ingest FAILED"; exit 1; }
is "edge ingested 12 readings"         "$(q senseable 'select count(*) from readings')" "12"
is "all 12 are queued for the cloud"   "$(q senseable 'select count(*) from readings where not synced')" "12"
is "all 12 marked origin=local"        "$(q senseable "select count(*) from readings where origin = 'local'")" "12"

echo
echo "=== 3. deterministic identity: two tiers, never synced, same uuids ==="
EH=$(q senseable        "select md5(string_agg(x,',' order by x)) from (select device_id::text x from devices union all select module_id::text from modules union all select port_id::text from ports union all select actuator_id::text from actuators union all select tenant_id::text from tenants union all select user_id::text from users) t")
CH=$(q senseable_cloud  "select md5(string_agg(x,',' order by x)) from (select device_id::text x from devices union all select module_id::text from modules union all select port_id::text from ports union all select actuator_id::text from actuators union all select tenant_id::text from tenants union all select user_id::text from users) t")
is "identity fingerprints match"       "$EH" "$CH"

echo
echo "=== 4. link restored: reconciliation pass ==="
OUT=$(npm run sync:once 2>&1)
echo "$OUT" | grep -E '↑|↓|FAILED' | sed 's/^/     /'
is "no table failed"                   "$(echo "$OUT" | grep -c FAILED)" "0"
is "cloud now holds both streams"      "$(q senseable_cloud 'select count(*) from readings')" "24"
is "backlog fully drained"             "$(q senseable 'select count(*) from readings where not synced')" "0"
is "failover rows survived the merge"  "$(q senseable_cloud "select count(*) from readings where origin = 'local'")" "12"
if [ "$BRIDGE" = "true" ]; then
  is "bridge mode: no mirror pulled back" "$(q senseable 'select count(*) from readings')" "12"
else
  is "edge mirrored the cloud stream"    "$(q senseable 'select count(*) from readings')" "24"
fi

echo
echo "=== 5. idempotency: a second pass must move nothing ==="
OUT2=$(npm run sync:once 2>&1)
MOVED=$(echo "$OUT2" | grep -oE '[0-9]+ row\(s\) replicated' | grep -oE '^[0-9]+')
is "second pass replicates 0 rows"     "${MOVED:-x}" "0"
is "no row duplication on cloud"       "$(q senseable_cloud 'select count(*) from readings')" "24"
if [ "$BRIDGE" != "true" ]; then
  is "no row duplication on edge"      "$(q senseable 'select count(*) from readings')" "24"
fi

echo
echo "=== 6. mirror gap: cloud keeps ingesting while the edge is not listening ==="
TIER=cloud DATABASE_URL="$CLOUD_APP" DATABASE_URL_OWNER="$CLOUD_OWNER" \
  TEST_TS_BASE=$(node -e "console.log(Date.parse('2026-09-15T00:20:00Z'))") \
  node scripts/test-cloudfirst.js >/dev/null 2>&1
is "cloud ahead of edge"               "$(q senseable_cloud 'select count(*) from readings')" "36"
npm run sync:once >/dev/null 2>&1
if [ "$BRIDGE" = "true" ]; then
  is "bridge mode: pull-down stays off" "$(q senseable 'select count(*) from readings')" "12"
else
  is "edge caught up by pull-down"     "$(q senseable 'select count(*) from readings')" "36"
fi
is "pulled rows are not re-queued"     "$(q senseable 'select count(*) from readings where not synced')" "0"

echo
echo "=== 7. dedupe key holds on both tiers ==="
is "edge has no (port_id, ts) dupes"   "$(q senseable 'select count(*) from (select port_id, ts from readings group by 1,2 having count(*)>1) d')" "0"
is "cloud has no (port_id, ts) dupes"  "$(q senseable_cloud 'select count(*) from (select port_id, ts from readings group by 1,2 having count(*)>1) d')" "0"

echo
echo "=== 8. same sample ingested by BOTH tiers converges to one row ==="
TIER=cloud DATABASE_URL="$CLOUD_APP" DATABASE_URL_OWNER="$CLOUD_OWNER" \
  TEST_TS_BASE=$(node -e "console.log(Date.parse('2026-09-15T00:30:00Z'))") TEST_PACKETS=1 \
  node scripts/test-cloudfirst.js >/dev/null 2>&1
DATABASE_URL="$EDGE_APP" DATABASE_URL_OWNER="$EDGE_OWNER" \
  TEST_TS_BASE=$(node -e "console.log(Date.parse('2026-09-15T00:30:00Z'))") TEST_PACKETS=1 \
  node scripts/test-cloudfirst.js --edge-failover >/dev/null 2>&1
npm run sync:once >/dev/null 2>&1
is "overlap collapsed, not duplicated" "$(q senseable_cloud "select count(*) from readings where ts = '2026-09-15T00:30:00Z'")" "4"

echo
echo "────────────────────────────────────────────"
echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ] || exit 1
