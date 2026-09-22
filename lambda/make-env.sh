#!/usr/bin/env bash
# Prints the Lambda's environment variables, built from the credentials already
# in .env (HiveMQ) and .env.cloud (Supabase). Output is JSON for
# `aws lambda update-function-configuration --environment file://...`, written
# to ~/senseable-lambda-env.json (mode 600, outside the repo).
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:-$HOME/senseable-lambda-env.json}
get() { grep -E "^$1=" "$2" 2>/dev/null | tail -1 | cut -d= -f2- | sed -E 's/^"(.*)"$/\1/; s/^'"'"'(.*)'"'"'$/\1/'; }
APP=$(get CLOUD_DATABASE_URL_POOLED .env.cloud); [ -n "$APP" ] || APP=$(get CLOUD_DATABASE_URL .env.cloud)
OWN=$(get CLOUD_DATABASE_URL_OWNER .env.cloud)
MURL=$(get MQTT_URL .env)
MU=$(get MQTT_CLOUD_USERNAME .env); [ -n "$MU" ] || MU=$(get MQTT_USERNAME .env)
MP=$(get MQTT_CLOUD_PASSWORD .env); [ -n "$MP" ] || MP=$(get MQTT_PASSWORD .env)
for v in APP OWN MURL MU MP; do [ -n "${!v}" ] || { echo "missing value for $v (check .env / .env.cloud)" >&2; exit 1; }; done
case "$MURL" in *hivemq*) ;; *) echo "MQTT_URL in .env is not HiveMQ: $MURL" >&2; exit 1 ;; esac
umask 077
node -e '
const [app, own, url, u, p] = process.argv.slice(1);
const Variables = {
  TIER: "cloud", BRIDGE_SIDE_EFFECTS: "false",
  DATABASE_URL: app, DATABASE_URL_OWNER: own, PG_SSL: "no-verify",
  PG_POOL_MAX: "2", PG_ADMIN_POOL_MAX: "1",
  MQTT_URL: url, MQTT_USERNAME: u, MQTT_PASSWORD: p,
  MQTT_CLIENT_ID: "senseable-lambda-bridge",
  INGEST_STRICT_TENANT: "true", AUTO_PROVISION: "true", CLAIM_ON_CONNECT: "false",
};
process.stdout.write(JSON.stringify({ Variables }, null, 2));
' "$APP" "$OWN" "$MURL" "$MU" "$MP" > "$OUT"
echo "wrote $OUT (mode $(stat -c %a "$OUT"))"
