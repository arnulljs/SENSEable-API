#!/usr/bin/env bash
# Wipe test-flagged organizations (and their Supabase Auth users) on demand.
# Usage: bash wipe-test-accounts.sh            # wipe ALL test accounts now
#        bash wipe-test-accounts.sh 24         # wipe test accounts older than 24 hours
# Needs ADMIN_WIPE_TOKEN set in the Vercel env AND passed here as ADMIN_WIPE_TOKEN.
set -euo pipefail
SITE=${SITE:-https://senseable-iot.vercel.app}
HOURS=${1:-0}
: "${ADMIN_WIPE_TOKEN:?set ADMIN_WIPE_TOKEN (same value as the Vercel env var)}"
curl -s -X POST "$SITE/api/orgs/wipe" \
  -H "x-admin-token: $ADMIN_WIPE_TOKEN" \
  -H 'content-type: application/json' \
  -d "{\"olderThanHours\": $HOURS}" | jq . 2>/dev/null || cat
echo
