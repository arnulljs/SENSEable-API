#!/usr/bin/env bash
# Builds dist/senseable-bridge.zip for AWS Lambda (Node.js 22, handler
# lambda/bridge.handler). Run from anywhere; production dependencies only.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
cp -r "$ROOT/src" "$ROOT/lambda" "$STAGE/"
mkdir -p "$STAGE/db" && cp "$ROOT/db/pool.js" "$STAGE/db/"
cp "$ROOT/package.json" "$ROOT/package-lock.json" "$STAGE/"
(cd "$STAGE" && npm ci --omit=dev --no-audit --no-fund --silent)
mkdir -p "$ROOT/dist"
rm -f "$ROOT/dist/senseable-bridge.zip"
(cd "$STAGE" && zip -qr "$ROOT/dist/senseable-bridge.zip" .)
echo "built $ROOT/dist/senseable-bridge.zip ($(du -h "$ROOT/dist/senseable-bridge.zip" | cut -f1))"
