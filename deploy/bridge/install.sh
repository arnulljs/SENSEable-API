#!/usr/bin/env bash
# Run on the BRIDGE VM (Ubuntu 22.04/24.04) as root:
#   sudo bash install.sh /path/to/senseable-bridge.env
# Idempotent: re-running it updates the code to the tip of $BRANCH and restarts.
set -euo pipefail
ENV_SRC=${1:-}
REPO=${REPO:-https://github.com/arnulljs/SENSEable-API.git}
BRANCH=${BRANCH:-cloud-first}
DIR=/opt/senseable-bridge
ENVF=/etc/senseable-bridge.env

if ! command -v node >/dev/null || [ "$(node -p 'process.versions.node.split(".")[0]')" -lt 20 ]; then
  apt-get update -qq
  apt-get install -y -qq ca-certificates curl gnupg git
  curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
  apt-get install -y -qq nodejs
fi
command -v git >/dev/null || apt-get install -y -qq git

id -u senseable >/dev/null 2>&1 || useradd --system --home "$DIR" --shell /usr/sbin/nologin senseable
if [ -d "$DIR/.git" ]; then
  git -C "$DIR" fetch -q origin
  git -C "$DIR" checkout -q "$BRANCH"
  git -C "$DIR" reset -q --hard "origin/$BRANCH"
else
  git clone -q -b "$BRANCH" "$REPO" "$DIR"
fi
(cd "$DIR" && npm ci --omit=dev --no-audit --no-fund)
chown -R senseable:senseable "$DIR"

if [ -n "$ENV_SRC" ]; then install -m 600 -o root -g root "$ENV_SRC" "$ENVF"; fi
[ -f "$ENVF" ] || { echo "no $ENVF — pass the file made by make-env.sh: sudo bash install.sh ./senseable-bridge.env"; exit 1; }
grep -qE '^TIER="?cloud"?$' "$ENVF" || { echo "$ENVF does not set TIER=cloud — refusing to start a second EDGE against Supabase"; exit 1; }

install -m 644 "$DIR/deploy/bridge/senseable-bridge.service" /etc/systemd/system/senseable-bridge.service
systemctl daemon-reload
systemctl enable -q senseable-bridge
systemctl restart senseable-bridge
sleep 6
systemctl --no-pager --lines=0 status senseable-bridge | head -3
journalctl -u senseable-bridge --no-pager -n 25 | grep -E "role|hydrated|mqtt:cloud\]|rror|REJECT" || true
curl -s 127.0.0.1:4000/api/health | head -c 400; echo
