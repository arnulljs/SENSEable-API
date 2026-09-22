# Cloud bridge

The `[bridge]` in `docs/CLOUD-FIRST.md`: this repository's server, run with
`TIER=cloud` on an always-on machine, subscribed to HiveMQ and writing straight
into Supabase. Without it the laptop is in the cloud's data path
(`EDGE_BRIDGES_CLOUD`), and turning the laptop off freezes the deployed site.

```
ESP32 → HiveMQ ─┬─→ bridge (TIER=cloud) → Supabase → Vercel → browser
                └─→ edge (laptop, mirror) → local Postgres
failover: ESP32 → local Mosquitto → edge → local Postgres → sync → Supabase
```

## Who does what

| | bridge | edge, bridge running | edge, no bridge |
|---|---|---|---|
| Ingest cloud-broker packets | yes, into Supabase | yes, local mirror | yes, and syncs up |
| Notifications, command dispatch | yes | only while failed over | yes |
| Claim-on-connect | no (cannot see dashboards) | yes, pins sync up | yes |

Pins (`node_tenant_assignments`) and deletes replicate both ways through
`npm run sync` (migrations 012 and 013), and each server re-reads pins every
`ROUTING_REFRESH_MS`, so both tiers file a board under the same organization.

## Requirements

An always-on Linux machine with outbound 8883 (HiveMQ) and 5432 (Supabase
session pooler). Nothing listens publicly: the server binds 127.0.0.1. A host
that sleeps idle services will not work — the process must stay subscribed.

## Install

1. Laptop: `bash deploy/bridge/make-env.sh` → `~/senseable-bridge.env`
2. Copy that file and `deploy/bridge/install.sh` to the VM.
3. VM: `sudo bash install.sh ./senseable-bridge.env`
4. Laptop `.env`: `CLOUD_BRIDGE_RUNNING=true`, `EDGE_BRIDGES_CLOUD=false`;
   restart `npm start` and `npm run sync`.

Update later: re-run `sudo bash /opt/senseable-bridge/deploy/bridge/install.sh`.
Logs: `journalctl -u senseable-bridge -f`.
