# Cloud-First with Edge Failover

How SENSEable routes data after the September 2026 re-architecture, what changed
in this repository, and what is still open.

## The three phases

**Phase 1, normal operation.** The node publishes to the cloud MQTT broker. A
bridge writes those packets into Supabase. The edge server subscribes to the same
cloud broker and mirrors every packet into local Postgres as
`origin='cloud', synced=true`. The dashboard reads Supabase, so it works from
anywhere.

**Phase 2, failover.** The cloud link drops. The node tears down its cloud socket
and publishes to the local Mosquitto broker instead. The edge ingests those
packets as `origin='local', synced=false`. It is now the only holder of that
data. The dashboard on the LAN keeps working; a remote viewer sees a frozen
picture and is told so.

**Phase 3, reconciliation.** The link returns. The node resumes publishing to the
cloud. The sync worker drains the `synced = false` backlog upward, and pulls down
anything the cloud ingested while the edge was not listening.

## What makes it safe

**Identity is derived, not generated.** Under the old topology only the edge ever
created a row, so `gen_random_uuid()` was unique by construction. Cloud-first
lets either tier see new hardware first, and two random uuids for the same
physical port would collide on `UNIQUE (tenant_id, node_id)` the first time sync
ran. Migration 009 makes every id a UUIDv5 of its natural key:

    tenant   = f(slug)
    user     = f(email)
    device   = f(tenant_id, node_id)
    module   = f(device_id, i2c_address)
    port     = f(module_id, port_code)
    actuator = f(device_id, port)

Two databases that have never communicated derive identical ids. Existing rows
are realigned in place, which is only safe because every foreign key is rebuilt
with `ON UPDATE CASCADE` first. `derive_pk()` BEFORE INSERT triggers keep future
rows aligned wherever they are created: seed files, migrations, provisioning, or
the dashboard.

**Readings dedupe on a portable key.** `reading_id` is a bigint identity minted
independently on both tiers, so it can no longer be carried across. The backlog
push drops it and conflicts on `UNIQUE (port_id, ts)` instead. This is the bug
that mattered most: the old code shipped the local id with `OVERRIDING SYSTEM
VALUE` and `ON CONFLICT (reading_id) DO NOTHING`, which under cloud-first would
have made every failover row collide with an unrelated cloud row and vanish
silently, while the worker reported success.

**Mirrored rows never echo.** A row the edge received over the cloud broker is
already in Supabase, so it is written `synced = true` and the backlog drain never
sees it. Without that flag the sync worker would push it back up, the cloud
broker would fan it out to the edge again, and the two would feed each other.

**Replicated rows do not ping-pong.** `set_updated_at()` now honours
`app.sync_replay`, so a row copied between tiers keeps its source timestamp
instead of looking freshly edited to the other direction's watermark.

## Wire protocol note

The frozen `tlm` packet carries no timestamp, so `ts` defaults to ingest time.
Ingest time differs between the cloud path and the failover path, which means a
sample ingested by both tiers during the changeover window can appear twice.
`sampleTime()` in `src/ingest.js` already honours a device-supplied `ts` (epoch
ms or ISO-8601) the moment the firmware sends one. **This needs a firmware
change and it is the one open item with lead time.**

## Configuration

    MQTT_URL=mqtts://xxxx.hivemq.cloud:8883    # CLOUD broker (primary)
    MQTT_LOCAL_URL=mqtt://192.168.1.50:1883    # LOCAL broker (failover ingest)
    MQTT_USERNAME= / MQTT_PASSWORD=            # shared, or per broker:
    MQTT_CLOUD_USERNAME= / MQTT_LOCAL_USERNAME=
    MQTT_CLOUD_CA_CERT= / MQTT_LOCAL_TLS_INSECURE=true

    TIER=edge|cloud            # 'cloud' makes every ingested row born synced
    SYNC_PULL=true             # downward pass; false = legacy one-way mode
    STATIC_DIR=./public        # failover dashboard, see below
    COMMAND_MAX_AGE_MS=600000  # outbox expiry

Both brokers are optional. With only `MQTT_LOCAL_URL` set the server behaves
exactly like the previous edge-primary build.

## The failover dashboard

A browser will not let an `https://` page fetch `http://192.168.x.x`. That is
mixed content and no CSP entry or header gets around it, so the Vercel-hosted
dashboard cannot fall back to the edge on its own. The edge therefore serves its
own copy of the app:

    cd ../SENSEable && npm run build && cp -r dist ../SENSEable-API/public

During an outage the operator opens `http://<edge-host>:4000` and gets a
same-origin dashboard with full write access. `VITE_EDGE_URL` puts that address
in the banner the cloud build shows when it cannot reach its server.

## The downlink

A Vercel function cannot hold an MQTT connection, so the cloud tier does not
publish commands. `POST /api/commands` writes a row to the `commands` outbox with
`published_at` NULL. The sync worker's downward pass brings it to the edge, and
`src/dispatch.js` publishes it on whichever broker last carried traffic. Commands
older than `COMMAND_MAX_AGE_MS` are marked failed rather than delivered late: an
actuator instruction authored an hour ago and executed now is worse than one
never executed.

`published_at` is deliberately separate from `status`. `status` is the ack
lifecycle and belongs to the hardware; `published_at` is transport. A command can
be published and still pending, and conflating them would re-send anything
awaiting an ack on every sweep.

## Running the tests

    bash scripts/run-cloudfirst-tests.sh   # 20 assertions, rebuilds both tiers
    node scripts/test-dispatch.js          # outbox drain and expiry
    node ../SENSEable/api/test-writes.mjs  # cloud mutations under real RLS

The first drops and rebuilds both databases from migrations, so never point it at
anything you care about.

## Still open

1. Device-supplied `ts` in the `tlm` packet (firmware).
2. The cloud MQTT to Supabase bridge itself. Broker rule engine posting to a
   Vercel function needs no new host; an always-on worker on Fly or Railway is
   more flexible. Whichever is chosen must import the same ingest logic rather
   than reimplementing calibration.
3. `src/read.js` was deleted in the audit commit, so the edge projection
   (`store.js`) and the cloud projection (`api/_read.js`) can now drift. Under
   cloud-first the cloud one is what evaluators see. Worth a diff pass.
4. Notification generation exists only on the edge. If the cloud bridge also
   generates them, they will duplicate; if it does not, there are no alerts
   during normal operation.
