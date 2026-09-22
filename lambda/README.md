# Cloud bridge on AWS Lambda

HiveMQ → Supabase with no server to run. EventBridge Scheduler invokes
`lambda/bridge.handler` once a minute. The function holds a **persistent MQTT
session** (fixed client id, `clean: false`), so HiveMQ queues every QoS 1
message published while it is not running; each run drains the queue through
the same ingest code as the edge server and writes to Supabase (`TIER=cloud`).

```
ESP32 → HiveMQ ──(queued in the bridge's session)──→ Lambda, every minute → Supabase → Vercel
              └──→ edge server (mirror; notifications + command dispatch)
```

- Latency: up to about a minute. Readings keep the device timestamp.
- Data-only (`BRIDGE_SIDE_EFFECTS=false`): alerts and command dispatch stay on
  the edge server. With the edge off, readings still reach Supabase; alerts and
  commands wait for it.
- Build: `bash lambda/build.sh` → `dist/senseable-bridge.zip`.
- Environment: `bash lambda/make-env.sh` → `~/senseable-lambda-env.json`.
