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

- Latency: messages are ingested as they arrive while a run is connected. With
  the defaults (`COLLECT_IDLE_MS=3000`, `COLLECT_MAX_MS=35000`) a run ends soon
  after the queue drains, so latency is up to about a minute. To keep the run
  connected for most of each minute (readings in seconds; an unplugged board
  shows Offline about 25–35 s later, set by the board's 15 s keepalive), set
  `COLLECT_IDLE_MS=60000`, `COLLECT_MAX_MS=50000` and a function timeout of
  60 s. Keep `COLLECT_MAX_MS` well under 60 s so runs never overlap: two runs
  share one client id and would disconnect each other. At 128 MB this is about
  300,000 GB-s a month, inside Lambda's free tier (400,000).
  Readings keep the device timestamp.
- Data-only (`BRIDGE_SIDE_EFFECTS=false`): alerts and command dispatch stay on
  the edge server. With the edge off, readings still reach Supabase; alerts and
  commands wait for it.
- Build: `bash lambda/build.sh` → `dist/senseable-bridge.zip`.
- Environment: `bash lambda/make-env.sh` → `~/senseable-lambda-env.json`.
