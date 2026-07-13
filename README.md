# SENSEable — Barebones Backend

Ingests raw ADC telemetry (frozen wire protocol), applies calibration in the
backend (never on the edge), and serves the React dashboard's exact data shape
over REST. Runs with **no database and no broker** — in-memory store seeded
with dummy data.

## Run

```bash
npm install
npm start                # http://localhost:4000
npm run simulate         # (separate terminal) fake ESP32 posting telemetry
```

Then point the frontend at it: copy `frontend/api.js` into the app as
`src/api.js`, set `VITE_API_URL=http://localhost:4000` if needed, and swap the
static `mockData` imports for the fetch helpers (see the example at the bottom
of `api.js`). Data shapes are identical, so component render logic is untouched.

## Data flow

```
ESP32 → (raw ADC counts) → ingest.js → calibration.js → store.js → REST → React
                                            │
                        raw count ──► engineering value (mg/L, PSU, °C)
```

The edge sends only raw 16-bit counts + per-port health codes. `calibration.js`
converts to engineering units (linear by default, optional expression formula
with a hardened mathjs evaluator). `status.js` folds health codes + safe-range
checks + staleness into the UI's `Normal | Warning | Fault | Offline` states.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET  | `/api/devices` | Full node→module→port tree (frontend shape) |
| GET/POST | `/api/notifications`, `/notifications/read-all` | Notifications |
| GET/POST/DELETE | `/api/formulas` | Calibration formula bank |
| POST | `/api/formulas/fit` | Least-squares fit from `{raw,value}` points |
| GET/PUT | `/api/channel-assignments/...` | Per-board channel→formula map |
| GET/PUT | `/api/map-sensors` | Interactive-map sensor placements |
| POST | `/api/ingest/telemetry` | Ingest a `tlm` payload (broker-free) |
| POST | `/api/ingest/discovery` | Ingest a `dsc` payload (broker-free) |
| GET  | `/api/health` | Liveness |

## Live MQTT (optional)

Set `MQTT_URL` (+ credentials) in `.env` to also subscribe to
`telemetry/{appId}/+` and `discovery/{appId}/+/init`. Unset, the server runs
fine on HTTP ingest alone.

## ⚠ Spec vs. firmware discrepancy

Your frozen spec and your current firmware disagree on port health codes:

| Code | Frozen spec | firmware `evaluate_port_status()` |
|------|-------------|-----------------------------------|
| 1 | CELLULAR_ACTIVE | open circuit |
| 2 | I2C_BUS_TIMEOUT | saturation/overflow |
| 3 | DRV_THERMAL_SHUTDOWN | I²C timeout |
| 5 | PORT_OPEN_CIRCUIT | (unused) |

The firmware also omits `sid`, `q`, and the `st` block and hardcodes
`tid:"tenant-123"`. Ingest tolerates the missing fields, and
`STATUS_PROFILE=spec|firmware` (in `.env`) picks which code table to trust.
Reconcile the firmware to the frozen spec before this matters in production —
it's a single mapping table in `status.js`.
