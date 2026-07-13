// 1730000000000_init.js
// SENSEable initial schema (thesis ERDs I.1 Profiles, I.2 Tenant & Access,
// I.3 Devices & Sensing). Validated against PostgreSQL 16 before commit.
//
// ESM file — ensure senseable-backend/package.json has "type": "module"
// (or rename this to .mjs). node-pg-migrate executes the SQL below verbatim.

export const up = (pgm) => {
  pgm.sql(`
-- Enums (closed, stable value sets — these mirror the frozen protocol vocab)
CREATE TYPE tenant_status AS ENUM ('active','suspended','trial');
CREATE TYPE user_status   AS ENUM ('active','invited','disabled');
CREATE TYPE device_status AS ENUM ('online','warning','fault','offline');
CREATE TYPE comm_mode     AS ENUM ('Wi-Fi','Cellular');
CREATE TYPE sensor_status AS ENUM ('Normal','Warning','Fault','Offline');
CREATE TYPE actuator_mode AS ENUM ('pwm','binary');
CREATE TYPE actuator_ack  AS ENUM ('ok','executed','pending','rejected','expired');

-- generic updated_at trigger
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

-- ── Tenant & Access (thesis ERD I.2) ─────────────────────────────────────────
CREATE TABLE roles (
  role_id     text PRIMARY KEY,                       -- 'designer','operator'
  role_name   text NOT NULL,
  tier        smallint NOT NULL CHECK (tier BETWEEN 1 AND 3),
  description text NOT NULL DEFAULT ''
);

CREATE TABLE tenants (
  tenant_id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  slug       text NOT NULL UNIQUE,                    -- 'aquatech'
  org_code   text NOT NULL UNIQUE,                    -- 'AQUA-7421' (signup join code)
  name       text NOT NULL,
  status     tenant_status NOT NULL DEFAULT 'active',
  plan       text NOT NULL DEFAULT 'Pilot',
  max_users  integer NOT NULL DEFAULT 5 CHECK (max_users > 0),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE users (
  user_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id     uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  role_id       text NOT NULL REFERENCES roles(role_id),
  full_name     text NOT NULL,
  email         citext NOT NULL UNIQUE,               -- case-insensitive login
  password_hash text NOT NULL,                        -- bcrypt; never plaintext
  status        user_status NOT NULL DEFAULT 'active',
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_users_tenant ON users(tenant_id);

-- ── Profiles (thesis ERD I.1) ────────────────────────────────────────────────
-- Reusable sensor metadata templates ("saved sensor profiles" in the paper).
-- Reconstructed from sensorProfiles[] in mockData.js — confirm columns against
-- Fig. I-1 in your paper (that figure did not render in the compressed PDF).
CREATE TABLE sensor_profiles (
  profile_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  name       text NOT NULL,                           -- 'Dissolved Oxygen (mg/L)'
  label      text NOT NULL,                           -- 'Dissolved Oxygen'
  unit       text NOT NULL,                           -- 'mg/L'
  range_min  double precision NOT NULL,
  range_max  double precision NOT NULL,
  safe_min   double precision NOT NULL,
  safe_max   double precision NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, name),
  CHECK (range_max > range_min),
  CHECK (safe_min >= range_min AND safe_max <= range_max AND safe_max >= safe_min)
);

-- ── Devices & Sensing (thesis ERD I.3) ───────────────────────────────────────
CREATE TABLE devices (
  device_id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  node_id    text NOT NULL,                           -- 'N001' (firmware/topic address)
  name       text NOT NULL,                           -- 'ESP32 Module 1'
  status     device_status NOT NULL DEFAULT 'offline',
  comm_mode  comm_mode NOT NULL DEFAULT 'Wi-Fi',
  uptime_s   bigint  NOT NULL DEFAULT 0,
  rssi       integer,
  free_heap  integer,
  last_seen  timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, node_id)                          -- node id unique within a tenant
);
CREATE INDEX ix_devices_tenant ON devices(tenant_id);

CREATE TABLE modules (
  module_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  device_id   uuid NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
  tenant_id   uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE, -- denormalized for RLS
  i2c_address text NOT NULL,                           -- '0x48'
  name        text NOT NULL,                           -- 'Expansion Board 1'
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (device_id, i2c_address)
);
CREATE INDEX ix_modules_device ON modules(device_id);
CREATE INDEX ix_modules_tenant ON modules(tenant_id);

CREATE TABLE calibration_formulas (
  formula_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  label      text NOT NULL,                            -- 'DO','salinity','temperaTURE'
  expression text NOT NULL,                            -- mathjs-hardened expression
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, label)
);
CREATE INDEX ix_formulas_tenant ON calibration_formulas(tenant_id);

CREATE TABLE ports (
  port_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  module_id   uuid NOT NULL REFERENCES modules(module_id) ON DELETE CASCADE,
  tenant_id   uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE, -- denormalized for RLS
  port_code   text NOT NULL,                           -- 'A0' (ADC channel on the board)
  label       text NOT NULL,                           -- 'Dissolved Oxygen'
  unit        text NOT NULL,                           -- 'mg/L'
  range_min   double precision NOT NULL,
  range_max   double precision NOT NULL,
  safe_min    double precision NOT NULL,
  safe_max    double precision NOT NULL,
  -- channel assignment: which calibration formula converts this port's raw ADC.
  -- ON DELETE SET NULL mirrors the frontend rule "deleting a formula clears it
  -- from any channel it was assigned to".
  formula_id  uuid REFERENCES calibration_formulas(formula_id) ON DELETE SET NULL,
  -- cached most-recent converted reading (source of truth is \`readings\`)
  last_value  double precision,
  last_status sensor_status,
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (module_id, port_code),
  CHECK (range_max > range_min)
);
CREATE INDEX ix_ports_module  ON ports(module_id);
CREATE INDEX ix_ports_tenant  ON ports(tenant_id);
CREATE INDEX ix_ports_formula ON ports(formula_id);

CREATE TABLE actuators (
  actuator_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  device_id     uuid NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
  tenant_id     uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  actuator_code text NOT NULL,                         -- 'fan01'
  name          text NOT NULL,                         -- 'Circulation Fan'
  port          text NOT NULL,                         -- 'OUT1'..'OUT16'
  channel       integer,                               -- display metadata
  gpio          integer,                               -- display metadata
  mode          actuator_mode NOT NULL DEFAULT 'pwm',
  state         smallint NOT NULL DEFAULT 0 CHECK (state IN (0,1)),
  duty          smallint NOT NULL DEFAULT 0 CHECK (duty BETWEEN 0 AND 100), -- operator %
  dur           integer  NOT NULL DEFAULT 0 CHECK (dur >= 0),               -- auto-off seconds
  last_ack      actuator_ack NOT NULL DEFAULT 'pending',
  updated_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (device_id, port)
);
CREATE INDEX ix_actuators_device ON actuators(device_id);
CREATE INDEX ix_actuators_tenant ON actuators(tenant_id);
CREATE TRIGGER trg_actuators_updated BEFORE UPDATE ON actuators
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Telemetry. The frozen wire protocol: the edge node ships only raw 16-bit ADC
-- counts; the backend calibration engine fills \`value\` + \`status\`. High-volume
-- append-only time series → bigint identity PK, indexed on (port, ts).
CREATE TABLE readings (
  reading_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  port_id    uuid NOT NULL REFERENCES ports(port_id) ON DELETE CASCADE,
  tenant_id  uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  ts         timestamptz NOT NULL DEFAULT now(),
  raw_adc    integer NOT NULL,                         -- raw ADS1115 count (frozen contract)
  value      double precision,                         -- converted engineering unit
  status     sensor_status
);
CREATE INDEX ix_readings_port_ts   ON readings(port_id, ts DESC);
CREATE INDEX ix_readings_tenant_ts ON readings(tenant_id, ts DESC);

CREATE TABLE notifications (
  notification_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id  uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  port_id    uuid REFERENCES ports(port_id) ON DELETE SET NULL,
  type       text NOT NULL CHECK (type IN ('info','success','warning','fault')),
  title      text NOT NULL,
  message    text NOT NULL,
  is_read    boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_notifications_tenant ON notifications(tenant_id, created_at DESC);

-- Interactive-map layouts. Replaces the localStorage-only "Save as Profile"
-- feature with a tenant-scoped, team-shared resource (a SaaS-readiness gap you
-- flagged). Layout JSON keeps the exact {sensors:[...], shapes:[...]} shape the
-- Konva canvas already exports.
CREATE TABLE map_profiles (
  map_profile_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  name       text NOT NULL,
  layout     jsonb NOT NULL,
  created_by uuid REFERENCES users(user_id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, name)
);
CREATE INDEX ix_map_profiles_tenant ON map_profiles(tenant_id);
CREATE TRIGGER trg_map_profiles_updated BEFORE UPDATE ON map_profiles
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
`);
};

export const down = (pgm) => {
  pgm.sql(`
    DROP TABLE IF EXISTS map_profiles, notifications, readings, actuators, ports,
      calibration_formulas, modules, devices, sensor_profiles, users, tenants, roles CASCADE;
    DROP FUNCTION IF EXISTS set_updated_at() CASCADE;
    DROP TYPE IF EXISTS actuator_ack, actuator_mode, sensor_status, comm_mode,
      device_status, user_status, tenant_status;
  `);
};
