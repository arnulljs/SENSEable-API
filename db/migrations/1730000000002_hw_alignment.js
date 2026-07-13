// 1730000000002_hw_alignment.js
// Aligns the schema with what the ESP32 firmware ACTUALLY sends
// (verified against SENSEable-HW/blink/main/blink_example_main.c):
//
//   telemetry: {"t":"tlm","v":1,"tid":"tenant-123","nid":"N001","ts":...,
//               "adc":[{"a":"0x48","p":[[channel, raw_int16, status_code], ...]}]}
//
//   * `tid` is an opaque broker-side tenant string ("tenant-123") that does NOT
//     match our slug. -> tenants.mqtt_tid maps it to the real tenant uuid.
//     WITHOUT this, ingest resolves nodes by nid alone and a node claiming
//     "N001" writes into whichever tenant happens to own N001 globally.
//   * ports are addressed by INTEGER channel (0..3), not the 'A0' string.
//     -> ports.port_index. (This is `port_index` in thesis ERD Fig. I-1.)
//   * discovery reports which ports actually have a sensor attached.
//     -> ports.active_flag (also Fig. I-1).
//
// Also adds the per-port calibration the backend's applyCalibration() expects
// ({type:'linear', slope, offset}) and a tenant-scoped map_sensors table so the
// interactive map stops living in localStorage.

export const up = (pgm) => {
  pgm.sql(`
    -- Broker-side tenant string -> our tenant. Nullable: a tenant that has no
    -- hardware yet (llba) simply has no mqtt_tid.
    ALTER TABLE tenants ADD COLUMN mqtt_tid text UNIQUE;

    ALTER TABLE ports
      ADD COLUMN port_index  integer,
      ADD COLUMN active_flag boolean NOT NULL DEFAULT true,
      ADD COLUMN cal_type    text    NOT NULL DEFAULT 'linear',
      ADD COLUMN cal_slope   double precision NOT NULL DEFAULT 1,
      ADD COLUMN cal_offset  double precision NOT NULL DEFAULT 0;

    -- Backfill port_index from the existing 'A0'/'A1'/... codes.
    UPDATE ports SET port_index = CAST(substring(port_code from 2) AS integer)
      WHERE port_code ~ '^A[0-9]+$';

    ALTER TABLE ports ALTER COLUMN port_index SET NOT NULL;
    ALTER TABLE ports ADD CONSTRAINT ports_cal_type_chk
      CHECK (cal_type IN ('linear','expr'));
    ALTER TABLE ports ADD CONSTRAINT ports_module_index_uniq
      UNIQUE (module_id, port_index);

    -- A node addresses a chip by I2C address and a port by integer channel, so
    -- this is the index the ingest hot path actually uses.
    CREATE INDEX ix_ports_index ON ports(module_id, port_index);

    -- Interactive-map placements. Only position + which physical channel is
    -- stored; label/unit/status are resolved from the live device tree at
    -- render time (exactly how InteractiveMap.jsx already works).
    CREATE TABLE map_sensors (
      map_sensor_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
      port_id   uuid NOT NULL REFERENCES ports(port_id) ON DELETE CASCADE,
      x double precision NOT NULL,
      y double precision NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE (tenant_id, port_id)
    );
    CREATE INDEX ix_map_sensors_tenant ON map_sensors(tenant_id);

    ALTER TABLE map_sensors ENABLE ROW LEVEL SECURITY;
    CREATE POLICY tenant_isolation ON map_sensors
      USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
      WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);
  `);
};

export const down = (pgm) => {
  pgm.sql(`
    DROP TABLE IF EXISTS map_sensors CASCADE;
    DROP INDEX IF EXISTS ix_ports_index;
    ALTER TABLE ports
      DROP CONSTRAINT IF EXISTS ports_module_index_uniq,
      DROP CONSTRAINT IF EXISTS ports_cal_type_chk;
    ALTER TABLE ports
      DROP COLUMN IF EXISTS port_index,
      DROP COLUMN IF EXISTS active_flag,
      DROP COLUMN IF EXISTS cal_type,
      DROP COLUMN IF EXISTS cal_slope,
      DROP COLUMN IF EXISTS cal_offset;
    ALTER TABLE tenants DROP COLUMN IF EXISTS mqtt_tid;
  `);
};
