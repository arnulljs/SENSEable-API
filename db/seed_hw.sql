-- seed_hw.sql
-- Run AFTER db/seed.sql and after migration 1730000000002_hw_alignment.
-- Idempotent. Run as the OWNER:
--   psql "postgres://senseable_owner:PASS@localhost:5432/senseable" -f db/seed_hw.sql
--
-- Fills in the three things the hardware alignment needs:
--   1. tenants.mqtt_tid  -- the firmware hardcodes tid="tenant-123"
--   2. ports cal_slope/cal_offset -- raw ADC -> engineering units (server-side,
--      per the frozen wire protocol; the node ships raw counts only)
--   3. map_sensors -- the interactive map's placements, moved out of localStorage
DO $$
DECLARE
  v_aqua uuid;
  v_p0 uuid; v_p1 uuid; v_p2 uuid;
BEGIN
  SELECT tenant_id INTO v_aqua FROM tenants WHERE slug = 'aquatech';

  -- 1. Map the firmware's broker-side tenant string to the real tenant.
  --    Change 'tenant-123' here (or the #define in the firmware) once you pick
  --    a convention; this row is the ONLY place the two vocabularies meet.
  UPDATE tenants SET mqtt_tid = 'tenant-123' WHERE tenant_id = v_aqua;

  -- 2. Per-port linear calibration (matches the backend's store.js seed).
  UPDATE ports SET cal_type='linear', cal_slope=0.0005, cal_offset=1.0745
    WHERE tenant_id=v_aqua AND port_code='A0';
  UPDATE ports SET cal_type='linear', cal_slope=0.0020, cal_offset=9.67
    WHERE tenant_id=v_aqua AND port_code='A1';
  UPDATE ports SET cal_type='linear', cal_slope=0.0009, cal_offset=15.20
    WHERE tenant_id=v_aqua AND port_code='A2';

  SELECT port_id INTO v_p0 FROM ports WHERE tenant_id=v_aqua AND port_code='A0';
  SELECT port_id INTO v_p1 FROM ports WHERE tenant_id=v_aqua AND port_code='A1';
  SELECT port_id INTO v_p2 FROM ports WHERE tenant_id=v_aqua AND port_code='A2';

  -- 3. Interactive-map placements (same coords as the old mockData mapSensors).
  INSERT INTO map_sensors(tenant_id, port_id, x, y) VALUES
    (v_aqua, v_p0, 210, 145),
    (v_aqua, v_p1, 375, 148),
    (v_aqua, v_p2, 295, 265)
  ON CONFLICT (tenant_id, port_id) DO NOTHING;
END $$;
