-- seed.sql
-- Idempotent development seed. Run as the OWNER (owner bypasses RLS, so it can
-- write across every tenant):
--
--   psql "postgres://senseable_owner:PASS@localhost:5432/senseable" -f db/seed.sql
--
-- Mirrors mockData.js: tenants tnt_aquatech + tnt_llba, devices N001 (with one
-- board + 3 ports + 3 actuators) and N002 (online, no board), and an empty
-- second tenant to exercise empty-state UI. Passwords are hashed with bcrypt
-- via pgcrypto's crypt()/gen_salt('bf') — no plaintext ever lands in a column.
DO $$
DECLARE
  v_aqua uuid; v_llba uuid;
  v_n1 uuid; v_n2 uuid; v_board uuid;
  v_f_do uuid; v_f_sal uuid; v_f_tmp uuid;
  v_p_do uuid;
BEGIN
  -- roles (global lookup table, no tenant)
  INSERT INTO roles(role_id, role_name, tier, description) VALUES
    ('designer','Designer',2,'Client admin. Full operational access plus interactive-map editing.'),
    ('operator','Operator',3,'Operational access. Views map read-only.')
  ON CONFLICT (role_id) DO NOTHING;

  -- tenants
  INSERT INTO tenants(slug, org_code, name, status, plan, max_users) VALUES
    ('aquatech','AQUA-7421','AquaTech Hatchery Corp','active','Pilot',6)
  ON CONFLICT (slug) DO NOTHING;
  INSERT INTO tenants(slug, org_code, name, status, plan, max_users) VALUES
    ('llba','LLBA-3309','Lapu-Lapu Bay Aquafarms','active','Pilot',4)
  ON CONFLICT (slug) DO NOTHING;
  SELECT tenant_id INTO v_aqua FROM tenants WHERE slug='aquatech';
  SELECT tenant_id INTO v_llba FROM tenants WHERE slug='llba';

  -- users (bcrypt-hashed)
  INSERT INTO users(tenant_id, role_id, full_name, email, password_hash, status) VALUES
    (v_aqua,'designer','Mariz Santos','mariz@aquatech.ph', crypt('designer123', gen_salt('bf')),'active'),
    (v_aqua,'operator','Jay Bautista','jay@aquatech.ph',   crypt('operator123', gen_salt('bf')),'active'),
    (v_llba,'designer','Dane Lim','dane@llba.ph',          crypt('designer123', gen_salt('bf')),'active')
  ON CONFLICT (email) DO NOTHING;

  -- calibration formulas (AquaTech)
  INSERT INTO calibration_formulas(tenant_id, label, expression) VALUES
    (v_aqua,'DO','x * 0.0021 - 0.38'),
    (v_aqua,'salinity','x * 0.05182 + 0.0'),
    (v_aqua,'temperaTURE','(x * 0.003906) - 40.0')
  ON CONFLICT (tenant_id, label) DO NOTHING;
  SELECT formula_id INTO v_f_do  FROM calibration_formulas WHERE tenant_id=v_aqua AND label='DO';
  SELECT formula_id INTO v_f_sal FROM calibration_formulas WHERE tenant_id=v_aqua AND label='salinity';
  SELECT formula_id INTO v_f_tmp FROM calibration_formulas WHERE tenant_id=v_aqua AND label='temperaTURE';

  -- sensor profiles (reusable templates)
  INSERT INTO sensor_profiles(tenant_id, name, label, unit, range_min, range_max, safe_min, safe_max) VALUES
    (v_aqua,'Dissolved Oxygen (mg/L)','Dissolved Oxygen','mg/L',0,20,6,9),
    (v_aqua,'Salinity (PSU)','Salinity','PSU',0,70,35,50),
    (v_aqua,'Temperature (C)','Temperature','C',0,50,25,32)
  ON CONFLICT (tenant_id, name) DO NOTHING;

  -- devices
  INSERT INTO devices(tenant_id, node_id, name, status, comm_mode, uptime_s, rssi, free_heap, last_seen) VALUES
    (v_aqua,'N001','ESP32 Module 1','online','Wi-Fi',86400,-61,138240, now())
  ON CONFLICT (tenant_id, node_id) DO NOTHING;
  INSERT INTO devices(tenant_id, node_id, name, status, comm_mode, uptime_s, rssi, free_heap, last_seen) VALUES
    (v_aqua,'N002','ESP32 Module 2','online','Wi-Fi',41760,-54,151040, now())
  ON CONFLICT (tenant_id, node_id) DO NOTHING;
  SELECT device_id INTO v_n1 FROM devices WHERE tenant_id=v_aqua AND node_id='N001';
  SELECT device_id INTO v_n2 FROM devices WHERE tenant_id=v_aqua AND node_id='N002';

  -- module (only on N001; N002 stays board-less on purpose)
  INSERT INTO modules(device_id, tenant_id, i2c_address, name) VALUES
    (v_n1, v_aqua, '0x48', 'Expansion Board 1')
  ON CONFLICT (device_id, i2c_address) DO NOTHING;
  SELECT module_id INTO v_board FROM modules WHERE device_id=v_n1 AND i2c_address='0x48';

  -- ports (with calibration formula assigned)
  INSERT INTO ports(module_id, tenant_id, port_code, label, unit, range_min, range_max, safe_min, safe_max, last_value, last_status, formula_id) VALUES
    (v_board, v_aqua, 'A0','Dissolved Oxygen','mg/L',0,20,6,9, 8.19,'Normal', v_f_do),
    (v_board, v_aqua, 'A1','Salinity','PSU',0,70,35,50, 44.97,'Normal', v_f_sal),
    (v_board, v_aqua, 'A2','Temperature','C',0,50,25,32, 29.62,'Normal', v_f_tmp)
  ON CONFLICT (module_id, port_code) DO NOTHING;
  SELECT port_id INTO v_p_do FROM ports WHERE module_id=v_board AND port_code='A0';

  -- actuators (on N001 mainboard)
  INSERT INTO actuators(device_id, tenant_id, actuator_code, name, port, channel, gpio, mode, state, duty, dur, last_ack) VALUES
    (v_n1, v_aqua, 'fan01','Circulation Fan','OUT1',0,25,'pwm',   1,166,0,'success'),
    (v_n1, v_aqua, 'htr01','Water Heater',   'OUT2',1,26,'bin',   0,  0,0,'success'),
    (v_n1, v_aqua, 'aer01','Aerator Pump',   'OUT3',2,27,'pwm',   0,102,0,'pending')
  ON CONFLICT (device_id, port) DO NOTHING;

  -- a couple of notifications
  INSERT INTO notifications(tenant_id, port_id, type, title, message, is_read) VALUES
    (v_aqua, v_p_do, 'warning','Dissolved Oxygen Low','Port A0 is approaching the lower safe threshold.', false),
    (v_aqua, NULL,   'info','Device Connected','ESP32 Module 1 connected via Wi-Fi at -61 dBm.', false)
  ON CONFLICT DO NOTHING;

  -- one seed telemetry row so readings isn't empty
  INSERT INTO readings(port_id, tenant_id, raw_adc, value, status)
  SELECT v_p_do, v_aqua, 4085, 8.19, 'Normal'
  WHERE NOT EXISTS (SELECT 1 FROM readings WHERE port_id = v_p_do);
END $$;
