// 1730000000003_commands_ack.js
// Aligns the schema with the FROZEN command/ack wire vocabulary
// ("Environment-Adaptable IoT Monitoring Framework: Payload Schemas") and adds
// a command log so an inbound ack can correlate back to the cmd it answers.
//
//   * actuate mode is "bin" on the wire (not "binary")  -> actuator_mode += 'bin'
//   * ack status is a lifecycle: started|completed|stopped|failed|error|success
//                                                       -> actuator_ack  += those
//   * PWM duty is 8-bit (0..255), not an operator percent -> widen duty check
//   * commands(cid) table: every downlink is logged; the ack updates its status
//
// Runs WITHOUT a transaction because `ALTER TYPE ... ADD VALUE` cannot execute
// inside one, and the data migration below must see the newly-committed value.
// Each ADD VALUE is its own statement so it commits before it's used.

export const up = (pgm) => {
  pgm.noTransaction();

  // 1a. Wire mode literal.
  pgm.sql(`ALTER TYPE actuator_mode ADD VALUE IF NOT EXISTS 'bin';`);

  // 1b. Ack lifecycle states (each its own committed statement).
  for (const v of ['started', 'completed', 'stopped', 'failed', 'error', 'success']) {
    pgm.sql(`ALTER TYPE actuator_ack ADD VALUE IF NOT EXISTS '${v}';`);
  }

  // 2. Migrate existing actuator rows: 'binary' -> 'bin', operator-percent duty
  //    -> 8-bit. Guarded so a fresh DB (or a re-run) is a no-op.
  pgm.sql(`ALTER TABLE actuators DROP CONSTRAINT IF EXISTS actuators_duty_check;`);
  pgm.sql(`UPDATE actuators SET mode = 'bin' WHERE mode = 'binary';`);
  pgm.sql(`
    UPDATE actuators
      SET duty = LEAST(255, ROUND(duty * 255.0 / 100.0))::smallint
      WHERE duty > 0 AND duty <= 100;
  `);
  pgm.sql(`
    ALTER TABLE actuators
      ADD CONSTRAINT actuators_duty_check CHECK (duty BETWEEN 0 AND 255);
  `);

  // 3. Command log — one row per downlink command, updated in place by its ack.
  pgm.sql(`
    CREATE TABLE commands (
      command_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id  uuid NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
      device_id  uuid NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
      cid        text NOT NULL,                             -- correlation id (matches ack.cid)
      action     text NOT NULL
                 CHECK (action IN ('bus_recovery','actuate','sensor_port_up','sensor_port_down')),
      mode       actuator_mode,                             -- actuate only
      port       smallint CHECK (port IS NULL OR port BETWEEN 1 AND 6),
      payload    jsonb NOT NULL,                            -- the exact wire envelope
      status     actuator_ack NOT NULL DEFAULT 'pending',   -- latest ack status
      msg        text,
      created_at timestamptz NOT NULL DEFAULT now(),
      acked_at   timestamptz,
      UNIQUE (tenant_id, cid)
    );
    CREATE INDEX ix_commands_tenant ON commands(tenant_id, created_at DESC);
    CREATE INDEX ix_commands_device ON commands(device_id, created_at DESC);
    CREATE INDEX ix_commands_cid    ON commands(cid);

    ALTER TABLE commands ENABLE ROW LEVEL SECURITY;
    CREATE POLICY tenant_isolation ON commands
      USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
      WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);
  `);
};

export const down = (pgm) => {
  pgm.noTransaction();
  pgm.sql(`DROP TABLE IF EXISTS commands CASCADE;`);
  // Restore the pre-migration duty band. NOTE: PostgreSQL cannot DROP individual
  // enum values, so 'bin' and the added actuator_ack states persist after a
  // down-migration — they're harmless and simply become unused.
  pgm.sql(`ALTER TABLE actuators DROP CONSTRAINT IF EXISTS actuators_duty_check;`);
  pgm.sql(`
    ALTER TABLE actuators
      ADD CONSTRAINT actuators_duty_check CHECK (duty BETWEEN 0 AND 255);
  `);
};
