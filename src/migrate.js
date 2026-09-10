// migrate.js ─────────────────────────────────────────────────────────────────
// Transfer an operator's configuration from one physical node to another.
//
// WHY THIS EXISTS
// The firmware now derives its node id from the ESP32's MAC address
// (`NODE-A1B2C3` from the last three bytes) instead of a hardcoded "N001".
// That is the right call — two nodes can finally coexist without both claiming
// N001, and identity survives a reflash. But it changes an assumption the rest
// of the system quietly relied on: that a given piece of hardware keeps the
// same id forever.
//
// It does not. A new id appears whenever:
//   * an existing deployment upgrades to MAC-based firmware (N001 -> NODE-…)
//   * a failed ESP32 is swapped for a spare (new MAC, new id)
//   * a node is moved between sites
//
// Auto-provisioning handles the NEW device fine — it appears within seconds
// with a full set of default ports. The problem is everything the operator
// built on the OLD one. Port labels, units, safe ranges, calibration formula
// assignments, per-channel enable state and actuator names all hang off the old
// device row, which is now permanently offline and will never report again.
// Without this module the only recovery is to redo every calibration by hand,
// which for a 4-board 16-channel node is an afternoon's work and is exactly the
// kind of thing that does not get done carefully the second time.
//
// WHAT MOVES, AND WHY MATCHING IS BY NATURAL KEY
// UUIDs differ between the two device rows, so nothing can be matched by
// primary key. Ports are matched on (i2c_address, port_code) and actuators on
// port — the physical position of the sensor or output on the board. That is
// the thing the operator's configuration actually describes: "the channel at
// 0x48/A0 is a dissolved oxygen probe reading 0-20 mg/L". Which silicon carries
// that channel is irrelevant to the calibration, which is why swapping the
// board is safe and why matching on position is correct rather than merely
// convenient.
//
// WHAT DOES NOT MOVE
// Live state — last_value, last_status, last_seen, readings history, ack
// status. Those describe the old hardware's past and would be actively
// misleading attached to a node that has never reported them. History stays
// with the device that recorded it.

import { store } from './store.js';
import { withTenant } from '../db/pool.js';

// Columns that describe operator INTENT rather than observed state. This split
// is the whole design: intent transfers, observation does not.
const PORT_CONFIG_COLUMNS = [
  'label', 'unit',
  'range_min', 'range_max',
  'safe_min', 'safe_max',
  'formula_id',
  'cal_type', 'cal_slope', 'cal_offset',
  'enabled', 'disabled_reason',
];

export class MigrationError extends Error {
  constructor(message, status = 400) {
    super(message);
    this.status = status;
  }
}

/**
 * Preview what a migration would move, without changing anything.
 *
 * Offered because a migration is destructive in one direction — it overwrites
 * the target's configuration — and an operator should be able to see that four
 * boards and sixteen channels are about to be rewritten before agreeing to it.
 */
export async function planMigration(fromDev, toDev) {
  assertMigratable(fromDev, toDev);

  const matched = [];
  const unmatched = [];

  for (const mod of fromDev.modules ?? []) {
    const target = (toDev.modules ?? []).find((m) => m.address === mod.address);
    for (const port of mod.ports ?? []) {
      // Only carry channels the operator actually configured. A port still
      // sitting at its provisioning defaults has nothing worth moving, and
      // copying it would overwrite a target the new node may have discovered
      // more accurately.
      if (!isConfigured(port)) continue;

      const tPort = target?.ports?.find((p) => p.id === port.id);
      const entry = {
        address: mod.address,
        port: port.id,
        label: port.label,
        unit: port.unit,
      };
      if (tPort) matched.push(entry);
      else unmatched.push({ ...entry, reason: target ? 'channel not present on target' : 'board not present on target' });
    }
  }

  const actuators = (fromDev.actuators ?? [])
    .filter((a) => a.name && !/^OUT\d+$/i.test(a.name))   // renamed by a human
    .map((a) => ({ port: a.port, name: a.name }))
    .filter((a) => (toDev.actuators ?? []).some((t) => t.port === a.port));

  return {
    from: { id: fromDev.id, nodeId: fromDev.nodeId, status: fromDev.status },
    to: { id: toDev.id, nodeId: toDev.nodeId, status: toDev.status },
    ports: { matched, unmatched },
    actuators,
    summary: `${matched.length} channel(s) and ${actuators.length} actuator name(s) would transfer` +
             (unmatched.length ? `; ${unmatched.length} could not be matched` : ''),
  };
}

/**
 * Copy configuration from `fromDev` to `toDev`.
 *
 * Runs as ONE transaction. A half-applied migration is worse than none: the
 * operator would be left unable to tell which channels carry the old
 * calibration and which are still at defaults, and the readings would be
 * silently wrong for the difference.
 */
export async function migrateConfiguration(fromDev, toDev, { removeSource = false } = {}) {
  assertMigratable(fromDev, toDev);

  const plan = await planMigration(fromDev, toDev);
  if (plan.ports.matched.length === 0 && plan.actuators.length === 0) {
    throw new MigrationError(
      'nothing to transfer — no configured channels on the source match the target', 409);
  }

  const setClause = PORT_CONFIG_COLUMNS.map((c, i) => `${c} = $${i + 1}`).join(', ');

  await withTenant(toDev._tenantUuid, async (c) => {
    await c.query('BEGIN');
    try {
      for (const mod of fromDev.modules ?? []) {
        const target = (toDev.modules ?? []).find((m) => m.address === mod.address);
        if (!target) continue;

        for (const port of mod.ports ?? []) {
          if (!isConfigured(port)) continue;
          const tPort = target.ports?.find((p) => p.id === port.id);
          if (!tPort) continue;

          // Read the source row rather than trusting the in-memory projection:
          // the projection carries derived and formatted values, and this needs
          // the stored ones.
          const { rows } = await c.query(
            `SELECT ${PORT_CONFIG_COLUMNS.join(', ')} FROM ports WHERE port_id = $1`,
            [port._uuid]);
          if (!rows[0]) continue;

          const values = PORT_CONFIG_COLUMNS.map((col) => rows[0][col]);
          await c.query(
            `UPDATE ports SET ${setClause}, configured = true, updated_at = now()
             WHERE port_id = $${PORT_CONFIG_COLUMNS.length + 1}`,
            [...values, tPort._uuid]);
        }
      }

      // Actuator names only. Mode, duty and duration describe what the OLD
      // hardware was last told to do; replaying that onto new hardware would
      // actuate it on the operator's behalf without them asking.
      for (const a of fromDev.actuators ?? []) {
        if (!a.name || /^OUT\d+$/i.test(a.name)) continue;
        const t = (toDev.actuators ?? []).find((x) => x.port === a.port);
        if (!t) continue;
        await c.query(
          `UPDATE actuators SET name = $1, updated_at = now()
           WHERE device_id = $2 AND port = $3`,
          [a.name, toDev._uuid, a.port]);
      }

      // Carry the operator's device name across, so "North Pond Node" follows
      // the pond rather than staying with the dead board. Only when it was
      // actually renamed — the auto-provisioned "ESP32 N001" is not a name
      // anyone chose.
      if (fromDev.name && !/^ESP32 /.test(fromDev.name)) {
        await c.query('UPDATE devices SET name = $1 WHERE device_id = $2',
          [fromDev.name, toDev._uuid]);
      }

      await c.query('COMMIT');
    } catch (e) {
      await c.query('ROLLBACK');
      throw e;
    }
  });

  let removed = null;
  if (removeSource) {
    // Reuses the existing offline-only guard rather than deleting directly, so
    // a source that came back to life between plan and apply is refused here
    // for the same reason it would be refused anywhere else.
    const { removeDevice } = await import('./store.js');
    removed = await removeDevice(fromDev);
  }

  return { ...plan, applied: true, sourceRemoved: !!removed };
}

// A port is "configured" if the operator touched it — a label they chose, a
// calibration they assigned, or a range they narrowed from the ADC defaults.
function isConfigured(port) {
  if (port.configured) return true;
  if (port.label && port.label !== 'Unassigned channel') return true;
  if (port.unit && port.unit !== 'raw') return true;
  if (port.formulaLabel) return true;
  if (port.safeMin !== 0 || port.safeMax !== 32767) return true;
  return false;
}

function assertMigratable(fromDev, toDev) {
  if (!fromDev) throw new MigrationError('source device not found', 404);
  if (!toDev) throw new MigrationError('target device not found', 404);
  if (fromDev.id === toDev.id) {
    throw new MigrationError('source and target are the same device', 400);
  }
  // Cross-tenant transfer would move one organisation's calibration into
  // another's hardware. RLS would block the write, but failing here gives a
  // clear reason instead of an opaque permission error.
  if (fromDev.tenantId !== toDev.tenantId) {
    throw new MigrationError('cannot transfer configuration between tenants', 403);
  }
  // The source must be offline. Migrating away from a node that is still
  // reporting means two devices now claim the same configuration, and the
  // operator has no way to tell which one the dashboard is showing.
  if (fromDev.status !== 'offline' && fromDev.status !== 'Offline') {
    throw new MigrationError(
      `source device '${fromDev.name}' is still reporting — transfer configuration ` +
      'only from hardware that has been retired or replaced', 409);
  }
}

/**
 * Suggest replacement candidates for an offline device: same tenant, currently
 * online, and never configured by anyone. Surfacing this is what turns "my
 * calibration is gone" into a one-click recovery, because the operator who just
 * swapped a board does not necessarily connect the new NODE-… id in the list to
 * the N001 that vanished.
 */
export function suggestReplacements(fromDev) {
  return store.devices
    .filter((d) =>
      d.tenantId === fromDev.tenantId &&
      d.id !== fromDev.id &&
      d.status !== 'offline' && d.status !== 'Offline')
    .map((d) => ({
      id: d.id,
      nodeId: d.nodeId,
      name: d.name,
      status: d.status,
      // A node whose channels are all still at defaults is almost certainly the
      // replacement, rather than an unrelated node that has been running for
      // months.
      looksUnconfigured: (d.modules ?? []).every((m) =>
        (m.ports ?? []).every((p) => !isConfigured(p))),
      boardsMatch: matchingBoardCount(fromDev, d),
    }))
    .sort((a, b) => (b.looksUnconfigured - a.looksUnconfigured) || (b.boardsMatch - a.boardsMatch));
}

function matchingBoardCount(a, b) {
  const addrs = new Set((b.modules ?? []).map((m) => m.address));
  return (a.modules ?? []).filter((m) => addrs.has(m.address)).length;
}

/**
 * Parse the MAC suffix back out of a MAC-derived node id, for display.
 * Returns null for legacy ids like "N001", which is the caller's cue to show
 * the raw id instead.
 */
export function macSuffixOf(nodeId) {
  const m = /^NODE-([0-9A-Fa-f]{6})$/.exec(nodeId ?? '');
  if (!m) return null;
  const h = m[1].toUpperCase();
  return `${h.slice(0, 2)}:${h.slice(2, 4)}:${h.slice(4, 6)}`;
}
