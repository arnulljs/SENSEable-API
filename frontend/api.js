// api.js ────────────────────────────────────────────────────────────────────
// Drop this into the React app (e.g. src/api.js) to replace the static
// mockData imports with live backend calls. Same data shapes, so components
// need only swap `import { devices } from './mockData'` for a fetch in
// useEffect. Set VITE_API_URL in the app's .env if the backend isn't on :4000.

const BASE = import.meta.env?.VITE_API_URL ?? 'http://localhost:4000';

async function get(path) {
  const res = await fetch(`${BASE}/api${path}`);
  if (!res.ok) throw new Error(`GET ${path} -> ${res.status}`);
  return res.json();
}
async function send(method, path, body) {
  const res = await fetch(`${BASE}/api${path}`, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: body == null ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}`);
  return res.json();
}

// Reads — return exactly the shapes the current components already expect.
export const fetchDevices = () => get('/devices');
export const fetchNotifications = () => get('/notifications');
export const fetchFormulas = () => get('/formulas');
export const fetchChannelAssignments = () => get('/channel-assignments');
export const fetchMapSensors = () => get('/map-sensors');

// Writes.
export const markAllNotificationsRead = () => send('POST', '/notifications/read-all');
export const markNotificationRead = (id) => send('POST', `/notifications/${id}/read`);
export const createFormula = (label, formula) => send('POST', '/formulas', { label, formula });
export const deleteFormula = (id) => send('DELETE', `/formulas/${id}`);
export const fitFormula = (points) => send('POST', '/formulas/fit', { points });
export const assignChannel = (board, channel, formulaLabel) =>
  send('PUT', `/channel-assignments/${board}/${channel}`, { formulaLabel });
export const saveMapSensors = (sensors) => send('PUT', '/map-sensors', sensors);

// ── Renames (persist to the DB; response echoes the updated device) ─────────
export const renameDevice = (deviceId, name) =>
  send('PATCH', `/devices/${encodeURIComponent(deviceId)}`, { name });
export const renameModule = (deviceId, moduleId, name) =>
  send('PATCH', `/devices/${encodeURIComponent(deviceId)}/modules/${encodeURIComponent(moduleId)}`, { name });
export const renameActuator = (deviceId, actuatorId, name) =>
  send('PATCH', `/devices/${encodeURIComponent(deviceId)}/actuators/${encodeURIComponent(actuatorId)}`, { name });

// ── Downward commands (actuate / bus_recovery / sensor_port_up|down) ─────────
// The backend resolves the broker `tid` from tenants.mqtt_tid, builds the exact
// wire envelope, logs it (cid), and publishes to usc/thesis/{tid}/{nid}/cmd if a
// broker is connected. It returns { ok, cid, topic, published, envelope }.
export const sendCommand = (deviceId, action, params = {}) =>
  send('POST', '/commands', { deviceId, action, ...params });

// Actuate an output. Target by actuatorId (preferred) or a raw port (1..6 /
// "OUT3"). mode 'bin' -> pass state 0|1; mode 'pwm' -> pass duty 0..255.
export const actuate = (deviceId, { actuatorId, port, mode, state, duty, dur = 0 }) =>
  sendCommand(deviceId, 'actuate', { actuatorId, port, mode, state, duty, dur });

export const busRecovery = (deviceId, busId = 0) =>
  sendCommand(deviceId, 'bus_recovery', { busId });

// direction: 'up' (enable) | 'down' (disable). chip 0..3 (0x48..0x4B), ch 0..3.
export const sensorPortToggle = (deviceId, direction, chip, ch) =>
  sendCommand(deviceId, `sensor_port_${direction}`, { chip, ch });

// Recent command log (with latest ack status), optionally scoped to one device.
export const fetchCommands = (deviceId) =>
  get(`/commands${deviceId ? `?device=${encodeURIComponent(deviceId)}` : ''}`);

// Example wiring for DeviceOverview.jsx:
//
//   import { useEffect, useState } from 'react';
//   import { fetchDevices } from '../api';
//
//   const [devices, setDevices] = useState([]);
//   useEffect(() => {
//     fetchDevices().then(setDevices).catch(console.error);
//     const id = setInterval(() => fetchDevices().then(setDevices), 3000); // live poll
//     return () => clearInterval(id);
//   }, []);
//
// Everything below that (`.map()` over devices -> modules -> ports) stays as-is.
