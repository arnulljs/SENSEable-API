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
