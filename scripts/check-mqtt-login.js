// check-mqtt-login.js ─────────────────────────────────────────────────────────
// Proves what one HiveMQ login can and cannot do, before a node or the backend
// depends on it. Publishes only to an unused `aclcheck` subtopic, so nothing
// it sends is ever ingested or executed.
//
//   Node login (should reach only its own topics):
//     node scripts/check-mqtt-login.js node NODE-C045FC aquatech-856f
//   Backend or listener login (should reach everything):
//     node scripts/check-mqtt-login.js backend
//
// The username and password are read from the environment so they stay out of
// shell history:  read -r CHECK_USER; read -rs CHECK_PASS; export CHECK_USER CHECK_PASS
// The broker URL comes from .env.cloud (MQTT_URL) unless CHECK_URL is set.
//
// The client ID is "<username>-aclcheck", never the node's own: HiveMQ drops an
// existing session when a second client connects with the same ID, and that
// would knock the real node off the broker.

import dotenv from 'dotenv';
import mqtt from 'mqtt';

dotenv.config({ path: '.env.cloud', override: false });
const [mode, nid, tid = 'aclcheck-tid'] = process.argv.slice(2);
const url = process.env.CHECK_URL ?? process.env.MQTT_URL;
const username = process.env.CHECK_USER;
const password = process.env.CHECK_PASS;
if (!['node', 'backend'].includes(mode) || (mode === 'node' && !nid) || !url || !username || !password) {
  console.error('usage: CHECK_USER=.. CHECK_PASS=.. node scripts/check-mqtt-login.js node <NODE-ID> [tid] | backend');
  process.exit(2);
}

const other = 'NODE-ACLCHECK-OTHER';
const T = (n, kind) => `usc/thesis/${tid}/${n}/${kind}`;
const tests = mode === 'node' ? [
  ['publish',   T(nid, 'aclcheck'),          true,  'publish on its own topics'],
  ['subscribe', `usc/thesis/+/${nid}/cmd`,   true,  'receive its own commands'],
  ['publish',   T(other, 'aclcheck'),        false, "publish as another node"],
  ['subscribe', `usc/thesis/+/${other}/cmd`, false, "read another node's commands"],
  ['subscribe', 'usc/thesis/#',              false, 'read every topic'],
] : [
  ['subscribe', 'usc/thesis/#',              true,  'read every topic'],
  ['publish',   T(other, 'aclcheck'),        true,  'publish to a node topic (commands)'],
];

const client = mqtt.connect(url, {
  username, password, clientId: `${username}-aclcheck`, clean: true,
  protocolVersion: 5, reconnectPeriod: 0, connectTimeout: 15_000,
});

// A denied PUBLISH comes back as a PUBACK error on MQTT 5, or the broker
// closes the connection; either way it was not allowed.
function tryPublish(topic) {
  return new Promise((resolve) => {
    const onClose = () => resolve(false);
    client.once('close', onClose);
    client.publish(topic, JSON.stringify({ aclcheck: Date.now() }), { qos: 1 }, (err) => {
      client.off('close', onClose);
      resolve(!err);
    });
  });
}
function trySubscribe(topic) {
  return new Promise((resolve) => {
    client.subscribe(topic, { qos: 1 }, (err, granted) => {
      resolve(!err && granted?.[0] && granted[0].qos < 128);
    });
  });
}

client.on('error', (e) => { console.error(`connect failed: ${e.message}`); process.exit(1); });
client.on('connect', async () => {
  console.log(`connected to ${url} as ${username}\n`);
  let ok = 0;
  for (const [op, topic, want, what] of tests) {
    if (!client.connected) {
      await new Promise((r) => { client.reconnect(); client.once('connect', r); });
    }
    const got = op === 'publish' ? await tryPublish(topic) : await trySubscribe(topic);
    const pass = got === want;
    if (pass) ok++;
    console.log(`  ${pass ? 'PASS' : 'FAIL'}  ${want ? 'can' : 'cannot'} ${what}` +
                `  (${op} ${topic} → ${got ? 'allowed' : 'denied'})`);
  }
  console.log(`\n  ${ok}/${tests.length} as intended`);
  if (ok !== tests.length && mode === 'node') {
    console.log('  If the node is denied its OWN topics, this plan likely does not accept "+" in a\n' +
                '  permission filter — see the HiveMQ notes before moving any node.');
  }
  client.end(true, () => process.exit(ok === tests.length ? 0 : 1));
});
