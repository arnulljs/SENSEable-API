// 1730000000014_network_type.js
//
// The firmware now tags every tlm and disco packet with "net": "wifi" | "cell",
// the interface that actually carried it.
//
//   readings.network_type  which network delivered THIS sample. Per reading, so a
//                          node that changes deployment keeps an honest history,
//                          and a backlog replayed from flash shows it came in
//                          over cellular.
//   devices.comm_mode      the node's CURRENT interface (the existing column the
//                          dashboard already renders on every device card). It
//                          was NOT NULL DEFAULT 'Wi-Fi', so every node claimed
//                          Wi-Fi whether or not it said so. Now nullable with no
//                          default — blank until the node reports — and cleared
//                          once here, because every stored value was that
//                          default, never an observation. The next packet from
//                          each node fills it in.

export const up = (pgm) => {
  pgm.sql(`
ALTER TABLE readings ADD COLUMN IF NOT EXISTS network_type text;
ALTER TABLE readings DROP CONSTRAINT IF EXISTS readings_network_type_check;
ALTER TABLE readings ADD CONSTRAINT readings_network_type_check
  CHECK (network_type IS NULL OR network_type IN ('wifi', 'cell'));

ALTER TABLE devices ALTER COLUMN comm_mode DROP DEFAULT;
ALTER TABLE devices ALTER COLUMN comm_mode DROP NOT NULL;
UPDATE devices SET comm_mode = NULL;
`);
};

export const down = (pgm) => {
  pgm.sql(`
ALTER TABLE readings DROP CONSTRAINT IF EXISTS readings_network_type_check;
ALTER TABLE readings DROP COLUMN IF EXISTS network_type;
UPDATE devices SET comm_mode = 'Wi-Fi' WHERE comm_mode IS NULL;
ALTER TABLE devices ALTER COLUMN comm_mode SET DEFAULT 'Wi-Fi';
ALTER TABLE devices ALTER COLUMN comm_mode SET NOT NULL;
`);
};
