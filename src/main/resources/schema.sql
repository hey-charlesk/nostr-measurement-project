CREATE TABLE IF NOT EXISTS relays (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  host TEXT NOT NULL,
  first_seen INTEGER NOT NULL,
  last_seen INTEGER NOT NULL,
  flags TEXT
);

CREATE TABLE IF NOT EXISTS relay_sightings (
  relay_id INTEGER NOT NULL,
  source TEXT NOT NULL,
  day TEXT NOT NULL,
  PRIMARY KEY (relay_id, source, day)
);

CREATE TABLE IF NOT EXISTS relay_snapshots (
  id INTEGER PRIMARY KEY,
  relay_id INTEGER NOT NULL,
  fetched_at INTEGER NOT NULL,
  http_status INTEGER,
  ok INTEGER NOT NULL,
  name TEXT,
  pubkey TEXT,
  software TEXT,
  software_name TEXT,
  version TEXT,
  supported_nips TEXT,
  payment_required INTEGER,
  auth_required INTEGER,
  restricted_writes INTEGER,
  max_subscriptions INTEGER,
  max_filters INTEGER,
  max_limit INTEGER,
  max_message_length INTEGER,
  fees_json TEXT,
  server_header TEXT,
  cf_ray INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  raw_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_snap_relay ON relay_snapshots (relay_id, fetched_at);

CREATE VIEW IF NOT EXISTS latest_snapshot AS
  SELECT s.* FROM relay_snapshots s
  JOIN (SELECT relay_id, MAX(fetched_at) AS m FROM relay_snapshots GROUP BY relay_id) l
    ON s.relay_id = l.relay_id AND s.fetched_at = l.m;

CREATE TABLE IF NOT EXISTS probes (
  relay_id INTEGER NOT NULL,
  ts INTEGER NOT NULL,
  ws_ok INTEGER NOT NULL,
  eose_ok INTEGER NOT NULL,
  connect_ms INTEGER,
  eose_ms INTEGER,
  error_class TEXT NOT NULL,
  error_msg TEXT,
  events_returned INTEGER,
  PRIMARY KEY (relay_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_probes_ts ON probes (ts);

CREATE TABLE IF NOT EXISTS probe_ticks (
  ts INTEGER PRIMARY KEY,
  relays_total INTEGER NOT NULL,
  probed INTEGER NOT NULL,
  ws_ok INTEGER NOT NULL,
  duration_ms INTEGER NOT NULL
);

CREATE VIEW IF NOT EXISTS latest_probe AS
  SELECT p.* FROM probes p WHERE p.ts = (SELECT MAX(ts) FROM probe_ticks);

CREATE TABLE IF NOT EXISTS relay_ip (
  relay_id INTEGER NOT NULL,
  ip TEXT NOT NULL,
  day TEXT NOT NULL,
  PRIMARY KEY (relay_id, ip, day)
);

CREATE TABLE IF NOT EXISTS ip_info (
  ip TEXT PRIMARY KEY,
  asn INTEGER,
  as_name TEXT,
  org TEXT,
  isp TEXT,
  country TEXT,
  hosting INTEGER,
  fetched_at INTEGER NOT NULL,
  error TEXT
);

CREATE VIEW IF NOT EXISTS latest_relay_ip AS
  SELECT r.relay_id, r.ip FROM relay_ip r
  JOIN (SELECT relay_id, MAX(day) AS d FROM relay_ip GROUP BY relay_id) l
    ON r.relay_id = l.relay_id AND r.day = l.d;

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  pubkey TEXT NOT NULL,
  kind INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  first_relay_id INTEGER,
  first_seen INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_kind ON events (kind, created_at);

CREATE TABLE IF NOT EXISTS samples (
  set_name TEXT NOT NULL,
  event_id TEXT NOT NULL,
  PRIMARY KEY (set_name, event_id)
);

CREATE TABLE IF NOT EXISTS event_relay (
  event_id TEXT NOT NULL,
  relay_id INTEGER NOT NULL,
  via TEXT NOT NULL,
  seen_at INTEGER NOT NULL,
  PRIMARY KEY (event_id, relay_id)
);
CREATE INDEX IF NOT EXISTS idx_event_relay_relay ON event_relay (relay_id);

CREATE TABLE IF NOT EXISTS replicate_relays (
  set_name TEXT NOT NULL,
  relay_id INTEGER NOT NULL,
  status TEXT NOT NULL,
  batches_done INTEGER NOT NULL,
  batches_total INTEGER NOT NULL,
  hits INTEGER NOT NULL,
  note TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (set_name, relay_id)
);

CREATE TABLE IF NOT EXISTS relay_lists (
  pubkey TEXT NOT NULL,
  relay_url TEXT NOT NULL,
  read INTEGER NOT NULL,
  write INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (pubkey, relay_url)
);

CREATE TABLE IF NOT EXISTS relay_list_authors (
  pubkey TEXT PRIMARY KEY,
  event_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  n_write INTEGER NOT NULL,
  n_read INTEGER NOT NULL,
  fetched_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS relay_list_checked (
  pubkey TEXT PRIMARY KEY,
  checked_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rate_obs (
  relay_id INTEGER NOT NULL,
  started_at INTEGER NOT NULL,
  seconds INTEGER NOT NULL,
  kinds TEXT NOT NULL,
  events INTEGER NOT NULL,
  eose_ms INTEGER,
  ia_p50_ms INTEGER,
  ia_p99_ms INTEGER,
  PRIMARY KEY (relay_id, started_at)
);
