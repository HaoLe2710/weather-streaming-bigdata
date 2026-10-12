PRAGMA foreign_keys = ON;

CREATE TABLE monitoring_snapshots (
  environment TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  stream_id TEXT,
  cohort_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  source_identity TEXT NOT NULL,
  agent_version TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version = 1),
  collected_at TEXT NOT NULL,
  received_at TEXT NOT NULL,
  collector_status TEXT NOT NULL CHECK (collector_status IN ('COMPLETE', 'PARTIAL', 'FAILED')),
  pipeline_status TEXT NOT NULL,
  quality_status TEXT NOT NULL CHECK (quality_status IN ('PASS', 'FAIL', 'UNKNOWN')),
  validation_status TEXT NOT NULL CHECK (validation_status IN ('ACCEPTED', 'QUARANTINED')),
  quarantine_reason TEXT,
  stream_applied INTEGER NOT NULL DEFAULT 0 CHECK (stream_applied IN (0, 1)),
  payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
  payload_json TEXT NOT NULL CHECK (length(CAST(payload_json AS BLOB)) <= 32768),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  PRIMARY KEY (environment, snapshot_id)
);

CREATE INDEX idx_snapshots_stream_collected
  ON monitoring_snapshots(environment, cohort_id, source_identity, collected_at DESC, snapshot_id DESC);
CREATE INDEX idx_snapshots_stream_time
  ON monitoring_snapshots(stream_id, collected_at DESC, snapshot_id DESC);
CREATE INDEX idx_snapshots_received_at ON monitoring_snapshots(received_at);
CREATE INDEX idx_snapshots_validation_received ON monitoring_snapshots(validation_status, received_at);
CREATE INDEX idx_snapshots_unapplied ON monitoring_snapshots(stream_applied, validation_status, received_at);
CREATE INDEX idx_snapshots_quarantine_pending
  ON monitoring_snapshots(quarantine_reason, received_at)
  WHERE validation_status = 'QUARANTINED' AND quarantine_reason IS NOT NULL;

CREATE TABLE monitoring_streams (
  stream_id TEXT PRIMARY KEY,
  environment TEXT NOT NULL,
  cohort_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  source_identity TEXT NOT NULL,
  latest_snapshot_id TEXT,
  latest_collected_at TEXT,
  latest_received_at TEXT,
  latest_collector_status TEXT,
  latest_pipeline_status TEXT,
  latest_quality_status TEXT,
  last_live_heartbeat_at TEXT,
  incomplete_collector_streak INTEGER NOT NULL DEFAULT 0 CHECK (incomplete_collector_streak >= 0),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  UNIQUE(environment, cohort_id, source_identity),
  FOREIGN KEY (environment, latest_snapshot_id)
    REFERENCES monitoring_snapshots(environment, snapshot_id)
);

CREATE TABLE monitoring_targets (
  stream_id TEXT PRIMARY KEY,
  environment TEXT NOT NULL,
  cohort_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  source_identity TEXT NOT NULL,
  expected_interval_seconds INTEGER NOT NULL DEFAULT 300 CHECK (expected_interval_seconds BETWEEN 60 AND 3600),
  heartbeat_timeout_seconds INTEGER NOT NULL DEFAULT 900 CHECK (heartbeat_timeout_seconds BETWEEN 60 AND 86400),
  startup_grace_seconds INTEGER NOT NULL DEFAULT 900 CHECK (startup_grace_seconds BETWEEN 0 AND 86400),
  disk_threshold_percent REAL NOT NULL DEFAULT 85 CHECK (disk_threshold_percent BETWEEN 1 AND 100),
  max_partial_streak INTEGER NOT NULL DEFAULT 3 CHECK (max_partial_streak BETWEEN 1 AND 100),
  monitoring_enabled INTEGER NOT NULL DEFAULT 0 CHECK (monitoring_enabled IN (0, 1)),
  activated_at TEXT,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  UNIQUE(environment, cohort_id, source_identity)
);

CREATE INDEX idx_targets_enabled ON monitoring_targets(monitoring_enabled, activated_at, stream_id);

CREATE TABLE monitoring_alerts (
  alert_id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  alert_type TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('WARNING', 'CRITICAL')),
  status TEXT NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
  fingerprint TEXT NOT NULL,
  first_triggered_at TEXT NOT NULL,
  last_observed_at TEXT NOT NULL,
  resolved_at TEXT,
  details_json TEXT NOT NULL,
  notification_status TEXT NOT NULL DEFAULT 'DISABLED'
    CHECK (notification_status IN ('DISABLED', 'PENDING', 'SENT', 'FAILED'))
);

CREATE UNIQUE INDEX idx_alerts_one_open_fingerprint
  ON monitoring_alerts(fingerprint) WHERE status = 'OPEN';
CREATE INDEX idx_alerts_stream_status_time
  ON monitoring_alerts(stream_id, status, last_observed_at DESC, alert_id DESC);
CREATE INDEX idx_alerts_resolved_retention ON monitoring_alerts(status, resolved_at);

CREATE TABLE monitoring_alert_events (
  event_id TEXT PRIMARY KEY,
  alert_id TEXT NOT NULL REFERENCES monitoring_alerts(alert_id),
  event_type TEXT NOT NULL CHECK (event_type IN (
    'OPENED', 'OBSERVED_AGAIN', 'RESOLVED', 'NOTIFICATION_SENT', 'NOTIFICATION_FAILED'
  )),
  dedupe_key TEXT NOT NULL,
  occurred_at TEXT NOT NULL,
  details_json TEXT NOT NULL,
  UNIQUE(alert_id, dedupe_key)
);

CREATE INDEX idx_alert_events_alert_time
  ON monitoring_alert_events(alert_id, occurred_at DESC, event_id DESC);
CREATE INDEX idx_alert_events_time ON monitoring_alert_events(occurred_at);
CREATE INDEX idx_alert_events_snapshot_id
  ON monitoring_alert_events(json_extract(details_json, '$.snapshotId'))
  WHERE event_type = 'OPENED';

CREATE TABLE monitoring_notification_outbox (
  notification_id TEXT PRIMARY KEY,
  alert_id TEXT NOT NULL REFERENCES monitoring_alerts(alert_id),
  transition TEXT NOT NULL CHECK (transition IN ('OPENED', 'RESOLVED')),
  state TEXT NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'SENDING', 'SENT', 'FAILED')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at TEXT NOT NULL,
  locked_until TEXT,
  last_error_code TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  sent_at TEXT,
  UNIQUE(alert_id, transition)
);

CREATE INDEX idx_notification_due ON monitoring_notification_outbox(state, next_attempt_at, locked_until, created_at);
