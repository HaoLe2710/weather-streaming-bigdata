ALTER TABLE monitoring_streams ADD COLUMN last_live_received_at TEXT;
ALTER TABLE monitoring_streams ADD COLUMN last_live_snapshot_id TEXT;

-- Backfill the receipt identity only where the current snapshot is also the live snapshot.
UPDATE monitoring_streams
SET last_live_received_at = latest_received_at,
    last_live_snapshot_id = latest_snapshot_id
WHERE last_live_heartbeat_at IS NOT NULL
  AND last_live_heartbeat_at = latest_collected_at
  AND latest_snapshot_id IS NOT NULL
  AND latest_received_at IS NOT NULL;

ALTER TABLE monitoring_snapshots
  ADD COLUMN quarantine_alert_state TEXT NOT NULL DEFAULT 'NOT_APPLICABLE'
  CHECK (quarantine_alert_state IN ('NOT_APPLICABLE', 'PENDING', 'PROCESSING', 'PROCESSED', 'FAILED'));
ALTER TABLE monitoring_snapshots ADD COLUMN quarantine_alert_processed_at TEXT;
ALTER TABLE monitoring_snapshots
  ADD COLUMN quarantine_alert_id TEXT REFERENCES monitoring_alerts(alert_id);
ALTER TABLE monitoring_snapshots
  ADD COLUMN quarantine_alert_attempts INTEGER NOT NULL DEFAULT 0 CHECK (quarantine_alert_attempts >= 0);
ALTER TABLE monitoring_snapshots ADD COLUMN quarantine_alert_next_attempt_at TEXT;
ALTER TABLE monitoring_snapshots ADD COLUMN quarantine_alert_locked_until TEXT;
ALTER TABLE monitoring_snapshots ADD COLUMN quarantine_alert_error_code TEXT;

ALTER TABLE monitoring_snapshots
  ADD COLUMN stream_recovery_state TEXT NOT NULL DEFAULT 'PENDING'
  CHECK (stream_recovery_state IN ('PENDING', 'PROCESSING', 'RECOVERED', 'FAILED'));
ALTER TABLE monitoring_snapshots
  ADD COLUMN stream_recovery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (stream_recovery_attempts >= 0);
ALTER TABLE monitoring_snapshots ADD COLUMN stream_recovery_next_attempt_at TEXT;
ALTER TABLE monitoring_snapshots ADD COLUMN stream_recovery_locked_until TEXT;
ALTER TABLE monitoring_snapshots ADD COLUMN stream_recovery_error_code TEXT;

UPDATE monitoring_snapshots
SET stream_recovery_state = 'RECOVERED'
WHERE stream_applied = 1;

-- Existing contract quarantines are eligible for recovery unless their snapshot was already
-- represented by an OPENED event. Source allowlist failures intentionally create no alert.
UPDATE monitoring_snapshots
SET quarantine_alert_state = 'PENDING'
WHERE validation_status = 'QUARANTINED'
  AND quarantine_reason IN ('COHORT_CONTRACT_MISMATCH', 'MODEL_CONTRACT_MISMATCH');

UPDATE monitoring_snapshots
SET quarantine_alert_state = 'PROCESSED',
    quarantine_alert_processed_at = received_at
WHERE validation_status = 'QUARANTINED'
  AND quarantine_reason = 'SOURCE_NOT_ALLOWLISTED';

UPDATE monitoring_snapshots
SET quarantine_alert_state = 'PROCESSED',
    quarantine_alert_processed_at = received_at,
    quarantine_alert_id = (
      SELECT e.alert_id
      FROM monitoring_alert_events e
      WHERE e.event_type = 'OPENED'
        AND json_extract(e.details_json, '$.snapshotId') = monitoring_snapshots.snapshot_id
      ORDER BY e.occurred_at ASC, e.event_id ASC
      LIMIT 1
    )
WHERE validation_status = 'QUARANTINED'
  AND quarantine_reason IN ('COHORT_CONTRACT_MISMATCH', 'MODEL_CONTRACT_MISMATCH')
  AND EXISTS (
    SELECT 1 FROM monitoring_alert_events e
    WHERE e.event_type = 'OPENED'
      AND json_extract(e.details_json, '$.snapshotId') = monitoring_snapshots.snapshot_id
  );

CREATE INDEX idx_snapshots_quarantine_recovery
  ON monitoring_snapshots(quarantine_reason, quarantine_alert_state, quarantine_alert_next_attempt_at, quarantine_alert_locked_until, received_at, snapshot_id)
  WHERE validation_status = 'QUARANTINED';

CREATE INDEX idx_snapshots_stream_recovery
  ON monitoring_snapshots(stream_recovery_state, stream_recovery_next_attempt_at, stream_recovery_locked_until, received_at, snapshot_id)
  WHERE validation_status = 'ACCEPTED' AND stream_applied = 0;
