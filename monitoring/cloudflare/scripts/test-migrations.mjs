import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const initial = readFileSync(path.join(root, "migrations", "0001_initial.sql"), "utf8");
const hardening = readFileSync(path.join(root, "migrations", "0002_phase2a1_hardening.sql"), "utf8");
const streamId = "a".repeat(64);
const collectedAt = "2026-10-13T11:59:00.000Z";
const receivedAt = "2026-10-13T11:59:02.000Z";

function freshDatabase() {
  const db = new DatabaseSync(":memory:");
  db.exec(initial);
  return db;
}

function assertHardeningSchema(db) {
  const snapshotColumns = new Set(db.prepare("PRAGMA table_info(monitoring_snapshots)").all().map((row) => row.name));
  const streamColumns = new Set(db.prepare("PRAGMA table_info(monitoring_streams)").all().map((row) => row.name));
  for (const name of ["quarantine_alert_state", "quarantine_alert_attempts", "stream_recovery_state", "stream_recovery_attempts"]) {
    assert(snapshotColumns.has(name), `missing monitoring_snapshots.${name}`);
  }
  for (const name of ["last_live_received_at", "last_live_snapshot_id"]) {
    assert(streamColumns.has(name), `missing monitoring_streams.${name}`);
  }
  const indexes = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type = 'index'").all().map((row) => row.name));
  assert(indexes.has("idx_snapshots_quarantine_recovery"));
  assert(indexes.has("idx_snapshots_stream_recovery"));
  assert(indexes.has("idx_alerts_one_open_fingerprint"));
}

// Fresh install path.
const empty = freshDatabase();
empty.exec(hardening);
assertHardeningSchema(empty);
assert.equal(empty.prepare("SELECT COUNT(*) AS count FROM monitoring_snapshots").get().count, 0);
empty.close();

// Upgrade path with accepted state, an already-alerted quarantine, a source quarantine,
// and a queued notification present before the forward-only migration.
const upgrade = freshDatabase();
upgrade.prepare(`
  INSERT INTO monitoring_alerts (
    alert_id, stream_id, alert_type, severity, status, fingerprint,
    first_triggered_at, last_observed_at, details_json, notification_status
  ) VALUES (?, ?, ?, 'CRITICAL', 'OPEN', ?, ?, ?, ?, 'PENDING')
`).run("legacy-alert", streamId, "COHORT_CONTRACT_MISMATCH", "c".repeat(64), receivedAt, receivedAt,
  JSON.stringify({ snapshotId: "legacy-quarantine" }));

function insertSnapshot(snapshotId, validationStatus, quarantineReason = null) {
  upgrade.prepare(`
    INSERT INTO monitoring_snapshots (
      environment, snapshot_id, stream_id, cohort_id, run_id, source_identity,
      agent_version, schema_version, collected_at, received_at, collector_status,
      pipeline_status, quality_status, validation_status, quarantine_reason,
      stream_applied, payload_sha256, payload_json
    ) VALUES ('local', ?, ?, 'cohort', 'run', 'source', 'v1', 1, ?, ?, 'COMPLETE',
      'RUNNING', 'PASS', ?, ?, 1, ?, '{}')
  `).run(snapshotId, streamId, collectedAt, receivedAt, validationStatus, quarantineReason, "b".repeat(64));
}

insertSnapshot("legacy-accepted", "ACCEPTED");
insertSnapshot("legacy-quarantine", "QUARANTINED", "COHORT_CONTRACT_MISMATCH");
insertSnapshot("legacy-source-quarantine", "QUARANTINED", "SOURCE_NOT_ALLOWLISTED");
upgrade.prepare(`
  INSERT INTO monitoring_streams (
    stream_id, environment, cohort_id, run_id, source_identity,
    latest_snapshot_id, latest_collected_at, latest_received_at,
    latest_collector_status, latest_pipeline_status, latest_quality_status,
    last_live_heartbeat_at, incomplete_collector_streak, updated_at
  ) VALUES (?, 'local', 'cohort', 'run', 'source', 'legacy-accepted', ?, ?,
    'COMPLETE', 'RUNNING', 'PASS', ?, 0, ?)
`).run(streamId, collectedAt, receivedAt, collectedAt, receivedAt);
upgrade.prepare(`
  INSERT INTO monitoring_alert_events (event_id, alert_id, event_type, dedupe_key, occurred_at, details_json)
  VALUES ('legacy-opened', 'legacy-alert', 'OPENED', 'OPENED', ?, ?)
`).run(receivedAt, JSON.stringify({ snapshotId: "legacy-quarantine" }));
upgrade.prepare(`
  INSERT INTO monitoring_notification_outbox (notification_id, alert_id, transition, state, attempts, next_attempt_at)
  VALUES ('legacy-notification', 'legacy-alert', 'OPENED', 'PENDING', 0, ?)
`).run(receivedAt);

upgrade.exec(hardening);
assertHardeningSchema(upgrade);
assert.equal(upgrade.prepare("SELECT COUNT(*) AS count FROM monitoring_snapshots").get().count, 3);
assert.equal(upgrade.prepare("SELECT COUNT(*) AS count FROM monitoring_alerts").get().count, 1);
assert.equal(upgrade.prepare("SELECT COUNT(*) AS count FROM monitoring_alert_events").get().count, 1);
assert.equal(upgrade.prepare("SELECT COUNT(*) AS count FROM monitoring_notification_outbox").get().count, 1);
assert.deepEqual(
  { ...upgrade.prepare("SELECT last_live_received_at, last_live_snapshot_id FROM monitoring_streams WHERE stream_id = ?").get(streamId) },
  { last_live_received_at: receivedAt, last_live_snapshot_id: "legacy-accepted" },
);
assert.deepEqual(
  { ...upgrade.prepare(`SELECT quarantine_alert_state, quarantine_alert_id FROM monitoring_snapshots
    WHERE snapshot_id = 'legacy-quarantine'`).get() },
  { quarantine_alert_state: "PROCESSED", quarantine_alert_id: "legacy-alert" },
);
assert.deepEqual(
  { ...upgrade.prepare(`SELECT quarantine_alert_state, quarantine_alert_processed_at FROM monitoring_snapshots
    WHERE snapshot_id = 'legacy-source-quarantine'`).get() },
  { quarantine_alert_state: "PROCESSED", quarantine_alert_processed_at: receivedAt },
);
assert.equal(upgrade.prepare("SELECT stream_recovery_state FROM monitoring_snapshots WHERE snapshot_id = 'legacy-accepted'").get().stream_recovery_state, "RECOVERED");
assert.throws(() => upgrade.prepare(`
  INSERT INTO monitoring_alerts (
    alert_id, stream_id, alert_type, severity, status, fingerprint,
    first_triggered_at, last_observed_at, details_json
  ) VALUES ('duplicate-alert', ?, 'COHORT_CONTRACT_MISMATCH', 'CRITICAL', 'OPEN', ?, ?, ?, '{}')
`).run(streamId, "c".repeat(64), receivedAt, receivedAt));
upgrade.close();

process.stdout.write("Phase 2A.1 migration tests passed: fresh install, legacy backfill, evidence preservation, indexes, and uniqueness.\n");
