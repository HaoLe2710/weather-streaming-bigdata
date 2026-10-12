import type { CollectorStatus, MonitoringSnapshotV1 } from "../types";

export interface MonitoringTargetRow {
  stream_id: string;
  environment: string;
  cohort_id: string;
  run_id: string;
  source_identity: string;
  expected_interval_seconds: number;
  heartbeat_timeout_seconds: number;
  startup_grace_seconds: number;
  disk_threshold_percent: number;
  max_partial_streak: number;
  monitoring_enabled: number;
  activated_at: string | null;
}

export interface StreamStateRow {
  stream_id: string;
  environment: string;
  cohort_id: string;
  run_id: string;
  source_identity: string;
  latest_snapshot_id: string | null;
  latest_collected_at: string | null;
  latest_received_at: string | null;
  latest_collector_status: CollectorStatus | null;
  latest_pipeline_status: string | null;
  latest_quality_status: string | null;
  last_live_heartbeat_at: string | null;
  incomplete_collector_streak: number;
  updated_at: string;
}

export async function getTarget(db: D1Database, streamId: string): Promise<MonitoringTargetRow | null> {
  return db.prepare("SELECT * FROM monitoring_targets WHERE stream_id = ? LIMIT 1")
    .bind(streamId).first<MonitoringTargetRow>();
}

export async function upsertStreamFromSnapshot(
  db: D1Database,
  input: {
    streamId: string;
    snapshot: MonitoringSnapshotV1;
    collectedAt: string;
    receivedAt: string;
    liveFresh: boolean;
  },
): Promise<{ current: boolean; state: StreamStateRow | null }> {
  const { snapshot, streamId, collectedAt, receivedAt, liveFresh } = input;
  const initialStreak = liveFresh && snapshot.collector.status !== "COMPLETE" ? 1 : 0;
  const row = await db.prepare(`
    INSERT INTO monitoring_streams (
      stream_id, environment, cohort_id, run_id, source_identity,
      latest_snapshot_id, latest_collected_at, latest_received_at,
      latest_collector_status, latest_pipeline_status, latest_quality_status,
      last_live_heartbeat_at, incomplete_collector_streak, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(stream_id) DO UPDATE SET
      latest_snapshot_id = excluded.latest_snapshot_id,
      latest_collected_at = excluded.latest_collected_at,
      latest_received_at = excluded.latest_received_at,
      latest_collector_status = excluded.latest_collector_status,
      latest_pipeline_status = excluded.latest_pipeline_status,
      latest_quality_status = excluded.latest_quality_status,
      last_live_heartbeat_at = CASE WHEN ? = 1
        THEN excluded.latest_collected_at ELSE monitoring_streams.last_live_heartbeat_at END,
      incomplete_collector_streak = CASE WHEN excluded.latest_collector_status = 'COMPLETE'
        THEN CASE WHEN ? = 1 THEN 0 ELSE monitoring_streams.incomplete_collector_streak END
        ELSE CASE WHEN ? = 1 THEN monitoring_streams.incomplete_collector_streak + 1
          ELSE monitoring_streams.incomplete_collector_streak END END,
      updated_at = excluded.updated_at
    WHERE monitoring_streams.latest_collected_at IS NULL
      OR excluded.latest_collected_at > monitoring_streams.latest_collected_at
      OR (excluded.latest_collected_at = monitoring_streams.latest_collected_at
        AND excluded.latest_snapshot_id > monitoring_streams.latest_snapshot_id)
    RETURNING *
  `).bind(
    streamId,
    snapshot.source.environment,
    snapshot.cohort.cohortId,
    snapshot.cohort.runId,
    snapshot.source.hostname,
    snapshot.snapshotId,
    collectedAt,
    receivedAt,
    snapshot.collector.status,
    snapshot.pipeline.status,
    snapshot.quality.status,
    liveFresh ? collectedAt : null,
    initialStreak,
    receivedAt,
    liveFresh ? 1 : 0,
    liveFresh ? 1 : 0,
    liveFresh ? 1 : 0,
  ).first<StreamStateRow>();

  if (row) return { current: row.latest_snapshot_id === snapshot.snapshotId, state: row };
  const current = await db.prepare("SELECT * FROM monitoring_streams WHERE stream_id = ? LIMIT 1")
    .bind(streamId).first<StreamStateRow>();
  return { current: false, state: current };
}

export async function markSnapshotApplied(db: D1Database, environment: string, snapshotId: string): Promise<void> {
  await db.prepare(`
    UPDATE monitoring_snapshots SET stream_applied = 1
    WHERE environment = ? AND snapshot_id = ? AND validation_status = 'ACCEPTED'
  `).bind(environment, snapshotId).run();
}

export async function listEnabledTargets(db: D1Database, limit: number): Promise<MonitoringTargetRow[]> {
  const result = await db.prepare(`
    SELECT * FROM monitoring_targets
    WHERE monitoring_enabled = 1
    ORDER BY activated_at ASC, stream_id ASC
    LIMIT ?
  `).bind(limit).all<MonitoringTargetRow>();
  return result.results;
}

export async function getStreamState(db: D1Database, streamId: string): Promise<StreamStateRow | null> {
  return db.prepare("SELECT * FROM monitoring_streams WHERE stream_id = ? LIMIT 1")
    .bind(streamId).first<StreamStateRow>();
}

export async function getLatestAcceptedSnapshot(db: D1Database, streamId: string): Promise<MonitoringSnapshotV1 | null> {
  const row = await db.prepare(`
    SELECT s.payload_json
    FROM monitoring_streams st
    JOIN monitoring_snapshots s
      ON s.environment = st.environment AND s.snapshot_id = st.latest_snapshot_id
    WHERE st.stream_id = ? AND s.validation_status = 'ACCEPTED'
    LIMIT 1
  `).bind(streamId).first<{ payload_json: string }>();
  if (!row) return null;
  return JSON.parse(row.payload_json) as MonitoringSnapshotV1;
}
