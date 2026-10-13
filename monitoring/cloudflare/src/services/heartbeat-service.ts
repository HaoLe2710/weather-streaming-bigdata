import { databaseNow, boundedInteger } from "../repositories/database";
import { heartbeatFreshGuard, heartbeatMissingGuard, openAlert, resolveAlert } from "../repositories/alerts";
import { getStreamState, getTarget, listEnabledTargets, markSnapshotApplied, upsertStreamFromSnapshot } from "../repositories/streams";
import { applyLiveRules } from "./ingestion-service";
import { recoverUnalertedQuarantines } from "./quarantine-service";
import { isLiveHeartbeatCandidate, resolveHeartbeatState } from "./heartbeat-status";
import type { Env, MonitoringSnapshotV1 } from "../types";

interface UnappliedSnapshot {
  environment: string;
  snapshot_id: string;
  stream_id: string;
  collected_at: string;
  received_at: string;
  payload_json: string;
}

export interface HeartbeatExecutionSummary {
  heartbeatTargetsChecked: number;
  offlineTargets: number;
  unknownTargets: number;
  snapshotsRecovered: number;
  snapshotRecoveryErrors: number;
  snapshotsPending: number | null;
  snapshotsFailed: number | null;
  quarantinesScanned: number;
  quarantinesProcessed: number;
  quarantineRecoveryErrors: number;
  quarantinesPending: number | null;
  quarantinesFailed: number | null;
  diagnosticErrors: number;
  recoveryErrors: number;
}

async function recordSnapshotRecoveryFailure(db: D1Database, env: Env, row: UnappliedSnapshot): Promise<void> {
  const maxAttempts = boundedInteger(env.MONITOR_RECOVERY_MAX_ATTEMPTS, 5, 1, 10);
  const baseDelay = boundedInteger(env.MONITOR_RECOVERY_BASE_DELAY_SECONDS, 30, 1, 300);
  await db.prepare(`
    UPDATE monitoring_snapshots
    SET stream_recovery_attempts = stream_recovery_attempts + 1,
      stream_recovery_error_code = 'SNAPSHOT_RECOVERY_FAILED',
      stream_recovery_state = CASE WHEN stream_recovery_attempts + 1 >= ? THEN 'FAILED' ELSE 'PENDING' END,
      stream_recovery_locked_until = NULL,
      stream_recovery_next_attempt_at = CASE WHEN stream_recovery_attempts + 1 >= ? THEN NULL
        ELSE strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' ||
          MIN(3600, ? * (1 << MIN(stream_recovery_attempts, 10))) || ' seconds') END
    WHERE environment = ? AND snapshot_id = ? AND stream_recovery_state = 'PROCESSING'
  `).bind(maxAttempts, maxAttempts, baseDelay, row.environment, row.snapshot_id).run();
}

async function processUnappliedSnapshots(db: D1Database, env: Env): Promise<{ recovered: number; errors: number }> {
  const limit = boundedInteger(env.MONITOR_RECOVERY_BATCH_SIZE, 1, 1, 10);
  const pending = await db.prepare(`
    SELECT environment, snapshot_id, stream_id, collected_at, received_at, payload_json
    FROM monitoring_snapshots
    WHERE validation_status = 'ACCEPTED' AND stream_applied = 0 AND (
      (stream_recovery_state = 'PENDING' AND
        (stream_recovery_next_attempt_at IS NULL OR stream_recovery_next_attempt_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))
      OR (stream_recovery_state = 'PROCESSING' AND
        stream_recovery_locked_until <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
    ORDER BY received_at ASC, snapshot_id ASC LIMIT ?
  `).bind(limit).all<UnappliedSnapshot>();

  const summary = { recovered: 0, errors: 0 };
  for (const row of pending.results) {
    try {
      const now = await databaseNow(db);
      const claimed = await db.prepare(`
        UPDATE monitoring_snapshots
        SET stream_recovery_state = 'PROCESSING',
          stream_recovery_locked_until = strftime('%Y-%m-%dT%H:%M:%fZ', ?, '+60 seconds')
        WHERE environment = ? AND snapshot_id = ? AND validation_status = 'ACCEPTED'
          AND stream_applied = 0 AND (
            (stream_recovery_state = 'PENDING' AND
              (stream_recovery_next_attempt_at IS NULL OR stream_recovery_next_attempt_at <= ?))
            OR (stream_recovery_state = 'PROCESSING' AND stream_recovery_locked_until <= ?)
          )
        RETURNING snapshot_id
      `).bind(now, row.environment, row.snapshot_id, now, now).first<{ snapshot_id: string }>();
      if (!claimed) continue;
      // Admission is immutable once the snapshot row is stored as ACCEPTED. Runtime allowlist
      // changes apply to new requests; they must not rewrite the reason or evidence of this row.
      const snapshot = JSON.parse(row.payload_json) as MonitoringSnapshotV1;
      const target = await getTarget(db, row.stream_id);
      const intervalSeconds = target?.expected_interval_seconds ??
        boundedInteger(env.MONITOR_DEFAULT_INTERVAL_SECONDS, 300, 60, 3600);
      const timeoutSeconds = target?.heartbeat_timeout_seconds ??
        boundedInteger(env.MONITOR_DEFAULT_HEARTBEAT_TIMEOUT_SECONDS, 900, 60, 86400);
      const liveFresh = isLiveHeartbeatCandidate(row.collected_at, row.received_at, intervalSeconds, timeoutSeconds);
      const { current, state } = await upsertStreamFromSnapshot(db, {
        streamId: row.stream_id,
        snapshot,
        collectedAt: row.collected_at,
        receivedAt: row.received_at,
        liveFresh,
      });
      if (current && state) {
        await applyLiveRules(db, env, row.stream_id, snapshot, liveFresh, state.incomplete_collector_streak);
      }
      // Mark only after state and guarded alert effects finish. A retry recognizes the same
      // latest snapshot as current and idempotently reapplies the alert rules.
      await markSnapshotApplied(db, row.environment, row.snapshot_id);
      summary.recovered += 1;
    } catch {
      summary.errors += 1;
      try {
        await recordSnapshotRecoveryFailure(db, env, row);
      } catch {
        console.error(JSON.stringify({ code: "SNAPSHOT_RECOVERY_ATTEMPT_NOT_RECORDED", snapshotId: row.snapshot_id }));
      }
      console.error(JSON.stringify({ code: "SNAPSHOT_RECOVERY_FAILED", snapshotId: row.snapshot_id }));
    }
  }
  return summary;
}

export async function runHeartbeatMonitor(db: D1Database, env: Env): Promise<HeartbeatExecutionSummary> {
  const summary: HeartbeatExecutionSummary = {
    heartbeatTargetsChecked: 0,
    offlineTargets: 0,
    unknownTargets: 0,
    snapshotsRecovered: 0,
    snapshotRecoveryErrors: 0,
    snapshotsPending: null,
    snapshotsFailed: null,
    quarantinesScanned: 0,
    quarantinesProcessed: 0,
    quarantineRecoveryErrors: 0,
    quarantinesPending: null,
    quarantinesFailed: null,
    diagnosticErrors: 0,
    recoveryErrors: 0,
  };

  // Heartbeat liveness is evaluated before any recoverable backlog. Recovery cannot starve it.
  const now = await databaseNow(db);
  const targetLimit = boundedInteger(env.MONITOR_TARGET_SCAN_LIMIT, 5, 1, 250);
  const targets = await listEnabledTargets(db, targetLimit + 1);
  if (targets.length > targetLimit) {
    console.error(JSON.stringify({ code: "TARGET_SCAN_LIMIT_EXCEEDED", configuredLimit: targetLimit }));
    throw new Error("TARGET_SCAN_LIMIT_EXCEEDED");
  }
  const telegramEnabled = env.TELEGRAM_ALERTS_ENABLED === "true";

  for (const target of targets) {
    const state = await getStreamState(db, target.stream_id);
    const heartbeatState = resolveHeartbeatState(target, state, now);
    summary.heartbeatTargetsChecked += 1;

    if (heartbeatState === "OFFLINE") {
      summary.offlineTargets += 1;
      await openAlert(db, {
        streamId: target.stream_id,
        alertType: "AGENT_HEARTBEAT_MISSING",
        severity: "CRITICAL",
        details: {
          heartbeatState,
          lastLiveHeartbeatAt: state?.last_live_heartbeat_at ?? null,
          timeoutSeconds: target.heartbeat_timeout_seconds,
          activatedAt: target.activated_at,
        },
        telegramEnabled,
        guard: heartbeatMissingGuard(target.stream_id),
      });
    } else if (heartbeatState === "HEALTHY") {
      await resolveAlert(db, {
        streamId: target.stream_id,
        alertType: "AGENT_HEARTBEAT_MISSING",
        details: {
          heartbeatState,
          recoveredBySnapshotId: state?.last_live_snapshot_id ?? null,
          lastLiveHeartbeatAt: state?.last_live_heartbeat_at ?? null,
        },
        telegramEnabled,
        guard: heartbeatFreshGuard(target.stream_id),
      });
    } else if (heartbeatState === "UNKNOWN") {
      summary.unknownTargets += 1;
    }
  }

  // Each recovery class has its own bounded budget and error accounting. One damaged record
  // or failed repair does not prevent the other class from making progress this tick.
  try {
    const snapshots = await processUnappliedSnapshots(db, env);
    summary.snapshotsRecovered = snapshots.recovered;
    summary.snapshotRecoveryErrors = snapshots.errors;
    summary.recoveryErrors += snapshots.errors;
  } catch {
    summary.snapshotRecoveryErrors += 1;
    summary.recoveryErrors += 1;
    console.error(JSON.stringify({ code: "SNAPSHOT_RECOVERY_QUERY_FAILED" }));
  }

  try {
    const quarantines = await recoverUnalertedQuarantines(db, env);
    summary.quarantinesScanned = quarantines.scanned;
    summary.quarantinesProcessed = quarantines.processed;
    summary.quarantineRecoveryErrors = quarantines.errors;
    summary.recoveryErrors += quarantines.errors;
  } catch {
    summary.quarantineRecoveryErrors += 1;
    summary.recoveryErrors += 1;
    console.error(JSON.stringify({ code: "QUARANTINE_RECOVERY_QUERY_FAILED" }));
  }

  try {
    const backlog = await db.prepare(`
      SELECT
        (SELECT COUNT(*) FROM monitoring_snapshots
          WHERE validation_status = 'ACCEPTED' AND stream_applied = 0
            AND stream_recovery_state IN ('PENDING', 'PROCESSING')) AS snapshots_pending,
        (SELECT COUNT(*) FROM monitoring_snapshots
          WHERE validation_status = 'ACCEPTED' AND stream_applied = 0
            AND stream_recovery_state = 'FAILED') AS snapshots_failed,
        (SELECT COUNT(*) FROM monitoring_snapshots
          WHERE validation_status = 'QUARANTINED'
            AND quarantine_reason IN ('COHORT_CONTRACT_MISMATCH', 'MODEL_CONTRACT_MISMATCH')
            AND quarantine_alert_state IN ('PENDING', 'PROCESSING')) AS quarantines_pending,
        (SELECT COUNT(*) FROM monitoring_snapshots
          WHERE validation_status = 'QUARANTINED'
            AND quarantine_reason IN ('COHORT_CONTRACT_MISMATCH', 'MODEL_CONTRACT_MISMATCH')
            AND quarantine_alert_state = 'FAILED') AS quarantines_failed
    `).first<{ snapshots_pending: number; snapshots_failed: number; quarantines_pending: number; quarantines_failed: number }>();
    summary.snapshotsPending = backlog?.snapshots_pending ?? 0;
    summary.snapshotsFailed = backlog?.snapshots_failed ?? 0;
    summary.quarantinesPending = backlog?.quarantines_pending ?? 0;
    summary.quarantinesFailed = backlog?.quarantines_failed ?? 0;
  } catch {
    summary.diagnosticErrors += 1;
    summary.recoveryErrors += 1;
    console.error(JSON.stringify({ code: "RECOVERY_BACKLOG_METRICS_FAILED" }));
  }

  return summary;
}
