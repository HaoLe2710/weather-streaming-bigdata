import { openAlert } from "../repositories/alerts";
import { boundedInteger, compactJson, databaseNow } from "../repositories/database";
import type { Env } from "../types";

export interface QuarantineRecoverySummary {
  scanned: number;
  processed: number;
  errors: number;
}

interface PendingQuarantine {
  environment: string;
  stream_id: string;
  snapshot_id: string;
  quarantine_reason: "COHORT_CONTRACT_MISMATCH" | "MODEL_CONTRACT_MISMATCH";
}

/** Links one quarantined snapshot to its deduplicated alert and durably marks it processed. */
export async function recordQuarantineAlert(
  db: D1Database,
  env: Env,
  item: PendingQuarantine,
): Promise<boolean> {
  const alert = await openAlert(db, {
    streamId: item.stream_id,
    alertType: item.quarantine_reason,
    severity: "CRITICAL",
    details: { reason: item.quarantine_reason, snapshotId: item.snapshot_id },
    telegramEnabled: env.TELEGRAM_ALERTS_ENABLED === "true",
  });
  if (!alert) throw new Error("QUARANTINE_ALERT_NOT_AVAILABLE");

  const processedAt = await databaseNow(db);
  const dedupeKey = `QUARANTINE_LINKED:${item.environment}:${item.snapshot_id}`;
  const detailsJson = compactJson({ reason: item.quarantine_reason, snapshotId: item.snapshot_id });
  const batchResults = await db.batch([
    db.prepare(`
      UPDATE monitoring_snapshots
      SET quarantine_alert_state = 'PROCESSED', quarantine_alert_processed_at = ?, quarantine_alert_id = ?,
        quarantine_alert_attempts = 0, quarantine_alert_next_attempt_at = NULL,
        quarantine_alert_locked_until = NULL, quarantine_alert_error_code = NULL
      WHERE environment = ? AND snapshot_id = ? AND validation_status = 'QUARANTINED'
        AND quarantine_reason = ? AND quarantine_alert_state IN ('PENDING', 'PROCESSING')
    `).bind(processedAt, alert.alert_id, item.environment, item.snapshot_id, item.quarantine_reason),
    db.prepare(`
      INSERT OR IGNORE INTO monitoring_alert_events
        (event_id, alert_id, event_type, dedupe_key, occurred_at, details_json)
      SELECT ?, ?, 'OBSERVED_AGAIN', ?, ?, ?
      WHERE EXISTS (
        SELECT 1 FROM monitoring_snapshots
        WHERE environment = ? AND snapshot_id = ? AND quarantine_alert_state = 'PROCESSED'
          AND quarantine_alert_id = ?
      )
    `).bind(crypto.randomUUID(), alert.alert_id, dedupeKey, processedAt, detailsJson,
      item.environment, item.snapshot_id, alert.alert_id),
  ]);

  const persisted = await db.prepare(`
    SELECT quarantine_alert_state, quarantine_alert_id
    FROM monitoring_snapshots WHERE environment = ? AND snapshot_id = ? LIMIT 1
  `).bind(item.environment, item.snapshot_id)
    .first<{ quarantine_alert_state: string; quarantine_alert_id: string | null }>();
  if (persisted?.quarantine_alert_state !== "PROCESSED" || persisted.quarantine_alert_id !== alert.alert_id) {
    throw new Error("QUARANTINE_RECOVERY_STATE_NOT_COMMITTED");
  }
  return (batchResults[0].meta.changes || 0) > 0;
}

async function recordQuarantineRecoveryFailure(db: D1Database, env: Env, item: PendingQuarantine): Promise<void> {
  const maxAttempts = boundedInteger(env.MONITOR_RECOVERY_MAX_ATTEMPTS, 5, 1, 10);
  const baseDelay = boundedInteger(env.MONITOR_RECOVERY_BASE_DELAY_SECONDS, 30, 1, 300);
  await db.prepare(`
    UPDATE monitoring_snapshots
    SET quarantine_alert_attempts = quarantine_alert_attempts + 1,
      quarantine_alert_error_code = 'QUARANTINE_RECOVERY_FAILED',
      quarantine_alert_state = CASE WHEN quarantine_alert_attempts + 1 >= ? THEN 'FAILED' ELSE 'PENDING' END,
      quarantine_alert_locked_until = NULL,
      quarantine_alert_next_attempt_at = CASE WHEN quarantine_alert_attempts + 1 >= ? THEN NULL
        ELSE strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' ||
          MIN(3600, ? * (1 << MIN(quarantine_alert_attempts, 10))) || ' seconds') END
    WHERE environment = ? AND snapshot_id = ? AND quarantine_alert_state = 'PROCESSING'
  `).bind(maxAttempts, maxAttempts, baseDelay, item.environment, item.snapshot_id).run();
}

export async function recoverUnalertedQuarantines(db: D1Database, env: Env): Promise<QuarantineRecoverySummary> {
  const limit = boundedInteger(env.MONITOR_RECOVERY_BATCH_SIZE, 1, 1, 10);
  const pending = await db.prepare(`
    SELECT environment, stream_id, snapshot_id, quarantine_reason
    FROM monitoring_snapshots
    WHERE validation_status = 'QUARANTINED'
      AND quarantine_reason IN ('COHORT_CONTRACT_MISMATCH', 'MODEL_CONTRACT_MISMATCH')
      AND (
        (quarantine_alert_state = 'PENDING' AND
          (quarantine_alert_next_attempt_at IS NULL OR quarantine_alert_next_attempt_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))
        OR (quarantine_alert_state = 'PROCESSING' AND quarantine_alert_locked_until <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      )
    ORDER BY received_at ASC, snapshot_id ASC LIMIT ?
  `).bind(limit).all<PendingQuarantine>();

  const summary: QuarantineRecoverySummary = { scanned: pending.results.length, processed: 0, errors: 0 };
  for (const item of pending.results) {
    try {
      const now = await databaseNow(db);
      const claimed = await db.prepare(`
        UPDATE monitoring_snapshots
        SET quarantine_alert_state = 'PROCESSING',
          quarantine_alert_locked_until = strftime('%Y-%m-%dT%H:%M:%fZ', ?, '+60 seconds')
        WHERE environment = ? AND snapshot_id = ? AND validation_status = 'QUARANTINED'
          AND quarantine_reason = ? AND (
            (quarantine_alert_state = 'PENDING' AND
              (quarantine_alert_next_attempt_at IS NULL OR quarantine_alert_next_attempt_at <= ?))
            OR (quarantine_alert_state = 'PROCESSING' AND quarantine_alert_locked_until <= ?)
          )
        RETURNING snapshot_id
      `).bind(now, item.environment, item.snapshot_id, item.quarantine_reason, now, now)
        .first<{ snapshot_id: string }>();
      if (!claimed) continue;
      if (await recordQuarantineAlert(db, env, item)) summary.processed += 1;
    } catch {
      summary.errors += 1;
      try {
        await recordQuarantineRecoveryFailure(db, env, item);
      } catch {
        console.error(JSON.stringify({ code: "QUARANTINE_RECOVERY_ATTEMPT_NOT_RECORDED", snapshotId: item.snapshot_id }));
      }
      console.error(JSON.stringify({
        code: "QUARANTINE_RECOVERY_FAILED",
        snapshotId: item.snapshot_id,
        reason: item.quarantine_reason,
      }));
    }
  }
  return summary;
}
