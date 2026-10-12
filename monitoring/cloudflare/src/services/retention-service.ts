import { boundedInteger } from "../repositories/database";
import type { Env } from "../types";

export async function cleanupRetainedData(db: D1Database, env: Env): Promise<{ snapshots: number; events: number; alerts: number; notifications: number }> {
  const snapshotDays = boundedInteger(env.MONITOR_RETENTION_SNAPSHOT_DAYS, 14, 1, 90);
  const alertDays = boundedInteger(env.MONITOR_RETENTION_ALERT_DAYS, 90, 30, 3650);
  const batchSize = boundedInteger(env.MONITOR_RETENTION_BATCH_SIZE, 250, 1, 1000);

  const [snapshotResult, eventResult, notificationResult, alertResult] = await db.batch([
    db.prepare(`
      DELETE FROM monitoring_snapshots
      WHERE rowid IN (
        SELECT s.rowid FROM monitoring_snapshots s
        WHERE s.received_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-' || ? || ' days')
          AND NOT EXISTS (
            SELECT 1 FROM monitoring_streams st
            WHERE st.environment = s.environment AND st.latest_snapshot_id = s.snapshot_id
          )
        ORDER BY s.received_at ASC LIMIT ?
      )
    `).bind(snapshotDays, batchSize),
    db.prepare(`
      DELETE FROM monitoring_alert_events
      WHERE rowid IN (
        SELECT rowid FROM monitoring_alert_events
        WHERE occurred_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-' || ? || ' days')
        ORDER BY occurred_at ASC LIMIT ?
      )
    `).bind(alertDays, batchSize),
    db.prepare(`
      DELETE FROM monitoring_notification_outbox
      WHERE rowid IN (
        SELECT n.rowid FROM monitoring_notification_outbox n
        JOIN monitoring_alerts a ON a.alert_id = n.alert_id
        WHERE a.status = 'RESOLVED'
          AND a.resolved_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-' || ? || ' days')
          AND n.state IN ('SENT', 'FAILED')
        ORDER BY a.resolved_at ASC LIMIT ?
      )
    `).bind(alertDays, batchSize),
    db.prepare(`
      DELETE FROM monitoring_alerts
      WHERE rowid IN (
        SELECT a.rowid FROM monitoring_alerts a
        WHERE a.status = 'RESOLVED'
          AND a.resolved_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-' || ? || ' days')
          AND NOT EXISTS (SELECT 1 FROM monitoring_notification_outbox n WHERE n.alert_id = a.alert_id)
        ORDER BY a.resolved_at ASC LIMIT ?
      )
    `).bind(alertDays, batchSize),
  ]);
  return {
    snapshots: snapshotResult.meta.changes || 0,
    events: eventResult.meta.changes || 0,
    notifications: notificationResult.meta.changes || 0,
    alerts: alertResult.meta.changes || 0,
  };
}
