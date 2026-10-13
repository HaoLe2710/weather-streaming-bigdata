import { databaseNow, compactJson } from "./database";
import type { AlertRow } from "../types";

export interface AlertGuard {
  sql: string;
  values?: Array<string | number | null>;
}

const ALWAYS = { sql: "1 = 1", values: [] as Array<string | number | null> };

async function fingerprint(streamId: string, alertType: string): Promise<string> {
  const source = new TextEncoder().encode(`${streamId}\n${alertType}`);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", source));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function currentSnapshotGuard(streamId: string, snapshotId: string): AlertGuard {
  return {
    sql: "EXISTS (SELECT 1 FROM monitoring_streams s WHERE s.stream_id = ? AND s.latest_snapshot_id = ?)",
    values: [streamId, snapshotId],
  };
}

export function heartbeatMissingGuard(streamId: string): AlertGuard {
  return {
    sql: `EXISTS (
      SELECT 1 FROM monitoring_targets t
      LEFT JOIN monitoring_streams s ON s.stream_id = t.stream_id
      WHERE t.stream_id = ? AND t.monitoring_enabled = 1
        AND (
          (s.last_live_heartbeat_at IS NULL
            AND s.last_live_received_at IS NULL AND s.last_live_snapshot_id IS NULL
            AND t.activated_at IS NOT NULL AND julianday(t.activated_at) IS NOT NULL
            AND t.startup_grace_seconds >= 0
            AND julianday(t.activated_at) <= julianday('now') - (t.startup_grace_seconds / 86400.0))
          OR
          (
            s.last_live_heartbeat_at IS NOT NULL
            AND s.last_live_received_at IS NOT NULL
            AND s.last_live_snapshot_id IS NOT NULL
            AND julianday(s.last_live_heartbeat_at) IS NOT NULL
            AND julianday(s.last_live_received_at) IS NOT NULL
            AND t.heartbeat_timeout_seconds > 0
            AND t.expected_interval_seconds > 0
            AND julianday(s.last_live_heartbeat_at) <= julianday('now') + (60.0 / 86400.0)
            AND julianday(s.last_live_received_at) <= julianday('now') + (60.0 / 86400.0)
            AND julianday(s.last_live_received_at) >= julianday(s.last_live_heartbeat_at) - (60.0 / 86400.0)
            AND julianday(s.last_live_received_at) <= julianday(s.last_live_heartbeat_at) + (t.expected_interval_seconds * 2.0 / 86400.0)
            AND julianday(s.last_live_received_at) <= julianday(s.last_live_heartbeat_at) + (t.heartbeat_timeout_seconds / 86400.0)
            AND julianday(s.last_live_heartbeat_at) <= julianday('now') - (t.heartbeat_timeout_seconds / 86400.0)
          )
        )
    )`,
    values: [streamId],
  };
}

export function heartbeatFreshGuard(streamId: string): AlertGuard {
  return {
    sql: `EXISTS (
      SELECT 1 FROM monitoring_targets t
      JOIN monitoring_streams s ON s.stream_id = t.stream_id
      WHERE t.stream_id = ? AND t.monitoring_enabled = 1
        AND s.last_live_heartbeat_at IS NOT NULL
        AND s.last_live_received_at IS NOT NULL
        AND s.last_live_snapshot_id IS NOT NULL
        AND julianday(s.last_live_heartbeat_at) IS NOT NULL
        AND julianday(s.last_live_received_at) IS NOT NULL
        AND t.heartbeat_timeout_seconds > 0
        AND t.expected_interval_seconds > 0
        AND julianday(s.last_live_heartbeat_at) > julianday('now') - (t.heartbeat_timeout_seconds / 86400.0)
        AND julianday(s.last_live_heartbeat_at) <= julianday('now') + (60.0 / 86400.0)
        AND julianday(s.last_live_received_at) <= julianday('now') + (60.0 / 86400.0)
        AND julianday(s.last_live_received_at) >= julianday(s.last_live_heartbeat_at) - (60.0 / 86400.0)
        AND julianday(s.last_live_received_at) <= julianday(s.last_live_heartbeat_at) + (t.expected_interval_seconds * 2.0 / 86400.0)
        AND julianday(s.last_live_received_at) <= julianday(s.last_live_heartbeat_at) + (t.heartbeat_timeout_seconds / 86400.0)
    )`,
    values: [streamId],
  };
}

export async function openAlert(
  db: D1Database,
  input: {
    streamId: string;
    alertType: string;
    severity: "WARNING" | "CRITICAL";
    details: Record<string, unknown>;
    telegramEnabled: boolean;
    guard?: AlertGuard;
  },
): Promise<AlertRow | null> {
  const now = await databaseNow(db);
  const guard = input.guard || ALWAYS;
  const alertId = crypto.randomUUID();
  const notificationStatus = input.telegramEnabled ? "PENDING" : "DISABLED";
  const detailsJson = compactJson(input.details);
  const digest = await fingerprint(input.streamId, input.alertType);

  await db.batch([
    db.prepare(`
      INSERT OR IGNORE INTO monitoring_alerts (
        alert_id, stream_id, alert_type, severity, status, fingerprint,
        first_triggered_at, last_observed_at, resolved_at, details_json, notification_status
      )
      SELECT ?, ?, ?, ?, 'OPEN', ?, ?, ?, NULL, ?, ?
      WHERE ${guard.sql}
    `).bind(
      alertId,
      input.streamId,
      input.alertType,
      input.severity,
      digest,
      now,
      now,
      detailsJson,
      notificationStatus,
      ...(guard.values || []),
    ),
    db.prepare(`
      INSERT OR IGNORE INTO monitoring_alert_events (event_id, alert_id, event_type, dedupe_key, occurred_at, details_json)
      SELECT ?, ?, 'OPENED', 'OPENED', ?, ?
      WHERE EXISTS (SELECT 1 FROM monitoring_alerts WHERE alert_id = ?)
    `).bind(crypto.randomUUID(), alertId, now, detailsJson, alertId),
    db.prepare(`
      INSERT OR IGNORE INTO monitoring_notification_outbox (
        notification_id, alert_id, transition, state, attempts, next_attempt_at
      )
      SELECT ?, ?, 'OPENED', 'PENDING', 0, ?
      WHERE ? = 1 AND EXISTS (SELECT 1 FROM monitoring_alerts WHERE alert_id = ?)
    `).bind(crypto.randomUUID(), alertId, now, input.telegramEnabled ? 1 : 0, alertId),
  ]);

  const active = await db.prepare(
    "SELECT * FROM monitoring_alerts WHERE fingerprint = ? AND status = 'OPEN' LIMIT 1",
  ).bind(digest).first<AlertRow>();
  if (!active) return null;

  if (active.alert_id !== alertId) {
    const refreshed = await db.prepare(`
      UPDATE monitoring_alerts
      SET last_observed_at = MAX(last_observed_at, ?),
        details_json = CASE WHEN last_observed_at <= ? THEN ? ELSE details_json END
      WHERE alert_id = ? AND status = 'OPEN' AND ${guard.sql}
    `).bind(now, now, detailsJson, active.alert_id, ...(guard.values || [])).run();
    if ((refreshed.meta.changes || 0) === 0) return null;
  }
  return active;
}

export async function resolveAlert(
  db: D1Database,
  input: {
    streamId: string;
    alertType: string;
    details?: Record<string, unknown>;
    telegramEnabled: boolean;
    guard?: AlertGuard;
  },
): Promise<boolean> {
  const guard = input.guard || ALWAYS;
  const now = await databaseNow(db);
  const digest = await fingerprint(input.streamId, input.alertType);
  const detailsJson = compactJson(input.details || { resolved: true });
  const [update] = await db.batch([
    db.prepare(`
      UPDATE monitoring_alerts SET status = 'RESOLVED', resolved_at = ?, last_observed_at = ?,
        details_json = ?, notification_status = CASE WHEN ? = 1 THEN 'PENDING' ELSE notification_status END
      WHERE fingerprint = ? AND status = 'OPEN' AND ${guard.sql}
    `).bind(now, now, detailsJson, input.telegramEnabled ? 1 : 0, digest, ...(guard.values || [])),
    db.prepare(`
      INSERT OR IGNORE INTO monitoring_alert_events (event_id, alert_id, event_type, dedupe_key, occurred_at, details_json)
      SELECT ?, alert_id, 'RESOLVED', 'RESOLVED', ?, ?
      FROM monitoring_alerts WHERE fingerprint = ? AND status = 'RESOLVED' AND resolved_at = ?
    `).bind(crypto.randomUUID(), now, detailsJson, digest, now),
    db.prepare(`
      INSERT OR IGNORE INTO monitoring_notification_outbox (
        notification_id, alert_id, transition, state, attempts, next_attempt_at
      )
      SELECT ?, alert_id, 'RESOLVED', 'PENDING', 0, ?
      FROM monitoring_alerts
      WHERE ? = 1 AND fingerprint = ? AND status = 'RESOLVED' AND resolved_at = ?
    `).bind(crypto.randomUUID(), now, input.telegramEnabled ? 1 : 0, digest, now),
  ]);
  return (update.meta.changes || 0) > 0;
}

export async function listOpenAlerts(db: D1Database, streamId: string, limit = 50): Promise<AlertRow[]> {
  const result = await db.prepare(`
    SELECT * FROM monitoring_alerts
    WHERE stream_id = ? AND status = 'OPEN'
    ORDER BY first_triggered_at DESC, alert_id DESC LIMIT ?
  `).bind(streamId, limit).all<AlertRow>();
  return result.results;
}
