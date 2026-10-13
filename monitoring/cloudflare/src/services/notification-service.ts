import { databaseNow, boundedInteger, compactJson } from "../repositories/database";
import { sendTelegramAlert } from "../integrations/telegram";
import type { Env } from "../types";

interface PendingNotification {
  notification_id: string;
  alert_id: string;
  transition: "OPENED" | "RESOLVED";
  attempts: number;
  alert_type: string;
  severity: string;
  stream_id: string;
  details_json: string;
  locked_until: string | null;
}

function messageFor(row: PendingNotification): string {
  const details = JSON.parse(row.details_json) as Record<string, unknown>;
  const safeDetails = Object.entries(details)
    .filter(([key, value]) => /^[A-Za-z][A-Za-z0-9]{0,39}$/.test(key) && (typeof value === "number" || typeof value === "string" || typeof value === "boolean"))
    .slice(0, 8)
    .map(([key, value]) => `${key}=${String(value).slice(0, 100)}`)
    .join(" ");
  return `[${row.severity}] ${row.transition} ${row.alert_type} stream=${row.stream_id.slice(0, 12)} ${safeDetails}`.trim();
}

export async function processNotificationOutbox(db: D1Database, env: Env): Promise<number> {
  if (env.TELEGRAM_ALERTS_ENABLED !== "true") return 0;
  const token = env.TELEGRAM_BOT_TOKEN;
  const chatId = env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) {
    console.error(JSON.stringify({ code: "TELEGRAM_CONFIGURATION_MISSING" }));
    throw new Error("TELEGRAM_CONFIGURATION_MISSING");
  }

  const now = await databaseNow(db);
  const maxAttempts = boundedInteger(env.TELEGRAM_MAX_ATTEMPTS, 5, 1, 10);
  const due = await db.prepare(`
    SELECT n.notification_id, n.alert_id, n.transition, n.attempts, n.locked_until,
      a.alert_type, a.severity, a.stream_id, e.details_json
    FROM monitoring_notification_outbox n
    JOIN monitoring_alerts a ON a.alert_id = n.alert_id
    JOIN monitoring_alert_events e ON e.alert_id = a.alert_id AND e.dedupe_key = n.transition
    WHERE (n.state = 'PENDING' AND n.next_attempt_at <= ?)
      OR (n.state = 'SENDING' AND n.locked_until <= ?)
    ORDER BY n.next_attempt_at ASC, n.notification_id ASC LIMIT 1
  `).bind(now, now).all<PendingNotification>();

  let processed = 0;
  for (const item of due.results) {
    const claimUntil = new Date(Date.parse(now) + 60_000).toISOString();
    const claimed = await db.prepare(`
      UPDATE monitoring_notification_outbox
      SET state = 'SENDING', attempts = attempts + 1, locked_until = ?
      WHERE notification_id = ? AND (
        (state = 'PENDING' AND next_attempt_at <= ?)
        OR (state = 'SENDING' AND locked_until <= ?)
      )
      RETURNING attempts
    `).bind(claimUntil, item.notification_id, now, now).first<{ attempts: number }>();
    if (!claimed) continue;
    processed += 1;

    const sent = await sendTelegramAlert(token, chatId, messageFor(item));
    const nextAttempts = claimed.attempts;
    if (sent.ok) {
      const sentAt = await databaseNow(db);
      await db.batch([
        db.prepare(`
          UPDATE monitoring_notification_outbox
          SET state = 'SENT', sent_at = ?, last_error_code = NULL, locked_until = NULL
          WHERE notification_id = ? AND state = 'SENDING'
        `).bind(sentAt, item.notification_id),
        db.prepare(`
          UPDATE monitoring_alerts SET notification_status = 'SENT' WHERE alert_id = ? AND status = ?
        `).bind(item.alert_id, item.transition === "OPENED" ? "OPEN" : "RESOLVED"),
        db.prepare(`
          INSERT OR IGNORE INTO monitoring_alert_events
            (event_id, alert_id, event_type, dedupe_key, occurred_at, details_json)
          SELECT ?, ?, 'NOTIFICATION_SENT', ?, ?, ?
          WHERE EXISTS (SELECT 1 FROM monitoring_notification_outbox WHERE notification_id = ? AND state = 'SENT')
        `).bind(crypto.randomUUID(), item.alert_id, `NOTIFICATION_SENT:${item.transition}`, sentAt,
          compactJson({ transition: item.transition }), item.notification_id),
      ]);
      continue;
    }

    const failedAt = await databaseNow(db);
    const terminal = nextAttempts >= maxAttempts;
    const delaySeconds = Math.min(3600, 30 * (2 ** Math.min(item.attempts, 6)));
    const nextAttemptAt = new Date(Date.parse(failedAt) + delaySeconds * 1000).toISOString();
    await db.batch([
      db.prepare(`
        UPDATE monitoring_notification_outbox
        SET state = ?, next_attempt_at = ?, last_error_code = ?, locked_until = NULL
        WHERE notification_id = ? AND state = 'SENDING'
      `).bind(terminal ? "FAILED" : "PENDING", nextAttemptAt, sent.errorCode || "TELEGRAM_SEND_FAILED", item.notification_id),
      ...(terminal ? [
        db.prepare(`
          UPDATE monitoring_alerts SET notification_status = 'FAILED' WHERE alert_id = ? AND status = ?
        `).bind(item.alert_id, item.transition === "OPENED" ? "OPEN" : "RESOLVED"),
        db.prepare(`
          INSERT OR IGNORE INTO monitoring_alert_events
            (event_id, alert_id, event_type, dedupe_key, occurred_at, details_json)
          SELECT ?, ?, 'NOTIFICATION_FAILED', ?, ?, ?
          WHERE EXISTS (SELECT 1 FROM monitoring_notification_outbox WHERE notification_id = ? AND state = 'FAILED')
        `).bind(crypto.randomUUID(), item.alert_id, `NOTIFICATION_FAILED:${item.transition}`, failedAt,
          compactJson({ transition: item.transition, errorCode: sent.errorCode || "TELEGRAM_SEND_FAILED", attempts: nextAttempts }), item.notification_id),
      ] : []),
    ]);
  }
  return processed;
}
