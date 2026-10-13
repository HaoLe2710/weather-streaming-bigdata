import { processNotificationOutbox } from "./notification-service";
import { runHeartbeatMonitor } from "./heartbeat-service";
import { cleanupRetainedData } from "./retention-service";
import type { HeartbeatExecutionSummary } from "./heartbeat-service";
import type { Env } from "../types";

export async function runScheduledTask(cron: string, env: Env): Promise<void> {
  let heartbeat: HeartbeatExecutionSummary;
  try {
    heartbeat = await runHeartbeatMonitor(env.DB, env);
  } catch {
    console.error(JSON.stringify({ code: "HEARTBEAT_SCAN_FAILED", cron }));
    throw new Error("SCHEDULED_TASK_FAILED");
  }

  const failures: string[] = [];
  let notificationsProcessed = 0;
  let cleanup: Awaited<ReturnType<typeof cleanupRetainedData>> | null = null;

  try {
    notificationsProcessed = await processNotificationOutbox(env.DB, env);
  } catch {
    failures.push("NOTIFICATION_OUTBOX_FAILED");
    console.error(JSON.stringify({ code: "NOTIFICATION_OUTBOX_FAILED", cron }));
  }

  if (cron === "17 3 * * *") {
    try {
      cleanup = await cleanupRetainedData(env.DB, env);
    } catch {
      failures.push("RETENTION_CLEANUP_FAILED");
      console.error(JSON.stringify({ code: "RETENTION_CLEANUP_FAILED", cron }));
    }
  }

  const summary = {
    code: "SCHEDULED_TASK_SUMMARY",
    cron,
    ...heartbeat,
    notificationsProcessed,
    cleanup,
    failures,
  };
  console.log(JSON.stringify(summary));

  if (heartbeat.snapshotsFailed || heartbeat.quarantinesFailed) {
    console.error(JSON.stringify({
      code: "RECOVERY_BACKLOG_TERMINAL_FAILURE",
      snapshotsFailed: heartbeat.snapshotsFailed,
      quarantinesFailed: heartbeat.quarantinesFailed,
      cron,
    }));
  }
  if (heartbeat.recoveryErrors > 0 || heartbeat.diagnosticErrors > 0 ||
      (heartbeat.snapshotsFailed ?? 0) > 0 || (heartbeat.quarantinesFailed ?? 0) > 0 || failures.length > 0) {
    throw new Error("SCHEDULED_TASK_FAILED");
  }
}
