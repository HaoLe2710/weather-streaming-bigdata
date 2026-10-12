import { processNotificationOutbox } from "./notification-service";
import { runHeartbeatMonitor } from "./heartbeat-service";
import { cleanupRetainedData } from "./retention-service";
import type { Env } from "../types";

export async function runScheduledTask(cron: string, env: Env): Promise<void> {
  try {
    if (cron === "17 3 * * *") {
      await cleanupRetainedData(env.DB, env);
      return;
    }
    await runHeartbeatMonitor(env.DB, env);
    await processNotificationOutbox(env.DB, env);
  } catch {
    console.error(JSON.stringify({ code: "SCHEDULED_TASK_FAILED", cron }));
    throw new Error("SCHEDULED_TASK_FAILED");
  }
}
