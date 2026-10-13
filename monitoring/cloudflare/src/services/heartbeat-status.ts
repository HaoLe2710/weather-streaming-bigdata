import type { MonitoringTargetRow, StreamStateRow } from "../repositories/streams";

export type HeartbeatState = "DISABLED" | "PENDING" | "HEALTHY" | "OFFLINE" | "UNKNOWN";

const MAX_CLOCK_SKEW_SECONDS = 60;

export function isLiveHeartbeatCandidate(
  collectedAt: string,
  receivedAt: string,
  expectedIntervalSeconds: number,
  heartbeatTimeoutSeconds: number,
): boolean {
  const collectedMs = Date.parse(collectedAt);
  const receivedMs = Date.parse(receivedAt);
  if (!Number.isFinite(collectedMs) || !Number.isFinite(receivedMs) ||
      !Number.isFinite(expectedIntervalSeconds) || expectedIntervalSeconds <= 0 ||
      !Number.isFinite(heartbeatTimeoutSeconds) || heartbeatTimeoutSeconds <= 0) {
    return false;
  }
  const deliveryLagSeconds = (receivedMs - collectedMs) / 1000;
  return deliveryLagSeconds >= -MAX_CLOCK_SKEW_SECONDS &&
    deliveryLagSeconds <= heartbeatTimeoutSeconds &&
    deliveryLagSeconds <= expectedIntervalSeconds * 2;
}

/** Pure classification shared by the Cron evaluator and read APIs. */
export function resolveHeartbeatState(
  target: Pick<MonitoringTargetRow,
    "monitoring_enabled" | "activated_at" | "startup_grace_seconds" |
    "heartbeat_timeout_seconds" | "expected_interval_seconds"> | null,
  stream: Pick<StreamStateRow,
    "last_live_heartbeat_at" | "last_live_received_at" | "last_live_snapshot_id"> | null,
  now: string,
): HeartbeatState {
  if (!target) return "UNKNOWN";
  if (target.monitoring_enabled !== 1) return "DISABLED";

  const nowMs = Date.parse(now);
  if (!Number.isFinite(nowMs)) return "UNKNOWN";

  const heartbeatAt = stream?.last_live_heartbeat_at ?? null;
  const receivedAt = stream?.last_live_received_at ?? null;
  const snapshotId = stream?.last_live_snapshot_id ?? null;

  if (heartbeatAt === null) {
    if (receivedAt !== null || snapshotId !== null || target.activated_at === null) return "UNKNOWN";
    const activatedAtMs = Date.parse(target.activated_at);
    if (!Number.isFinite(activatedAtMs) || !Number.isFinite(target.startup_grace_seconds) || target.startup_grace_seconds < 0) {
      return "UNKNOWN";
    }
    return nowMs < activatedAtMs + target.startup_grace_seconds * 1000 ? "PENDING" : "OFFLINE";
  }

  if (receivedAt === null || snapshotId === null ||
      !Number.isFinite(target.heartbeat_timeout_seconds) || target.heartbeat_timeout_seconds <= 0 ||
      !Number.isFinite(target.expected_interval_seconds) || target.expected_interval_seconds <= 0) {
    return "UNKNOWN";
  }

  const heartbeatMs = Date.parse(heartbeatAt);
  const receivedMs = Date.parse(receivedAt);
  if (!Number.isFinite(heartbeatMs) || !Number.isFinite(receivedMs)) return "UNKNOWN";

  const collectionAgeSeconds = (nowMs - heartbeatMs) / 1000;
  const receiptAgeSeconds = (nowMs - receivedMs) / 1000;
  const deliveryLagSeconds = (receivedMs - heartbeatMs) / 1000;
  if (collectionAgeSeconds < -MAX_CLOCK_SKEW_SECONDS || receiptAgeSeconds < -MAX_CLOCK_SKEW_SECONDS ||
      deliveryLagSeconds < -MAX_CLOCK_SKEW_SECONDS ||
      deliveryLagSeconds > target.expected_interval_seconds * 2 ||
      deliveryLagSeconds > target.heartbeat_timeout_seconds) {
    return "UNKNOWN";
  }

  return collectionAgeSeconds < target.heartbeat_timeout_seconds ? "HEALTHY" : "OFFLINE";
}
