import { PINNED_CONTRACT } from "../config/contract";
import { ApiError } from "../errors";
import { admissionDecision } from "../validation/admission";
import { validateSnapshot } from "../validation/snapshot";
import { deterministicStreamId } from "../security/stream-id";
import { databaseNow, boundedInteger, compactJson } from "../repositories/database";
import { currentSnapshotGuard, heartbeatFreshGuard, openAlert, resolveAlert } from "../repositories/alerts";
import { getTarget, markSnapshotApplied, upsertStreamFromSnapshot } from "../repositories/streams";
import type { Env, MonitoringSnapshotV1 } from "../types";

export type IngestionResult =
  | { status: "ACCEPTED"; snapshotId: string; receivedAt: string; schemaVersion: 1 }
  | { status: "DUPLICATE"; snapshotId: string; message: string }
  | { status: "QUARANTINED"; snapshotId: string; reason: string };

function combineGuards(...guards: Array<{ sql: string; values?: Array<string | number | null> }>) {
  return {
    sql: guards.map((guard) => `(${guard.sql})`).join(" AND "),
    values: guards.flatMap((guard) => guard.values || []),
  };
}

export async function applyLiveRules(
  db: D1Database,
  env: Env,
  streamId: string,
  snapshot: MonitoringSnapshotV1,
  liveFresh: boolean,
  streak: number,
): Promise<void> {
  const target = await getTarget(db, streamId);
  if (!target || target.monitoring_enabled !== 1) return;
  const guard = currentSnapshotGuard(streamId, snapshot.snapshotId);
  const notifications = env.TELEGRAM_ALERTS_ENABLED === "true";

  if (liveFresh) {
    await resolveAlert(db, {
      streamId,
      alertType: "AGENT_HEARTBEAT_MISSING",
      details: { recoveredBySnapshotId: snapshot.snapshotId },
      telegramEnabled: notifications,
      guard: combineGuards(guard, heartbeatFreshGuard(streamId)),
    });

    if (snapshot.quality.status === "FAIL") {
      await openAlert(db, {
        streamId,
        alertType: "PIPELINE_DATA_QUALITY_FAILED",
        severity: "CRITICAL",
        details: { qualityStatus: "FAIL", snapshotId: snapshot.snapshotId },
        telegramEnabled: notifications,
        guard,
      });
    } else if (snapshot.quality.status === "PASS") {
      await resolveAlert(db, {
        streamId,
        alertType: "PIPELINE_DATA_QUALITY_FAILED",
        details: { recoveredBySnapshotId: snapshot.snapshotId, qualityStatus: "PASS" },
        telegramEnabled: notifications,
        guard,
      });
    }

    const reportStatus = snapshot.infrastructure.watchdog.latestReport?.status;
    if (reportStatus === "FAIL" || reportStatus === "FAILED") {
      await openAlert(db, {
        streamId,
        alertType: "WATCHDOG_FAILED",
        severity: "CRITICAL",
        details: { watchdogStatus: reportStatus, snapshotId: snapshot.snapshotId },
        telegramEnabled: notifications,
        guard,
      });
    } else if (reportStatus === "PASS") {
      await resolveAlert(db, {
        streamId,
        alertType: "WATCHDOG_FAILED",
        details: { recoveredBySnapshotId: snapshot.snapshotId, watchdogStatus: "PASS" },
        telegramEnabled: notifications,
        guard,
      });
    }

    const diskUsage = snapshot.infrastructure.host?.disk?.usagePercent;
    if (typeof diskUsage === "number") {
      if (diskUsage >= target.disk_threshold_percent) {
        await openAlert(db, {
          streamId,
          alertType: "HOST_DISK_HIGH",
          severity: "WARNING",
          details: { usagePercent: diskUsage, thresholdPercent: target.disk_threshold_percent, snapshotId: snapshot.snapshotId },
          telegramEnabled: notifications,
          guard,
        });
      } else {
        await resolveAlert(db, {
          streamId,
          alertType: "HOST_DISK_HIGH",
          details: { usagePercent: diskUsage, recoveredBySnapshotId: snapshot.snapshotId },
          telegramEnabled: notifications,
          guard,
        });
      }
    }

    if (streak >= target.max_partial_streak) {
      await openAlert(db, {
        streamId,
        alertType: "COLLECTOR_PARTIAL",
        severity: "WARNING",
        details: { collectorStatus: snapshot.collector.status, consecutiveIncompleteSnapshots: streak },
        telegramEnabled: notifications,
        guard,
      });
    } else if (snapshot.collector.status === "COMPLETE") {
      await resolveAlert(db, {
        streamId,
        alertType: "COLLECTOR_PARTIAL",
        details: { recoveredBySnapshotId: snapshot.snapshotId },
        telegramEnabled: notifications,
        guard,
      });
    }
  }
}

async function storeNewSnapshot(
  db: D1Database,
  env: Env,
  snapshot: MonitoringSnapshotV1,
  rawPayload: string,
  payloadHash: string,
  receivedAt: string,
): Promise<IngestionResult | null> {
  const identity = snapshot.source.hostname;
  const streamId = await deterministicStreamId(snapshot.source.environment, snapshot.cohort.cohortId, identity);
  const admission = admissionDecision(snapshot, env);
  const validationStatus = admission.admitted ? "ACCEPTED" : "QUARANTINED";
  const collectedAt = new Date(Date.parse(snapshot.timestamp)).toISOString();

  const inserted = await db.prepare(`
    INSERT OR IGNORE INTO monitoring_snapshots (
      environment, snapshot_id, stream_id, cohort_id, run_id, source_identity,
      agent_version, schema_version, collected_at, received_at, collector_status,
      pipeline_status, quality_status, validation_status, quarantine_reason, stream_applied, payload_sha256, payload_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    RETURNING snapshot_id
  `).bind(
    snapshot.source.environment,
    snapshot.snapshotId,
    streamId,
    snapshot.cohort.cohortId,
    snapshot.cohort.runId,
    identity,
    snapshot.source.agentVersion,
    collectedAt,
    receivedAt,
    snapshot.collector.status,
    snapshot.pipeline.status,
    snapshot.quality.status,
    validationStatus,
    admission.admitted ? null : admission.reason,
    admission.admitted ? 0 : 1,
    payloadHash,
    rawPayload,
  ).first<{ snapshot_id: string }>();

  if (!inserted) {
    const existing = await db.prepare(`
      SELECT payload_sha256 FROM monitoring_snapshots WHERE environment = ? AND snapshot_id = ? LIMIT 1
    `).bind(snapshot.source.environment, snapshot.snapshotId).first<{ payload_sha256: string }>();
    if (existing?.payload_sha256 === payloadHash) {
      return { status: "DUPLICATE", snapshotId: snapshot.snapshotId, message: "Snapshot already ingested" };
    }
    throw new ApiError(409, "SNAPSHOT_ID_CONFLICT", "Snapshot ID was already used for a different payload");
  }

  if (!admission.admitted) {
    if (admission.reason !== "SOURCE_NOT_ALLOWLISTED") {
      await openAlert(db, {
        streamId,
        alertType: admission.reason,
        severity: "CRITICAL",
        details: { reason: admission.reason, snapshotId: snapshot.snapshotId },
        telegramEnabled: env.TELEGRAM_ALERTS_ENABLED === "true",
      });
    }
    return { status: "QUARANTINED", snapshotId: snapshot.snapshotId, reason: admission.reason };
  }

  const target = await getTarget(db, streamId);
  const intervalSeconds = target?.expected_interval_seconds ?? boundedInteger(env.MONITOR_DEFAULT_INTERVAL_SECONDS, 300, 60, 3600);
  const timeoutSeconds = target?.heartbeat_timeout_seconds ?? boundedInteger(env.MONITOR_DEFAULT_HEARTBEAT_TIMEOUT_SECONDS, 900, 60, 86400);
  const ageSeconds = (Date.parse(receivedAt) - Date.parse(collectedAt)) / 1000;
  const liveFresh = ageSeconds >= -60 && ageSeconds <= timeoutSeconds && ageSeconds <= intervalSeconds * 2;

  const { current, state } = await upsertStreamFromSnapshot(db, {
    streamId,
    snapshot,
    collectedAt,
    receivedAt,
    liveFresh,
  });
  await markSnapshotApplied(db, snapshot.source.environment, snapshot.snapshotId);
  if (current && state) {
    await applyLiveRules(db, env, streamId, snapshot, liveFresh, state.incomplete_collector_streak);
  }

  return { status: "ACCEPTED", snapshotId: snapshot.snapshotId, receivedAt, schemaVersion: 1 };
}

export async function ingestSnapshot(
  db: D1Database,
  env: Env,
  bodyBytes: Uint8Array,
  payloadHash: string,
): Promise<IngestionResult> {
  let bodyText: string;
  let parsed: unknown;
  try {
    bodyText = new TextDecoder("utf-8", { fatal: true }).decode(bodyBytes);
    parsed = JSON.parse(bodyText);
  } catch {
    throw new ApiError(400, "INVALID_JSON", "Request body must be valid UTF-8 JSON");
  }

  const validation = validateSnapshot(parsed);
  if (!validation.valid) {
    throw new ApiError(400, "INVALID_SNAPSHOT_SCHEMA", "Snapshot validation failed");
  }
  const snapshot = parsed as MonitoringSnapshotV1;
  const receivedAt = await databaseNow(db);
  const result = await storeNewSnapshot(db, env, snapshot, bodyText, payloadHash, receivedAt);
  if (result) return result;

  // The pre-insert and write are separate because the D1 unique key is the concurrency gate.
  // A concurrent identical request will be detected by the unique-key read in storeNewSnapshot.
  return {
    status: "ACCEPTED",
    snapshotId: snapshot.snapshotId,
    receivedAt,
    schemaVersion: 1,
  };
}

export function isFrozenContract(snapshot: MonitoringSnapshotV1): boolean {
  return snapshot.cohort.runId === PINNED_CONTRACT.runId && snapshot.cohort.cohortId === PINNED_CONTRACT.cohortId;
}
