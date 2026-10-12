import { databaseNow, boundedInteger } from "../repositories/database";
import { heartbeatFreshGuard, heartbeatMissingGuard, openAlert, resolveAlert } from "../repositories/alerts";
import { getStreamState, getTarget, listEnabledTargets, markSnapshotApplied, upsertStreamFromSnapshot } from "../repositories/streams";
import { applyLiveRules } from "./ingestion-service";
import { admissionDecision } from "../validation/admission";
import type { Env, MonitoringSnapshotV1 } from "../types";

interface UnappliedSnapshot {
  environment: string;
  snapshot_id: string;
  stream_id: string;
  collected_at: string;
  received_at: string;
  payload_json: string;
}

async function processUnappliedSnapshots(db: D1Database, env: Env): Promise<boolean> {
  const pending = await db.prepare(`
    SELECT environment, snapshot_id, stream_id, collected_at, received_at, payload_json
    FROM monitoring_snapshots
    WHERE validation_status = 'ACCEPTED' AND stream_applied = 0
    ORDER BY received_at ASC, snapshot_id ASC LIMIT 1
  `).all<UnappliedSnapshot>();

  for (const row of pending.results) {
    const snapshot = JSON.parse(row.payload_json) as MonitoringSnapshotV1;
    if (!admissionDecision(snapshot, env).admitted) {
      await db.prepare(`
        UPDATE monitoring_snapshots SET validation_status = 'QUARANTINED', quarantine_reason = 'SOURCE_NOT_ALLOWLISTED'
        WHERE environment = ? AND snapshot_id = ? AND stream_applied = 0
      `).bind(row.environment, row.snapshot_id).run();
      return true;
    }
    const target = await getTarget(db, row.stream_id);
    const intervalSeconds = target?.expected_interval_seconds ?? boundedInteger(env.MONITOR_DEFAULT_INTERVAL_SECONDS, 300, 60, 3600);
    const timeoutSeconds = target?.heartbeat_timeout_seconds ?? boundedInteger(env.MONITOR_DEFAULT_HEARTBEAT_TIMEOUT_SECONDS, 900, 60, 86400);
    const ageSeconds = (Date.parse(row.received_at) - Date.parse(row.collected_at)) / 1000;
    const liveFresh = ageSeconds >= -60 && ageSeconds <= timeoutSeconds && ageSeconds <= intervalSeconds * 2;
    const { current, state } = await upsertStreamFromSnapshot(db, {
      streamId: row.stream_id,
      snapshot,
      collectedAt: row.collected_at,
      receivedAt: row.received_at,
      liveFresh,
    });
    await markSnapshotApplied(db, row.environment, row.snapshot_id);
    if (current && state) {
      await applyLiveRules(db, env, row.stream_id, snapshot, liveFresh, state.incomplete_collector_streak);
    }
    return true;
  }
  return false;
}

async function recoverUnalertedQuarantines(db: D1Database, env: Env): Promise<boolean> {
  const pending = await db.prepare(`
    SELECT s.stream_id, s.snapshot_id, s.quarantine_reason
    FROM monitoring_snapshots s
    WHERE s.validation_status = 'QUARANTINED'
      AND s.quarantine_reason IN ('COHORT_CONTRACT_MISMATCH', 'MODEL_CONTRACT_MISMATCH')
      AND NOT EXISTS (
        SELECT 1 FROM monitoring_alert_events e
        WHERE e.event_type = 'OPENED' AND json_extract(e.details_json, '$.snapshotId') = s.snapshot_id
      )
    ORDER BY s.received_at DESC, s.snapshot_id ASC LIMIT 1
  `).all<{ stream_id: string; snapshot_id: string; quarantine_reason: string }>();

  for (const row of pending.results) {
    await openAlert(db, {
      streamId: row.stream_id,
      alertType: row.quarantine_reason,
      severity: "CRITICAL",
      details: { reason: row.quarantine_reason, snapshotId: row.snapshot_id },
      telegramEnabled: env.TELEGRAM_ALERTS_ENABLED === "true",
    });
    return true;
  }
  return false;
}

function isFresh(state: {
  last_live_heartbeat_at: string | null;
  latest_collected_at: string | null;
  latest_received_at: string | null;
}, now: string, target: { heartbeat_timeout_seconds: number; expected_interval_seconds: number }): boolean {
  if (!state.last_live_heartbeat_at || !state.latest_collected_at || !state.latest_received_at) return false;
  const collectedAge = (Date.parse(now) - Date.parse(state.last_live_heartbeat_at)) / 1000;
  const deliveryLag = (Date.parse(state.latest_received_at) - Date.parse(state.latest_collected_at)) / 1000;
  return collectedAge >= -60 && collectedAge <= target.heartbeat_timeout_seconds &&
    deliveryLag >= -60 && deliveryLag <= target.expected_interval_seconds * 2;
}

export async function runHeartbeatMonitor(db: D1Database, env: Env): Promise<void> {
  const repairedSnapshot = await processUnappliedSnapshots(db, env);
  const repairedQuarantine = await recoverUnalertedQuarantines(db, env);
  if (repairedSnapshot || repairedQuarantine) return;
  const now = await databaseNow(db);
  const targetLimit = boundedInteger(env.MONITOR_TARGET_SCAN_LIMIT, 6, 1, 250);
  const targets = await listEnabledTargets(db, targetLimit + 1);
  if (targets.length > targetLimit) {
    console.error(JSON.stringify({ code: "TARGET_SCAN_LIMIT_EXCEEDED", configuredLimit: targetLimit }));
    throw new Error("TARGET_SCAN_LIMIT_EXCEEDED");
  }
  const telegramEnabled = env.TELEGRAM_ALERTS_ENABLED === "true";

  for (const target of targets) {
    const state = await getStreamState(db, target.stream_id);
    const fresh = state ? isFresh(state, now, target) : false;
    if (!fresh) {
      await openAlert(db, {
        streamId: target.stream_id,
        alertType: "AGENT_HEARTBEAT_MISSING",
        severity: "CRITICAL",
        details: {
          lastLiveHeartbeatAt: state?.last_live_heartbeat_at ?? null,
          timeoutSeconds: target.heartbeat_timeout_seconds,
          activatedAt: target.activated_at,
        },
        telegramEnabled,
        guard: heartbeatMissingGuard(target.stream_id),
      });
      continue;
    }

    await resolveAlert(db, {
      streamId: target.stream_id,
      alertType: "AGENT_HEARTBEAT_MISSING",
      details: { recoveredBySnapshotId: state?.latest_snapshot_id ?? null, lastLiveHeartbeatAt: state?.last_live_heartbeat_at ?? null },
      telegramEnabled,
      guard: heartbeatFreshGuard(target.stream_id),
    });
  }
}
