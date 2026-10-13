import { Hono } from "hono";
import { ApiError } from "../errors";
import { readAuth } from "../security/read-auth";
import { databaseNow } from "../repositories/database";
import { latestSnapshot } from "../repositories/snapshots";
import { listOpenAlerts } from "../repositories/alerts";
import { getStreamState, getTarget } from "../repositories/streams";
import { resolveHeartbeatState } from "../services/heartbeat-status";
import type { MonitoringTargetRow, StreamStateRow } from "../repositories/streams";
import type { AlertRow, AppEnv, StoredSnapshot } from "../types";

const monitoring = new Hono<AppEnv>();
monitoring.use("*", readAuth);

const STREAM_ID = /^[a-f0-9]{64}$/;
const ISO_UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/;
const DAY_MS = 24 * 60 * 60 * 1000;

function streamParam(value: string): string {
  if (!STREAM_ID.test(value)) throw new ApiError(404, "NOT_FOUND", "Stream was not found");
  return value;
}

function encodeCursor(value: object): string {
  const bytes = new TextEncoder().encode(JSON.stringify(value));
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/g, "");
}

function decodeCursor<T>(value: string | undefined, parse: (decoded: unknown) => T | null): T | null {
  if (!value) return null;
  if (value.length > 1024 || !/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new ApiError(400, "INVALID_QUERY", "Cursor is invalid");
  }
  try {
    const normalized = value.replaceAll("-", "+").replaceAll("_", "/");
    const binary = atob(normalized + "=".repeat((4 - normalized.length % 4) % 4));
    const decoded = JSON.parse(new TextDecoder().decode(Uint8Array.from(binary, (c) => c.charCodeAt(0)))) as unknown;
    const result = parse(decoded);
    if (result === null) throw new Error("invalid cursor shape");
    return result;
  } catch {
    throw new ApiError(400, "INVALID_QUERY", "Cursor is invalid");
  }
}

function parseLimit(value: string | undefined, fallback = 50): number {
  if (value === undefined) return fallback;
  if (!/^\d+$/.test(value)) throw new ApiError(400, "INVALID_QUERY", "Limit must be an integer from 1 to 200");
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 200) {
    throw new ApiError(400, "INVALID_QUERY", "Limit must be an integer from 1 to 200");
  }
  return parsed;
}

function payload(row: StoredSnapshot | null): unknown | null {
  if (!row) return null;
  return JSON.parse(row.payload_json) as unknown;
}

function decodeAlert(row: AlertRow) {
  return {
    alertId: row.alert_id,
    streamId: row.stream_id,
    type: row.alert_type,
    severity: row.severity,
    status: row.status,
    firstTriggeredAt: row.first_triggered_at,
    lastObservedAt: row.last_observed_at,
    resolvedAt: row.resolved_at,
    details: JSON.parse(row.details_json) as unknown,
    notificationStatus: row.notification_status,
  };
}

monitoring.get("/streams", async (ctx) => {
  const limit = parseLimit(ctx.req.query("limit"), 50);
  const cursor = decodeCursor(ctx.req.query("cursor"), (value) => {
    if (!value || typeof value !== "object") return null;
    const item = value as Record<string, unknown>;
    if (typeof item.streamId !== "string" || !STREAM_ID.test(item.streamId)) return null;
    return { streamId: item.streamId };
  });
  const now = await databaseNow(ctx.env.DB);
  type StreamListRow = MonitoringTargetRow & Pick<StreamStateRow,
    "latest_collected_at" | "latest_received_at" | "latest_collector_status" |
    "latest_pipeline_status" | "last_live_heartbeat_at" | "last_live_received_at" |
    "last_live_snapshot_id">;
  const result = await ctx.env.DB.prepare(`
    SELECT t.stream_id, t.environment, t.cohort_id, t.run_id, t.source_identity,
      t.expected_interval_seconds, t.heartbeat_timeout_seconds, t.startup_grace_seconds,
      t.disk_threshold_percent, t.max_partial_streak, t.monitoring_enabled, t.activated_at,
      s.latest_collected_at, s.latest_received_at, s.latest_collector_status,
      s.latest_pipeline_status, s.last_live_heartbeat_at, s.last_live_received_at,
      s.last_live_snapshot_id
    FROM monitoring_targets t
    LEFT JOIN monitoring_streams s ON s.stream_id = t.stream_id
    WHERE (? IS NULL OR t.stream_id > ?)
    ORDER BY t.stream_id ASC LIMIT ?
  `).bind(cursor?.streamId ?? null, cursor?.streamId ?? null, limit + 1).all<StreamListRow>();
  const rows = result.results;
  const hasMore = rows.length > limit;
  const data = rows.slice(0, limit).map((row) => ({
    streamId: row.stream_id,
    environment: row.environment,
    cohortId: row.cohort_id,
    pipelineStatus: row.latest_pipeline_status,
    collectorStatus: row.latest_collector_status,
    lastCollectedAt: row.latest_collected_at,
    lastReceivedAt: row.latest_received_at,
    heartbeatState: resolveHeartbeatState(row, row, now),
    monitoringEnabled: row.monitoring_enabled === 1,
  }));
  return ctx.json({ apiVersion: 1, streams: data, nextCursor: hasMore && data.length ? encodeCursor({ streamId: data.at(-1)?.streamId }) : null });
});

monitoring.get("/streams/:streamId/latest", async (ctx) => {
  const streamId = streamParam(ctx.req.param("streamId"));
  const row = await latestSnapshot(ctx.env.DB, streamId);
  return ctx.json(row
    ? { apiVersion: 1, status: "AVAILABLE", streamId, snapshot: payload(row) }
    : { apiVersion: 1, status: "EMPTY", streamId, snapshot: null });
});

monitoring.get("/streams/:streamId/history", async (ctx) => {
  const streamId = streamParam(ctx.req.param("streamId"));
  const limit = parseLimit(ctx.req.query("limit"));
  const fromArg = ctx.req.query("from");
  const toArg = ctx.req.query("to");
  const cursor = decodeCursor(ctx.req.query("cursor"), (value) => {
    if (!value || typeof value !== "object") return null;
    const item = value as Record<string, unknown>;
    if (typeof item.collectedAt !== "string" || !ISO_UTC.test(item.collectedAt)) return null;
    if (typeof item.snapshotId !== "string" || !UUID.test(item.snapshotId)) return null;
    return { collectedAt: item.collectedAt, snapshotId: item.snapshotId };
  });
  let to = new Date().toISOString();
  let from = new Date(Date.now() - 14 * DAY_MS).toISOString();
  if ((fromArg && !toArg) || (!fromArg && toArg)) {
    throw new ApiError(400, "INVALID_QUERY", "Both from and to must be provided together");
  }
  if (fromArg && toArg) {
    if (!ISO_UTC.test(fromArg) || !ISO_UTC.test(toArg) || !Number.isFinite(Date.parse(fromArg)) || !Number.isFinite(Date.parse(toArg))) {
      throw new ApiError(400, "INVALID_QUERY", "from and to must be UTC ISO timestamps");
    }
    from = new Date(Date.parse(fromArg)).toISOString();
    to = new Date(Date.parse(toArg)).toISOString();
  }
  const span = Date.parse(to) - Date.parse(from);
  if (span < 0 || span > 14 * DAY_MS) throw new ApiError(400, "INVALID_QUERY", "History window must be between zero and 14 days");

  const result = await ctx.env.DB.prepare(`
    SELECT * FROM monitoring_snapshots
    WHERE stream_id = ? AND validation_status = 'ACCEPTED'
      AND collected_at >= ? AND collected_at <= ?
      AND (? IS NULL OR collected_at < ? OR (collected_at = ? AND snapshot_id < ?))
    ORDER BY collected_at DESC, snapshot_id DESC LIMIT ?
  `).bind(streamId, from, to, cursor?.collectedAt ?? null, cursor?.collectedAt ?? null, cursor?.collectedAt ?? null, cursor?.snapshotId ?? null, limit + 1).all<StoredSnapshot>();
  const rows = result.results;
  const hasMore = rows.length > limit;
  const items = rows.slice(0, limit);
  const last = items.at(-1);
  return ctx.json({
    apiVersion: 1,
    streamId,
    snapshots: items.map((row) => ({ snapshotId: row.snapshot_id, collectedAt: row.collected_at, receivedAt: row.received_at, snapshot: payload(row) })),
    nextCursor: hasMore && last ? encodeCursor({ collectedAt: last.collected_at, snapshotId: last.snapshot_id }) : null,
  });
});

monitoring.get("/streams/:streamId/summary", async (ctx) => {
  const streamId = streamParam(ctx.req.param("streamId"));
  const [snapshotRow, streamRow, target, now, alerts] = await Promise.all([
    latestSnapshot(ctx.env.DB, streamId),
    getStreamState(ctx.env.DB, streamId),
    getTarget(ctx.env.DB, streamId),
    databaseNow(ctx.env.DB),
    listOpenAlerts(ctx.env.DB, streamId, 100),
  ]);
  const snapshot = payload(snapshotRow) as Record<string, any> | null;
  return ctx.json({
    apiVersion: 1,
    streamId,
    latestPipelineHealth: snapshot?.pipeline ?? null,
    collectorCompleteness: snapshot?.collector ?? null,
    cohortProgress: snapshot?.progress ?? null,
    validEvaluations: snapshot?.progress?.validEvaluations ?? null,
    forecastPersistence: snapshot ? {
      forecastsPersisted: snapshot.progress?.forecastsPersisted ?? null,
      persistenceReceiptCount: snapshot.quality?.persistenceReceiptCount ?? null,
      persistenceReceiptAudit: snapshot.quality?.persistenceReceiptAudit ?? null,
      pendingReference: snapshot.progress?.pendingReference ?? null,
    } : null,
    dataQuality: snapshot?.quality ?? null,
    latestInferenceMetadata: snapshot?.inference ?? null,
    watchdog: snapshot?.infrastructure?.watchdog ?? null,
    hostMetrics: snapshot?.infrastructure?.host ?? null,
    activeAlerts: alerts.map(decodeAlert),
    lastHeartbeat: streamRow?.last_live_heartbeat_at ?? null,
    heartbeatState: resolveHeartbeatState(target, streamRow, now),
  });
});

monitoring.get("/streams/:streamId/alerts", async (ctx) => {
  const streamId = streamParam(ctx.req.param("streamId"));
  const status = ctx.req.query("status");
  if (status !== undefined && status !== "OPEN" && status !== "RESOLVED") {
    throw new ApiError(400, "INVALID_QUERY", "status must be OPEN or RESOLVED");
  }
  const limit = parseLimit(ctx.req.query("limit"));
  const cursor = decodeCursor(ctx.req.query("cursor"), (value) => {
    if (!value || typeof value !== "object") return null;
    const item = value as Record<string, unknown>;
    if (typeof item.firstTriggeredAt !== "string" || !ISO_UTC.test(item.firstTriggeredAt)) return null;
    if (typeof item.alertId !== "string" || !UUID.test(item.alertId)) return null;
    return { firstTriggeredAt: item.firstTriggeredAt, alertId: item.alertId };
  });
  const result = await ctx.env.DB.prepare(`
    SELECT * FROM monitoring_alerts
    WHERE stream_id = ? AND (? IS NULL OR status = ?)
      AND (? IS NULL OR first_triggered_at < ? OR (first_triggered_at = ? AND alert_id < ?))
    ORDER BY first_triggered_at DESC, alert_id DESC LIMIT ?
  `).bind(streamId, status ?? null, status ?? null, cursor?.firstTriggeredAt ?? null,
    cursor?.firstTriggeredAt ?? null, cursor?.firstTriggeredAt ?? null, cursor?.alertId ?? null, limit + 1).all<AlertRow>();
  const rows = result.results;
  const hasMore = rows.length > limit;
  const items = rows.slice(0, limit);
  const last = items.at(-1);
  return ctx.json({
    apiVersion: 1,
    streamId,
    alerts: items.map(decodeAlert),
    nextCursor: hasMore && last ? encodeCursor({ firstTriggeredAt: last.first_triggered_at, alertId: last.alert_id }) : null,
  });
});

export default monitoring;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
