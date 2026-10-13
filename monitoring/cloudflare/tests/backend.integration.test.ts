/// <reference types="vite/client" />
import { beforeEach, describe, expect, it, vi } from "vitest";
import { env as runtimeEnv } from "cloudflare:workers";
import { createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import worker from "../src/index";
import { validateSnapshot } from "../src/validation/snapshot";
import { deterministicStreamId } from "../src/security/stream-id";
import { heartbeatFreshGuard, heartbeatMissingGuard, openAlert, resolveAlert } from "../src/repositories/alerts";
import { cleanupRetainedData } from "../src/services/retention-service";
import { processNotificationOutbox } from "../src/services/notification-service";
import type { Env, MonitoringSnapshotV1 } from "../src/types";
import healthyFixture from "../fixtures/snapshot-healthy.json";
import partialFixture from "../fixtures/snapshot-partial.json";
import qualityFailFixture from "../fixtures/snapshot-quality-fail.json";
import contractMismatchFixture from "../fixtures/snapshot-contract-mismatch.json";
import staleFixture from "../fixtures/snapshot-stale.json";
import recoveredFixture from "../fixtures/snapshot-recovered.json";

const env = runtimeEnv as unknown as Env;
const primarySecret = "local-test-primary-secret-not-for-deployment";
const previousSecret = "local-test-previous-secret-not-for-deployment";
const readToken = "local-test-read-token-not-for-deployment";
const API = "https://monitoring.test";

function asArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const buffer = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(buffer).set(bytes);
  return buffer;
}

function base64url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/g, "");
}

async function signedHeaders(
  body: string,
  options: { secret?: string; keyId?: string; timestamp?: string } = {},
): Promise<HeadersInit> {
  const secret = options.secret || primarySecret;
  const keyId = options.keyId || "agent-key-v1";
  const timestamp = options.timestamp || new Date().toISOString();
  const bodyBytes = new TextEncoder().encode(body);
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", asArrayBuffer(bodyBytes)));
  const bodyHash = Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
  const canonical = `v1\n${keyId}\n${timestamp}\n${bodyHash}`;
  const key = await crypto.subtle.importKey("raw", asArrayBuffer(new TextEncoder().encode(secret)),
    { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", key, asArrayBuffer(new TextEncoder().encode(canonical))));
  return {
    "Content-Type": "application/json",
    "X-Monitor-Key-Id": keyId,
    "X-Monitor-Timestamp": timestamp,
    "X-Monitor-Signature": `v1=${base64url(signature)}`,
  };
}

async function invoke(request: Request, workerEnv: Env = env): Promise<Response> {
  const context = createExecutionContext();
  const response = await worker.fetch(request, workerEnv, context);
  await waitOnExecutionContext(context);
  return response;
}

async function postRaw(body: string, options: { secret?: string; keyId?: string; timestamp?: string; headers?: HeadersInit } = {}) {
  const signed = await signedHeaders(body, options);
  const headers = new Headers(signed);
  new Headers(options.headers).forEach((value, key) => headers.set(key, value));
  return invoke(new Request(`${API}/api/v1/ingest/snapshots`, { method: "POST", headers, body }));
}

function liveSnapshot(
  fixture: MonitoringSnapshotV1 = healthyFixture as MonitoringSnapshotV1,
  mutate?: (snapshot: MonitoringSnapshotV1) => void,
): MonitoringSnapshotV1 {
  const snapshot = structuredClone(fixture);
  const now = new Date().toISOString();
  snapshot.snapshotId = crypto.randomUUID();
  snapshot.timestamp = now;
  snapshot.source.environment = "local";
  snapshot.source.hostname = "phase2a-synthetic";
  snapshot.cohort.lastUpdateAt = now;
  snapshot.cohort.statusFileUpdatedAt = now;
  const latestReport = snapshot.infrastructure.watchdog.latestReport as (typeof snapshot.infrastructure.watchdog.latestReport & { checkedAt?: string }) | null;
  if (latestReport) latestReport.checkedAt = now;
  mutate?.(snapshot);
  return snapshot;
}

async function postSnapshot(snapshot: MonitoringSnapshotV1) {
  const body = JSON.stringify(snapshot);
  const response = await postRaw(body);
  return { response, body };
}

async function json(response: Response): Promise<Record<string, any>> {
  return response.json() as Promise<Record<string, any>>;
}

async function rows(sql: string, ...values: Array<string | number | null>) {
  const result = await env.DB.prepare(sql).bind(...values).all<Record<string, unknown>>();
  return result.results;
}

async function hashHex(value: string): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", asArrayBuffer(new TextEncoder().encode(value))));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function seedRecoveryRows(options: {
  count: number;
  kind: "accepted" | "quarantine";
  reason?: "SOURCE_NOT_ALLOWLISTED" | "COHORT_CONTRACT_MISMATCH" | "MODEL_CONTRACT_MISMATCH";
  sourceIdentity?: string;
  fixture?: MonitoringSnapshotV1;
  mutate?: (snapshot: MonitoringSnapshotV1, index: number) => void;
  ageSeconds?: number;
}) {
  const reason = options.reason ?? "COHORT_CONTRACT_MISMATCH";
  const sourceIdentity = options.sourceIdentity ?? "phase2a-synthetic";
  const statements: D1PreparedStatement[] = [];
  let streamId = "";
  for (let index = 0; index < options.count; index += 1) {
    const snapshot = liveSnapshot(options.fixture ?? healthyFixture as MonitoringSnapshotV1);
    const rowIdentity = options.kind === "quarantine" && reason === "SOURCE_NOT_ALLOWLISTED"
      ? `unapproved-source-${index}`
      : sourceIdentity;
    snapshot.source.hostname = rowIdentity;
    snapshot.timestamp = new Date(Date.now() - (options.ageSeconds ?? 60 * 60) * 1000 + index * 1000).toISOString();
    if (options.kind === "quarantine") {
      if (reason === "COHORT_CONTRACT_MISMATCH") snapshot.cohort.protocolId = "synthetic-contract-mismatch";
      if (reason === "MODEL_CONTRACT_MISMATCH") snapshot.cohort.modelSha256 = "0".repeat(64);
    }
    options.mutate?.(snapshot, index);
    streamId = await deterministicStreamId("local", snapshot.cohort.cohortId, rowIdentity);
    const body = JSON.stringify(snapshot);
    const receivedAt = new Date().toISOString();
    const quarantine = options.kind === "quarantine";
    const state = quarantine
      ? (reason === "SOURCE_NOT_ALLOWLISTED" ? "PROCESSED" : "PENDING")
      : "NOT_APPLICABLE";
    statements.push(env.DB.prepare(`
      INSERT INTO monitoring_snapshots (
        environment, snapshot_id, stream_id, cohort_id, run_id, source_identity,
        agent_version, schema_version, collected_at, received_at, collector_status,
        pipeline_status, quality_status, validation_status, quarantine_reason,
        stream_applied, payload_sha256, payload_json, quarantine_alert_state,
        quarantine_alert_processed_at, stream_recovery_state
      ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      "local", snapshot.snapshotId, streamId, snapshot.cohort.cohortId, snapshot.cohort.runId,
      rowIdentity, snapshot.source.agentVersion, new Date(Date.parse(snapshot.timestamp)).toISOString(),
      receivedAt, snapshot.collector.status, snapshot.pipeline.status, snapshot.quality.status,
      quarantine ? "QUARANTINED" : "ACCEPTED", quarantine ? reason : null,
      quarantine ? 1 : 0, await hashHex(body), body, state,
      quarantine && reason === "SOURCE_NOT_ALLOWLISTED" ? receivedAt : null,
      quarantine ? "RECOVERED" : "PENDING",
    ));
    if (statements.length === 20) {
      await env.DB.batch(statements.splice(0));
    }
  }
  if (statements.length) await env.DB.batch(statements);
  return streamId;
}

async function forceSnapshotRecoveryDue(snapshotId: string) {
  await env.DB.prepare(`
    UPDATE monitoring_snapshots
    SET stream_recovery_state = 'PENDING', stream_recovery_next_attempt_at = '2000-01-01T00:00:00.000Z',
      stream_recovery_locked_until = NULL
    WHERE snapshot_id = ?
  `).bind(snapshotId).run();
}

async function forceQuarantineRecoveryDue(snapshotId: string) {
  await env.DB.prepare(`
    UPDATE monitoring_snapshots
    SET quarantine_alert_state = 'PENDING', quarantine_alert_next_attempt_at = '2000-01-01T00:00:00.000Z',
      quarantine_alert_locked_until = NULL
    WHERE snapshot_id = ?
  `).bind(snapshotId).run();
}

function failNextBatchMatching(db: D1Database, matches: (sql: string) => boolean): D1Database {
  const failingStatements = new WeakSet<object>();
  let failed = false;
  return new Proxy(db, {
    get(target, property) {
      if (property === "prepare") {
        return (sql: string) => {
          const statement = target.prepare(sql);
          if (!matches(sql)) return statement;
          return {
            bind: (...values: Array<unknown>) => {
              const bound = Reflect.apply(statement.bind, statement, values) as D1PreparedStatement;
              failingStatements.add(bound as object);
              return bound;
            },
          } as D1PreparedStatement;
        };
      }
      if (property === "batch") {
        return (statements: D1PreparedStatement[]) => {
          if (!failed && statements.some((statement) => failingStatements.has(statement as object))) {
            failed = true;
            throw new Error("synthetic D1 batch failure");
          }
          return target.batch(statements);
        };
      }
      const value = Reflect.get(target, property, target) as unknown;
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

async function seedTarget(options: {
  enabled?: boolean;
  activatedAgoSeconds?: number | null;
  startupGraceSeconds?: number;
  heartbeatTimeoutSeconds?: number;
  expectedIntervalSeconds?: number;
  diskThresholdPercent?: number;
  maxPartialStreak?: number;
  sourceIdentity?: string;
} = {}) {
  const snapshot = healthyFixture as MonitoringSnapshotV1;
  const sourceIdentity = options.sourceIdentity ?? "phase2a-synthetic";
  const streamId = await deterministicStreamId("local", snapshot.cohort.cohortId, sourceIdentity);
  const activatedAt = options.activatedAgoSeconds == null
    ? null
    : new Date(Date.now() - options.activatedAgoSeconds * 1000).toISOString();
  await env.DB.prepare(`
    INSERT INTO monitoring_targets (
      stream_id, environment, cohort_id, run_id, source_identity,
      expected_interval_seconds, heartbeat_timeout_seconds, startup_grace_seconds,
      disk_threshold_percent, max_partial_streak, monitoring_enabled, activated_at
    ) VALUES (?, 'local', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    streamId,
    snapshot.cohort.cohortId,
    snapshot.cohort.runId,
    sourceIdentity,
    options.expectedIntervalSeconds ?? 300,
    options.heartbeatTimeoutSeconds ?? 900,
    options.startupGraceSeconds ?? 900,
    options.diskThresholdPercent ?? 85,
    options.maxPartialStreak ?? 3,
    options.enabled === false ? 0 : 1,
    activatedAt,
  ).run();
  return streamId;
}

async function getResponse(path: string, token = readToken) {
  return invoke(new Request(`${API}${path}`, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  }));
}

async function runCron(cron = "*/5 * * * *", workerEnv: Env = env) {
  await worker.scheduled({ cron, scheduledTime: Date.now(), noRetry: () => undefined }, workerEnv);
}

beforeEach(async () => {
  await env.DB.batch([
    env.DB.prepare("DELETE FROM monitoring_notification_outbox"),
    env.DB.prepare("DELETE FROM monitoring_alert_events"),
    env.DB.prepare("DELETE FROM monitoring_streams"),
    env.DB.prepare("DELETE FROM monitoring_targets"),
    env.DB.prepare("DELETE FROM monitoring_snapshots"),
    env.DB.prepare("DELETE FROM monitoring_alerts"),
  ]);
});

describe("Phase 1 Monitoring Snapshot V1 schema", () => {
  it("accepts all six schema-derived synthetic fixtures", () => {
    for (const fixture of [healthyFixture, partialFixture, qualityFailFixture, contractMismatchFixture, staleFixture, recoveredFixture]) {
      expect(validateSnapshot(fixture).valid, validateSnapshot(fixture).errors.join(", ")).toBe(true);
    }
  });

  it("rejects missing fields, unsupported versions, wrong types and unknown fields", () => {
    const base = healthyFixture as MonitoringSnapshotV1;
    const missing = structuredClone(base) as Record<string, unknown>;
    delete missing.progress;
    expect(validateSnapshot(missing).valid).toBe(false);

    expect(validateSnapshot({ ...base, schemaVersion: 2 }).valid).toBe(false);
    expect(validateSnapshot({ ...base, snapshotId: 7 }).valid).toBe(false);
    expect(validateSnapshot({ ...base, unexpected: "must be rejected" }).valid).toBe(false);
    expect(validateSnapshot({ ...base, snapshotId: "not-a-uuid" }).valid).toBe(false);
  });

  it("preserves zero separately from null and rejects non-UTC or future collection timestamps", () => {
    const base = structuredClone(healthyFixture) as MonitoringSnapshotV1;
    base.progress.forecastsPersisted = 0;
    expect(validateSnapshot(base).valid).toBe(true);
    base.progress.forecastsPersisted = null;
    expect(validateSnapshot(base).valid).toBe(true);
    base.timestamp = "2026-10-12T12:00:00+07:00";
    expect(validateSnapshot(base).valid).toBe(false);
    base.timestamp = new Date(Date.now() + 5 * 60_000).toISOString();
    expect(validateSnapshot(base).valid).toBe(false);
  });
});

describe("signed snapshot ingestion and admission", () => {
  it("verifies the documented Node.js HMAC test vector in the Workers runtime", async () => {
    const vectorTimestamp = "2026-10-12T12:00:00.000Z";
    const vectorSecret = "phase2a-hmac-vector-only-secret-32-bytes";
    const body = "{}";
    const request = new Request(`${API}/api/v1/ingest/snapshots`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Monitor-Key-Id": "test-key-v1",
        "X-Monitor-Timestamp": vectorTimestamp,
        "X-Monitor-Signature": "v1=uFP9QjCLHx1j3qP60obYN009Arba44UqJOrGVWK3Quc",
      },
      body,
    });
    vi.useFakeTimers();
    vi.setSystemTime(new Date(vectorTimestamp));
    try {
      const response = await invoke(request, {
        ...env,
        MONITOR_HMAC_SECRET_PRIMARY: vectorSecret,
        MONITOR_HMAC_KEY_ID_PRIMARY: "test-key-v1",
      });
      expect(response.status).toBe(400);
      expect((await json(response)).error.code).toBe("INVALID_SNAPSHOT_SCHEMA");
    } finally {
      vi.useRealTimers();
    }
  });

  it("accepts a current valid snapshot and returns the required response shape", async () => {
    const snapshot = liveSnapshot();
    const { response } = await postSnapshot(snapshot);
    expect(response.status).toBe(201);
    const body = await json(response);
    expect(body).toMatchObject({ status: "ACCEPTED", snapshotId: snapshot.snapshotId, schemaVersion: 1 });
    expect(typeof body.receivedAt).toBe("string");
  });

  it("rejects missing authentication, invalid HMAC, invalid key IDs, expired and future requests", async () => {
    const snapshot = liveSnapshot();
    const raw = JSON.stringify(snapshot);
    const missing = await invoke(new Request(`${API}/api/v1/ingest/snapshots`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: raw,
    }));
    expect(missing.status).toBe(401);

    const validHeaders = new Headers(await signedHeaders(raw));
    validHeaders.set("X-Monitor-Signature", "v1=invalid");
    expect((await invoke(new Request(`${API}/api/v1/ingest/snapshots`, { method: "POST", headers: validHeaders, body: raw }))).status).toBe(401);

    const unknownKeyHeaders = await signedHeaders(raw, { keyId: "unknown-key" });
    expect((await invoke(new Request(`${API}/api/v1/ingest/snapshots`, { method: "POST", headers: unknownKeyHeaders, body: raw }))).status).toBe(401);

    const oldHeaders = await signedHeaders(raw, { timestamp: new Date(Date.now() - 10 * 60_000).toISOString() });
    expect((await invoke(new Request(`${API}/api/v1/ingest/snapshots`, { method: "POST", headers: oldHeaders, body: raw }))).status).toBe(401);

    const futureHeaders = await signedHeaders(raw, { timestamp: new Date(Date.now() + 10 * 60_000).toISOString() });
    expect((await invoke(new Request(`${API}/api/v1/ingest/snapshots`, { method: "POST", headers: futureHeaders, body: raw }))).status).toBe(401);
  });

  it("detects body tampering and accepts the configured previous HMAC key during rotation", async () => {
    const snapshot = liveSnapshot();
    const original = JSON.stringify(snapshot);
    const headers = await signedHeaders(original);
    const tampered = original.replace("RUNNING", "STOPPED");
    const tamperResponse = await invoke(new Request(`${API}/api/v1/ingest/snapshots`, { method: "POST", headers, body: tampered }));
    expect(tamperResponse.status).toBe(401);

    const previous = await postRaw(JSON.stringify(liveSnapshot()), { secret: previousSecret, keyId: "agent-key-v0" });
    expect(previous.status).toBe(201);
  });

  it("enforces media type, payload size, JSON syntax and the complete JSON Schema", async () => {
    const noMediaType = await invoke(new Request(`${API}/api/v1/ingest/snapshots`, { method: "POST", body: "{}" }));
    expect(noMediaType.status).toBe(415);

    const oversized = await postRaw(`{"large":"${"x".repeat(33 * 1024)}"}`);
    expect(oversized.status).toBe(413);

    const malformed = await postRaw("{bad json}");
    expect(malformed.status).toBe(400);
    expect((await json(malformed)).error.code).toBe("INVALID_JSON");

    const invalidSchema = await postRaw(JSON.stringify({ ...(healthyFixture as object), unexpected: "field" }));
    expect(invalidSchema.status).toBe(400);
    expect((await json(invalidSchema)).error.code).toBe("INVALID_SNAPSHOT_SCHEMA");
  });

  it("is idempotent without refreshing state and returns 409 for a reused ID with different bytes", async () => {
    const snapshot = liveSnapshot();
    const body = JSON.stringify(snapshot);
    const first = await postRaw(body);
    expect(first.status).toBe(201);
    const before = await env.DB.prepare(`SELECT received_at FROM monitoring_snapshots WHERE snapshot_id = ?`)
      .bind(snapshot.snapshotId).first<{ received_at: string }>();
    const duplicate = await postRaw(body);
    expect(duplicate.status).toBe(200);
    expect((await json(duplicate)).status).toBe("DUPLICATE");
    const after = await env.DB.prepare(`SELECT received_at FROM monitoring_snapshots WHERE snapshot_id = ?`)
      .bind(snapshot.snapshotId).first<{ received_at: string }>();
    expect(after?.received_at).toBe(before?.received_at);

    const changed = structuredClone(snapshot);
    changed.quality.status = "UNKNOWN";
    expect((await postSnapshot(changed)).response.status).toBe(409);
    expect((await rows("SELECT snapshot_id FROM monitoring_snapshots")).length).toBe(1);
  });

  it("quarantines cohort, model and source allowlist mismatches without changing healthy current state", async () => {
    const healthy = liveSnapshot();
    expect((await postSnapshot(healthy)).response.status).toBe(201);
    const streamId = await deterministicStreamId("local", healthy.cohort.cohortId, healthy.source.hostname);

    const cohortMismatch = liveSnapshot(contractMismatchFixture as MonitoringSnapshotV1);
    cohortMismatch.cohort.cohortId = "foreign-cohort";
    const cohortResponse = await postSnapshot(cohortMismatch);
    expect(cohortResponse.response.status).toBe(202);
    expect((await json(cohortResponse.response)).reason).toBe("COHORT_CONTRACT_MISMATCH");

    const modelMismatch = liveSnapshot();
    modelMismatch.cohort.modelSha256 = "a".repeat(64);
    modelMismatch.inference.modelSha256 = "a".repeat(64);
    const modelResponse = await postSnapshot(modelMismatch);
    expect(modelResponse.response.status).toBe(202);
    expect((await json(modelResponse.response)).reason).toBe("MODEL_CONTRACT_MISMATCH");

    const sourceMismatch = liveSnapshot();
    sourceMismatch.source.hostname = "not-allowlisted";
    const sourceResponse = await postSnapshot(sourceMismatch);
    expect(sourceResponse.response.status).toBe(202);
    expect((await json(sourceResponse.response)).reason).toBe("SOURCE_NOT_ALLOWLISTED");

    const latest = await getResponse(`/api/v1/monitoring/streams/${streamId}/latest`);
    expect((await json(latest)).snapshot.snapshotId).toBe(healthy.snapshotId);
    expect((await rows("SELECT validation_status FROM monitoring_snapshots WHERE validation_status = 'QUARANTINED'")).length).toBe(3);
    expect((await rows("SELECT alert_type FROM monitoring_alerts WHERE status = 'OPEN'")).map((row) => row.alert_type))
      .toEqual(expect.arrayContaining(["COHORT_CONTRACT_MISMATCH", "MODEL_CONTRACT_MISMATCH"]));
  });

  it("keeps the newest collected snapshot when an older valid snapshot arrives later", async () => {
    const newest = liveSnapshot();
    newest.timestamp = new Date(Date.now() - 1_000).toISOString();
    expect((await postSnapshot(newest)).response.status).toBe(201);
    const older = liveSnapshot();
    older.timestamp = new Date(Date.now() - 60 * 60_000).toISOString();
    expect((await postSnapshot(older)).response.status).toBe(201);

    const streamId = await deterministicStreamId("local", newest.cohort.cohortId, newest.source.hostname);
    const latest = await getResponse(`/api/v1/monitoring/streams/${streamId}/latest`);
    expect((await json(latest)).snapshot.snapshotId).toBe(newest.snapshotId);
  });

  it("fails closed when required authentication secrets are missing", async () => {
    const snapshot = liveSnapshot();
    const raw = JSON.stringify(snapshot);
    const headers = await signedHeaders(raw);
    const routeContext = createExecutionContext();
    const response = await worker.fetch(new Request(`${API}/api/v1/ingest/snapshots`, { method: "POST", headers, body: raw }),
      { ...env, MONITOR_HMAC_SECRET_PRIMARY: undefined } as Env, routeContext);
    expect(response.status).toBe(503);
    expect((await json(response)).error.code).toBe("AUTHENTICATION_NOT_CONFIGURED");
  });
});

describe("current stream APIs", () => {
  it("requires read authentication and returns empty instead of fabricated snapshots", async () => {
    const unauthorized = await getResponse("/api/v1/monitoring/streams", "");
    expect(unauthorized.status).toBe(401);
    const streamId = "f".repeat(64);
    const empty = await getResponse(`/api/v1/monitoring/streams/${streamId}/latest`);
    expect(await json(empty)).toMatchObject({ apiVersion: 1, status: "EMPTY", snapshot: null });
  });

  it("provides bounded cursor history, a versioned summary and stream alert pages", async () => {
    const targetId = await seedTarget({ enabled: true });
    const snapshots = [0, 1, 2].map(() => liveSnapshot());
    for (const snapshot of snapshots) expect((await postSnapshot(snapshot)).response.status).toBe(201);

    const firstPage = await getResponse(`/api/v1/monitoring/streams/${targetId}/history?limit=2`);
    const first = await json(firstPage);
    expect(first.snapshots).toHaveLength(2);
    expect(typeof first.nextCursor).toBe("string");
    const secondPage = await getResponse(`/api/v1/monitoring/streams/${targetId}/history?limit=2&cursor=${encodeURIComponent(first.nextCursor)}`);
    const second = await json(secondPage);
    expect(second.snapshots).toHaveLength(1);
    expect(new Set([...first.snapshots, ...second.snapshots].map((item: any) => item.snapshotId)).size).toBe(3);

    expect((await getResponse(`/api/v1/monitoring/streams/${targetId}/history?limit=201`)).status).toBe(400);
    expect((await getResponse(`/api/v1/monitoring/streams/${targetId}/history?from=2026-10-01T00:00:00Z`)).status).toBe(400);
    expect((await getResponse(`/api/v1/monitoring/streams/${targetId}/history?cursor=bad%20cursor`)).status).toBe(400);

    const summary = await json(await getResponse(`/api/v1/monitoring/streams/${targetId}/summary`));
    expect(summary).toMatchObject({ apiVersion: 1, streamId: targetId, validEvaluations: null, dataQuality: { status: "PASS" } });
    const alerts = await json(await getResponse(`/api/v1/monitoring/streams/${targetId}/alerts?status=OPEN`));
    expect(alerts.alerts).toEqual([]);
    expect((await getResponse(`/api/v1/monitoring/streams/${targetId}/alerts?status=OPEN%20OR%201%3D1`)).status).toBe(400);
  });

  it("rejects SQL-shaped stream IDs and unbounded pagination without leaking credentials", async () => {
    const injection = await getResponse("/api/v1/monitoring/streams/%27%20OR%201%3D1/history");
    expect(injection.status).toBe(404);
    const streamId = await deterministicStreamId("local", healthyFixture.cohort.cohortId, "phase2a-synthetic");
    const excessiveLimit = await getResponse(`/api/v1/monitoring/streams/${streamId}/history?limit=1000000`);
    expect(excessiveLimit.status).toBe(400);
    const errorBody = await excessiveLimit.text();
    expect(errorBody).not.toContain(primarySecret);
    expect(errorBody).not.toContain(readToken);
  });

  it("returns a generic error when D1 is unavailable", async () => {
    const snapshot = liveSnapshot();
    const body = JSON.stringify(snapshot);
    const headers = await signedHeaders(body);
    const failedDb = { prepare: () => { throw new Error("sensitive local database diagnostic"); } } as unknown as D1Database;
    const response = await invoke(new Request(`${API}/api/v1/ingest/snapshots`, {
      method: "POST",
      headers,
      body,
    }), { ...env, DB: failedDb });
    expect(response.status).toBe(500);
    const textBody = await response.text();
    expect(textBody).toContain("INTERNAL_ERROR");
    expect(textBody).not.toContain("sensitive local database diagnostic");
    expect(textBody).not.toContain(primarySecret);
  });
});

describe("heartbeat and alert lifecycle", () => {
  it("scans heartbeat on every tick while repairing one interrupted snapshot per tick", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 60 * 60 });
    const first = liveSnapshot();
    const second = liveSnapshot();
    first.timestamp = new Date(Date.now() - 61 * 60_000).toISOString();
    second.timestamp = new Date(Date.now() - 60 * 60_000).toISOString();
    const firstPost = await postSnapshot(first);
    const secondPost = await postSnapshot(second);
    expect(firstPost.response.status).toBe(201);
    expect(secondPost.response.status).toBe(201);
    await env.DB.prepare(`UPDATE monitoring_snapshots
      SET stream_applied = 0, stream_recovery_state = 'PENDING', stream_recovery_next_attempt_at = NULL
      WHERE snapshot_id IN (?, ?)`)
      .bind(first.snapshotId, second.snapshotId).run();
    await env.DB.prepare("DELETE FROM monitoring_streams WHERE stream_id = ?").bind(streamId).run();

    await runCron();
    expect((await rows("SELECT snapshot_id FROM monitoring_snapshots WHERE stream_applied = 0")).length).toBe(1);
    expect((await rows("SELECT latest_snapshot_id FROM monitoring_streams WHERE stream_id = ?", streamId)).length).toBe(1);
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'", streamId)).length).toBe(1);
    await runCron();
    expect((await rows("SELECT snapshot_id FROM monitoring_snapshots WHERE stream_applied = 0")).length).toBe(0);
    expect((await rows("SELECT latest_snapshot_id FROM monitoring_streams WHERE stream_id = ?", streamId))[0].latest_snapshot_id).toBe(second.snapshotId);
    expect((await rows(`SELECT e.event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.alert_type = 'AGENT_HEARTBEAT_MISSING' AND e.event_type = 'OPENED'`)).length).toBe(1);
  });

  it.each([1, 20, 100])("checks missing heartbeat despite %i pending snapshot recoveries", async (count) => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 60 * 60 });
    await seedRecoveryRows({ count, kind: "accepted" });

    await runCron();

    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'`, streamId))).toHaveLength(1);
    expect((await rows("SELECT snapshot_id FROM monitoring_snapshots WHERE validation_status = 'ACCEPTED' AND stream_applied = 0")))
      .toHaveLength(count - 1);
  });

  it("continues checking heartbeat as new recovery work arrives each tick", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 60 * 60 });
    for (let index = 0; index < 3; index += 1) {
      await seedRecoveryRows({ count: 1, kind: "accepted" });
      await runCron();
      expect((await rows(`SELECT alert_id FROM monitoring_alerts
        WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'`, streamId))).toHaveLength(1);
    }
    expect((await rows(`SELECT event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.alert_type = 'AGENT_HEARTBEAT_MISSING' AND e.event_type = 'OPENED'`))).toHaveLength(1);
  });

  it("records a recovery error but still performs heartbeat evaluation", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 60 * 60 });
    await seedRecoveryRows({ count: 1, kind: "accepted" });
    await env.DB.prepare("UPDATE monitoring_snapshots SET payload_json = '{' WHERE validation_status = 'ACCEPTED'").run();
    const errors = vi.spyOn(console, "error").mockImplementation(() => undefined);

    await expect(runCron()).rejects.toThrow("SCHEDULED_TASK_FAILED");

    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'`, streamId))).toHaveLength(1);
    expect((await rows(`SELECT stream_recovery_attempts FROM monitoring_snapshots
      WHERE validation_status = 'ACCEPTED'`))[0].stream_recovery_attempts).toBe(1);
    expect(errors.mock.calls.some(([line]) => String(line).includes("SNAPSHOT_RECOVERY_FAILED"))).toBe(true);
    errors.mockRestore();
  });

  it("caps malformed snapshot recovery attempts and keeps terminal evidence visible", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 60 * 60 });
    await seedRecoveryRows({ count: 1, kind: "accepted" });
    const pending = await env.DB.prepare("SELECT snapshot_id FROM monitoring_snapshots WHERE validation_status = 'ACCEPTED'")
      .first<{ snapshot_id: string }>();
    await env.DB.prepare("UPDATE monitoring_snapshots SET payload_json = '{' WHERE snapshot_id = ?")
      .bind(pending!.snapshot_id).run();

    for (let attempt = 1; attempt <= 5; attempt += 1) {
      await expect(runCron()).rejects.toThrow("SCHEDULED_TASK_FAILED");
      if (attempt < 5) await forceSnapshotRecoveryDue(pending!.snapshot_id);
    }

    expect((await rows(`SELECT stream_applied, stream_recovery_state, stream_recovery_attempts
      FROM monitoring_snapshots WHERE snapshot_id = ?`, pending!.snapshot_id))[0])
      .toMatchObject({ stream_applied: 0, stream_recovery_state: "FAILED", stream_recovery_attempts: 5 });
    await expect(runCron()).rejects.toThrow("SCHEDULED_TASK_FAILED");
    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'`, streamId))).toHaveLength(1);
    expect((await rows("SELECT stream_recovery_attempts FROM monitoring_snapshots WHERE snapshot_id = ?", pending!.snapshot_id))[0]
      .stream_recovery_attempts).toBe(5);
  });

  it("replays alert rules after an interrupted current-state update and preserves snapshot ordering", async () => {
    const snapshot = liveSnapshot(qualityFailFixture as MonitoringSnapshotV1);
    const streamId = await seedTarget({ enabled: true, sourceIdentity: snapshot.source.hostname });
    await seedRecoveryRows({ count: 1, kind: "accepted", fixture: qualityFailFixture as MonitoringSnapshotV1, ageSeconds: 0 });
    const pending = await env.DB.prepare("SELECT snapshot_id FROM monitoring_snapshots WHERE validation_status = 'ACCEPTED'")
      .first<{ snapshot_id: string }>();
    expect(pending).not.toBeNull();
    const failingDb = failNextBatchMatching(env.DB, (sql) => sql.includes("INSERT OR IGNORE INTO monitoring_alerts") &&
      sql.includes("SELECT ?"));
    await expect(runCron("*/5 * * * *", { ...env, DB: failingDb })).rejects.toThrow("SCHEDULED_TASK_FAILED");
    expect((await rows("SELECT latest_snapshot_id FROM monitoring_streams WHERE stream_id = ?", streamId))[0].latest_snapshot_id)
      .toBe(pending!.snapshot_id);
    expect((await rows("SELECT stream_applied FROM monitoring_snapshots WHERE snapshot_id = ?", pending!.snapshot_id))[0].stream_applied).toBe(0);

    await forceSnapshotRecoveryDue(pending!.snapshot_id);
    await runCron();

    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'PIPELINE_DATA_QUALITY_FAILED' AND status = 'OPEN'`, streamId))).toHaveLength(1);
    expect((await rows("SELECT stream_applied, stream_recovery_state FROM monitoring_snapshots WHERE snapshot_id = ?", pending!.snapshot_id))[0])
      .toMatchObject({ stream_applied: 1, stream_recovery_state: "RECOVERED" });
    expect((await rows("SELECT latest_snapshot_id FROM monitoring_streams WHERE stream_id = ?", streamId))[0].latest_snapshot_id)
      .toBe(pending!.snapshot_id);
  });

  it("fails closed when enabled targets exceed the configured scan budget", async () => {
    const firstStream = await seedTarget();
    const sourceIdentity = "phase2a-synthetic-second";
    const secondStream = await deterministicStreamId("local", healthyFixture.cohort.cohortId, sourceIdentity);
    await env.DB.prepare(`
      INSERT INTO monitoring_targets (stream_id, environment, cohort_id, run_id, source_identity, monitoring_enabled)
      VALUES (?, 'local', ?, ?, ?, 1)
    `).bind(secondStream, healthyFixture.cohort.cohortId, healthyFixture.cohort.runId, sourceIdentity).run();
    expect(firstStream).not.toBe(secondStream);

    await expect(runCron("*/5 * * * *", { ...env, MONITOR_TARGET_SCAN_LIMIT: "1" }))
      .rejects.toThrow("SCHEDULED_TASK_FAILED");
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE alert_type = 'AGENT_HEARTBEAT_MISSING'")).length).toBe(0);
  });

  it("does not alert disabled targets or targets still inside startup grace", async () => {
    const disabledId = await seedTarget({ enabled: false, activatedAgoSeconds: 4_000, sourceIdentity: "phase2a-synthetic-disabled" });
    const graceId = await seedTarget({ enabled: true, activatedAgoSeconds: 8 * 60, startupGraceSeconds: 15 * 60, sourceIdentity: "phase2a-synthetic-grace" });
    const missingActivationId = await seedTarget({ enabled: true, sourceIdentity: "phase2a-synthetic-no-activation" });
    await runCron();
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE status = 'OPEN'")).length).toBe(0);
    const streams = await json(await getResponse("/api/v1/monitoring/streams"));
    expect(streams.streams).toEqual(expect.arrayContaining([
      expect.objectContaining({ streamId: disabledId, heartbeatState: "DISABLED" }),
      expect.objectContaining({ streamId: graceId, heartbeatState: "PENDING" }),
      expect.objectContaining({ streamId: missingActivationId, heartbeatState: "UNKNOWN" }),
    ]));
    expect((await json(await getResponse(`/api/v1/monitoring/streams/${disabledId}/summary`))).heartbeatState).toBe("DISABLED");
    expect((await json(await getResponse(`/api/v1/monitoring/streams/${graceId}/summary`))).heartbeatState).toBe("PENDING");
    expect((await json(await getResponse(`/api/v1/monitoring/streams/${missingActivationId}/summary`))).heartbeatState).toBe("UNKNOWN");
  });

  it("uses the same heartbeat classification in Cron, stream list and summary", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 60 * 60 });
    const list = await json(await getResponse("/api/v1/monitoring/streams"));
    const listed = (list.streams as Array<Record<string, unknown>>).find((item) => item.streamId === streamId);
    const summary = await json(await getResponse(`/api/v1/monitoring/streams/${streamId}/summary`));
    expect(listed?.heartbeatState).toBe("OFFLINE");
    expect(summary.heartbeatState).toBe("OFFLINE");

    const logs: string[] = [];
    const log = vi.spyOn(console, "log").mockImplementation((value) => { logs.push(String(value)); });
    await runCron();
    const cronSummary = logs.map((value) => { try { return JSON.parse(value); } catch { return null; } })
      .find((value) => value?.code === "SCHEDULED_TASK_SUMMARY");
    expect(cronSummary).toMatchObject({ heartbeatTargetsChecked: 1, offlineTargets: 1 });
    log.mockRestore();

    const fresh = liveSnapshot();
    expect((await postSnapshot(fresh)).response.status).toBe(201);
    const healthyList = await json(await getResponse("/api/v1/monitoring/streams"));
    const healthyListed = (healthyList.streams as Array<Record<string, unknown>>).find((item) => item.streamId === streamId);
    const healthySummary = await json(await getResponse(`/api/v1/monitoring/streams/${streamId}/summary`));
    expect(healthyListed?.heartbeatState).toBe("HEALTHY");
    expect(healthySummary.heartbeatState).toBe("HEALTHY");

    const healthyLogs: string[] = [];
    const healthyLog = vi.spyOn(console, "log").mockImplementation((value) => { healthyLogs.push(String(value)); });
    await runCron();
    const healthyCron = healthyLogs.map((value) => { try { return JSON.parse(value); } catch { return null; } })
      .find((value) => value?.code === "SCHEDULED_TASK_SUMMARY");
    expect(healthyCron).toMatchObject({ heartbeatTargetsChecked: 1, offlineTargets: 0 });
    expect((await rows(`SELECT status FROM monitoring_alerts WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING'`, streamId))[0].status)
      .toBe("RESOLVED");
    healthyLog.mockRestore();
  });

  it("resolves an existing heartbeat alert for a valid five-minute-old heartbeat through the D1 fresh SQL guard", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 60 * 60 });
    await runCron();
    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'`, streamId))).toHaveLength(1);

    const collectedAt = new Date(Date.now() - 5 * 60_000).toISOString();
    const snapshot = liveSnapshot(undefined, (value) => {
      value.timestamp = collectedAt;
      value.cohort.lastUpdateAt = collectedAt;
      value.cohort.statusFileUpdatedAt = collectedAt;
    });
    expect((await postSnapshot(snapshot)).response.status).toBe(201);
    expect((await json(await getResponse(`/api/v1/monitoring/streams/${streamId}/summary`))).heartbeatState).toBe("HEALTHY");

    const alert = await env.DB.prepare(`SELECT status FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING'`).bind(streamId).first<{ status: string }>();
    expect(alert?.status).toBe("RESOLVED");
    expect((await rows(`SELECT event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND a.alert_type = 'AGENT_HEARTBEAT_MISSING' AND e.event_type = 'RESOLVED'`, streamId))
    ).toHaveLength(1);

    await runCron();
    expect((await rows(`SELECT event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND a.alert_type = 'AGENT_HEARTBEAT_MISSING' AND e.event_type = 'RESOLVED'`, streamId))
    ).toHaveLength(1);
  });

  it("transitions a healthy heartbeat to offline at 20 minutes and resolves one alert exactly once", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 60 * 60 });
    const first = liveSnapshot(undefined, (value) => {
      const collectedAt = new Date(Date.now() - 60_000).toISOString();
      value.timestamp = collectedAt;
      value.cohort.lastUpdateAt = collectedAt;
      value.cohort.statusFileUpdatedAt = collectedAt;
    });
    expect((await postSnapshot(first)).response.status).toBe(201);
    await runCron();
    expect((await json(await getResponse(`/api/v1/monitoring/streams/${streamId}/summary`))).heartbeatState).toBe("HEALTHY");
    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING'`, streamId))).toHaveLength(0);

    const expiredAt = new Date(Date.now() - 20 * 60_000).toISOString();
    await env.DB.prepare(`UPDATE monitoring_streams
      SET last_live_heartbeat_at = ?, last_live_received_at = ? WHERE stream_id = ?`)
      .bind(expiredAt, expiredAt, streamId).run();
    expect((await json(await getResponse(`/api/v1/monitoring/streams/${streamId}/summary`))).heartbeatState).toBe("OFFLINE");

    await runCron();
    await runCron();
    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'`, streamId))).toHaveLength(1);
    expect((await rows(`SELECT e.event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND a.alert_type = 'AGENT_HEARTBEAT_MISSING' AND e.event_type = 'OPENED'`, streamId))
    ).toHaveLength(1);

    const recovered = liveSnapshot();
    expect((await postSnapshot(recovered)).response.status).toBe(201);
    let alert = await env.DB.prepare(`SELECT status FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING'`).bind(streamId).first<{ status: string }>();
    expect(alert?.status).toBe("RESOLVED");
    expect((await rows(`SELECT e.event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND a.alert_type = 'AGENT_HEARTBEAT_MISSING' AND e.event_type = 'RESOLVED'`, streamId))
    ).toHaveLength(1);

    await runCron();
    alert = await env.DB.prepare(`SELECT status FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING'`).bind(streamId).first<{ status: string }>();
    expect(alert?.status).toBe("RESOLVED");
    expect((await rows(`SELECT e.event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND a.alert_type = 'AGENT_HEARTBEAT_MISSING' AND e.event_type = 'RESOLVED'`, streamId))
    ).toHaveLength(1);
  });

  it("does not let either heartbeat SQL guard classify incomplete evidence as offline or healthy", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 60 * 60 });
    const snapshot = liveSnapshot();
    expect((await postSnapshot(snapshot)).response.status).toBe(201);
    const expiredAt = new Date(Date.now() - 20 * 60_000).toISOString();
    await env.DB.prepare(`UPDATE monitoring_streams
      SET last_live_heartbeat_at = ?, last_live_received_at = NULL WHERE stream_id = ?`)
      .bind(expiredAt, streamId).run();
    expect((await json(await getResponse(`/api/v1/monitoring/streams/${streamId}/summary`))).heartbeatState).toBe("UNKNOWN");

    const deniedOpen = await openAlert(env.DB, {
      streamId,
      alertType: "AGENT_HEARTBEAT_MISSING",
      severity: "CRITICAL",
      details: { heartbeatState: "OFFLINE" },
      telegramEnabled: false,
      guard: heartbeatMissingGuard(streamId),
    });
    expect(deniedOpen).toBeNull();

    const open = await openAlert(env.DB, {
      streamId,
      alertType: "AGENT_HEARTBEAT_MISSING",
      severity: "CRITICAL",
      details: { seededForGuardTest: true },
      telegramEnabled: false,
    });
    expect(open).not.toBeNull();
    const resolved = await resolveAlert(env.DB, {
      streamId,
      alertType: "AGENT_HEARTBEAT_MISSING",
      details: { heartbeatState: "HEALTHY" },
      telegramEnabled: false,
      guard: heartbeatFreshGuard(streamId),
    });
    expect(resolved).toBe(false);
    expect((await rows(`SELECT status FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING'`, streamId))[0].status).toBe("OPEN");
  });

  it("preserves the 60-second future skew and resolves correctly during concurrent Cron and ingestion", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 60 * 60 });
    const first = liveSnapshot(undefined, (value) => {
      const collectedAt = new Date(Date.now() - 60_000).toISOString();
      value.timestamp = collectedAt;
      value.cohort.lastUpdateAt = collectedAt;
      value.cohort.statusFileUpdatedAt = collectedAt;
    });
    expect((await postSnapshot(first)).response.status).toBe(201);
    const expiredAt = new Date(Date.now() - 20 * 60_000).toISOString();
    await env.DB.prepare(`UPDATE monitoring_streams
      SET last_live_heartbeat_at = ?, last_live_received_at = ? WHERE stream_id = ?`)
      .bind(expiredAt, expiredAt, streamId).run();
    await runCron();
    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'`, streamId))).toHaveLength(1);

    const futureAt = new Date(Date.now() + 30_000).toISOString();
    const fresh = liveSnapshot(undefined, (value) => {
      value.timestamp = futureAt;
      value.cohort.lastUpdateAt = futureAt;
      value.cohort.statusFileUpdatedAt = futureAt;
    });
    const [_, ingestion] = await Promise.all([runCron(), postSnapshot(fresh)]);
    expect(ingestion.response.status).toBe(201);
    expect((await json(await getResponse(`/api/v1/monitoring/streams/${streamId}/summary`))).heartbeatState).toBe("HEALTHY");
    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'`, streamId))).toHaveLength(0);
    expect((await rows(`SELECT status FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING'`, streamId))[0].status).toBe("RESOLVED");
    expect((await rows(`SELECT e.event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND a.alert_type = 'AGENT_HEARTBEAT_MISSING' AND e.event_type = 'OPENED'`, streamId))
    ).toHaveLength(1);
    expect((await rows(`SELECT e.event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND a.alert_type = 'AGENT_HEARTBEAT_MISSING' AND e.event_type = 'RESOLVED'`, streamId))
    ).toHaveLength(1);
  });

  it.each(["PARTIAL", "FAILED"] as const)("treats a fresh %s collector result as an agent heartbeat", async (collectorStatus) => {
    const streamId = await seedTarget({ enabled: true });
    const snapshot = liveSnapshot(undefined, (value) => { value.collector.status = collectorStatus; });
    expect((await postSnapshot(snapshot)).response.status).toBe(201);
    const summary = await json(await getResponse(`/api/v1/monitoring/streams/${streamId}/summary`));
    expect(summary.heartbeatState).toBe("HEALTHY");
    expect(summary.collectorCompleteness.status).toBe(collectorStatus);
    await runCron();
    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'`, streamId))).toHaveLength(0);
  });

  it("opens missing-heartbeat after grace and resolves it only on a fresh accepted snapshot", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 16 * 60 });
    await runCron();
    expect((await rows("SELECT alert_type FROM monitoring_alerts WHERE status = 'OPEN'")).map((row) => row.alert_type))
      .toContain("AGENT_HEARTBEAT_MISSING");

    const snapshot = liveSnapshot();
    expect((await postSnapshot(snapshot)).response.status).toBe(201);
    const alert = await env.DB.prepare(`SELECT status, resolved_at FROM monitoring_alerts WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING'`)
      .bind(streamId).first<{ status: string; resolved_at: string | null }>();
    expect(alert?.status).toBe("RESOLVED");
    expect(alert?.resolved_at).not.toBeNull();
  });

  it("does not let duplicate or historical snapshot retries restore heartbeat health", async () => {
    const streamId = await seedTarget({ enabled: true, activatedAgoSeconds: 2_000 });
    const old = liveSnapshot(staleFixture as MonitoringSnapshotV1);
    old.timestamp = new Date(Date.now() - 60 * 60_000).toISOString();
    expect((await postSnapshot(old)).response.status).toBe(201);
    const originalHeartbeat = await env.DB.prepare("SELECT last_live_heartbeat_at FROM monitoring_streams WHERE stream_id = ?")
      .bind(streamId).first<{ last_live_heartbeat_at: string | null }>();
    expect(originalHeartbeat?.last_live_heartbeat_at).toBeNull();
    await runCron();
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE stream_id = ? AND alert_type = 'AGENT_HEARTBEAT_MISSING' AND status = 'OPEN'", streamId)).length).toBe(1);

    const duplicate = await postRaw(JSON.stringify(old));
    expect(duplicate.status).toBe(200);
    const afterDuplicate = await env.DB.prepare("SELECT last_live_heartbeat_at FROM monitoring_streams WHERE stream_id = ?")
      .bind(streamId).first<{ last_live_heartbeat_at: string | null }>();
    expect(afterDuplicate?.last_live_heartbeat_at).toBeNull();
  });

  it("alerts after sustained collector incompleteness and resolves only after a complete fresh snapshot", async () => {
    const streamId = await seedTarget({ enabled: true, maxPartialStreak: 3 });
    for (let index = 0; index < 3; index += 1) {
      const partial = liveSnapshot(partialFixture as MonitoringSnapshotV1);
      partial.timestamp = new Date(Date.now() - (2 - index) * 1_000).toISOString();
      expect((await postSnapshot(partial)).response.status).toBe(201);
    }
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE stream_id = ? AND alert_type = 'COLLECTOR_PARTIAL' AND status = 'OPEN'", streamId)).length).toBe(1);
    const recovered = liveSnapshot(recoveredFixture as MonitoringSnapshotV1);
    recovered.timestamp = new Date(Date.now() + 100).toISOString();
    // The small future skew is removed so the fixture remains a valid current collection.
    recovered.timestamp = new Date().toISOString();
    expect((await postSnapshot(recovered)).response.status).toBe(201);
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE stream_id = ? AND alert_type = 'COLLECTOR_PARTIAL' AND status = 'OPEN'", streamId)).length).toBe(0);
  });

  it("opens and recovers data-quality alerts while UNKNOWN evidence leaves an alert sticky", async () => {
    const streamId = await seedTarget({ enabled: true });
    const failed = liveSnapshot(qualityFailFixture as MonitoringSnapshotV1);
    expect((await postSnapshot(failed)).response.status).toBe(201);
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE stream_id = ? AND alert_type = 'PIPELINE_DATA_QUALITY_FAILED' AND status = 'OPEN'", streamId)).length).toBe(1);

    const unknown = liveSnapshot();
    unknown.quality.status = "UNKNOWN";
    expect((await postSnapshot(unknown)).response.status).toBe(201);
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE stream_id = ? AND alert_type = 'PIPELINE_DATA_QUALITY_FAILED' AND status = 'OPEN'", streamId)).length).toBe(1);

    const recovered = liveSnapshot(recoveredFixture as MonitoringSnapshotV1);
    expect((await postSnapshot(recovered)).response.status).toBe(201);
    expect((await rows("SELECT status FROM monitoring_alerts WHERE stream_id = ? AND alert_type = 'PIPELINE_DATA_QUALITY_FAILED'", streamId)).map((row) => row.status))
      .toEqual(["RESOLVED"]);
  });

  it("evaluates watchdog and disk conditions without treating missing metrics as healthy", async () => {
    const streamId = await seedTarget({ enabled: true, diskThresholdPercent: 85 });
    const unhealthy = liveSnapshot(undefined, (snapshot) => {
      snapshot.infrastructure.watchdog.latestReport!.status = "FAIL";
      snapshot.infrastructure.host!.disk!.usagePercent = 90;
    });
    expect((await postSnapshot(unhealthy)).response.status).toBe(201);
    expect((await rows("SELECT alert_type FROM monitoring_alerts WHERE stream_id = ? AND status = 'OPEN'", streamId)).map((row) => row.alert_type))
      .toEqual(expect.arrayContaining(["WATCHDOG_FAILED", "HOST_DISK_HIGH"]));

    const unknownDisk = liveSnapshot(undefined, (snapshot) => {
      snapshot.infrastructure.watchdog.latestReport!.status = "UNKNOWN";
      snapshot.infrastructure.host!.disk!.usagePercent = null;
    });
    expect((await postSnapshot(unknownDisk)).response.status).toBe(201);
    expect((await rows("SELECT alert_type FROM monitoring_alerts WHERE stream_id = ? AND status = 'OPEN'", streamId)).map((row) => row.alert_type))
      .toEqual(expect.arrayContaining(["WATCHDOG_FAILED", "HOST_DISK_HIGH"]));

    const recovered = liveSnapshot(recoveredFixture as MonitoringSnapshotV1, (snapshot) => {
      snapshot.infrastructure.host!.disk!.usagePercent = 70;
    });
    expect((await postSnapshot(recovered)).response.status).toBe(201);
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE stream_id = ? AND status = 'OPEN'", streamId)).length).toBe(0);
  });

  it("deduplicates concurrent alert opens and serializes simultaneous recovery updates", async () => {
    const streamId = await seedTarget({ enabled: true });
    await Promise.all(Array.from({ length: 12 }, () => openAlert(env.DB, {
      streamId,
      alertType: "PIPELINE_DATA_QUALITY_FAILED",
      severity: "CRITICAL",
      details: { qualityStatus: "FAIL" },
      telegramEnabled: false,
    })));
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE stream_id = ? AND status = 'OPEN'", streamId)).length).toBe(1);
    expect((await rows("SELECT event_id FROM monitoring_alert_events WHERE event_type = 'OPENED'")).length).toBe(1);

    await Promise.all(Array.from({ length: 12 }, () => resolveAlert(env.DB, {
      streamId,
      alertType: "PIPELINE_DATA_QUALITY_FAILED",
      details: { qualityStatus: "PASS" },
      telegramEnabled: false,
    })));
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE stream_id = ? AND status = 'OPEN'", streamId)).length).toBe(0);
    expect((await rows("SELECT event_id FROM monitoring_alert_events WHERE event_type = 'RESOLVED'")).length).toBe(1);
  });
});

describe("durable quarantine alert recovery", () => {
  it("links a single quarantine to its alert without changing the reason or reopening work", async () => {
    const streamId = await seedRecoveryRows({ count: 1, kind: "quarantine", reason: "MODEL_CONTRACT_MISMATCH" });
    const snapshot = await env.DB.prepare("SELECT snapshot_id FROM monitoring_snapshots WHERE validation_status = 'QUARANTINED'")
      .first<{ snapshot_id: string }>();
    await runCron();

    expect((await rows("SELECT alert_type, status FROM monitoring_alerts WHERE stream_id = ?", streamId))
      .map((row) => row)).toEqual([{ alert_type: "MODEL_CONTRACT_MISMATCH", status: "OPEN" }]);
    expect((await rows(`SELECT quarantine_reason, quarantine_alert_state, quarantine_alert_id
      FROM monitoring_snapshots WHERE snapshot_id = ?`, snapshot!.snapshot_id))[0])
      .toMatchObject({ quarantine_reason: "MODEL_CONTRACT_MISMATCH", quarantine_alert_state: "PROCESSED" });
    expect((await rows(`SELECT event_id FROM monitoring_alert_events
      WHERE alert_id = (SELECT alert_id FROM monitoring_alerts WHERE stream_id = ?)
        AND event_type = 'OBSERVED_AGAIN'`, streamId))).toHaveLength(1);

    await runCron();
    expect((await rows(`SELECT event_id FROM monitoring_alert_events
      WHERE alert_id = (SELECT alert_id FROM monitoring_alerts WHERE stream_id = ?)
        AND event_type = 'OBSERVED_AGAIN'`, streamId))).toHaveLength(1);
  });

  it("processes 100 same-fingerprint quarantines in bounded batches with one active alert", async () => {
    const streamId = await seedRecoveryRows({ count: 100, kind: "quarantine", reason: "COHORT_CONTRACT_MISMATCH" });
    const batchEnv = { ...env, MONITOR_RECOVERY_BATCH_SIZE: "10" };
    for (let tick = 0; tick < 10; tick += 1) await runCron("*/5 * * * *", batchEnv);

    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'COHORT_CONTRACT_MISMATCH' AND status = 'OPEN'`, streamId))).toHaveLength(1);
    expect((await rows(`SELECT quarantine_alert_state, COUNT(*) AS count FROM monitoring_snapshots
      WHERE validation_status = 'QUARANTINED' GROUP BY quarantine_alert_state`)))
      .toEqual([{ quarantine_alert_state: "PROCESSED", count: 100 }]);
    expect((await rows(`SELECT e.event_type, COUNT(*) AS count FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id WHERE a.stream_id = ? GROUP BY e.event_type`, streamId)))
      .toEqual(expect.arrayContaining([
        { event_type: "OPENED", count: 1 },
        { event_type: "OBSERVED_AGAIN", count: 100 },
      ]));

    await runCron("*/5 * * * *", batchEnv);
    expect((await rows(`SELECT event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND e.event_type = 'OBSERVED_AGAIN'`, streamId))).toHaveLength(100);
  }, 60_000);

  it("links each pending quarantine to an already-open fingerprint instead of adding OPENED events", async () => {
    const streamId = await seedRecoveryRows({ count: 1, kind: "quarantine", reason: "COHORT_CONTRACT_MISMATCH" });
    await openAlert(env.DB, {
      streamId,
      alertType: "COHORT_CONTRACT_MISMATCH",
      severity: "CRITICAL",
      details: { reason: "COHORT_CONTRACT_MISMATCH", snapshotId: "prior-snapshot" },
      telegramEnabled: false,
    });

    await runCron();

    expect((await rows(`SELECT event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND e.event_type = 'OPENED'`, streamId))).toHaveLength(1);
    expect((await rows(`SELECT event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND e.event_type = 'OBSERVED_AGAIN'`, streamId))).toHaveLength(1);
  });

  it("claims recovery work safely under concurrent Cron invocations", async () => {
    const streamId = await seedRecoveryRows({ count: 1, kind: "quarantine", reason: "MODEL_CONTRACT_MISMATCH" });
    await Promise.all([runCron(), runCron()]);

    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'MODEL_CONTRACT_MISMATCH' AND status = 'OPEN'`, streamId))).toHaveLength(1);
    expect((await rows("SELECT quarantine_alert_state FROM monitoring_snapshots WHERE validation_status = 'QUARANTINED'"))[0])
      .toEqual({ quarantine_alert_state: "PROCESSED" });
    expect((await rows(`SELECT event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND e.event_type = 'OBSERVED_AGAIN'`, streamId))).toHaveLength(1);
  });

  it("retries a partial D1 failure with the same reason and without duplicate active alerts", async () => {
    const streamId = await seedRecoveryRows({ count: 1, kind: "quarantine", reason: "MODEL_CONTRACT_MISMATCH" });
    const item = await env.DB.prepare("SELECT snapshot_id FROM monitoring_snapshots WHERE validation_status = 'QUARANTINED'")
      .first<{ snapshot_id: string }>();
    const failingDb = failNextBatchMatching(env.DB, (sql) => sql.includes("UPDATE monitoring_snapshots") &&
      sql.includes("quarantine_alert_state = 'PROCESSED'"));
    await expect(runCron("*/5 * * * *", { ...env, DB: failingDb })).rejects.toThrow("SCHEDULED_TASK_FAILED");
    expect((await rows(`SELECT quarantine_reason, quarantine_alert_state, quarantine_alert_attempts
      FROM monitoring_snapshots WHERE snapshot_id = ?`, item!.snapshot_id))[0])
      .toMatchObject({ quarantine_reason: "MODEL_CONTRACT_MISMATCH", quarantine_alert_state: "PENDING", quarantine_alert_attempts: 1 });
    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'MODEL_CONTRACT_MISMATCH' AND status = 'OPEN'`, streamId))).toHaveLength(1);

    await forceQuarantineRecoveryDue(item!.snapshot_id);
    await runCron();

    expect((await rows(`SELECT quarantine_reason, quarantine_alert_state, quarantine_alert_id
      FROM monitoring_snapshots WHERE snapshot_id = ?`, item!.snapshot_id))[0])
      .toMatchObject({ quarantine_reason: "MODEL_CONTRACT_MISMATCH", quarantine_alert_state: "PROCESSED" });
    expect((await rows(`SELECT alert_id FROM monitoring_alerts
      WHERE stream_id = ? AND alert_type = 'MODEL_CONTRACT_MISMATCH' AND status = 'OPEN'`, streamId))).toHaveLength(1);
    expect((await rows(`SELECT event_id FROM monitoring_alert_events e
      JOIN monitoring_alerts a ON a.alert_id = e.alert_id
      WHERE a.stream_id = ? AND e.event_type = 'OPENED'`, streamId))).toHaveLength(1);
  });

  it("does not create an alert for source allowlist quarantines", async () => {
    const streamId = await seedRecoveryRows({ count: 1, kind: "quarantine", reason: "SOURCE_NOT_ALLOWLISTED" });
    await runCron();
    expect((await rows("SELECT alert_id FROM monitoring_alerts WHERE stream_id = ?", streamId))).toHaveLength(0);
    expect((await rows("SELECT quarantine_alert_state, quarantine_reason FROM monitoring_snapshots"))[0])
      .toMatchObject({ quarantine_alert_state: "PROCESSED", quarantine_reason: "SOURCE_NOT_ALLOWLISTED" });
  });
});

describe("cron, persistence, retention and notifications", () => {
  it("does not send Telegram when disabled and does not record successful delivery", async () => {
    const streamId = "a".repeat(64);
    await openAlert(env.DB, {
      streamId,
      alertType: "WATCHDOG_FAILED",
      severity: "WARNING",
      details: { reason: "fixture" },
      telegramEnabled: false,
    });
    await processNotificationOutbox(env.DB, env);
    expect((await rows("SELECT notification_id FROM monitoring_notification_outbox")).length).toBe(0);
    expect((await rows("SELECT event_id FROM monitoring_alert_events WHERE event_type = 'NOTIFICATION_SENT'")).length).toBe(0);
  });

  it("retries Telegram API failure without marking SENT, then records success", async () => {
    const streamId = "b".repeat(64);
    await openAlert(env.DB, {
      streamId,
      alertType: "WATCHDOG_FAILED",
      severity: "WARNING",
      details: { snapshotId: crypto.randomUUID() },
      telegramEnabled: true,
    });
    const failingFetch = vi.fn().mockResolvedValue(new Response("unavailable", { status: 503 }));
    vi.stubGlobal("fetch", failingFetch);
    const telegramEnv = { ...env, TELEGRAM_ALERTS_ENABLED: "true", TELEGRAM_BOT_TOKEN: "test-bot-token", TELEGRAM_CHAT_ID: "-100123" };
    await processNotificationOutbox(env.DB, telegramEnv);
    expect(failingFetch).toHaveBeenCalledTimes(1);
    expect((await rows("SELECT state FROM monitoring_notification_outbox")).map((row) => row.state)).toEqual(["PENDING"]);
    expect((await rows("SELECT event_id FROM monitoring_alert_events WHERE event_type = 'NOTIFICATION_SENT'")).length).toBe(0);

    await env.DB.prepare("UPDATE monitoring_notification_outbox SET next_attempt_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 minute')").run();
    const successfulFetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
    vi.stubGlobal("fetch", successfulFetch);
    await processNotificationOutbox(env.DB, telegramEnv);
    expect(successfulFetch).toHaveBeenCalledTimes(1);
    expect((await rows("SELECT state FROM monitoring_notification_outbox")).map((row) => row.state)).toEqual(["SENT"]);
    expect((await rows("SELECT event_type FROM monitoring_alert_events WHERE event_type = 'NOTIFICATION_SENT'")).length).toBe(1);
    vi.unstubAllGlobals();
  });

  it("processes at most one due Telegram notification in a Cron invocation", async () => {
    const streamId = await seedTarget();
    await openAlert(env.DB, {
      streamId,
      alertType: "SYNTHETIC_ALERT_ONE",
      severity: "WARNING",
      details: { synthetic: true },
      telegramEnabled: true,
    });
    await openAlert(env.DB, {
      streamId,
      alertType: "SYNTHETIC_ALERT_TWO",
      severity: "WARNING",
      details: { synthetic: true },
      telegramEnabled: true,
    });
    const send = vi.fn().mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
    vi.stubGlobal("fetch", send);
    await processNotificationOutbox(env.DB, {
      ...env,
      TELEGRAM_ALERTS_ENABLED: "true",
      TELEGRAM_BOT_TOKEN: "test-bot-token",
      TELEGRAM_CHAT_ID: "-100123",
    });
    expect(send).toHaveBeenCalledTimes(1);
    expect((await rows("SELECT state FROM monitoring_notification_outbox ORDER BY notification_id"))
      .map((row) => row.state).sort()).toEqual(["PENDING", "SENT"]);
    vi.unstubAllGlobals();
  });

  it("runs bounded retention and keeps the latest snapshot pointer intact", async () => {
    const newest = liveSnapshot();
    const older = liveSnapshot();
    older.timestamp = new Date(Date.now() - 60_000).toISOString();
    await postSnapshot(older);
    await postSnapshot(newest);
    await env.DB.prepare("UPDATE monitoring_snapshots SET received_at = '2000-01-01T00:00:00.000Z' WHERE snapshot_id = ?")
      .bind(older.snapshotId).run();
    const result = await cleanupRetainedData(env.DB, env);
    expect(result.snapshots).toBe(1);
    expect((await rows("SELECT snapshot_id FROM monitoring_snapshots")).map((row) => row.snapshot_id)).toEqual([newest.snapshotId]);
  });
});
