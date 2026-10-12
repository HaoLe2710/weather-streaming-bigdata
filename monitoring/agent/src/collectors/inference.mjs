import { performance } from "node:perf_hooks";
import { inspectPath, readJsonFile, readJsonlTail } from "../readers.mjs";
import { collectorResult, isRecord, nonNegativeInteger, sourceWarning, utcTimestamp } from "../collector-utils.mjs";

const EXPORTED_BATCH_FIELDS = [
  "batch_id",
  "source_rows",
  "feature_ready_rows",
  "new_forecast_rows",
  "duplicate_conflict_keys",
  "gap_in_history_rows",
  "history_gap_rows",
  "invalid_provenance_rows",
  "rejected_rows",
  "total_batch_seconds",
  "execution_origin",
  "live_positive_lead_forecast_count",
  "nonpositive_live_lead_rows",
];

function sanitizedMetrics(record) {
  if (!isRecord(record)) return null;
  const result = {};
  for (const field of EXPORTED_BATCH_FIELDS) {
    const value = record[field];
    if (typeof value === "string") result[field] = value.slice(0, 80);
    else if (typeof value === "number" && Number.isFinite(value) && value >= 0) result[field] = value;
  }
  return result;
}

export async function collectInference(config, { readJson = readJsonFile, readJsonl = readJsonlTail, inspect = inspectPath } = {}) {
  const started = performance.now();
  const [batchesResult, startupResult, receiptsResult] = await Promise.all([
    readJsonl(config.paths.inferenceBatches),
    readJson(config.paths.modelStartupValidation),
    inspect(config.paths.persistenceReceipts),
  ]);
  const warnings = [
    sourceWarning("INFERENCE_BATCHES", batchesResult),
    sourceWarning("MODEL_STARTUP_VALIDATION", startupResult),
    receiptsResult.status === "READABLE" ? null : `PERSISTENCE_RECEIPTS_${receiptsResult.status}`,
  ].filter(Boolean);
  if (batchesResult.invalidLines) warnings.push("INFERENCE_BATCHES_CONTAIN_INVALID_LINES");
  if (batchesResult.incompleteTail) warnings.push("INFERENCE_BATCHES_HAVE_INCOMPLETE_TAIL");
  if (batchesResult.truncated) warnings.push("INFERENCE_BATCHES_TAIL_TRUNCATED");
  const batches = Array.isArray(batchesResult.records) ? batchesResult.records : [];
  const latest = batches.at(-1) ?? null;
  const startup = isRecord(startupResult.value) ? startupResult.value : null;
  const actualModelSha = typeof startup?.model_sha256 === "string" ? startup.model_sha256 : null;
  const modelContractStatus = startup?.status === "PASS" && actualModelSha === config.contract.modelSha256
    ? "PASS"
    : startup && startup.status !== "PASS"
      ? "FAIL"
      : "UNKNOWN_OR_MISMATCH";
  if (modelContractStatus !== "PASS") warnings.push("FROZEN_MODEL_STARTUP_VALIDATION_NOT_PASS");
  const latestMetrics = sanitizedMetrics(latest);
  const latestBatchId = latest ? nonNegativeInteger(latest.batch_id) : null;
  if (latest && latestBatchId === null) warnings.push("LATEST_INFERENCE_BATCH_ID_INVALID");
  const durationSeconds = latest && Number.isFinite(latest.total_batch_seconds) && latest.total_batch_seconds >= 0
    ? latest.total_batch_seconds
    : null;
  const data = {
    latestBatchId,
    latestBatchAt: utcTimestamp(latest?.completed_at ?? latest?.processed_at ?? latest?.timestamp_utc),
    latestBatchObservedAt: batchesResult.modifiedAt ?? null,
    latestBatchMetrics: latestMetrics,
    modelSha256: actualModelSha,
    expectedModelSha256: config.contract.modelSha256,
    modelContractStatus,
    modelStartupStatus: startup?.status ?? null,
    modelStartupObservedAt: startupResult.modifiedAt ?? null,
    durationMs: durationSeconds === null ? null : Math.round(durationSeconds * 1000),
    persistenceReceiptFile: {
      status: receiptsResult.status,
      modifiedAt: receiptsResult.modifiedAt ?? null,
      sizeBytes: nonNegativeInteger(receiptsResult.sizeBytes) ?? null,
    },
  };
  return collectorResult(warnings.length ? "PARTIAL" : "PASS", {
    ...data,
    collectorDurationMs: Math.round((performance.now() - started) * 100) / 100,
  }, warnings);
}
