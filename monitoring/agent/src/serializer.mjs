export const MAX_SNAPSHOT_BYTES = 32 * 1024;
const FORBIDDEN_KEYS = /(?:password|secret|credential|private.?key|token|raw.?json|temperature|humidity|precipitation|wind.?speed|payload.?hash)/i;

function collectForbiddenKeys(value, path = "$") {
  if (!value || typeof value !== "object") return [];
  const errors = [];
  for (const [key, child] of Object.entries(value)) {
    if (FORBIDDEN_KEYS.test(key)) errors.push(`${path}.${key}:forbidden_field`);
    errors.push(...collectForbiddenKeys(child, `${path}.${key}`));
  }
  return errors;
}

const COUNT_FIELDS = [
  "targetHours", "targetLocations", "targetForecastSlots", "microbatchUpdateCount", "completedTargetHours",
  "expectedTargetHours", "expectedLocations", "expectedForecastSlots", "forecastsPersisted", "validEvaluations",
  "pendingTarget", "pendingReference", "missingForecasts", "missingReferences", "restartCount",
  "duplicateConflictsLatestBatch", "duplicateForecastIds", "duplicateLogicalForecasts", "duplicatePersistenceReceipts",
  "duplicateEvaluationIds", "referenceConflicts", "invalidProvenance", "historyGapRowsLatestBatch",
  "rejectedRowsLatestBatch", "persistenceReceiptCount", "latestBatchId", "durationMs", "passCount", "failCount",
  "failedMonitorReportCount", "receiptCount", "sizeBytes", "ageSeconds", "windowSeconds",
];

function findInvalidCounts(value, path = "$") {
  if (!value || typeof value !== "object") return [];
  const errors = [];
  for (const [key, child] of Object.entries(value)) {
    const current = `${path}.${key}`;
    if (COUNT_FIELDS.includes(key) && child !== null && (!Number.isSafeInteger(child) || child < 0)) errors.push(`${current}:invalid_count`);
    errors.push(...findInvalidCounts(child, current));
  }
  return errors;
}

export function validateSnapshot(snapshot) {
  const errors = [];
  if (!snapshot || typeof snapshot !== "object" || Array.isArray(snapshot)) return { valid: false, errors: ["$:expected_object"] };
  if (snapshot.schemaVersion !== 1) errors.push("$.schemaVersion:unsupported");
  if (typeof snapshot.snapshotId !== "string" || !snapshot.snapshotId) errors.push("$.snapshotId:required");
  if (typeof snapshot.timestamp !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(snapshot.timestamp) || !Number.isFinite(Date.parse(snapshot.timestamp))) {
    errors.push("$.timestamp:expected_utc_iso8601");
  }
  if (!snapshot.source || typeof snapshot.source.environment !== "string" || typeof snapshot.source.agentVersion !== "string") {
    errors.push("$.source:invalid");
  }
  if (!snapshot.cohort || typeof snapshot.cohort.runId !== "string" || typeof snapshot.cohort.cohortId !== "string") {
    errors.push("$.cohort:identity_required");
  }
  if (!snapshot.progress || !snapshot.quality || !snapshot.inference || !snapshot.infrastructure || !snapshot.pipeline || !snapshot.collector) {
    errors.push("$:required_sections_missing");
  }
  if (!new Set(["COMPLETE", "PARTIAL", "FAILED"]).has(snapshot.collector?.status)) errors.push("$.collector.status:invalid");
  if (!new Set(["PASS", "PARTIAL", "UNAVAILABLE"]).has(snapshot.infrastructure?.docker?.status)) errors.push("$.infrastructure.docker.status:invalid");
  if (!new Set(["PASS", "FAIL", "UNKNOWN_OR_MISMATCH", "UNKNOWN"]).has(snapshot.cohort?.identityValidation)) errors.push("$.cohort.identityValidation:invalid");
  errors.push(...findInvalidCounts(snapshot));
  errors.push(...collectForbiddenKeys(snapshot));
  try {
    if (Buffer.byteLength(JSON.stringify(snapshot), "utf8") > MAX_SNAPSHOT_BYTES) errors.push("$:snapshot_too_large");
  } catch {
    errors.push("$:not_json_serializable");
  }
  return { valid: errors.length === 0, errors };
}

export function serializeSnapshot(snapshot) {
  const validation = validateSnapshot(snapshot);
  if (!validation.valid) {
    const error = new Error("SNAPSHOT_VALIDATION_FAILED");
    error.validationErrors = validation.errors;
    throw error;
  }
  return JSON.stringify(snapshot);
}
