import { performance } from "node:perf_hooks";
import { readJsonFile } from "../readers.mjs";
import { collectorResult, isRecord, nonNegativeInteger, sourceWarning, utcTimestamp } from "../collector-utils.mjs";

function fieldCount(record, name, warnings, warningPrefix = "COHORT_METRIC") {
  if (!isRecord(record) || !(name in record)) {
    warnings.push(`${warningPrefix}_MISSING_${name.toUpperCase()}`);
    return null;
  }
  const value = nonNegativeInteger(record[name]);
  if (value === null) warnings.push(`${warningPrefix}_INVALID_${name.toUpperCase()}`);
  return value;
}

export async function collectCohort(config, { readJson = readJsonFile } = {}) {
  const started = performance.now();
  const entries = await Promise.all([
    ["active_run", config.paths.activeRun],
    ["start_request", config.paths.startRequest],
    ["cohort_manifest", config.paths.cohortManifest],
    ["cohort_status", config.paths.cohortStatus],
  ].map(async ([name, filePath]) => [name, await readJson(filePath)]));
  const sources = Object.fromEntries(entries);
  const records = Object.fromEntries(entries.map(([name, result]) => [name, isRecord(result.value) ? result.value : null]));
  const warnings = entries.map(([name, result]) => sourceWarning(`SOURCE_${name.toUpperCase()}`, result)).filter(Boolean);
  const { active_run: active, start_request: request, cohort_manifest: manifest, cohort_status: status } = records;
  const possibleRunIds = [active?.run_id, request?.run_id, manifest?.run_id, status?.run_id].filter((value) => value != null);
  const possibleCohortIds = [request?.cohort_id, manifest?.cohort_id, status?.cohort_id].filter((value) => value != null);
  const runMatches = active?.run_id === config.runId && possibleRunIds.length > 0 && possibleRunIds.every((value) => value === config.runId);
  const cohortMatches = possibleCohortIds.length > 0 && possibleCohortIds.every((value) => value === config.cohortId);
  const protocolId = manifest?.cohort_protocol_id ?? manifest?.protocol_id ?? request?.cohort_protocol_id ?? request?.protocol_id ?? null;
  const modelSha256 = manifest?.model_sha256 ?? request?.model_sha256 ?? null;
  const featureListSha256 = manifest?.feature_list_sha256 ?? request?.feature_list_sha256 ?? null;
  const providerModel = manifest?.provider_model ?? request?.provider_model ?? null;
  const horizonHours = manifest?.forecast_horizon_hours ?? request?.forecast_horizon_hours ?? null;
  if (!runMatches) warnings.push("COHORT_RUN_ID_UNKNOWN_OR_MISMATCH");
  if (!cohortMatches) warnings.push("COHORT_ID_UNKNOWN_OR_MISMATCH");
  const contractFields = [
    ["cohort_protocol_id", config.contract.protocolId],
    ["model_sha256", config.contract.modelSha256],
    ["feature_list_sha256", config.contract.featureListSha256],
    ["provider_model", config.contract.providerModel],
    ["forecast_horizon_hours", config.contract.forecastHorizonHours],
  ];
  const contractRecords = [request, manifest].filter(isRecord);
  const contractMatches = contractRecords.length === 2
    && contractFields.every(([field, expected]) => contractRecords.every((record) => record[field] === expected));
  if (!contractMatches) warnings.push("COHORT_FROZEN_CONTRACT_UNKNOWN_OR_MISMATCH");

  const progress = {
    microbatchUpdateCount: fieldCount(status, "microbatch_update_count", warnings),
    completedTargetHours: fieldCount(status, "completed_target_hours", warnings),
    expectedTargetHours: fieldCount(status, "expected_target_hours", warnings),
    expectedLocations: fieldCount(status, "expected_locations", warnings),
    expectedForecastSlots: fieldCount(status, "expected_slots", warnings),
    forecastsPersisted: fieldCount(status, "prospective_forecast_count", warnings),
    validEvaluations: fieldCount(status, "valid_evaluation_count", warnings),
    pendingTarget: fieldCount(status, "pending_target", warnings),
    pendingReference: fieldCount(status, "pending_reference", warnings),
    missingForecasts: fieldCount(status, "missing_forecasts", warnings),
    missingReferences: fieldCount(status, "missing_references", warnings),
    restartCount: fieldCount(status, "restart_count", warnings),
  };
  const duplicateValidation = isRecord(status?.duplicate_validation) ? status.duplicate_validation : null;
  const duplicateMetrics = [
    "duplicate_forecast_ids",
    "duplicate_logical_forecasts",
    "duplicate_persistence_receipts",
    "duplicate_evaluation_ids",
  ];
  const duplicates = Object.fromEntries(duplicateMetrics.map((key) => {
    const value = nonNegativeInteger(duplicateValidation?.[key]);
    if (value === null) warnings.push(`COHORT_DUPLICATE_METRIC_MISSING_OR_INVALID_${key.toUpperCase()}`);
    return [key, value];
  }));
  if (!duplicateValidation) warnings.push("COHORT_DUPLICATE_VALIDATION_UNAVAILABLE");
  const provenanceValidation = isRecord(status?.provenance_validation) ? status.provenance_validation : null;
  if (!provenanceValidation) warnings.push("COHORT_PROVENANCE_VALIDATION_UNAVAILABLE");
  const blockingViolations = Array.isArray(provenanceValidation?.blocking_violations)
    ? provenanceValidation.blocking_violations.filter((value) => typeof value === "string").slice(0, 20)
    : null;
  if (blockingViolations === null) warnings.push("COHORT_PROVENANCE_BLOCKING_VIOLATIONS_UNKNOWN");
  const observedStatus = typeof status?.status === "string" ? status.status : null;
  if (!observedStatus) warnings.push("COHORT_STATUS_FIELD_UNAVAILABLE");

  const cohort = {
    protocolId,
    runId: status?.run_id ?? manifest?.run_id ?? request?.run_id ?? active?.run_id ?? null,
    cohortId: status?.cohort_id ?? manifest?.cohort_id ?? request?.cohort_id ?? null,
    status: observedStatus,
    startsAt: utcTimestamp(status?.cohort_start_target_time ?? manifest?.cohort_start_target_time),
    endsAt: utcTimestamp(status?.cohort_end_target_time ?? manifest?.cohort_end_target_time),
    targetHours: progress.expectedTargetHours,
    targetLocations: progress.expectedLocations,
    targetForecastSlots: progress.expectedForecastSlots,
    modelSha256,
    featureListSha256,
    providerModel,
    forecastHorizonHours: horizonHours,
    lastUpdateAt: utcTimestamp(status?.last_update_at),
    statusFileUpdatedAt: sources.cohort_status.modifiedAt ?? null,
    identityValidation: runMatches && cohortMatches ? "PASS" : "UNKNOWN_OR_MISMATCH",
    frozenContractValidation: contractMatches ? "PASS" : "UNKNOWN_OR_MISMATCH",
  };
  const quality = {
    duplicates,
    referenceConflicts: fieldCount(status, "reference_conflicts", warnings),
    referenceConflictKeys: fieldCount(status, "reference_conflict_key_count", warnings),
    invalidProvenance: fieldCount(status, "invalid_provenance", warnings),
    provenanceBlockingViolations: blockingViolations,
  };
  const collectorStatus = warnings.length === 0 ? "PASS" : "PARTIAL";
  return collectorResult(collectorStatus, {
    cohort,
    progress,
    quality,
    sourceFiles: Object.fromEntries(entries.map(([name, result]) => [name, {
      status: result.status,
      modifiedAt: result.modifiedAt ?? null,
    }])),
    durationMs: Math.round((performance.now() - started) * 100) / 100,
  }, warnings);
}
