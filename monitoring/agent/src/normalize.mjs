import { randomUUID } from "node:crypto";
import os from "node:os";
import { AGENT_VERSION } from "./config.mjs";
import { nonNegativeInteger } from "./collector-utils.mjs";

function collectorWarnings(results) {
  return Object.entries(results).flatMap(([collector, result]) =>
    (result?.warnings || []).map((code) => ({ collector, code: String(code).slice(0, 120) })),
  ).slice(0, 80);
}

export function normalizeSnapshot(config, results, { timestamp = new Date().toISOString(), hostname = results.host?.data?.hostname || os.hostname(), snapshotId = randomUUID(), durationMs = null } = {}) {
  const cohortData = results.cohort?.data || {};
  const progressData = cohortData.progress || {};
  const qualityData = cohortData.quality || {};
  const inferenceData = results.inference?.data || {};
  const watchdogData = results.watchdog?.data || {};
  const dockerData = results.docker?.data || { status: "UNAVAILABLE", containers: [] };
  const audit = watchdogData.latestAudit || null;
  const pipelineDataErrors = watchdogData.latestReport?.currentDataErrors || [];
  const collectorStates = Object.values(results).map((result) => result?.status || "UNAVAILABLE");
  const collectorStatus = collectorStates.every((status) => status === "PASS")
    ? "COMPLETE"
    : collectorStates.every((status) => status === "UNAVAILABLE")
      ? "FAILED"
      : "PARTIAL";
  const watchdogWarnings = watchdogData.latestReport?.warnings || [];
  const monitorFreshness = watchdogWarnings.includes("COHORT_UPDATE_STALE_OVER_90_MINUTES")
    ? "STALE"
    : watchdogWarnings.includes("COHORT_UPDATE_TIMESTAMP_MISSING")
      ? "UNKNOWN"
      : watchdogData.latestReport?.checkedAt
        ? "OBSERVED"
        : "UNKNOWN";
  const latestBatch = inferenceData.latestBatchMetrics;
  const gapRowsLatestBatch = nonNegativeInteger(latestBatch?.gap_in_history_rows ?? latestBatch?.history_gap_rows);
  const quality = {
    status: audit?.effectiveStatus === "FAIL" || pipelineDataErrors.length > 0
      ? "FAIL"
      : audit?.effectiveStatus === "PASS" && pipelineDataErrors.length === 0
        ? "PASS"
        : "UNKNOWN",
    pipelineDataErrors,
    duplicateConflictsLatestBatch: nonNegativeInteger(latestBatch?.duplicate_conflict_keys),
    duplicateForecastIds: nonNegativeInteger(qualityData.duplicates?.duplicate_forecast_ids),
    duplicateLogicalForecasts: nonNegativeInteger(qualityData.duplicates?.duplicate_logical_forecasts),
    duplicatePersistenceReceipts: nonNegativeInteger(qualityData.duplicates?.duplicate_persistence_receipts),
    duplicateEvaluationIds: nonNegativeInteger(qualityData.duplicates?.duplicate_evaluation_ids),
    referenceConflicts: nonNegativeInteger(qualityData.referenceConflicts),
    invalidProvenance: nonNegativeInteger(qualityData.invalidProvenance),
    provenanceBlockingViolations: Array.isArray(qualityData.provenanceBlockingViolations)
      ? qualityData.provenanceBlockingViolations.slice(0, 20)
      : null,
    historyGapRowsLatestBatch: gapRowsLatestBatch,
    rejectedRowsLatestBatch: nonNegativeInteger(latestBatch?.rejected_rows),
    latestAudit: audit,
    auditHistory: watchdogData.auditHistory || { windowSeconds: 86400, passCount: 0, failCount: 0, recentFailures: [] },
    persistenceReceiptCount: nonNegativeInteger(watchdogData.persistenceReceiptCount),
    persistenceReceiptAudit: watchdogData.persistenceReceiptAudit || null,
    unavailableReasonCodes: [...new Set([
      ...(results.cohort?.warnings || []),
      ...(results.inference?.warnings || []),
      ...(results.watchdog?.warnings || []),
    ])].slice(0, 60),
  };

  return {
    schemaVersion: 1,
    snapshotId,
    timestamp,
    source: {
      environment: config.environment,
      agentVersion: AGENT_VERSION,
      hostname,
    },
    cohort: {
      runId: cohortData.cohort?.runId ?? config.runId,
      cohortId: cohortData.cohort?.cohortId ?? config.cohortId,
      protocolId: cohortData.cohort?.protocolId ?? config.contract.protocolId,
      status: cohortData.cohort?.status ?? "UNKNOWN",
      startsAt: cohortData.cohort?.startsAt ?? null,
      endsAt: cohortData.cohort?.endsAt ?? null,
      targetHours: nonNegativeInteger(cohortData.cohort?.targetHours),
      targetLocations: nonNegativeInteger(cohortData.cohort?.targetLocations),
      targetForecastSlots: nonNegativeInteger(cohortData.cohort?.targetForecastSlots),
      lastUpdateAt: cohortData.cohort?.lastUpdateAt ?? null,
      statusFileUpdatedAt: cohortData.cohort?.statusFileUpdatedAt ?? null,
      identityValidation: cohortData.cohort?.identityValidation ?? "UNKNOWN",
      frozenContractValidation: cohortData.cohort?.frozenContractValidation ?? "UNKNOWN",
      modelSha256: cohortData.cohort?.modelSha256 ?? null,
      featureListSha256: cohortData.cohort?.featureListSha256 ?? null,
      providerModel: cohortData.cohort?.providerModel ?? null,
      forecastHorizonHours: nonNegativeInteger(cohortData.cohort?.forecastHorizonHours),
    },
    progress: {
      microbatchUpdateCount: nonNegativeInteger(progressData.microbatchUpdateCount),
      completedTargetHours: nonNegativeInteger(progressData.completedTargetHours),
      expectedTargetHours: nonNegativeInteger(progressData.expectedTargetHours),
      expectedLocations: nonNegativeInteger(progressData.expectedLocations),
      expectedForecastSlots: nonNegativeInteger(progressData.expectedForecastSlots),
      forecastsPersisted: nonNegativeInteger(progressData.forecastsPersisted),
      validEvaluations: nonNegativeInteger(progressData.validEvaluations),
      pendingTarget: nonNegativeInteger(progressData.pendingTarget),
      pendingReference: nonNegativeInteger(progressData.pendingReference),
      missingForecasts: nonNegativeInteger(progressData.missingForecasts),
      missingReferences: nonNegativeInteger(progressData.missingReferences),
      restartCount: nonNegativeInteger(progressData.restartCount),
    },
    quality,
    inference: {
      latestBatchId: nonNegativeInteger(inferenceData.latestBatchId),
      latestBatchAt: inferenceData.latestBatchAt ?? null,
      latestBatchObservedAt: inferenceData.latestBatchObservedAt ?? null,
      modelSha256: inferenceData.modelSha256 ?? null,
      expectedModelSha256: inferenceData.expectedModelSha256 ?? config.contract.modelSha256,
      modelContractStatus: inferenceData.modelContractStatus ?? "UNKNOWN",
      modelStartupStatus: inferenceData.modelStartupStatus ?? null,
      modelStartupObservedAt: inferenceData.modelStartupObservedAt ?? null,
      durationMs: nonNegativeInteger(inferenceData.durationMs),
      latestBatchMetrics: latestBatch || null,
      persistenceReceiptFile: inferenceData.persistenceReceiptFile || { status: "UNKNOWN", modifiedAt: null, sizeBytes: null },
    },
    infrastructure: {
      host: results.host?.data || null,
      docker: dockerData,
      watchdog: {
        ...watchdogData,
        monitorFreshness,
      },
    },
    pipeline: {
      status: cohortData.cohort?.status ?? "UNKNOWN",
      watchdogStatus: watchdogData.latestReport?.status ?? "UNKNOWN",
      monitorFreshness,
      latestHourlyStatus: watchdogData.latestHourlyStatus ?? "UNKNOWN",
      auditStatus: audit?.effectiveStatus ?? "UNKNOWN",
    },
    collector: {
      status: collectorStatus,
      durationMs: nonNegativeInteger(durationMs),
      warnings: collectorWarnings(results),
    },
  };
}
