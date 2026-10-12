import test from "node:test";
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { collectCohort } from "../src/collectors/cohort.mjs";
import { collectDocker } from "../src/collectors/docker.mjs";
import { collectInference } from "../src/collectors/inference.mjs";
import { collectWatchdog } from "../src/collectors/watchdog.mjs";
import { FROZEN_CONTRACT } from "../src/config.mjs";
import { makeEnvironment, seedCohort, TEST_NOW, validStatus, writeJson, writeJsonl } from "./test_support.mjs";

test("cohort collector recognizes COLLECTING and preserves zero counters and UTC", async () => {
  const { config } = await makeEnvironment();
  await seedCohort(config, { status: validStatus({ microbatch_update_count: 0, prospective_forecast_count: 0, valid_evaluation_count: 0 }) });
  const result = await collectCohort(config);
  assert.equal(result.status, "PASS");
  assert.equal(result.data.cohort.status, "COLLECTING");
  assert.equal(result.data.cohort.identityValidation, "PASS");
  assert.equal(result.data.cohort.frozenContractValidation, "PASS");
  assert.equal(result.data.progress.microbatchUpdateCount, 0);
  assert.equal(result.data.progress.forecastsPersisted, 0);
  assert.equal(result.data.progress.validEvaluations, 0);
  assert.equal(result.data.cohort.lastUpdateAt, TEST_NOW);
});

test("missing cohort inputs and fields stay null with reason codes", async () => {
  const { config } = await makeEnvironment();
  await seedCohort(config, { status: validStatus({ restart_count: undefined }) });
  const result = await collectCohort(config);
  assert.equal(result.data.progress.restartCount, null);
  assert.ok(result.warnings.includes("COHORT_METRIC_MISSING_RESTART_COUNT"));
  const missing = await collectCohort({ ...config, paths: { ...config.paths, cohortStatus: `${config.paths.cohortStatus}.missing` } });
  assert.ok(missing.warnings.includes("SOURCE_COHORT_STATUS_MISSING"));
  assert.equal(missing.data.cohort.status, null);
});

test("cohort identity and frozen-contract drift are surfaced", async () => {
  const { config } = await makeEnvironment();
  await seedCohort(config, { request: { run_id: "other-run", model_sha256: "0".repeat(64) }, activeRunId: "other-run" });
  const result = await collectCohort(config);
  assert.equal(result.data.cohort.identityValidation, "UNKNOWN_OR_MISMATCH");
  assert.ok(result.warnings.includes("COHORT_RUN_ID_UNKNOWN_OR_MISMATCH"));
  assert.ok(result.warnings.includes("COHORT_FROZEN_CONTRACT_UNKNOWN_OR_MISMATCH"));
});

test("inference collector exports allowlisted metrics and frozen model SHA only", async () => {
  const { config } = await makeEnvironment();
  await writeJsonl(config.paths.inferenceBatches, [{
    batch_id: 0,
    source_rows: 63,
    feature_ready_rows: 0,
    new_forecast_rows: 0,
    duplicate_conflict_keys: 0,
    gap_in_history_rows: 0,
    invalid_provenance_rows: 0,
    rejected_rows: 0,
    total_batch_seconds: 0.5,
    execution_origin: "LIVE_PROSPECTIVE",
    temperature_c: 12.5,
  }]);
  await writeJson(config.paths.modelStartupValidation, { status: "PASS", model_sha256: FROZEN_CONTRACT.modelSha256, probe_prediction_temperature_c: 12.5 });
  await writeFile(config.paths.persistenceReceipts, "");
  const result = await collectInference(config);
  assert.equal(result.data.latestBatchId, 0);
  assert.equal(result.data.durationMs, 500);
  assert.equal(result.data.modelContractStatus, "PASS");
  assert.equal(result.data.latestBatchMetrics.temperature_c, undefined);
  assert.equal(result.data.modelStartupStatus, "PASS");
});

test("missing inference artifacts produce null metrics rather than fabricated zeros", async () => {
  const { config } = await makeEnvironment();
  const result = await collectInference(config);
  assert.equal(result.data.latestBatchId, null);
  assert.equal(result.data.modelSha256, null);
  assert.ok(result.warnings.includes("INFERENCE_BATCHES_MISSING"));
  assert.ok(result.warnings.includes("MODEL_STARTUP_VALIDATION_MISSING"));
});

test("Docker denial returns UNAVAILABLE and does not invoke a shell", async () => {
  const { config } = await makeEnvironment();
  let invocation;
  const result = await collectDocker(config, {
    platform: "linux",
    exec: async (file, args, options) => {
      invocation = { file, args, options };
      throw Object.assign(new Error("denied"), { code: "EACCES" });
    },
  });
  assert.equal(result.status, "UNAVAILABLE");
  assert.equal(invocation.file, "docker");
  assert.ok(Array.isArray(invocation.args));
  assert.equal(invocation.options.timeoutMs, 4000);
  assert.ok(!invocation.args.includes("sh"));
});

test("watchdog retains a failed hourly audit after a later PASS and treats inactive oneshot as successful", async () => {
  const { config } = await makeEnvironment();
  const old = "2026-10-12T11:00:00Z";
  const now = new Date(TEST_NOW);
  const hourlyRows = [
    { checked_at: old, status: "FAIL", persistence_receipt_count: 10, hourly_delta_audit: { status: "FAIL", checked_at: old, classification: "AUDIT_FAILED" } },
    { checked_at: TEST_NOW, status: "PASS", persistence_receipt_count: 20, hourly_delta_audit: { status: "PASS", checked_at: TEST_NOW, classification: "AUDIT_PASS" } },
  ];
  const monitorRows = [
    { checked_at: old, status: "FAIL", current_data_errors: ["HOURLY_DELTA_AUDIT_FAILED"], infrastructure_errors: [] },
    { checked_at: TEST_NOW, status: "PASS", current_data_errors: [], infrastructure_errors: [] },
  ];
  const readJsonl = async (filePath) => ({ status: "OK", records: filePath === config.paths.hourlyReports ? hourlyRows : monitorRows, modifiedAt: TEST_NOW, invalidLines: 0, incompleteTail: false, truncated: false });
  const exec = async (_file, args) => args[1].endsWith(".timer")
    ? "ActiveState=active\nSubState=waiting\nLastTriggerUSec=2026-10-12T11:55:00Z\nNextElapseUSecRealtime=2026-10-12T12:00:00Z\n"
    : "ActiveState=inactive\nSubState=dead\nResult=success\nExecMainStatus=0\nExecMainStartTimestamp=2026-10-12T11:55:00Z\nExecMainExitTimestamp=2026-10-12T11:55:02Z\n";
  const result = await collectWatchdog(config, { readJsonl, exec, platform: "linux", now });
  assert.equal(result.status, "PASS");
  assert.equal(result.data.timer.active, true);
  assert.equal(result.data.service.activeState, "inactive");
  assert.equal(result.data.service.lastResult, "success");
  assert.equal(result.data.latestAudit.effectiveStatus, "PASS");
  assert.equal(result.data.auditHistory.passCount, 1);
  assert.equal(result.data.auditHistory.failCount, 1);
  assert.equal(result.data.auditHistory.recentFailures[0].classification, "AUDIT_FAILED");
  assert.equal(result.data.failureHistory.failedMonitorReportCount, 1);
  assert.equal(result.data.persistenceReceiptCount, 20);
});

test("systemd and watchdog read failures remain explicit unavailable evidence", async () => {
  const { config } = await makeEnvironment();
  const result = await collectWatchdog(config, {
    readJsonl: async () => ({ status: "MISSING", records: [] }),
    exec: async () => { throw Object.assign(new Error("denied"), { code: "EACCES" }); },
    platform: "linux",
  });
  assert.equal(result.data.timer.active, null);
  assert.ok(result.warnings.includes("WATCHDOG_MONITOR_LOG_MISSING"));
  assert.ok(result.warnings.includes("SYSTEMCTL_ACCESS_UNAVAILABLE"));
});
