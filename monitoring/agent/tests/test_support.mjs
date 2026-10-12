import { mkdtemp, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { tmpdir } from "node:os";
import { loadConfig, FROZEN_CONTRACT } from "../src/config.mjs";

export const TEST_RUN_ID = "run-monitor-agent-test";
export const TEST_COHORT_ID = "cohort-monitor-agent-test";
export const TEST_NOW = "2026-10-12T12:00:00.000Z";

export async function makeEnvironment() {
  const directory = await mkdtemp(path.join(tmpdir(), "weather-monitor-agent-"));
  const repository = path.join(directory, "live-repository");
  const stateRoot = path.join(repository, "data", "runtime", "prospective-live-t2h-168h-v1");
  const resultsRoot = path.join(repository, "results");
  const env = {
    WEATHER_REPO_ROOT: repository,
    WEATHER_STATE_ROOT: stateRoot,
    WEATHER_RESULTS_ROOT: resultsRoot,
    WEATHER_DATA_PATH: path.join(repository, "data"),
    WEATHER_RUN_ID: TEST_RUN_ID,
    WEATHER_COHORT_ID: TEST_COHORT_ID,
    MONITOR_ENVIRONMENT: "test",
    MONITOR_OUTPUT_DIR: path.join(directory, "monitor-output"),
    MONITOR_DOCKER_PROJECT: "weather-streaming-bigdata",
  };
  const config = loadConfig(env);
  await Promise.all([
    mkdir(config.paths.runStateDir, { recursive: true }),
    mkdir(config.paths.inferenceDir, { recursive: true }),
    mkdir(config.paths.opsDir, { recursive: true }),
    mkdir(config.paths.dataPath, { recursive: true }),
  ]);
  return { directory, repository, config, env };
}

export function validStatus(overrides = {}) {
  return {
    status: "COLLECTING",
    run_id: TEST_RUN_ID,
    cohort_id: TEST_COHORT_ID,
    cohort_start_target_time: "2026-10-10T12:00:00Z",
    cohort_end_target_time: "2026-10-17T11:00:00Z",
    expected_target_hours: 168,
    expected_locations: 63,
    expected_slots: 10584,
    prospective_forecast_count: 0,
    valid_evaluation_count: 0,
    completed_target_hours: 0,
    pending_target: 0,
    pending_reference: 0,
    missing_forecasts: 0,
    missing_references: 0,
    reference_conflicts: 0,
    reference_conflict_key_count: 0,
    invalid_provenance: 0,
    duplicate_validation: {
      duplicate_forecast_ids: 0,
      duplicate_logical_forecasts: 0,
      duplicate_persistence_receipts: 0,
      duplicate_evaluation_ids: 0,
    },
    provenance_validation: { blocking_violations: [] },
    last_update_at: TEST_NOW,
    microbatch_update_count: 0,
    restart_count: 0,
    ...overrides,
  };
}

export function validRequest(overrides = {}) {
  return {
    run_id: TEST_RUN_ID,
    cohort_protocol_id: FROZEN_CONTRACT.protocolId,
    expected_target_hours: FROZEN_CONTRACT.targetHours,
    expected_locations: FROZEN_CONTRACT.targetLocations,
    expected_slots: FROZEN_CONTRACT.targetHours * FROZEN_CONTRACT.targetLocations,
    model_sha256: FROZEN_CONTRACT.modelSha256,
    feature_list_sha256: FROZEN_CONTRACT.featureListSha256,
    provider_model: FROZEN_CONTRACT.providerModel,
    forecast_horizon_hours: FROZEN_CONTRACT.forecastHorizonHours,
    ...overrides,
  };
}

export function validManifest(overrides = {}) {
  return {
    ...validRequest(),
    cohort_id: TEST_COHORT_ID,
    cohort_start_target_time: "2026-10-10T12:00:00Z",
    cohort_end_target_time: "2026-10-17T11:00:00Z",
    ...overrides,
  };
}

export async function seedCohort(config, { status = validStatus(), request = validRequest(), manifest = validManifest(), activeRunId = TEST_RUN_ID } = {}) {
  await writeJson(config.paths.activeRun, { run_id: activeRunId, started_at: TEST_NOW });
  await writeJson(config.paths.startRequest, request);
  await writeJson(config.paths.cohortManifest, manifest);
  await writeJson(config.paths.cohortStatus, status);
}

export async function writeJson(filePath, value) {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(value)}\n`, "utf8");
}

export async function writeJsonl(filePath, rows, { finalNewline = true } = {}) {
  await mkdir(path.dirname(filePath), { recursive: true });
  const text = rows.map((row) => JSON.stringify(row)).join("\n") + (finalNewline && rows.length ? "\n" : "");
  await writeFile(filePath, text, "utf8");
}
