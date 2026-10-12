import path from "node:path";
import { fileURLToPath } from "node:url";

export const AGENT_VERSION = "1.0.0";
export const FROZEN_CONTRACT = Object.freeze({
  protocolId: "T2H_LIVE_PROSPECTIVE_168H_V1",
  targetHours: 168,
  targetLocations: 63,
  modelSha256: "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a",
  featureListSha256: "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2",
  providerModel: "ecmwf_ifs",
  forecastHorizonHours: 2,
});

export class ConfigError extends Error {
  constructor(message) {
    super(message);
    this.name = "ConfigError";
  }
}

function required(env, name) {
  const value = String(env[name] ?? "").trim();
  if (!value) throw new ConfigError(`${name} is required`);
  return value;
}

function absolutePath(value, name) {
  if (!path.isAbsolute(value)) throw new ConfigError(`${name} must be an absolute path`);
  return path.resolve(value);
}

function boundedInteger(value, name, fallback, minimum, maximum) {
  if (value == null || value === "") return fallback;
  if (!/^\d+$/.test(String(value))) throw new ConfigError(`${name} must be an integer`);
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < minimum || number > maximum) {
    throw new ConfigError(`${name} must be between ${minimum} and ${maximum}`);
  }
  return number;
}

function containedBy(candidate, parent) {
  const relative = path.relative(parent, candidate);
  return relative === "" || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative));
}

function safeIdentifier(value, name) {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,119}$/.test(value)) {
    throw new ConfigError(`${name} contains unsupported characters`);
  }
  return value;
}

export function loadConfig(env = process.env) {
  const repoRoot = absolutePath(required(env, "WEATHER_REPO_ROOT"), "WEATHER_REPO_ROOT");
  const runId = safeIdentifier(required(env, "WEATHER_RUN_ID"), "WEATHER_RUN_ID");
  const cohortId = safeIdentifier(required(env, "WEATHER_COHORT_ID"), "WEATHER_COHORT_ID");
  const environment = safeIdentifier(env.MONITOR_ENVIRONMENT || "azure-production", "MONITOR_ENVIRONMENT");
  const stateRoot = absolutePath(
    env.WEATHER_STATE_ROOT || path.join(repoRoot, "data", "runtime", "prospective-live-t2h-168h-v1"),
    "WEATHER_STATE_ROOT",
  );
  const resultsRoot = absolutePath(env.WEATHER_RESULTS_ROOT || path.join(repoRoot, "results"), "WEATHER_RESULTS_ROOT");
  const dataPath = absolutePath(env.WEATHER_DATA_PATH || path.join(repoRoot, "data"), "WEATHER_DATA_PATH");
  const outputDir = absolutePath(env.MONITOR_OUTPUT_DIR || path.join(path.sep, "var", "lib", "weather-monitor-agent"), "MONITOR_OUTPUT_DIR");
  if (containedBy(outputDir, repoRoot)) {
    throw new ConfigError("MONITOR_OUTPUT_DIR must be outside WEATHER_REPO_ROOT and experiment evidence");
  }
  if (!/^[a-z0-9][a-z0-9_-]{0,62}$/.test(env.MONITOR_DOCKER_PROJECT || "weather-streaming-bigdata")) {
    throw new ConfigError("MONITOR_DOCKER_PROJECT must be a valid Compose project label");
  }

  const contractOverrides = [
    ["WEATHER_PROTOCOL_ID", FROZEN_CONTRACT.protocolId],
    ["WEATHER_EXPECTED_MODEL_SHA256", FROZEN_CONTRACT.modelSha256],
    ["WEATHER_EXPECTED_FEATURE_LIST_SHA256", FROZEN_CONTRACT.featureListSha256],
    ["WEATHER_EXPECTED_PROVIDER_MODEL", FROZEN_CONTRACT.providerModel],
  ];
  for (const [key, expected] of contractOverrides) {
    if (env[key] != null && env[key] !== expected) throw new ConfigError(`${key} conflicts with the frozen monitoring contract`);
  }

  const paths = Object.freeze({
    repoRoot,
    stateRoot,
    runStateDir: path.join(stateRoot, runId),
    activeRun: path.join(stateRoot, "active_run.json"),
    startRequest: path.join(stateRoot, runId, "start_request.json"),
    cohortManifest: path.join(stateRoot, runId, "cohort_manifest.json"),
    cohortStatus: path.join(stateRoot, runId, "cohort_status.json"),
    resultsRoot,
    resultDir: path.join(resultsRoot, "prospective-live-t2h", runId),
    inferenceDir: path.join(resultsRoot, "streaming-inference-t2h", runId, "spark_live"),
    inferenceBatches: path.join(resultsRoot, "streaming-inference-t2h", runId, "spark_live", "inference_batches.jsonl"),
    modelStartupValidation: path.join(resultsRoot, "streaming-inference-t2h", runId, "spark_live", "model_startup_validation.json"),
    persistenceReceipts: path.join(resultsRoot, "streaming-inference-t2h", runId, "spark_live", "forecast_persistence_receipts.jsonl"),
    opsDir: path.join(resultsRoot, "prospective-live-t2h", runId, "runtime", "ops"),
    monitorLog: path.join(resultsRoot, "prospective-live-t2h", runId, "runtime", "ops", "monitor.jsonl"),
    hourlyReports: path.join(resultsRoot, "prospective-live-t2h", runId, "runtime", "ops", "hourly_reports.jsonl"),
    dataPath,
    outputDir,
    latestSnapshot: path.join(outputDir, "latest.json"),
    historyDir: path.join(outputDir, "history"),
    lockPath: path.join(outputDir, ".agent.lock"),
  });

  return Object.freeze({
    runId,
    cohortId,
    environment,
    intervalSeconds: boundedInteger(env.MONITOR_INTERVAL_SECONDS, "MONITOR_INTERVAL_SECONDS", 300, 60, 3600),
    historyRetentionDays: boundedInteger(env.MONITOR_HISTORY_RETENTION_DAYS, "MONITOR_HISTORY_RETENTION_DAYS", 14, 1, 90),
    dockerProject: env.MONITOR_DOCKER_PROJECT || "weather-streaming-bigdata",
    contract: FROZEN_CONTRACT,
    paths,
  });
}

export function defaultRepositoryRoot() {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
}
