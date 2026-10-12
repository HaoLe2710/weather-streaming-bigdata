#!/usr/bin/env node
import { createHash } from "node:crypto";
import { access, stat } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { collectSnapshot } from "./agent.mjs";
import { loadConfig } from "./config.mjs";
import { withProcessLock } from "./lock.mjs";
import { validateRuntime } from "./runtime.mjs";
import { serializeSnapshot } from "./serializer.mjs";
import { writeSnapshot } from "./storage.mjs";

const HELP = `Weather Streaming read-only monitoring agent\n\nUsage:\n  node src/cli.mjs --validate-config\n  node src/cli.mjs --show-paths\n  node src/cli.mjs --once --dry-run\n  node src/cli.mjs --once\n`;

function parseArgs(argv) {
  const args = new Set(argv);
  if ([...args].some((value) => !["--help", "--validate-config", "--show-paths", "--once", "--dry-run"].includes(value))) {
    throw new Error("UNKNOWN_ARGUMENT");
  }
  if (args.size !== argv.length) throw new Error("DUPLICATE_ARGUMENT");
  if (args.has("--help")) return { command: "help" };
  if (args.has("--validate-config") && args.size === 1) return { command: "validate" };
  if (args.has("--show-paths") && args.size === 1) return { command: "paths" };
  if (args.has("--once") && (args.size === 1 || (args.size === 2 && args.has("--dry-run")))) {
    return { command: "once", dryRun: args.has("--dry-run") };
  }
  throw new Error("INVALID_ARGUMENT_COMBINATION");
}

async function pathStatus(filePath) {
  try {
    await access(filePath);
    const metadata = await stat(filePath);
    return { path: filePath, status: "READABLE", kind: metadata.isDirectory() ? "directory" : "file", sizeBytes: metadata.size, modifiedAt: metadata.mtime.toISOString() };
  } catch (error) {
    return { path: filePath, status: error?.code === "ENOENT" ? "MISSING" : error?.code === "EACCES" || error?.code === "EPERM" ? "UNAVAILABLE" : "INVALID", errorCode: error?.code || "PATH_ERROR" };
  }
}

async function showPaths(config) {
  const paths = [
    ["repository", config.paths.repoRoot],
    ["state_root", config.paths.stateRoot],
    ["active_run", config.paths.activeRun],
    ["start_request", config.paths.startRequest],
    ["cohort_manifest", config.paths.cohortManifest],
    ["cohort_status", config.paths.cohortStatus],
    ["inference_batches", config.paths.inferenceBatches],
    ["model_startup_validation", config.paths.modelStartupValidation],
    ["persistence_receipts", config.paths.persistenceReceipts],
    ["watchdog_monitor_log", config.paths.monitorLog],
    ["hourly_reports", config.paths.hourlyReports],
    ["data_filesystem", config.paths.dataPath],
    ["monitor_output", config.paths.outputDir],
  ];
  const statuses = await Promise.all(paths.map(async ([name, filePath]) => [name, await pathStatus(filePath)]));
  process.stdout.write(`${JSON.stringify({ status: "PATHS_RESOLVED", runId: config.runId, cohortId: config.cohortId, paths: Object.fromEntries(statuses) }, null, 2)}\n`);
}

async function once(config, { dryRun }) {
  const lockPath = dryRun
    ? path.join(os.tmpdir(), `weather-monitor-${createHash("sha256").update(`${config.paths.repoRoot}|${config.runId}`).digest("hex").slice(0, 24)}.lock`)
    : config.paths.lockPath;
  const result = await withProcessLock(lockPath, async () => {
    const snapshot = await collectSnapshot(config);
    const serialized = serializeSnapshot(snapshot);
    if (dryRun) {
      process.stdout.write(`${JSON.stringify(snapshot, null, 2)}\n`);
      return { snapshot, stored: null, ready: snapshot.cohort.identityValidation === "PASS" && snapshot.cohort.frozenContractValidation === "PASS" };
    }
    const stored = await writeSnapshot(snapshot, config);
    process.stdout.write(`${JSON.stringify({
      status: "SNAPSHOT_STORED",
      snapshotId: snapshot.snapshotId,
      collectorStatus: snapshot.collector.status,
      pipelineStatus: snapshot.pipeline.status,
      bytes: Buffer.byteLength(serialized, "utf8"),
      paths: stored,
    })}\n`);
    return { snapshot, stored, ready: true };
  });
  if (!result.acquired) {
    process.stdout.write(`${JSON.stringify({ status: "BLOCKED", reason: result.reason })}\n`);
    return 3;
  }
  return dryRun && !result.result?.ready ? 4 : 0;
}

export async function runCli(argv = process.argv.slice(2), env = process.env) {
  let args;
  try {
    args = parseArgs(argv);
  } catch (error) {
    process.stderr.write(`${error.message}\n${HELP}`);
    return 2;
  }
  if (args.command === "help") {
    process.stdout.write(HELP);
    return 0;
  }
  let config;
  try {
    config = loadConfig(env);
    await validateRuntime();
  } catch (error) {
    process.stderr.write(`${error.message === "NODE_LTS_VERSION_UNSUPPORTED" ? error.message : "AGENT_RUNTIME_PREREQUISITE_FAILED"}\n`);
    return 2;
  }
  if (args.command === "validate") {
    process.stdout.write(`${JSON.stringify({ status: "CONFIG_VALID", runId: config.runId, cohortId: config.cohortId, outputDir: config.paths.outputDir })}\n`);
    return 0;
  }
  if (args.command === "paths") {
    await showPaths(config);
    return 0;
  }
  return once(config, args);
}

if (import.meta.url === pathToFileURL(process.argv[1] || "").href) {
  runCli().then((code) => { process.exitCode = code; }).catch(() => {
    process.stderr.write("MONITOR_AGENT_FAILED\n");
    process.exitCode = 1;
  });
}
