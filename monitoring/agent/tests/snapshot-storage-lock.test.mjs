import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { loadConfig } from "../src/config.mjs";
import { normalizeSnapshot } from "../src/normalize.mjs";
import { serializeSnapshot, validateSnapshot } from "../src/serializer.mjs";
import { writeSnapshot } from "../src/storage.mjs";
import { withProcessLock } from "../src/lock.mjs";
import { makeEnvironment, TEST_NOW } from "./test_support.mjs";

function passingResults() {
  return Object.fromEntries(["host", "docker", "cohort", "inference", "watchdog"].map((name) => [name, { status: "PASS", warnings: [], data: {} }]));
}

test("snapshot has UTC time, explicit pipeline/collector states and no raw fields", () => {
  const root = path.join(os.tmpdir(), "weather-agent-sync-tests");
  const config = loadConfig({
    WEATHER_REPO_ROOT: path.join(root, "repo"),
    WEATHER_RUN_ID: "run-test",
    WEATHER_COHORT_ID: "cohort-test",
    MONITOR_OUTPUT_DIR: path.join(root, "monitor-output"),
  });
  const results = passingResults();
  results.cohort.data = {
    cohort: { runId: config.runId, cohortId: config.cohortId, identityValidation: "PASS", frozenContractValidation: "PASS", status: "COLLECTING" },
    progress: { microbatchUpdateCount: 0, forecastsPersisted: null },
    quality: { duplicates: {} },
  };
  results.inference.data = { modelContractStatus: "UNKNOWN", latestBatchMetrics: null };
  results.watchdog.data = { latestReport: { status: "PASS", checkedAt: TEST_NOW }, latestAudit: { effectiveStatus: "PASS" } };
  results.docker.data = { status: "PASS", containers: [] };
  results.host.data = { hostname: "test-host" };
  const snapshot = normalizeSnapshot(config, results, { timestamp: TEST_NOW, snapshotId: "snapshot-test" });
  assert.equal(validateSnapshot(snapshot).valid, true);
  assert.equal(snapshot.timestamp, TEST_NOW);
  assert.equal(snapshot.progress.microbatchUpdateCount, 0);
  assert.equal(snapshot.progress.forecastsPersisted, null);
  assert.equal(snapshot.pipeline.status, "COLLECTING");
  assert.equal(snapshot.collector.status, "COMPLETE");
  assert.equal(serializeSnapshot(snapshot).includes("temperature_c"), false);
});

test("snapshot validation rejects bad count, non-UTC timestamp and raw weather fields", () => {
  const root = path.join(os.tmpdir(), "weather-agent-sync-tests");
  const config = loadConfig({ WEATHER_REPO_ROOT: path.join(root, "repo"), WEATHER_RUN_ID: "run-test", WEATHER_COHORT_ID: "cohort-test", MONITOR_OUTPUT_DIR: path.join(root, "monitor-output") });
  const results = passingResults();
  results.cohort.data = { cohort: { runId: config.runId, cohortId: config.cohortId, identityValidation: "PASS", frozenContractValidation: "PASS" }, progress: { microbatchUpdateCount: -1 } };
  results.docker.data = { status: "PASS" };
  const snapshot = normalizeSnapshot(config, results, { timestamp: "2026-10-12T12:00:00+07:00" });
  snapshot.progress.microbatchUpdateCount = -1;
  snapshot.weatherRaw = { temperature_c: 9 };
  const validation = validateSnapshot(snapshot);
  assert.equal(validation.valid, false);
  assert.ok(validation.errors.some((error) => error.includes("expected_utc_iso8601")));
  assert.ok(validation.errors.some((error) => error.includes("invalid_count")));
  assert.ok(validation.errors.some((error) => error.includes("forbidden_field")));
});

test("atomic latest and append-only history writes stay outside cohort evidence", async () => {
  const { config } = await makeEnvironment();
  const evidence = path.join(config.paths.runStateDir, "cohort_status.json");
  await writeFile(evidence, "evidence-before\n");
  const results = passingResults();
  results.cohort.data = { cohort: { runId: config.runId, cohortId: config.cohortId, identityValidation: "PASS", frozenContractValidation: "PASS", status: "COLLECTING" }, progress: {} };
  results.docker.data = { status: "PASS", containers: [] };
  const snapshot = normalizeSnapshot(config, results, { timestamp: TEST_NOW, snapshotId: "snapshot-store-test" });
  const written = await writeSnapshot(snapshot, config, { now: new Date(TEST_NOW) });
  assert.equal(JSON.parse(await readFile(written.latestPath, "utf8")).snapshotId, "snapshot-store-test");
  assert.equal(JSON.parse((await readFile(written.historyPath, "utf8")).trim()).snapshotId, "snapshot-store-test");
  assert.equal(await readFile(evidence, "utf8"), "evidence-before\n");
  const outputStat = await stat(config.paths.outputDir);
  assert.equal(outputStat.isDirectory(), true);
});

test("retention removes only dated agent history files", async () => {
  const { config, directory } = await makeEnvironment();
  await mkdir(config.paths.historyDir, { recursive: true });
  const expiredAgentFile = path.join(config.paths.historyDir, "2020-01-01.jsonl");
  await writeFile(expiredAgentFile, "old\n");
  const unrelatedEvidence = path.join(directory, "results", "prospective-live-t2h", "old.jsonl");
  await mkdir(path.dirname(unrelatedEvidence), { recursive: true });
  await writeFile(unrelatedEvidence, "keep\n");
  const results = passingResults();
  results.cohort.data = { cohort: { runId: config.runId, cohortId: config.cohortId, identityValidation: "PASS", frozenContractValidation: "PASS", status: "COLLECTING" }, progress: {} };
  results.docker.data = { status: "PASS", containers: [] };
  const snapshot = normalizeSnapshot(config, results, { timestamp: TEST_NOW, snapshotId: "retention-test" });
  await writeSnapshot(snapshot, config, { now: new Date(TEST_NOW) });
  await assert.rejects(stat(expiredAgentFile), { code: "ENOENT" });
  assert.equal(await readFile(unrelatedEvidence, "utf8"), "keep\n");
});

test("concurrent process attempts fail closed while a first attempt holds the lock", async () => {
  const { directory } = await makeEnvironment();
  const lockPath = path.join(directory, "monitor", ".agent.lock");
  let release;
  let entered;
  const started = new Promise((resolve) => { entered = resolve; });
  const gate = new Promise((resolve) => { release = resolve; });
  const first = withProcessLock(lockPath, async () => { entered(); await gate; return "first"; }, { platform: "win32" });
  await started;
  const second = await withProcessLock(lockPath, async () => "should-not-run", { platform: "win32" });
  assert.deepEqual(second, { acquired: false, reason: "LOCK_BUSY" });
  release();
  assert.equal((await first).result, "first");
  const third = await withProcessLock(lockPath, async () => "released", { platform: "win32" });
  assert.equal(third.result, "released");
});

test("crashed process lock is reclaimed after the recorded PID is no longer alive", async () => {
  const { directory } = await makeEnvironment();
  const lockPath = path.join(directory, "monitor", ".agent.lock");
  await mkdir(path.dirname(lockPath), { recursive: true });
  await writeFile(lockPath, JSON.stringify({ token: "crashed", pid: 2147483647, createdAt: "2020-01-01T00:00:00Z" }));
  const result = await withProcessLock(lockPath, async () => "recovered", { platform: "win32" });
  assert.equal(result.acquired, true);
  assert.equal(result.result, "recovered");
});

test("configuration refuses output paths inside the repository", () => {
  const root = path.join(os.tmpdir(), "weather-agent-config-test");
  assert.throws(() => loadConfig({
    WEATHER_REPO_ROOT: path.join(root, "repo"),
    WEATHER_RUN_ID: "run-test",
    WEATHER_COHORT_ID: "cohort-test",
    MONITOR_OUTPUT_DIR: path.join(root, "repo", "results", "prospective-live-t2h"),
  }), /outside WEATHER_REPO_ROOT/);
});

test("Linux flock helper releases lock when the protected operation ends", { skip: process.platform !== "linux" }, async () => {
  const { directory } = await makeEnvironment();
  const lockPath = path.join(directory, "linux", ".agent.lock");
  const first = await withProcessLock(lockPath, async () => "locked", { platform: "linux" });
  assert.equal(first.acquired, true);
  assert.equal(first.result, "locked");
  const second = await withProcessLock(lockPath, async () => "released", { platform: "linux" });
  assert.equal(second.acquired, true);
  assert.equal(second.result, "released");
});

test("Linux lock remains held through the task, rejects overlap, and releases after task failure", { skip: process.platform !== "linux" }, async () => {
  const { directory } = await makeEnvironment();
  const lockPath = path.join(directory, "linux-concurrent", ".agent.lock");
  let release;
  let entered;
  const started = new Promise((resolve) => { entered = resolve; });
  const gate = new Promise((resolve) => { release = resolve; });
  const first = withProcessLock(lockPath, async () => { entered(); await gate; return "first"; }, { platform: "linux" });
  await started;
  const second = await withProcessLock(lockPath, async () => "must-not-run", { platform: "linux" });
  assert.deepEqual(second, { acquired: false, reason: "LOCK_BUSY" });
  release();
  assert.equal((await first).result, "first");

  await assert.rejects(
    withProcessLock(lockPath, async () => { throw new Error("TASK_FAILURE"); }, { platform: "linux" }),
    /TASK_FAILURE/,
  );
  const afterFailure = await withProcessLock(lockPath, async () => "released-after-error", { platform: "linux" });
  assert.equal(afterFailure.result, "released-after-error");
});

test("Linux flock helper releases lock after its owning process is killed", { skip: process.platform !== "linux" }, async () => {
  const { directory } = await makeEnvironment();
  const lockPath = path.join(directory, "linux-crash", ".agent.lock");
  const moduleUrl = new URL("../src/lock.mjs", import.meta.url).href;
  const code = `import { withProcessLock } from ${JSON.stringify(moduleUrl)}; await withProcessLock(process.argv[1], async () => { console.log("LOCK_HELD"); await new Promise(() => {}); }, { platform: "linux" });`;
  const owner = spawn(process.execPath, ["--input-type=module", "-e", code, lockPath], { stdio: ["ignore", "pipe", "pipe"] });
  let output = "";
  const acquired = new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("LOCK_HELPER_START_TIMEOUT")), 5000);
    owner.stdout.setEncoding("utf8");
    owner.stdout.on("data", (chunk) => {
      output += chunk;
      if (output.includes("LOCK_HELD")) {
        clearTimeout(timeout);
        resolve();
      }
    });
    owner.once("error", (error) => { clearTimeout(timeout); reject(error); });
    owner.once("close", (code, signal) => {
      if (!output.includes("LOCK_HELD")) {
        clearTimeout(timeout);
        reject(new Error(`LOCK_OWNER_EXITED:${code ?? signal}`));
      }
    });
  });

  try {
    await acquired;
    owner.kill("SIGKILL");
    await once(owner, "close");
    let recovered;
    const deadline = Date.now() + 5000;
    do {
      recovered = await withProcessLock(lockPath, async () => "recovered-after-crash", { platform: "linux" });
      if (recovered.acquired) break;
      await delay(100);
    } while (Date.now() < deadline);
    assert.equal(recovered.acquired, true, `lock not released: ${recovered.reason}`);
    assert.equal(recovered.result, "recovered-after-crash");
  } finally {
    if (owner.exitCode === null && owner.signalCode === null) owner.kill("SIGKILL");
  }
});
