import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { access, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { makeEnvironment, seedCohort } from "./test_support.mjs";

const cliPath = fileURLToPath(new URL("../src/cli.mjs", import.meta.url));

test("CLI dry-run verifies pinned cohort and writes no snapshot or cohort evidence", async () => {
  const { config, env } = await makeEnvironment();
  await seedCohort(config);
  const cohortStatusBefore = await readFile(config.paths.cohortStatus, "utf8");
  const childEnv = { ...process.env, ...env };
  for (const key of [
    "WEATHER_PROTOCOL_ID",
    "WEATHER_EXPECTED_MODEL_SHA256",
    "WEATHER_EXPECTED_FEATURE_LIST_SHA256",
    "WEATHER_EXPECTED_PROVIDER_MODEL",
  ]) delete childEnv[key];

  const result = spawnSync(process.execPath, [cliPath, "--once", "--dry-run"], {
    cwd: fileURLToPath(new URL("..", import.meta.url)),
    env: childEnv,
    encoding: "utf8",
    timeout: 15000,
    windowsHide: true,
  });
  assert.equal(result.error, undefined, result.error?.message);
  assert.equal(result.status, 0, result.stderr || result.stdout);
  const snapshot = JSON.parse(result.stdout);
  assert.equal(snapshot.cohort.runId, config.runId);
  assert.equal(snapshot.cohort.cohortId, config.cohortId);
  assert.equal(snapshot.cohort.identityValidation, "PASS");
  assert.equal(snapshot.cohort.frozenContractValidation, "PASS");
  assert.equal(snapshot.collector.status, "PARTIAL");
  await assert.rejects(access(config.paths.latestSnapshot), { code: "ENOENT" });
  await assert.rejects(access(config.paths.historyDir), { code: "ENOENT" });
  assert.equal(await readFile(config.paths.cohortStatus, "utf8"), cohortStatusBefore);
});
