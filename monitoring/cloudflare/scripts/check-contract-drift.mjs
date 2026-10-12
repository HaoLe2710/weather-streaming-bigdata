import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const directory = path.dirname(fileURLToPath(import.meta.url));
const phase1Config = await import(pathToFileURL(path.resolve(directory, "../../agent/src/config.mjs")));
const phase1 = phase1Config.FROZEN_CONTRACT;
const workerSource = await readFile(path.resolve(directory, "../src/config/contract.ts"), "utf8");
const expected = {
  protocolId: phase1.protocolId,
  targetHours: phase1.targetHours,
  targetLocations: phase1.targetLocations,
  targetForecastSlots: phase1.targetSlots,
  startsAt: phase1.cohortStartTargetTime,
  endsAt: phase1.cohortEndTargetTime,
  modelSha256: phase1.modelSha256,
  featureListSha256: phase1.featureListSha256,
  providerModel: phase1.providerModel,
  forecastHorizonHours: phase1.forecastHorizonHours,
  runId: "20261010T111433Z-prospective-live-t2h-v1",
  cohortId: "prospective-t2h-20261010T120000Z",
};

for (const [key, value] of Object.entries(expected)) {
  const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = workerSource.match(new RegExp(`\\b${escapedKey}:\\s*(?:"([^"]*)"|(\\d+))`));
  assert.ok(match, `Worker frozen contract must contain ${key}`);
  const actual = match[1] ?? Number(match[2]);
  assert.equal(actual, value, `Worker frozen contract drifted for ${key}`);
}

console.log("Worker frozen contract matches the Phase 1 source and pinned cohort identity.");
