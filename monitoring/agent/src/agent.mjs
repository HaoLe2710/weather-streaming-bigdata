import { performance } from "node:perf_hooks";
import { collectCohort } from "./collectors/cohort.mjs";
import { collectDocker } from "./collectors/docker.mjs";
import { collectHost } from "./collectors/host.mjs";
import { collectInference } from "./collectors/inference.mjs";
import { collectWatchdog } from "./collectors/watchdog.mjs";
import { normalizeSnapshot } from "./normalize.mjs";
import { serializeSnapshot } from "./serializer.mjs";

async function safeCollect(collector, config) {
  try {
    return await collector(config);
  } catch {
    return { status: "UNAVAILABLE", data: {}, warnings: ["COLLECTOR_UNEXPECTED_FAILURE"] };
  }
}

export async function collectSnapshot(config, { now = new Date(), collectors = {} } = {}) {
  const started = performance.now();
  const selected = {
    cohort: collectors.cohort || collectCohort,
    inference: collectors.inference || collectInference,
    watchdog: collectors.watchdog || collectWatchdog,
    docker: collectors.docker || collectDocker,
    host: collectors.host || collectHost,
  };
  const entries = await Promise.all(Object.entries(selected).map(async ([name, collector]) => [name, await safeCollect(collector, config)]));
  const results = Object.fromEntries(entries);
  const snapshot = normalizeSnapshot(config, results, {
    timestamp: now.toISOString(),
    durationMs: Math.round((performance.now() - started) * 100) / 100,
  });
  serializeSnapshot(snapshot);
  return snapshot;
}
