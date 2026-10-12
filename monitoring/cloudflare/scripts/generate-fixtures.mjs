import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const directory = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(directory, "..");
const schema = JSON.parse(await readFile(path.resolve(root, "../agent/schema/monitoring-snapshot-v1.schema.json"), "utf8"));
const timestamp = "2026-10-12T12:00:00.000Z";

function resolveRef(ref) {
  if (!ref?.startsWith("#/$defs/")) throw new Error(`Unsupported fixture schema ref: ${ref}`);
  return schema.$defs[ref.slice("#/$defs/".length)];
}

function sample(definition, preferNonNull = false) {
  const current = definition.$ref ? resolveRef(definition.$ref) : definition;
  if ("const" in current) return current.const;
  if (current.enum) return current.enum[0];
  let type = current.type;
  if (Array.isArray(type)) {
    type = preferNonNull ? type.find((item) => item !== "null") : type.includes("null") ? "null" : type[0];
  }
  if (type === "null") return null;
  if (type === "object") {
    const result = {};
    for (const key of current.required || []) result[key] = sample(current.properties[key]);
    return result;
  }
  if (type === "array") {
    return Array.from({ length: current.minItems || 0 }, () => sample(current.items));
  }
  if (type === "integer" || type === "number") return current.minimum ?? 0;
  if (type === "boolean") return false;
  if (type === "string") {
    if (current.format === "date-time") return timestamp;
    if (current.pattern?.includes("{64}")) return "a".repeat(64);
    return "x".repeat(Math.max(1, current.minLength || 1));
  }
  throw new Error(`Unsupported fixture schema type: ${String(type)}`);
}

const healthy = sample(schema);
healthy.snapshotId = "10000000-0000-4000-8000-000000000001";
healthy.timestamp = timestamp;
healthy.source.environment = "local";
healthy.source.agentVersion = "1.0.0-fixture";
healthy.source.hostname = "phase2a-synthetic";
Object.assign(healthy.cohort, {
  runId: "20261010T111433Z-prospective-live-t2h-v1",
  cohortId: "prospective-t2h-20261010T120000Z",
  protocolId: "T2H_LIVE_PROSPECTIVE_168H_V1",
  status: "COLLECTING",
  startsAt: "2026-10-10T12:00:00Z",
  endsAt: "2026-10-17T11:00:00Z",
  targetHours: 168,
  targetLocations: 63,
  targetForecastSlots: 10584,
  lastUpdateAt: timestamp,
  statusFileUpdatedAt: timestamp,
  identityValidation: "PASS",
  frozenContractValidation: "PASS",
  modelSha256: "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a",
  featureListSha256: "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2",
  providerModel: "ecmwf_ifs",
  forecastHorizonHours: 2,
});
healthy.inference.modelSha256 = healthy.cohort.modelSha256;
healthy.inference.expectedModelSha256 = healthy.cohort.modelSha256;
healthy.inference.modelContractStatus = "PASS";
healthy.infrastructure.host = sample(schema.$defs.hostOrNull, true);
healthy.infrastructure.host.hostname = healthy.source.hostname;
healthy.infrastructure.host.disk = sample(schema.$defs.diskOrNull, true);
healthy.infrastructure.host.disk.usagePercent = 50;
healthy.infrastructure.watchdog.latestReport = sample(schema.$defs.watchdogReportOrNull, true);
healthy.infrastructure.watchdog.latestReport.status = "PASS";
healthy.infrastructure.watchdog.latestReport.checkedAt = timestamp;
healthy.infrastructure.watchdog.latestReport.cohortRunId = healthy.cohort.runId;
healthy.infrastructure.watchdog.latestReport.cohortId = healthy.cohort.cohortId;
healthy.quality.status = "PASS";
healthy.collector.status = "COMPLETE";
healthy.pipeline.status = "RUNNING";

const fixtures = {
  "snapshot-healthy.json": structuredClone(healthy),
  "snapshot-partial.json": {
    ...structuredClone(healthy),
    snapshotId: "20000000-0000-4000-8000-000000000002",
    collector: { ...healthy.collector, status: "PARTIAL", warnings: [{ collector: "docker", code: "DOCKER_ACCESS_UNAVAILABLE" }] },
  },
  "snapshot-quality-fail.json": {
    ...structuredClone(healthy),
    snapshotId: "30000000-0000-4000-8000-000000000003",
    quality: { ...healthy.quality, status: "FAIL", pipelineDataErrors: ["DATA_QUALITY_FAILED"] },
  },
  "snapshot-contract-mismatch.json": {
    ...structuredClone(healthy),
    snapshotId: "40000000-0000-4000-8000-000000000004",
    cohort: { ...healthy.cohort, cohortId: "synthetic-foreign-cohort" },
  },
  "snapshot-stale.json": {
    ...structuredClone(healthy),
    snapshotId: "50000000-0000-4000-8000-000000000005",
    timestamp: "2026-10-12T10:00:00.000Z",
  },
  "snapshot-recovered.json": {
    ...structuredClone(healthy),
    snapshotId: "60000000-0000-4000-8000-000000000006",
    quality: { ...healthy.quality, status: "PASS", pipelineDataErrors: [] },
    collector: { ...healthy.collector, status: "COMPLETE" },
    infrastructure: {
      ...healthy.infrastructure,
      watchdog: {
        ...healthy.infrastructure.watchdog,
        latestReport: { ...healthy.infrastructure.watchdog.latestReport, status: "PASS", checkedAt: timestamp },
      },
    },
  },
};

const output = path.resolve(root, "fixtures");
await mkdir(output, { recursive: true });
for (const [name, value] of Object.entries(fixtures)) {
  await writeFile(path.join(output, name), `${JSON.stringify(value, null, 2)}\n`, "utf8");
}
console.log(`Generated ${Object.keys(fixtures).length} synthetic schema-derived fixtures.`);
