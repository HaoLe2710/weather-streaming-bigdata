import { performance } from "node:perf_hooks";
import { execFileText } from "../command.mjs";
import { collectorResult, nonNegativeInteger } from "../collector-utils.mjs";

export const WEATHER_SERVICES = Object.freeze([
  "broker",
  "spark-master",
  "spark-worker",
  "live-hourly-producer-t2h",
  "streaming-inference-t2h-live",
]);

function containerId(value) {
  return typeof value === "string" && /^[a-f0-9]{12,64}$/i.test(value) ? value : null;
}

function parseStats(bytes) {
  if (typeof bytes !== "string") return null;
  const match = bytes.trim().match(/^([\d.]+)\s*(B|KB|KIB|MB|MIB|GB|GIB|TB|TIB)$/i);
  if (!match) return null;
  const unit = match[2].toUpperCase();
  const power = unit.endsWith("IB") ? 1024 : 1000;
  const suffix = unit.replace(/I?B$/, "");
  const exponent = { "": 0, K: 1, M: 2, G: 3, T: 4 }[suffix];
  return Math.round(Number(match[1]) * power ** exponent);
}

export async function collectDocker(
  config,
  { exec = execFileText, platform = process.platform } = {},
) {
  const started = performance.now();
  if (platform !== "linux") {
    return collectorResult("UNAVAILABLE", { status: "UNAVAILABLE", containers: [] }, ["DOCKER_HOST_COLLECTION_REQUIRES_LINUX_RUNTIME"]);
  }
  let output;
  try {
    output = await exec("docker", [
      "ps", "--all", "--filter", `label=com.docker.compose.project=${config.dockerProject}`,
      "--format", '{{.ID}}|{{.Names}}|{{.State}}|{{.Status}}|{{.RunningFor}}|{{.Label "com.docker.compose.service"}}',
    ], { timeoutMs: 4000, maxBuffer: 512 * 1024 });
  } catch (error) {
    const reason = error?.code === "ENOENT" ? "DOCKER_CLI_UNAVAILABLE" : "DOCKER_ACCESS_UNAVAILABLE";
    return collectorResult("UNAVAILABLE", { status: "UNAVAILABLE", containers: [], reason }, [reason]);
  }

  const warnings = [];
  const selected = output.split(/\r?\n/).filter(Boolean).map((line) => {
    const [ID, Names, State, Status, RunningFor, Service] = line.split("|");
    return { ID, Names, State, Status, RunningFor, Service };
  }).filter((row) => WEATHER_SERVICES.includes(row.Service)).slice(0, WEATHER_SERVICES.length);
  const byService = new Map(selected.map((row) => [row.Service, row]));
  const ids = selected.map((row) => containerId(row.ID)).filter(Boolean);
  if (ids.length !== selected.length) warnings.push("DOCKER_CONTAINER_ID_INVALID");

  const inspectById = new Map();
  if (ids.length) {
    const format = "{{.Id}}|{{.RestartCount}}|{{.State.StartedAt}}|{{.State.Status}}{{if .State.Health}}|{{.State.Health.Status}}{{end}}";
    try {
      const details = await exec("docker", ["inspect", "--format", format, ...ids], { timeoutMs: 4000, maxBuffer: 128 * 1024 });
      for (const line of details.split(/\r?\n/).filter(Boolean)) {
        const [id, restart, startedAt, state, health] = line.split("|");
        if (id) inspectById.set(id.slice(0, 12), { restartCount: Number(restart), startedAt, state, health: health || null });
      }
    } catch {
      warnings.push("DOCKER_INSPECT_UNAVAILABLE");
    }
  }

  const runningIds = selected.filter((row) => row.State === "running").map((row) => containerId(row.ID)).filter(Boolean);
  const statsById = new Map();
  if (runningIds.length) {
    try {
      const stats = await exec("docker", ["stats", "--no-stream", "--format", "{{.ID}}|{{.CPUPerc}}|{{.MemUsage}}", ...runningIds], {
        timeoutMs: 5000,
        maxBuffer: 128 * 1024,
      });
      for (const line of stats.split(/\r?\n/).filter(Boolean)) {
        const [id, cpu, memory] = line.split("|");
        const [used, limit] = String(memory || "").split(" / ");
        if (id) statsById.set(id.slice(0, 12), {
          cpuPercent: Number.parseFloat(cpu),
          memoryUsageBytes: parseStats(used),
          memoryLimitBytes: parseStats(limit),
        });
      }
    } catch {
      warnings.push("DOCKER_STATS_UNAVAILABLE");
    }
  }

  const containers = WEATHER_SERVICES.map((service) => {
    const row = byService.get(service);
    if (!row) return { service, status: "MISSING", health: null, restartCount: null, uptimeSeconds: null, cpuPercent: null, memoryUsageBytes: null, memoryLimitBytes: null };
    const id = containerId(row.ID);
    const inspected = inspectById.get(id?.slice(0, 12)) || {};
    const stats = statsById.get(id?.slice(0, 12)) || {};
    const startedAt = Date.parse(inspected.startedAt || "");
    const healthFromStatus = typeof row.Status === "string" ? row.Status.match(/\((healthy|unhealthy|starting)\)/i)?.[1]?.toLowerCase() : null;
    return {
      service,
      name: String(row.Names || service).slice(0, 120),
      status: inspected.state || row.State || "unknown",
      statusDetail: String(row.Status || "unknown").slice(0, 120),
      health: inspected.health || healthFromStatus,
      restartCount: nonNegativeInteger(inspected.restartCount),
      uptimeSeconds: Number.isFinite(startedAt) && startedAt > 0 ? Math.max(0, Math.floor((Date.now() - startedAt) / 1000)) : null,
      cpuPercent: Number.isFinite(stats.cpuPercent) && stats.cpuPercent >= 0 ? stats.cpuPercent : null,
      memoryUsageBytes: stats.memoryUsageBytes ?? null,
      memoryLimitBytes: stats.memoryLimitBytes ?? null,
    };
  });
  const presentServices = new Set(selected.map((row) => row.Service));
  const missingServices = WEATHER_SERVICES.filter((service) => !presentServices.has(service));
  if (missingServices.length) warnings.push("DOCKER_EXPECTED_SERVICES_MISSING");
  const status = warnings.length ? "PARTIAL" : "PASS";
  return collectorResult(status, {
    status,
    project: config.dockerProject,
    containers,
    missingServices,
    collectorDurationMs: Math.round((performance.now() - started) * 100) / 100,
  }, warnings);
}
