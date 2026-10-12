import { statfs } from "node:fs/promises";
import os from "node:os";
import { performance } from "node:perf_hooks";
import { collectorResult } from "../collector-utils.mjs";

function cpuTotals(cpus) {
  let total = 0;
  let idle = 0;
  for (const cpu of cpus) {
    for (const value of Object.values(cpu.times)) total += value;
    idle += cpu.times.idle;
  }
  return { total, idle };
}

export async function collectHost(config, { wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms)), statfsFn = statfs } = {}) {
  const started = performance.now();
  const warnings = [];
  const before = cpuTotals(os.cpus());
  await wait(120);
  const after = cpuTotals(os.cpus());
  const totalDelta = after.total - before.total;
  const idleDelta = after.idle - before.idle;
  const cpuUsagePercent = totalDelta > 0 ? Math.max(0, Math.min(100, (1 - idleDelta / totalDelta) * 100)) : null;
  const memoryTotalBytes = os.totalmem();
  const memoryAvailableBytes = os.freemem();
  let disk = null;
  try {
    const stats = await statfsFn(config.paths.dataPath);
    const blockSize = Number(stats.bsize);
    const totalBytes = blockSize * Number(stats.blocks);
    const freeBytes = blockSize * Number(stats.bfree);
    const availableBytes = blockSize * Number(stats.bavail);
    if (![blockSize, totalBytes, freeBytes, availableBytes].every((value) => Number.isFinite(value) && value >= 0) || blockSize === 0) {
      throw Object.assign(new Error("invalid statfs values"), { code: "EIO" });
    }
    disk = {
      path: config.paths.dataPath,
      totalBytes,
      usedBytes: Math.max(0, totalBytes - freeBytes),
      freeBytes,
      availableBytes,
      usagePercent: totalBytes > 0 ? Math.round(((totalBytes - freeBytes) / totalBytes) * 10000) / 100 : null,
    };
  } catch (error) {
    warnings.push(error?.code === "EACCES" || error?.code === "EPERM" ? "HOST_DATA_FILESYSTEM_PERMISSION_DENIED" : "HOST_DATA_FILESYSTEM_UNAVAILABLE");
  }
  return collectorResult(warnings.length ? "PARTIAL" : "PASS", {
    hostname: os.hostname(),
    uptimeSeconds: Math.floor(os.uptime()),
    loadAverage: os.loadavg().every((value) => value === 0) && process.platform === "win32" ? null : os.loadavg(),
    cpuUsagePercent: Number.isFinite(cpuUsagePercent) ? Math.round(cpuUsagePercent * 100) / 100 : null,
    memory: {
      totalBytes: memoryTotalBytes,
      usedBytes: Math.max(0, memoryTotalBytes - memoryAvailableBytes),
      availableBytes: memoryAvailableBytes,
      usagePercent: memoryTotalBytes > 0 ? Math.round(((memoryTotalBytes - memoryAvailableBytes) / memoryTotalBytes) * 10000) / 100 : null,
    },
    disk,
    collectorDurationMs: Math.round((performance.now() - started) * 100) / 100,
  }, warnings);
}
