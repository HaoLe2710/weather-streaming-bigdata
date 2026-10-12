import { randomUUID } from "node:crypto";
import { open, lstat, mkdir, readdir, rename, unlink } from "node:fs/promises";
import path from "node:path";
import { MAX_SNAPSHOT_BYTES, serializeSnapshot } from "./serializer.mjs";

const MAX_DAILY_HISTORY_BYTES = 32 * 1024 * 1024;

function isWithin(candidate, parent) {
  const relative = path.relative(parent, candidate);
  return relative === "" || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative));
}

function utcDate(date) {
  return date.toISOString().slice(0, 10);
}

async function trimHistory(historyDir, retentionDays, now) {
  const entries = await readdir(historyDir, { withFileTypes: true });
  const cutoff = new Date(now.getTime() - retentionDays * 86400000);
  for (const entry of entries) {
    if (!entry.isFile() || !/^\d{4}-\d{2}-\d{2}\.jsonl$/.test(entry.name)) continue;
    const date = new Date(`${entry.name.slice(0, 10)}T00:00:00Z`);
    if (!Number.isFinite(date.getTime()) || date >= cutoff) continue;
    const target = path.resolve(historyDir, entry.name);
    if (!isWithin(target, historyDir)) continue;
    const stat = await lstat(target);
    if (stat.isFile() && !stat.isSymbolicLink()) await unlink(target);
  }
}

async function atomicWriteJson(filePath, serialized) {
  const temporaryPath = `${filePath}.${process.pid}.${randomUUID()}.tmp`;
  let handle;
  try {
    handle = await open(temporaryPath, "wx", 0o600);
    await handle.writeFile(`${serialized}\n`, "utf8");
    await handle.sync();
    await handle.close();
    handle = null;
    await rename(temporaryPath, filePath);
    try {
      const directory = await open(path.dirname(filePath), "r");
      await directory.sync();
      await directory.close();
    } catch {
      // Some supported filesystems do not allow fsync on a directory handle.
    }
  } finally {
    await handle?.close().catch(() => {});
    await unlink(temporaryPath).catch(() => {});
  }
}

export async function writeSnapshot(snapshot, config, { now = new Date() } = {}) {
  const serialized = serializeSnapshot(snapshot);
  const line = `${serialized}\n`;
  if (Buffer.byteLength(line, "utf8") > MAX_SNAPSHOT_BYTES) throw new Error("SNAPSHOT_TOO_LARGE");
  const historyDir = config.paths.historyDir;
  if (isWithin(historyDir, config.paths.repoRoot)) throw new Error("HISTORY_DIR_MUST_BE_OUTSIDE_EXPERIMENT_REPOSITORY");
  await mkdir(historyDir, { recursive: true, mode: 0o750 });
  await trimHistory(historyDir, config.historyRetentionDays, now);
  const historyPath = path.join(historyDir, `${utcDate(now)}.jsonl`);
  const historyStat = await lstat(historyPath).catch((error) => error?.code === "ENOENT" ? null : Promise.reject(error));
  if (historyStat && (!historyStat.isFile() || historyStat.isSymbolicLink())) throw new Error("HISTORY_TARGET_NOT_REGULAR_FILE");
  if (historyStat && historyStat.size + Buffer.byteLength(line, "utf8") > MAX_DAILY_HISTORY_BYTES) throw new Error("DAILY_HISTORY_LIMIT_REACHED");
  const history = await open(historyPath, "a", 0o600);
  try {
    await history.write(line, null, "utf8");
    await history.sync();
  } finally {
    await history.close();
  }
  await atomicWriteJson(config.paths.latestSnapshot, serialized);
  return { latestPath: config.paths.latestSnapshot, historyPath, bytes: Buffer.byteLength(line, "utf8") };
}
