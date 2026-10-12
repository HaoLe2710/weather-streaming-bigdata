import { spawn as nodeSpawn } from "node:child_process";
import { mkdir, open, readFile, unlink } from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";

const READY_MARKER = "WEATHER_MONITOR_LOCKED\n";
const LOCK_HELPER = `
const parentPid = Number(process.argv[1]);
process.stdout.write("WEATHER_MONITOR_LOCKED\\n");
const poll = setInterval(() => {
  try { process.kill(parentPid, 0); }
  catch (error) { if (error && error.code === "ESRCH") process.exit(0); }
}, 250);
`;

function waitForClose(child) {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) return resolve();
    child.once("close", resolve);
  });
}

async function runWithFlock(lockPath, task, spawnImpl) {
  let child;
  let output = "";
  let acquired = false;
  try {
    child = spawnImpl("flock", ["--nonblock", "--no-fork", lockPath, process.execPath, "--input-type=commonjs", "-e", LOCK_HELPER, String(process.pid)], {
      stdio: ["ignore", "pipe", "ignore"],
      windowsHide: true,
    });
    const started = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("LOCK_ACQUIRE_TIMEOUT")), 3000);
      child.once("error", (error) => {
        clearTimeout(timeout);
        reject(error);
      });
      child.stdout.setEncoding("utf8");
      child.stdout.on("data", (chunk) => {
        output += chunk;
        if (output.includes(READY_MARKER)) {
          acquired = true;
          clearTimeout(timeout);
          resolve(true);
        }
      });
      child.once("close", () => {
        clearTimeout(timeout);
        if (!acquired) resolve(false);
      });
    });
    if (!(await started)) return { acquired: false, reason: "LOCK_BUSY" };
    acquired = true;
    return { acquired: true, result: await task() };
  } catch (error) {
    if (acquired) throw error;
    if (child && !child.killed && child.exitCode === null) child.kill("SIGTERM");
    return { acquired: false, reason: error?.code === "ENOENT" ? "FLOCK_UNAVAILABLE" : "LOCK_UNAVAILABLE" };
  } finally {
    if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    if (child) await waitForClose(child);
  }
}

async function pidIsAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code === "EPERM";
  }
}

async function runWithPidFile(lockPath, task) {
  const token = `${process.pid}:${randomUUID()}`;
  const body = JSON.stringify({ token, pid: process.pid, createdAt: new Date().toISOString() });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    let handle;
    try {
      handle = await open(lockPath, "wx", 0o600);
      await handle.writeFile(body, "utf8");
      await handle.sync();
      await handle.close();
      handle = null;
      break;
    } catch (error) {
      await handle?.close().catch(() => {});
      if (error?.code !== "EEXIST") return { acquired: false, reason: "LOCK_UNAVAILABLE" };
      let prior;
      try {
        prior = JSON.parse(await readFile(lockPath, "utf8"));
      } catch {
        return { acquired: false, reason: "LOCK_UNVERIFIED" };
      }
      if (await pidIsAlive(prior.pid)) return { acquired: false, reason: "LOCK_BUSY" };
      try {
        await unlink(lockPath);
      } catch {
        return { acquired: false, reason: "LOCK_BUSY" };
      }
      if (attempt === 1) return { acquired: false, reason: "LOCK_BUSY" };
    }
  }
  try {
    return { acquired: true, result: await task() };
  } finally {
    try {
      const current = JSON.parse(await readFile(lockPath, "utf8"));
      if (current.token === token) await unlink(lockPath);
    } catch {
      // A missing or replaced lock is left untouched; the next run fails closed or reclaims a dead PID.
    }
  }
}

export async function withProcessLock(lockPath, task, { platform = process.platform, spawn = nodeSpawn } = {}) {
  await mkdir(path.dirname(lockPath), { recursive: true, mode: 0o750 });
  if (platform === "linux") return runWithFlock(lockPath, task, spawn);
  return runWithPidFile(lockPath, task);
}
