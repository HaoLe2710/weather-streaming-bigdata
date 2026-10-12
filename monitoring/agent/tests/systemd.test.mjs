import test from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { copyFile, lstat, mkdir, mkdtemp, readFile, readlink } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const unitDirectory = fileURLToPath(new URL("../../deployment/systemd/", import.meta.url));

test("systemd units are installable and timer enablement succeeds on Linux", { skip: process.platform !== "linux" }, async () => {
  const servicePath = path.join(unitDirectory, "weather-monitor-agent.service");
  const timerPath = path.join(unitDirectory, "weather-monitor-agent.timer");
  const [service, timer] = await Promise.all([readFile(servicePath, "utf8"), readFile(timerPath, "utf8")]);

  assert.match(service, /^\[Service\]$/m);
  assert.match(service, /^Type=oneshot$/m);
  assert.match(service, /^ExecStart=\/usr\/bin\/env node \/opt\/weather-monitor-agent\/monitoring\/agent\/src\/cli\.mjs --once$/m);
  assert.match(timer, /^\[Timer\]$/m);
  assert.match(timer, /^Unit=weather-monitor-agent\.service$/m);
  assert.match(timer, /^\[Install\]$/m);
  assert.match(timer, /^WantedBy=timers\.target$/m);

  const root = await mkdtemp(path.join(os.tmpdir(), "weather-monitor-systemd-root-"));
  const etcSystemd = path.join(root, "etc", "systemd", "system");
  await mkdir(etcSystemd, { recursive: true });
  await Promise.all([
    copyFile(servicePath, path.join(etcSystemd, "weather-monitor-agent.service")),
    copyFile(timerPath, path.join(etcSystemd, "weather-monitor-agent.timer")),
  ]);

  execFileSync("systemctl", [`--root=${root}`, "enable", "--no-reload", "weather-monitor-agent.timer"], { stdio: "pipe" });
  const enabledLink = path.join(etcSystemd, "timers.target.wants", "weather-monitor-agent.timer");
  assert.equal((await lstat(enabledLink)).isSymbolicLink(), true);
  assert.equal(path.basename(await readlink(enabledLink)), "weather-monitor-agent.timer");
});
