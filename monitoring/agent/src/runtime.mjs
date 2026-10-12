import { execFileText } from "./command.mjs";

export async function validateRuntime({ nodeVersion = process.versions.node, platform = process.platform, exec = execFileText } = {}) {
  const major = Number(String(nodeVersion).split(".")[0]);
  if (!Number.isSafeInteger(major) || major < 20 || major >= 25) throw new Error("NODE_LTS_VERSION_UNSUPPORTED");
  if (platform === "linux") {
    await exec("flock", ["--version"], { timeoutMs: 2500, maxBuffer: 16 * 1024 });
  }
  return { nodeVersion, lockProvider: platform === "linux" ? "flock" : "pid-file-development" };
}
