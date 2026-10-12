import { execFile as nodeExecFile } from "node:child_process";

export function execFileText(file, args, { timeoutMs = 2500, maxBuffer = 512 * 1024, execFile = nodeExecFile } = {}) {
  return new Promise((resolve, reject) => {
    execFile(file, args, { encoding: "utf8", timeout: timeoutMs, maxBuffer, windowsHide: true }, (error, stdout) => {
      if (error) {
        const normalized = new Error("EXTERNAL_COMMAND_FAILED");
        normalized.code = error.code || "COMMAND_FAILED";
        reject(normalized);
        return;
      }
      resolve(String(stdout || ""));
    });
  });
}
