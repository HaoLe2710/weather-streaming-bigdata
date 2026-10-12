import { open } from "node:fs/promises";

export const MAX_JSON_BYTES = 1024 * 1024;
export const MAX_JSONL_TAIL_BYTES = 2 * 1024 * 1024;
export const MAX_JSONL_RECORDS = 2000;

function classifyReadError(error) {
  if (error?.code === "ENOENT" || error?.code === "ENOTDIR") return "MISSING";
  if (error?.code === "EACCES" || error?.code === "EPERM") return "UNAVAILABLE";
  return "INVALID";
}

export async function readJsonFile(filePath, { maxBytes = MAX_JSON_BYTES, openFile = open } = {}) {
  let handle;
  try {
    handle = await openFile(filePath, "r");
    const stat = await handle.stat();
    if (stat.size === 0) return { status: "EMPTY", value: null, modifiedAt: stat.mtime.toISOString() };
    if (stat.size > maxBytes) return { status: "TOO_LARGE", value: null, modifiedAt: stat.mtime.toISOString() };
    const content = await handle.readFile({ encoding: "utf8" });
    try {
      return { status: "OK", value: JSON.parse(content), modifiedAt: stat.mtime.toISOString() };
    } catch {
      return { status: "INVALID", value: null, modifiedAt: stat.mtime.toISOString() };
    }
  } catch (error) {
    return { status: classifyReadError(error), value: null, errorCode: error?.code || "READ_ERROR" };
  } finally {
    await handle?.close().catch(() => {});
  }
}

export async function readJsonlTail(
  filePath,
  { maxBytes = MAX_JSONL_TAIL_BYTES, maxRecords = MAX_JSONL_RECORDS, openFile = open } = {},
) {
  let handle;
  try {
    handle = await openFile(filePath, "r");
    const stat = await handle.stat();
    if (stat.size === 0) return { status: "EMPTY", records: [], modifiedAt: stat.mtime.toISOString(), invalidLines: 0, incompleteTail: false, truncated: false };
    const start = Math.max(0, stat.size - maxBytes);
    const length = stat.size - start;
    const buffer = Buffer.alloc(length);
    await handle.read(buffer, 0, length, start);
    let content = buffer.toString("utf8");
    if (start > 0) {
      const firstNewline = content.indexOf("\n");
      content = firstNewline < 0 ? "" : content.slice(firstNewline + 1);
    }
    const incompleteTail = !content.endsWith("\n");
    if (incompleteTail) content = content.slice(0, Math.max(0, content.lastIndexOf("\n") + 1));
    const lines = content.split("\n").filter((line) => line.length > 0);
    const selected = lines.slice(-maxRecords);
    const records = [];
    let invalidLines = 0;
    for (const line of selected) {
      try {
        const value = JSON.parse(line);
        if (value && typeof value === "object" && !Array.isArray(value)) records.push(value);
        else invalidLines += 1;
      } catch {
        invalidLines += 1;
      }
    }
    return {
      status: "OK",
      records,
      modifiedAt: stat.mtime.toISOString(),
      invalidLines,
      omittedRecords: Math.max(0, lines.length - selected.length),
      incompleteTail,
      truncated: start > 0 || lines.length > selected.length,
    };
  } catch (error) {
    return { status: classifyReadError(error), records: [], errorCode: error?.code || "READ_ERROR" };
  } finally {
    await handle?.close().catch(() => {});
  }
}

export async function inspectPath(filePath) {
  try {
    const handle = await open(filePath, "r");
    try {
      const stat = await handle.stat();
      return { status: "READABLE", modifiedAt: stat.mtime.toISOString(), sizeBytes: stat.size };
    } finally {
      await handle.close();
    }
  } catch (error) {
    return { status: classifyReadError(error), errorCode: error?.code || "READ_ERROR" };
  }
}
