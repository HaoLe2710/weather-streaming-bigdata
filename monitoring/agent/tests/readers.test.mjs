import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { readJsonFile, readJsonlTail } from "../src/readers.mjs";

test("reads a valid JSON file and preserves numeric zero", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "weather-agent-reader-"));
  const file = path.join(directory, "status.json");
  await writeFile(file, '{"count":0}\n');
  const result = await readJsonFile(file);
  assert.equal(result.status, "OK");
  assert.equal(result.value.count, 0);
  assert.match(result.modifiedAt, /Z$/);
});

test("empty, malformed and missing JSON are distinct states", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "weather-agent-reader-"));
  const empty = path.join(directory, "empty.json");
  const malformed = path.join(directory, "malformed.json");
  await writeFile(empty, "");
  await writeFile(malformed, "{");
  assert.equal((await readJsonFile(empty)).status, "EMPTY");
  assert.equal((await readJsonFile(malformed)).status, "INVALID");
  assert.equal((await readJsonFile(path.join(directory, "missing.json"))).status, "MISSING");
});

test("permission failures remain unavailable instead of becoming zero", async () => {
  const deniedOpen = async () => { throw Object.assign(new Error("denied"), { code: "EACCES" }); };
  assert.equal((await readJsonFile("unreadable.json", { openFile: deniedOpen })).status, "UNAVAILABLE");
  assert.equal((await readJsonlTail("unreadable.json", { openFile: deniedOpen })).status, "UNAVAILABLE");
});

test("JSONL tail drops an unterminated append and bounds returned records", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "weather-agent-reader-"));
  const file = path.join(directory, "events.jsonl");
  await writeFile(file, '{"n":1}\n{"n":2}\n{"n":3}\n{"partial":');
  const result = await readJsonlTail(file, { maxRecords: 2 });
  assert.equal(result.status, "OK");
  assert.deepEqual(result.records, [{ n: 2 }, { n: 3 }]);
  assert.equal(result.incompleteTail, true);
  assert.equal(result.invalidLines, 0);
  assert.equal(result.omittedRecords, 1);
  assert.equal(result.truncated, true);
});

test("JSONL malformed complete rows are counted and the source remains readable", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "weather-agent-reader-"));
  const file = path.join(directory, "events.jsonl");
  await writeFile(file, '{"ok":true}\nnope\n');
  const result = await readJsonlTail(file);
  assert.equal(result.status, "OK");
  assert.deepEqual(result.records, [{ ok: true }]);
  assert.equal(result.invalidLines, 1);
});
