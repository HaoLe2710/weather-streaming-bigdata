import { createHash, createHmac, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2);
const fixtureName = args.find((arg) => !arg.startsWith("--")) || "snapshot-healthy.json";
const sendLocally = args.includes("--send");
const repeatLocally = args.includes("--repeat");
if (!/^snapshot-[a-z-]+\.json$/.test(fixtureName)) {
  throw new Error("Only synthetic snapshot fixtures may be signed by this development helper");
}

const secret = process.env.MONITOR_HMAC_SECRET_PRIMARY;
if (!secret || Buffer.byteLength(secret, "utf8") < 32) {
  throw new Error("Set MONITOR_HMAC_SECRET_PRIMARY to a local synthetic secret of at least 32 bytes");
}

const keyId = process.env.MONITOR_HMAC_KEY_ID_PRIMARY || "agent-key-v1";
if (!/^[A-Za-z0-9._-]{1,80}$/.test(keyId)) throw new Error("Invalid MONITOR_HMAC_KEY_ID_PRIMARY");

const snapshot = JSON.parse(await readFile(path.join(root, "fixtures", fixtureName), "utf8"));
if (snapshot.source?.environment !== "local" || !snapshot.source.hostname?.startsWith("phase2a-synthetic")) {
  throw new Error("Refusing to sign a fixture that is not marked as synthetic local data");
}

const now = new Date().toISOString();
snapshot.snapshotId = randomUUID();
snapshot.timestamp = now;
snapshot.cohort.lastUpdateAt = now;
snapshot.cohort.statusFileUpdatedAt = now;
snapshot.infrastructure.watchdog.latestReport &&= {
  ...snapshot.infrastructure.watchdog.latestReport,
  checkedAt: now,
};

const body = Buffer.from(JSON.stringify(snapshot), "utf8");
if (body.byteLength > 32 * 1024) throw new Error("Synthetic snapshot exceeds 32 KiB");
const timestamp = now;
const bodySha256Hex = createHash("sha256").update(body).digest("hex");
const canonical = `v1\n${keyId}\n${timestamp}\n${bodySha256Hex}`;
const signature = createHmac("sha256", Buffer.from(secret, "utf8"))
  .update(Buffer.from(canonical, "utf8"))
  .digest("base64url");
const streamId = createHash("sha256")
  .update(`${snapshot.source.environment}\n${snapshot.cohort.cohortId}\n${snapshot.source.hostname}`, "utf8")
  .digest("hex");

const request = {
  method: "POST",
  url: `${process.env.MONITOR_API_URL || "http://127.0.0.1:8787"}/api/v1/ingest/snapshots`,
  headers: {
    "Content-Type": "application/json",
    "X-Monitor-Key-Id": keyId,
    "X-Monitor-Timestamp": timestamp,
    "X-Monitor-Signature": `v1=${signature}`,
  },
  body: body.toString("utf8"),
};

if (sendLocally) {
  const destination = new URL(request.url);
  if (destination.protocol !== "http:" || !["localhost", "127.0.0.1", "[::1]"].includes(destination.hostname)) {
    throw new Error("--send is restricted to an HTTP localhost Worker; this helper cannot upload to a remote endpoint");
  }
  const responses = [];
  const count = repeatLocally ? 2 : 1;
  for (let index = 0; index < count; index += 1) {
    const response = await fetch(destination, {
      method: request.method,
      headers: request.headers,
      body,
    });
    const responseText = await response.text();
    let responseBody;
    try { responseBody = JSON.parse(responseText); } catch { responseBody = { message: "Non-JSON response" }; }
    responses.push({ httpStatus: response.status, response: responseBody });
    if (!response.ok) process.exitCode = 1;
  }
  console.log(JSON.stringify({ streamId, responses }, null, 2));
} else {
  console.log(JSON.stringify({ streamId, ...request }, null, 2));
}
