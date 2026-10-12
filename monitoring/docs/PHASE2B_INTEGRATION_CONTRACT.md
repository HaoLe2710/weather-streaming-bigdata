# Phase 2B Azure Agent Uploader Contract

This document defines a future uploader only. Phase 2A does not implement one, and the Azure Agent must not send live data until staging and production policies are separately approved.

## Endpoint and headers

`POST https://<staging-or-production-worker-host>/api/v1/ingest/snapshots`

Send the exact UTF-8 JSON bytes emitted by Phase 1, with:

```text
Content-Type: application/json
X-Monitor-Key-Id: agent-key-v1
X-Monitor-Timestamp: <current UTC ISO-8601 request time>
X-Monitor-Signature: v1=<unpadded base64url HMAC-SHA256>
```

Compute lowercase hex `SHA256(rawBody)`, then UTF-8 encode this canonical message with LF separators and no final LF:

```text
v1\n<keyId>\n<requestTimestamp>\n<sha256HexOfRawBody>
```

HMAC-SHA256 uses the UTF-8 secret bytes; encode the 32-byte MAC as unpadded base64url. Request time is checked within the configured 300-second window; do not substitute the snapshot collection timestamp. A delayed historical snapshot can be retried with a newly signed request timestamp while retaining its original `snapshotId` and body.

## Node.js pseudocode

```js
import { createHash, createHmac } from "node:crypto";

const rawBody = Buffer.from(JSON.stringify(snapshot), "utf8");
const timestamp = new Date().toISOString();
const keyId = process.env.MONITOR_HMAC_KEY_ID_PRIMARY;
const bodyHash = createHash("sha256").update(rawBody).digest("hex");
const canonical = `v1\n${keyId}\n${timestamp}\n${bodyHash}`;
const signature = createHmac("sha256", process.env.MONITOR_HMAC_SECRET_PRIMARY)
  .update(Buffer.from(canonical, "utf8"))
  .digest("base64url");
await fetch(`${baseUrl}/api/v1/ingest/snapshots`, {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "X-Monitor-Key-Id": keyId,
    "X-Monitor-Timestamp": timestamp,
    "X-Monitor-Signature": `v1=${signature}`,
  },
  body: rawBody,
});
```

The real Phase 2B implementation must persist the raw snapshot and `snapshotId` in a local outbox before sending, and must never reconstruct/reformat a body while retrying a stored payload.

## Test vector

This vector is deliberately synthetic and safe to publish; it is not a deployment credential.

| Item | Value |
|---|---|
| Secret | `phase2a-hmac-vector-only-secret-32-bytes` |
| Key ID | `test-key-v1` |
| Request timestamp | `2026-10-12T12:00:00.000Z` |
| Exact body bytes (UTF-8) | `{}` |
| SHA-256 body digest (hex) | `44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a` |
| Canonical bytes (LF shown as line breaks) | `v1` / `test-key-v1` / `2026-10-12T12:00:00.000Z` / digest above |
| Signature header | `v1=uFP9QjCLHx1j3qP60obYN009Arba44UqJOrGVWK3Quc` |

The Workers integration suite verifies this exact Node.js-produced vector through the HMAC middleware with the test clock pinned to the vector timestamp; schema validation then returns 400 for `{}`, proving authentication passed without storing data.

## Response and retry contract

- `201 ACCEPTED`: persist delivery success; do not resend.
- `200 DUPLICATE`: same stored body was already accepted; mark outbox item delivered. It does not refresh heartbeat.
- `202 QUARANTINED`: permanent operator-review state; preserve response and do not auto-rewrite/retry the snapshot.
- Retry transient network failures, 408, 429, and 5xx with bounded exponential backoff and jitter; honor `Retry-After` when present. Re-sign each retry with a fresh request timestamp while keeping body and snapshot ID unchanged.
- Do not retry 400, 401, 403, 409, 413, or 415 automatically. Preserve error code/request ID and alert the operator. Resolve payload conflicts by investigation, never by assigning a replacement ID to the same evidence.

Allow both current and previous key IDs during a deliberate rotation window, verify the new key before retiring the previous secret, and never store secrets in the outbox. Keep uploads ordered by collection timestamp when practical; server monotonicity protects current state if retries arrive out of order. Bound backlog replay per run so historical uploads do not masquerade as live heartbeat.
