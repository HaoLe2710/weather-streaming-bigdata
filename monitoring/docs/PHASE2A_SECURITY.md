# Phase 2A Security Model

## Authentication and request integrity

Ingestion uses Web Crypto HMAC-SHA256. The Worker bounds and reads the exact raw UTF-8 request body before parsing it, computes lowercase SHA-256 hex for those bytes, builds canonical UTF-8 bytes, and calls `crypto.subtle.verify`.

```text
v1\n<keyId>\n<requestTimestamp>\n<lowercase-sha256-hex-of-raw-body>
```

Signature header: `X-Monitor-Signature: v1=<unpadded-base64url-of-32-byte-HMAC>`. Timestamp must be UTC ISO-8601 and within the configured 300-second window (bounded to 30–900 seconds). It is an authentication timestamp, not the snapshot collection timestamp. Key IDs select the primary or optional previous secret; arbitrary IDs fail closed. This permits a previously collected snapshot to be re-signed during delayed delivery without treating the old collection time as a replay.

Secrets are Wrangler/Cloudflare secrets, never checked into source:

- `MONITOR_HMAC_SECRET_PRIMARY` (required for ingestion)
- `MONITOR_HMAC_SECRET_PREVIOUS` (optional rotation window)
- `MONITOR_READ_API_TOKEN` (required for read APIs)
- `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` (only if Telegram is deliberately enabled)

Use at least 32 random bytes for HMAC and a long random read token. Rotate by configuring previous key ID/secret, deploying, moving senders to the new primary, then removing the old key after the retry/backlog window. Never log signatures, bearer tokens, raw payloads, or secrets.

## Admission and data handling

Full schema validation occurs before database insertion. Source environment and hostname allowlists and frozen cohort/model hashes are separate admission checks. Schema-valid rejected evidence is `QUARANTINED` and cannot update `monitoring_streams`. The API stores only monitoring snapshots; Phase 1 serializer must continue excluding raw weather, logs, credentials, and payload hashes unrelated to the snapshot transport.

Request body is capped at 32 KiB both in streaming middleware and in D1. Malformed JSON, wrong content type, invalid UUID/time/schema, oversized body, invalid HMAC, and reused ID with changed bytes fail closed. Errors expose stable codes and request IDs, not SQL or stack details. SQL inputs are bound parameters; route stream IDs and cursors are validated before queries.

## Read access and outbound notification

The bearer token is an admin/server credential over the entire monitoring API. Do not ship it to a browser or public dashboard; Phase 3 needs Cloudflare Access/session validation or a BFF. `healthz` reveals liveness only.

Telegram is disabled in config by default. If enabled, notifications are queued in D1, claimed with a lease, bounded to five attempts by default, and retried with capped exponential backoff. A failed API response is never marked SENT. Alert state remains authoritative in D1; Telegram is only a delivery channel.

## Threat boundaries and operator checklist

- Restrict HMAC/read tokens to this Worker and rotate through Cloudflare secret management.
- Allowlist only the intended environment and agent hostname; staging defaults are synthetic/local.
- Do not configure `azure-production` ingestion until Phase 2B is reviewed and deployed separately.
- Review quarantined contract evidence and alert state; do not silently rewrite it.
- Use HTTPS and do not place credentials in URLs, query strings, source, CI logs, or issue comments.
- Keep the staging D1/Worker isolated from existing Cloudflare resources; inspect account/billing before creating anything.
