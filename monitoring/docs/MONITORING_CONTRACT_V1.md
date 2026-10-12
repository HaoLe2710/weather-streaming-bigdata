# Monitoring Snapshot Contract V1

Schema file: `monitoring/agent/schema/monitoring-snapshot-v1.schema.json`.

## Envelope

- `schemaVersion`: integer contract version, currently `1`.
- `snapshotId`: UUID generated once per collection; use as a future ingestion idempotency key.
- `timestamp`: collection time in ISO-8601 UTC (`Z`).
- `source`: environment, agent version and host name; never credentials.
- `cohort`: configured and observed run/cohort identities, protocol, collection window, frozen contract checks and validator status.
- `progress`: exact prospective validator counts. `microbatchUpdateCount` is intentionally distinct from 168 expected target hours.
- `quality`: cohort-level and latest-batch metrics, watchdog hourly-audit summary/history, persistence receipt summary and explicit unavailable-source codes.
- `inference`: latest batch ID/time when source provides one, source-file observation time, frozen model identity, startup-validation status, duration and receipt file metadata.
- `infrastructure`: host metrics, allowlisted Docker container metadata and watchdog/systemd state.
- `pipeline`: observed pipeline/watchdog/audit status; this is separate from collector completeness.
- `collector`: `COMPLETE`, `PARTIAL`, or `FAILED`, execution duration and bounded warning codes.

## Values and freshness

- Counts are nonnegative integers or `null`; zero is an observed zero, not a fallback.
- `null` means a source or field was not available, supported, or valid. Corresponding warning codes are in `collector.warnings` and `quality.unavailableReasonCodes`.
- `UNKNOWN` means the pipeline state cannot be verified; `STALE` is emitted only when the existing watchdog explicitly reports its 90-minute cohort heartbeat warning. The collector does not invent a new cohort freshness threshold.
- Watchdog audit `FAIL` remains visible in the recent 24-hour `auditHistory` even when a newer audit is `PASS`. `UNRESOLVED` retains its original and effective status.
- `latestBatchAt` is `null` if the inference JSONL row has no timestamp field. `latestBatchObservedAt` is the file mtime and is not represented as the event time.
- `persistenceReceiptCount` comes from the watchdog summary. Receipt JSONL itself contains raw forecast rows and is not parsed by the agent.

## Privacy and size

No raw weather fields, forecast payloads, PII, credentials, container environment, or logs are included. The serializer rejects prohibited field names and caps a snapshot at 32 KiB. Docker/container output contains only the selected service name, state, health, restart count, uptime, and optional CPU/memory resource values.

## Future Cloudflare Worker ingestion

Phase 1 writes local snapshots only. A Phase 2 endpoint should accept one JSON object matching V1, require authenticated TLS requests, and use `snapshotId` as an idempotency key. Prefer an HMAC signature with a rotating secret stored in a root-owned systemd credential or secret store; include a timestamp and reject stale/replayed signatures. Add endpoint-side schema validation, payload-size limits, environment allowlisting, retention rules and explicit error handling before enabling upload. Never place a token in this repository or telemetry snapshot. Keep network upload disabled until the Worker contract and secret rotation are implemented.
