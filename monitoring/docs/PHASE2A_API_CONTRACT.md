# Phase 2A API Contract

Base path: `/api/v1`. JSON responses include `apiVersion: 1` on read endpoints. Errors use `{ "error": { "code", "message", "requestId" } }`. The Worker sets `X-Request-Id` on responses.

## Ingestion

`POST /api/v1/ingest/snapshots`

- `Content-Type: application/json`; maximum exact body size: 32 KiB.
- Body is one complete Phase 1 Monitoring Snapshot V1. Unknown fields are rejected by the generated full Draft 2020-12 schema validator.
- Required HMAC headers are `X-Monitor-Key-Id`, `X-Monitor-Timestamp`, and `X-Monitor-Signature: v1=<base64url>`.
- `201`: `{status:"ACCEPTED",snapshotId,receivedAt,schemaVersion:1}`.
- `200`: `{status:"DUPLICATE",snapshotId,message:"Snapshot already ingested"}`. Duplicate bytes do not change current state or heartbeat.
- `202`: `{status:"QUARANTINED",snapshotId,reason}` for schema-valid but disallowed source/cohort/model evidence.
- `400 INVALID_JSON|INVALID_SNAPSHOT_SCHEMA`, `401 AUTHENTICATION_REQUIRED|AUTHENTICATION_FAILED`, `409 SNAPSHOT_ID_CONFLICT`, `413 PAYLOAD_TOO_LARGE`, `415 UNSUPPORTED_MEDIA_TYPE`, `503 AUTHENTICATION_NOT_CONFIGURED`, or generic `500 INTERNAL_ERROR`.

The unique idempotency key is `(environment, snapshotId)`. The hash is over the exact raw bytes. Reusing an ID with any byte difference returns 409. Historical samples can be delivered later if the request is freshly signed; they are retained and can advance latest collection state only when newer, but cannot refresh the live-heartbeat clock unless their receive/collection lag satisfies the freshness rules.

## Liveness and protected read endpoints

`GET /healthz` is unauthenticated and returns only `{status:"ok",apiVersion:1}`.

All `/api/v1/monitoring/*` endpoints require `Authorization: Bearer <MONITOR_READ_API_TOKEN>`.

| Endpoint | Response |
|---|---|
| `GET /api/v1/monitoring/streams` | `{apiVersion,streams,nextCursor}`; enabled targets, bounded cursor page |
| `GET /api/v1/monitoring/streams/:streamId/latest` | `{apiVersion,status:"AVAILABLE"|"EMPTY",streamId,snapshot}` |
| `GET /api/v1/monitoring/streams/:streamId/history` | `{apiVersion,streamId,snapshots,nextCursor}` |
| `GET /api/v1/monitoring/streams/:streamId/summary` | `{apiVersion,streamId,latestPipelineHealth,collectorCompleteness,cohortProgress,validEvaluations,forecastPersistence,dataQuality,latestInferenceMetadata,watchdog,hostMetrics,activeAlerts,lastHeartbeat,heartbeatState}` |
| `GET /api/v1/monitoring/streams/:streamId/alerts` | `{apiVersion,streamId,alerts,nextCursor}` |

History accepts paired UTC ISO `from` and `to` values over a maximum 14-day interval. Default `limit=50`, maximum 200. Alerts accept optional `status=OPEN|RESOLVED`, default `limit=50`, maximum 200. Cursors are opaque, validated, and stable-tie-broken by timestamp plus snapshot/alert ID. Invalid IDs, malformed cursors, invalid bounds, and excessive limits return 400. Read APIs return stored accepted payloads only; quarantine is visible through alert evidence, not the latest healthy snapshot.

## Retention and versioning

Snapshot retention defaults to 14 days, alert/event retention to 90 days, and cleanup is bounded per run. Current latest snapshot pointers are protected. Any incompatible response/schema change requires a new API version; the Phase 1 source schema remains the ingestion authority.
