# Phase 2A D1 Schema

Migration `monitoring/cloudflare/migrations/0001_initial.sql` creates the database from empty. Use prepared statements and apply future changes as new numbered migrations; never edit an applied migration for deployed databases.

| Table | Purpose and integrity controls |
|---|---|
| `monitoring_snapshots` | Accepted or quarantined V1 payload, raw-body SHA-256, source/cohort/status metadata, unique `(environment,snapshot_id)`, 32 KiB payload check, history and recovery indexes. |
| `monitoring_streams` | Current monotonic snapshot pointer, statuses, last live heartbeat, and collector incompleteness streak; unique environment/cohort/source identity. |
| `monitoring_targets` | Explicitly activated heartbeat targets, defaults disabled, validated interval/timeout/grace/disk/partial thresholds. |
| `monitoring_alerts` | Current and historical alert records, unique partial index for one open fingerprint, bounded history indexes. |
| `monitoring_alert_events` | Deduplicated append-only OPENED/RESOLVED/notification transitions. |
| `monitoring_notification_outbox` | Unique alert transition, claims, retry count/backoff state, and sent/failure timestamps. |

## Keys and query paths

- Snapshot idempotency: primary key `(environment,snapshot_id)`.
- Stream id: SHA-256-derived deterministic value from environment, cohort ID, and source hostname; a database uniqueness constraint independently enforces that identity.
- Current pointer: composite foreign key from stream environment/latest snapshot ID to the snapshot table.
- Alert fingerprint: SHA-256 of `stream_id + LF + alert_type`; a partial unique index protects one OPEN alert.
- Notification: unique `(alert_id,transition)` prevents duplicate delivery records.
- Snapshot indexes support both environment/cohort/source collection scans and API history lookup by stream/time; API history never returns an unbounded interval.

## Atomicity and recovery

The unique snapshot constraint is the concurrency gate. D1 batches group alert state, transition evidence, and outbox creation. Since snapshot insertion and current-state application are separate, `stream_applied=0` marks accepted work that a later heartbeat Cron may replay idempotently. The repair scan is indexed and bounded. Quarantined evidence is durable; missing contract-alert events are recovered by a separate bounded scan. Neither repair changes frozen payload fields.

## Retention

Snapshot deletion is by oldest received timestamp in a bounded batch and excludes the snapshot currently referenced by a stream. Alert events older than 90 days and resolved alerts older than that retention can be deleted in bounded batches; active alerts and current stream/target state remain. Notification outbox rows are retained until their resolved alert has aged out and delivery is terminal.

## Local commands

```bash
npm run migrate:local
npx wrangler d1 migrations list DB --local
```

The local D1 emulator lives under ignored `.wrangler/state`. Staging/production database IDs are separate and are never operated by these local commands.
