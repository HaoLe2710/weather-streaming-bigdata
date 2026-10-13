# Phase 2A.1 — Monitoring Backend Hardening Report

## Scope and frozen boundaries

This change hardens only the Cloudflare Worker/D1 monitoring backend, tests, and documentation. It preserves Monitoring Snapshot Contract V1 and its HMAC signing format. It does not modify the Phase 1 agent, Kafka/Spark/Delta/XGBoost, Azure, the prospective cohort, frozen model/feature hashes, or production evidence. No Cloudflare remote account, staging database, Worker, or production service was accessed or changed.

The implementation branch is `fix/cloudflare-monitoring-phase2a-hardening`, based on `520d70d509e0b8776596c3ba62631186d023d5dd`.

## Root cause analysis

### Cron heartbeat starvation

Previously, heartbeat monitoring could return early whenever snapshot or quarantine repair had work. A continuously replenished or failing recovery queue could therefore postpone the liveness check indefinitely. The heartbeat target scan now runs first on each scheduled heartbeat tick. Snapshot recovery, quarantine recovery, and notification work each have separate bounded budgets and error accounting. A failure in one recoverable record does not suppress the heartbeat scan or the other recovery class while D1 remains available. D1-wide failures are surfaced as execution failures; they are not represented as a successful heartbeat result.

### Quarantine recovery could repeat indefinitely

Recovery eligibility had depended on whether a snapshot-specific `OPENED` alert event existed. Alert rows are deduplicated by `(stream, alert type)`, so a second quarantine with the same fingerprint correctly reused the already-open alert but never received a new `OPENED` event. The recovery query then selected that same snapshot forever. Migration `0002_phase2a1_hardening.sql` adds durable per-snapshot recovery state, attempts, lock expiry, retry time, terminal error, processed time, and linked alert identity. Each snapshot is marked processed only after its link is committed and verified. An existing open alert can be linked without emitting a duplicate OPENED transition. Retries use a lease, bounded exponential backoff, and a configured attempt ceiling; terminal failures remain visible.

### Heartbeat state differed between Cron and APIs

The API summary endpoint inferred status from whether any heartbeat timestamp existed, while list and Cron paths applied freshness checks. Old telemetry could consequently appear observed in the summary after it had timed out. `heartbeat-status.ts` now provides a pure shared `DISABLED | PENDING | HEALTHY | OFFLINE | UNKNOWN` resolver used by Cron and both monitoring API views. Live candidacy requires acceptable collection age and delivery freshness. Duplicate, historical, and quarantined snapshots do not refresh the live receipt identity. Collector `PARTIAL` remains eligible as an agent heartbeat; collector `FAILED` and pipeline freshness are not used as independent network-liveness authorities.

### Current state, alerts, and concurrency

Live receipt time and snapshot identity are persisted together. Same-ID accepted retries can replay an interrupted current-state application without refreshing receipt time. Snapshot ordering stays monotonic by collection time; a historical arrival cannot replace a newer current pointer. Alert create/resolve decisions use conditional D1 statements, a partial unique active-fingerprint constraint, and fresh/current/heartbeat guards. Resolution is conditional and does not use an unprotected read-then-write decision. The notification outbox continues to deduplicate transitions. Retention excludes pending/failed recovery evidence.

## Implementation map

### New modules and migration

- `monitoring/cloudflare/src/services/heartbeat-status.ts` — shared heartbeat resolution and live-candidate policy.
- `monitoring/cloudflare/src/services/quarantine-service.ts` — bounded, leased, idempotent quarantine-to-alert recovery.
- `monitoring/cloudflare/migrations/0002_phase2a1_hardening.sql` — forward-only state/backfill/index migration.
- `monitoring/cloudflare/tests/heartbeat-status.test.ts` — pure status and freshness regressions.
- `monitoring/cloudflare/scripts/test-migrations.mjs` — fresh-install and populated-database upgrade checks.

### Existing code updated

Repositories now maintain paired live receipt identity and guarded alert transitions. Ingestion preserves admission/quarantine reasons and only marks accepted work applied after alert rules complete. The heartbeat service scans liveness before bounded recovery, records backlog/error diagnostics, and caps retries. Scheduled execution runs heartbeat monitoring on every scheduled invocation, including retention ticks, and propagates failures rather than swallowing them. Notification processing returns a bounded processed count and treats enabled-but-unconfigured delivery as an error. Retention preserves pending/failed recovery evidence. Wrangler configuration pins small recovery batches and retry limits.

## Recovery states and budgets

Quarantine alert recovery follows `PENDING → PROCESSING (lease) → PROCESSED`, with `PROCESSING → PENDING` after a retryable failure and `PENDING → FAILED` at the configured attempt limit. Snapshot current-state repair uses the corresponding stream recovery state. Leases permit later recovery if a Worker invocation terminates mid-operation. Per-tick defaults are one unapplied snapshot, one quarantine, one due notification, and at most five enabled heartbeat targets. The configured target limit is checked against `limit + 1`; exceeding it fails with `TARGET_SCAN_LIMIT_EXCEEDED` rather than silently skipping enabled targets.

The scheduled result reports targets checked, offline/unknown targets, recovered counts, pending/failed backlogs, and recovery/diagnostic errors. It contains identifiers/counts only, not snapshot bodies or secrets. Backoff and batch caps bound retry work. The implementation does not use an unbounded `Promise.all` for D1 writes.

## Performance and security assessment

The design performs a bounded target query, one stream read and guarded alert work per enabled target, indexed bounded recovery selection/claims, and a fixed number of backlog aggregate subqueries. A normal cohort with one monitoring stream and no backlog stays well below the Free-plan per-invocation query ceiling based on the statement count in code; recovery batches default to one per class. This is a source-based estimate, not a remotely measured query/CPU profile. The target scan limit is five by default and fails closed on overflow. Each 5-minute schedule is one invocation; retention work is batch-limited. Actual CPU time, D1 queries, storage, and Cron success must be observed in an isolated staging account before connecting Phase 2B.

No raw telemetry is added to structured logs. Recovery logs contain stable diagnostic codes and snapshot IDs/reasons only. HMAC, bearer-token, source allowlist, payload bound, frozen admission, and API response contracts are unchanged. The local and test runtime use synthetic fixtures only.

## Verification status

Local verification completed on Windows using Node 22:

| Check | Result |
|---|---|
| `npm run typecheck` | PASS |
| `npm run validate-schema` | PASS; generated validator matches Phase 1 schema |
| `npm run check-contract` | PASS; frozen contract matches Phase 1 source |
| `npm test` | PASS; 57 Worker/D1 tests plus migration tests |
| Migration tests | PASS; fresh install, legacy backfill, evidence preservation, indexes, uniqueness |
| `npm run build` | PASS; Wrangler deploy dry-run only, no remote deployment |
| `npm run migrate:local` | PASS; local migration `0002_phase2a1_hardening.sql` applied |
| `npx wrangler d1 migrations list DB --local --env local` | PASS; no local migrations pending |
| Signer syntax and synthetic fixture | PASS; `node --check` and local-only fixture signing |
| Phase 1 agent `npm test` | PASS; 22 passed, 4 Linux-only checks skipped on Windows, 0 failed |
| `git diff --check` | PASS |
| GitHub Actions | PASS on implementation commit `169e1f61562141e50a67993ce1101c2128cfc12e`; [run 38411227582](https://github.com/HaoLe2710/weather-streaming-bigdata/actions/runs/38411227582) completed successfully, including all 14 job steps |

Passing local tests do not establish remote staging readiness. The Phase 1 skipped tests are Linux-specific flock/systemd checks; they were exercised by the successful Ubuntu workflow. The CI path filter includes `PHASE2A1_*` hardening reports/checklists so documentation-only follow-ups in this phase also run the backend checks.

## Staging readiness

**READY WITH LIMITATIONS** for an operator-reviewed isolated staging deployment only after local checks and CI pass. Remote readiness is **UNVERIFIED**: no Cloudflare account authorization, staging database, Worker deployment, Cron execution, plan budget, or remote migration was inspected. See `PHASE2A1_STAGING_CHECKLIST.md`. No deployment was performed and no live Phase 2B uploader should be connected before staging evidence is reviewed.
