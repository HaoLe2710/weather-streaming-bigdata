# Phase 2A.1 — Cloudflare Staging Checklist

This is a gated runbook for a separately approved, isolated Cloudflare staging deployment. Completing code changes or local CI does not authorize or prove remote deployment. Do not use production Worker/D1 bindings, Azure credentials, or live cohort data.

## 1. Change and contract review

- [ ] Confirm deployment commit and GitHub Actions are green.
- [ ] Confirm only Cloudflare backend, migration, tests, and monitoring docs changed.
- [ ] Verify Snapshot Contract V1 schema and HMAC vector remain unchanged.
- [ ] Verify frozen T+2H cohort/model/feature/provider admission constants remain unchanged.
- [ ] Review forward-only `migrations/0002_phase2a1_hardening.sql`; confirm it preserves all rows/payloads and initializes recovery state safely.
- [ ] Confirm local `npm run test:migrations` passes fresh-install and populated-Phase-2A upgrade scenarios.

## 2. Isolated account and resource selection

- [ ] Select an explicitly approved development Cloudflare account and confirm plan/billing impact.
- [ ] Create or identify a staging-only Worker and D1 database; record their names/IDs in the operator's protected change record, not Git.
- [ ] Authenticate interactively with Wrangler and verify `wrangler whoami` returns the intended development account.
- [ ] Review staging `wrangler.jsonc` bindings, environment allowlists, and synthetic source hostnames. Keep Telegram disabled.
- [ ] Set unique staging-only HMAC and read API secrets through Wrangler secret management; do not paste or commit values.
- [ ] Confirm no production route, custom domain, secret, database ID, or Azure uploader is attached.

## 3. Migration and deploy

- [ ] Reconfirm the selected database ID is staging immediately before migration.
- [ ] Apply the forward migration only to the staging database with `wrangler d1 migrations apply DB --remote --env staging`.
- [ ] Verify migration history and new indexes/state columns using read-only schema queries.
- [ ] Deploy the reviewed Worker commit to the staging environment.
- [ ] Verify `/healthz` and authenticated read API using staging credentials.

## 4. Synthetic functional checks

- [ ] Submit one signed synthetic accepted snapshot and verify `201 ACCEPTED` plus persisted current state.
- [ ] Submit the exact same snapshot again and verify `200 DUPLICATE` without changing live heartbeat receipt identity.
- [ ] Submit a schema-valid frozen-contract mismatch and verify `202 QUARANTINED`, preserved reason, a single active deduplicated alert, and per-snapshot processed recovery state.
- [ ] Exercise the recovery retry path using isolated synthetic staging records; verify retry/backoff, terminal error visibility, and no repeated processing after `PROCESSED`.
- [ ] Verify `GET /streams`, per-stream `/summary`, and Cron agree on `DISABLED`, `PENDING`, `HEALTHY`, `OFFLINE`, and `UNKNOWN` cases.
- [ ] Verify historical, duplicate, future-out-of-skew, and quarantined telemetry cannot refresh heartbeat; verify accepted fresh recovery resolves heartbeat alert once.
- [ ] Verify pending recovery does not starve heartbeat checks and Cron diagnostics expose pending/failed counts and errors.
- [ ] Verify target overflow fails with `TARGET_SCAN_LIMIT_EXCEEDED` rather than silently ignoring targets.
- [ ] Verify alert/outbox idempotency and retention preserve pending/failed recovery evidence.

## 5. Runtime budget and handoff gate

- [ ] Observe several five-minute Cron invocations and the retention schedule in staging.
- [ ] Record CPU duration, D1 queries/read/write/storage, Cron errors, recovery backlog, and target count. Compare against the selected plan limits.
- [ ] Confirm structured logs contain diagnostic codes and counts, not raw snapshot payloads or secrets.
- [ ] Keep Phase 2B uploader disconnected until synthetic checks, budget review, and operational alert ownership are accepted.
- [ ] Archive deployment/migration/functional evidence in the approved staging evidence location.

## Current status

All remote checklist items are **UNVERIFIED** until an approved staging deployment is carried out. This Phase 2A.1 implementation task does not deploy or access Cloudflare staging or production.
