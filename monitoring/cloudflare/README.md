# Cloudflare Weather Monitoring Backend — Phase 2A

Workers + TypeScript strict mode + Hono + D1 implement the secured Phase 1 Monitoring Snapshot V1 receiver, read-only API, scheduled heartbeat/alert processing, bounded retention, and optional Telegram delivery (disabled by default).

## Quick start

```bash
npm ci
npm run typecheck
npm run validate-schema
npm run check-contract
npm run test
npm run build
npm run migrate:local
npm run dev
```

Copy `.dev.vars.example` to `.dev.vars.local` for local-only secrets. The local E2E procedure is in [`PHASE2A_TESTING.md`](../docs/PHASE2A_TESTING.md). API, security, database, architecture, and staging runbooks are linked from [`monitoring/docs`](../docs/PHASE2A_ARCHITECTURE.md).

## Safety boundary

The source allowlist is empty in the root config. Local and staging configs allow only synthetic local hostnames. Do not configure live Azure ingestion or deploy to production in Phase 2A. Frozen model, feature, protocol, T0, and cohort values are pinned in source. Secrets and `.wrangler/state` are ignored and must never be committed.

## Commands

- `npm run generate-validator` regenerates the full AJV validator from `monitoring/agent/schema/monitoring-snapshot-v1.schema.json`.
- `npm run validate-schema` fails if generated validator output is stale.
- `npm run check-contract` checks frozen values against the Phase 1 Node contract.
- `npm run sign:fixture -- snapshot-healthy.json` prints an exact HMAC-signed synthetic request; add `--send` only for an HTTP localhost Worker, optionally `--repeat` to verify idempotency.
- `npm run migrate:local` applies the D1 migration to local Miniflare state only.

See [`PHASE2B_INTEGRATION_CONTRACT.md`](../docs/PHASE2B_INTEGRATION_CONTRACT.md) before implementing an uploader.
