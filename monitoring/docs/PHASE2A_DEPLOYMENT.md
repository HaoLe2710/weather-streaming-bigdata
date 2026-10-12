# Phase 2A Deployment Runbook

## Local

See `PHASE2A_TESTING.md` for local D1, Worker startup, signed synthetic request, and read API verification. Local defaults use `ALLOWED_ENVIRONMENTS=local`, a synthetic hostname, and Telegram disabled.

## Staging prerequisite

No staging D1 database or Worker is created by this repository change. Before any remote resource creation, the operator must select the Cloudflare account, confirm the plan/billing impact, and name an isolated staging environment. Never reuse an existing production Worker or D1 database. The current deployment environment has not been authenticated or deployed as part of Phase 2A implementation.

When approved, operator steps:

1. Authenticate interactively with `npx wrangler login`; do not paste tokens into chat or source.
2. Confirm account with `npx wrangler whoami` and select a development account/plan with billing impact understood.
3. Create a new D1 database explicitly named for staging; keep its ID out of commits and configure it only in a reviewed deployment environment.
4. Review `wrangler.jsonc` `staging` environment and restrict `ALLOWED_ENVIRONMENTS`/`ALLOWED_SOURCE_HOSTNAMES` to synthetic staging sources. Keep Telegram disabled.
5. Set primary HMAC and read secrets with `wrangler secret put ... --env staging`. Set `MONITOR_HMAC_KEY_ID_PRIMARY` to the corresponding key ID.
6. Apply the migration to that named database with `wrangler d1 migrations apply DB --remote --env staging` only after verifying the target account and database ID.
7. Run `wrangler deploy --env staging`, check `/healthz`, submit one signed synthetic fixture, and verify `latest`, `history`, and `alerts` plus D1 persistence.
8. Verify Cron in staging and inspect CPU, D1 query, and storage metrics before any Phase 2B uploader is connected.

Do not run these remote steps from CI. No production alias, route, custom domain, Azure uploader, or live cohort is configured here. Telegram must remain disabled during first staging verification.

## Rollback

Workers deployments are versioned; select a previous known-good staging Worker version if a deployment fails. Do not drop D1 tables or delete snapshots to roll back code. Add a forward-only migration for schema changes and export/backup staging evidence before any manual data repair. Production rollout is out of scope and requires a separate approval/checklist.
