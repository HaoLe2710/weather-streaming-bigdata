# Phase 2A Testing and Local E2E

## Automated checks

Run from `monitoring/cloudflare`:

```bash
npm ci
npm run typecheck
npm run validate-schema
npm run check-contract
npm run test
npm run build
npm run migrate:local
```

`npm test` runs both the Cloudflare Vitest plugin/Workers runtime with isolated Miniflare D1 and the standalone migration upgrade suite. Coverage includes the complete Phase 1 schema fixtures, request validation, HMAC/key rotation/replay window, idempotency/conflict, quarantine, out-of-order state, read auth/pagination, shared heartbeat states, heartbeat scan under recovery backlog, bounded retries and terminal failure visibility, per-snapshot quarantine idempotency, concurrent alert/cron behavior, partial D1 batch recovery, API/Cron state parity, Telegram retry, and retention. Synthetic fixtures are generated from the Phase 1 JSON Schema; no production snapshot is used. The migration test applies both fresh-install and Phase 2A populated-database upgrade paths and asserts preservation/backfill/index constraints. CI runs on Ubuntu because Workerd's Windows-backed Vitest pool currently fails before test discovery with `File path must be a file: URL` in this environment.

The migration command exercises Wrangler's local D1 migration registry. `npm run test:migrations` separately tests forward migration against fresh and populated SQLite databases. The integration harness initializes its isolated in-memory D1 schema before each run. `.wrangler/state` is local-only and ignored.

## Local end-to-end request

1. Copy `.dev.vars.example` to ignored `.dev.vars.local`; set unique local synthetic values for `MONITOR_HMAC_SECRET_PRIMARY` (at least 32 bytes), `MONITOR_READ_API_TOKEN`, and `MONITOR_HMAC_KEY_ID_PRIMARY=agent-key-v1`. `npm run dev` selects the `local` Wrangler environment. Do not use production keys.
2. Apply the local migration and start Wrangler in one terminal:

   ```bash
   npm run migrate:local
   npm run dev
   ```

3. In another terminal, set the same local HMAC secret and send a request from the synthetic local fixture. The helper refreshes only the synthetic snapshot ID/timestamps. `--send` is hard-restricted to HTTP localhost; `--repeat` sends the exact same bytes and signature twice:

   ```powershell
   $env:MONITOR_HMAC_SECRET_PRIMARY = 'replace-with-at-least-32-local-random-bytes'
   $e2e = node scripts/sign-snapshot.mjs snapshot-healthy.json --send --repeat | ConvertFrom-Json
   $e2e.responses | Select-Object httpStatus, response
   $streamId = $e2e.streamId
   npx wrangler d1 execute DB --local --env local --command "INSERT OR IGNORE INTO monitoring_targets (stream_id, environment, cohort_id, run_id, source_identity, monitoring_enabled, activated_at) VALUES ('$streamId', 'local', 'prospective-t2h-20261010T120000Z', '20261010T111433Z-prospective-live-t2h-v1', 'phase2a-synthetic', 1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))"
   ```

   Expected HTTP statuses: `201` then `200` (`ACCEPTED` then `DUPLICATE`). Generating another request creates a different synthetic snapshot.

4. Read data with the local token:

   ```powershell
   $headers = @{ Authorization = 'Bearer <local-read-token>' }
   Invoke-RestMethod 'http://127.0.0.1:8787/healthz'
   Invoke-RestMethod 'http://127.0.0.1:8787/api/v1/monitoring/streams' -Headers $headers
   ```

5. Use the returned `streamId` to request `/latest`, `/history`, `/summary`, and `/alerts` under `/api/v1/monitoring/streams/<streamId>/`. Trigger the heartbeat path locally with Wrangler's scheduled-event endpoint at `http://127.0.0.1:8787/cdn-cgi/local/scheduled`; then query alerts. Inspect local D1 rows with `npx wrangler d1 execute DB --local --env local --command "SELECT validation_status, count(*) FROM monitoring_snapshots GROUP BY validation_status"`.

All commands above target local Miniflare data. Do not point this process at remote D1.
