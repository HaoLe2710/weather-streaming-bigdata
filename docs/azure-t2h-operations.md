# Azure operations: T+2h prospective 168-hour cohort

## Scope and immutable contracts

This runbook supports protocol `T2H_LIVE_PROSPECTIVE_168H_V1`, 63 canonical locations, and 10,584 target slots. It keeps the deployed XGBoost model SHA-256 `bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a`, feature-list SHA-256 `20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2`, provider model `ecmwf_ifs`, and the two-hour forecast horizon fixed.

The code does not train a model, move T0, repair conflicting weather payloads, reset Kafka offsets, remove checkpoints, prune volumes, or start an official cohort automatically. The deployment script refuses a fast-forward while an official cohort is unfinished. It also rejects a fetched change set that modifies an Azure-local Compose overlay.

## Why the runtime is isolated

The live producer and inference service previously defaulted to one shared hourly-observation topic and one shared producer cache. A new run now freezes its own Kafka topic, producer cache, and bootstrap receipt in `start_request.json`. The topic name includes the run ID. Legacy requests keep their recorded topic/cache when resumed; they are never silently migrated.

Before a new isolated run starts, the producer publishes and validates a contiguous 49-hour history for all 63 locations (48 prior hours plus the current safe hour: 3,087 unique location-hour rows). The startup gate requires every row delivered and flushed, zero history gaps, and a seeded cache. If a failed attempt left Kafka records without a PASS receipt, startup stops and keeps the evidence; it never replays that topic to hide a partial publish. The daemon cache then suppresses a second publish of the bootstrap safe hour.

The 168-hour Spark prospective state directory is bind-mounted at `data/runtime/prospective-live-t2h-168h-v1`, separate from the earlier 24-hour state root. The earlier finalized run and its `restart_count` remain intact. Kafka, Delta, checkpoint, result, and prior evidence paths are not deleted by these tools.

The incident values `duplicate_conflict_keys=567` and `gap_in_history_rows=1575` came from the supplied incident description; this workspace does not contain the corresponding live batch record. Source/configuration tracing found the shared topic/cache path and the missing fresh-history bootstrap path. A future run reports and stops on conflict/gap/provenance errors instead of selecting or rewriting payloads.

## Azure host prerequisites

- Ubuntu VM path: `/opt/weather-streaming/weather-streaming-bigdata`.
- The deployment account has GitHub read access, Docker access through the `docker` group, `sudo` for installing systemd units, and a working `.venv/bin/python`.
- Windows OpenSSH has the Azure host key already in `known_hosts`. The host wrapper sets `StrictHostKeyChecking=yes` and `BatchMode=yes`.
- Keep the private SSH key outside the repository. Use an external key path or the SSH agent; the wrapper only passes the path to OpenSSH.
- `sudo -n` must be allowed for the unit installer, or installation stops without prompting from a noninteractive SSH session.
- The Azure checkout must already contain `docker-compose.yml`, `compose.azure.yaml`, and `compose.azure.resources.yaml`. These Azure overlays are host-local inputs and are not created or replaced by this repository.
- Every Azure Compose entrypoint uses `COMPOSE_FILE=docker-compose.yml:compose.azure.yaml:compose.azure.resources.yaml` from the repository working directory and runs `docker compose config --quiet`. Missing files or a conflicting `COMPOSE_FILE` stop deployment, readiness, resume, or watchdog execution; the scripts never fall back to the base Compose file.

## Windows-to-Azure deployment

After this PR has been merged into the selected Azure checkout branch, run the PowerShell wrapper from Windows. Its default action is a remote preflight; it does not fetch code, enable services, or start a cohort. The preflight saves its result under `data/runtime/prospective-live-t2h-168h-v1/preflight_test_results.json` and Python may create ignored bytecode caches.

```powershell
.\ops\azure_t2h\deploy-azure.ps1 -SshHost 'azure-weather'
```

To fast-forward the Azure checkout from GitHub and install the systemd monitor/resume units, explicitly opt into deployment:

```powershell
.\ops\azure_t2h\deploy-azure.ps1 `
  -SshHost 'azure-weather' `
  -IdentityFile 'C:\Users\LEGION\.ssh\azure_weather_ed25519' `
  -Ref 'deploy/azure-phase17' `
  -Apply
```

The remote bootstrap requires the working tree's tracked files to be clean, the checkout branch to match `-Ref`, the three required Compose files to exist, the exact Compose stack to pass `docker compose config --quiet`, and no unfinished official cohort. It runs `git fetch` and `git merge --ff-only`; it does not switch branches, force-push, reset, clean, or run `docker compose down`. Untracked/ignored Azure-local files remain in place. Fast-forward is refused if the incoming commit changes a recognized Azure-local Compose overlay.

Fresh Formal Readiness is a separate explicit option. It creates an isolated readiness run/topic, tests the actual provider and runtime, and stops readiness services afterward. It is not an official cohort:

```powershell
.\ops\azure_t2h\deploy-azure.ps1 `
  -SshHost 'azure-weather' `
  -IdentityFile 'C:\Users\LEGION\.ssh\azure_weather_ed25519' `
  -Ref 'deploy/azure-phase17' `
  -Apply `
  -RunFreshReadiness
```

Do not pass `-Apply` or `-RunFreshReadiness` until the deployment window and Azure access are authorized. No Azure connection or deployment has been run as part of this repository change.

## Starting an official cohort

Official start remains a human action. First require all deployment/readiness gates below to pass. Then review `data/runtime/prospective-live-t2h-168h-v1/readiness.json`, `preflight_test_results.json`, the frozen contract, and the planned run ID. Only after approval, run the existing CLI `start` command once. Do not construct a replacement run ID after a failure; use `resume` for the active run. `T0` is frozen by the validation code only after the first complete valid forecast cycle.

```bash
source /etc/weather-streaming-t2h.env
.venv/bin/python -m validation.prospective_t2h \
  --state-root data/runtime/prospective-live-t2h-168h-v1 start
```

The `start` command itself rechecks the persisted preflight, Fresh Formal Readiness age/fingerprint, 63-location positive leads, provider model, protocol, and frozen model/feature contract. The watchdog and boot-resume service never call `start`.

## Systemd operation and evidence

`weather-t2h-resume.service` runs after Docker/network on boot. Both systemd services require `/etc/weather-streaming-t2h-compose.env`, installed only after overlay and Compose validation. It carries the exact `COMPOSE_FILE` list and runtime GID separately from `/etc/weather-streaming-t2h.env`, which may contain host-managed credentials. Resume revalidates the overlays/configuration, then resumes only the run ID in `active_run.json`, verifies that run's existing start request and protocol, exits for a finalized cohort, and does nothing when there is no official cohort. It calls `resume`; it cannot create a new run or choose a new T0.

`weather-t2h-watchdog.timer` runs every five minutes. Each sample records cohort/run IDs, frozen hashes/provider/horizon, progress counters, cohort `restart_count`, Docker `RestartCount`s, latest inference batch, persistence receipt count, and service state. Every UTC hour it runs the Spark/Delta audit for persisted forecasts and state. The state check requires exactly 49 contiguous hourly observations per location, 63 non-null locations, and no duplicate location-hours. If and only if it observes exactly 50 contiguous rows per location with no duplicates while the run-scoped Spark checkpoint has an offset batch ahead of its commit marker, it waits up to five minutes for that batch to commit and performs one fresh state scan. A remaining 50-row history, a timeout, or any other failed state invariant remains FAIL. Each state attempt is appended to `delta_audit_attempts.jsonl`; existing hourly audit evidence is never overwritten.

The watchdog takes a non-blocking host OS lock at `runtime/ops/delta_audit.lock` before dispatching an hourly audit. It holds that same lock across the recovery decision and any infrastructure action, so another watchdog cannot start a Spark audit while services are being recovered. The container-side supervisor at `spark/jobs/container_audit_process.py` also takes an exclusive lock before starting Spark. The base Compose configuration mounts `./spark` read-only at `/opt/project/spark` in the inference service; it does not mount `./ops`, so the watchdog invokes `/opt/project/spark/jobs/container_audit_process.py`. The supervisor launches the audit in its own Linux process session and records the audit ID, PID, process-group/session IDs, process start ticks, command hash, deadline, and cleanup outcome. If the host-side Docker CLI times out or disconnects, the container supervisor continues to own the audit; the next watchdog inspects its persisted state. Cleanup signals only the recorded audit process group after validating its identity. An unverified process or unconfirmed cleanup blocks recovery. Stopping the inference container is recorded as termination of its exec process. Hourly and daily summaries, process events, and alert-open/resolved transitions append under `results/prospective-live-t2h/<run_id>/runtime/ops/`.

The Ubuntu CI process-lifecycle regressions use real Linux child processes, `/proc` identities, process sessions, and `flock`, but mock the Docker Compose CLI; they do not exercise the Azure VM's Docker daemon, bind mount, container image, or Spark binary. Before enabling this behavior in a deployment, run a non-cohort integration check in a disposable instance of the same inference image: dispatch a short harmless child through `docker compose exec -d`, verify the shared job record and lock, force a short timeout, and confirm only that child session terminates. Do not run this integration check against the active official cohort.

Forecast count validation reads a fresh `cohort_status.json` and pins the exact Delta history version used for each scan. It accepts count equality only when the checkpoint is SETTLED with the same latest offset and commit batch IDs before and after that Delta snapshot, and the persistence receipts exactly match the snapshot. A pending or advancing checkpoint causes a bounded fresh-read retry. The only count mismatch eligible for retry is exactly one extra 63-forecast batch whose receipt shortfall is exactly those rows and whose checkpoint is pending or just settled. The expected count is never replaced by the observed Delta count; missing forecasts, unexplained extras, duplicates, provider/contract errors, and receipt mismatches fail.

A previous hourly audit failure remains visible in five-minute reports and alerts until a later completed hourly audit passes; skipped audits do not resolve it or erase evidence. Recovery eligibility distinguishes current `_data_findings` from carried audit history. Current data-integrity/contract errors always block restart. A carried audit permits infrastructure recovery only for `TRANSIENT_STATE_WINDOW_TIMEOUT` or `TRANSIENT_RETRY_LIMIT_EXCEEDED` when the persisted state report proves exactly 63 locations with 50 contiguous rows each, zero null IDs, duplicate location-hours, and hourly gaps; the only failed state check is the expected 49-row retention check; forecast and receipt checks pass; and the attempt history records the exact transient classification with an in-flight checkpoint batch. A duplicate key, missing location, null ID, gap, forecast/receipt failure, unknown classification, or missing/malformed evidence blocks recovery. A current failed audit, held host lock, active container audit, or unverified process state also blocks recovery. If inference is unavailable, the audit is recorded as skipped for infrastructure reasons; that skip is not treated as a data pass or a current data failure, and only independently confirmed data findings determine whether recovery is permitted.

For eligible infrastructure-only incidents, the watchdog starts/restarts only an existing stopped/unhealthy service, retries transient Docker failures at most three times with exponential delay, and allows at most three recovery actions per service in a rolling 24 hours. It does not create missing containers or alter Kafka offsets. Inspect the failure report and fix a missing service as an operator. A failed watchdog invocation is visible in systemd/journal and `alerts.jsonl`.

```bash
systemctl status weather-t2h-resume.service weather-t2h-watchdog.timer
journalctl -u weather-t2h-watchdog.service --since '2 hours ago'
tail -n 20 results/prospective-live-t2h/<run_id>/runtime/ops/monitor.jsonl
tail -n 20 results/prospective-live-t2h/<run_id>/runtime/ops/alerts.jsonl
.venv/bin/python -m validation.prospective_t2h \
  --state-root data/runtime/prospective-live-t2h-168h-v1 status
```

The run is blocked for duplicate conflict keys, history gaps, invalid provenance, duplicate persistence receipts, cohort reference conflicts, frozen-contract drift, a stale heartbeat, a nonzero hourly Delta audit, or batches with input but no ready feature/forecast rows. These values are reported; the watchdog never edits evidence to make them pass.

## Validation gates

### Before deployment

- Azure branch is the expected Git ref; tracked working tree is clean; protected Azure-local Compose overlays are untouched.
- `python -m validation.prospective_t2h preflight` passes compile, regression tests, diff check, and `docker compose config --quiet` using the exact three-file Azure stack.
- Runtime model contract reports the required model SHA, feature-list SHA, provider `ecmwf_ifs`, and two-hour horizon.
- Fresh Formal Readiness is current and passes 63/63 live forecasts, 63/63 positive leads, zero target-offset violations, the frozen provider/model contract, monitoring PASS, and readiness-service cleanup.
- Deployment and readiness do not write an official `active_run.json` entry.

### After official start

- Cohort protocol, run ID, cohort ID, T0, model SHA, feature SHA, provider model, and checkpoint remain unchanged through recovery.
- Every new run's isolated bootstrap confirms 63 locations × 49 contiguous hourly rows with unique keys and complete Kafka delivery/flush.
- Duplicate/conflict, history-gap, invalid-provenance, and duplicate-receipt counts remain zero; `microbatch_update_count`, `last_update_at`, prospective forecasts, and valid evaluations advance.
- Recovery preserves the cohort `restart_count`, existing checkpoints, Delta state, Kafka data, and all earlier evidence.
- Finalize only when the existing frozen protocol marks all 10,584 slots terminal and its final artifact checks pass. No watchdog action finalizes a cohort.

`READY_FOR_OFFICIAL_START` is reserved for an Azure run where both preflight and a fresh formal readiness check have actually passed on Azure. This repository work alone does not satisfy that gate.
