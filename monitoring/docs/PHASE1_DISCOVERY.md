# Phase 1 discovery — Weather Streaming Monitoring Agent

## Discovery status

**Repository source review: complete. Azure runtime discovery: pending.** The configured Azure VM was not reachable from this Windows session because no SSH host alias was present in the local SSH config. No production command was run. The run/cohort values below are pinned from the task; they are not claims about the VM's current live state.

Pinned observation target:

- Run: `20261010T111433Z-prospective-live-t2h-v1`
- Cohort: `prospective-t2h-20261010T120000Z`
- Protocol: `T2H_LIVE_PROSPECTIVE_168H_V1`
- Model SHA-256: `bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a`
- Feature list SHA-256: `20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2`
- Provider model / horizon: `ecmwf_ifs` / 2 hours

The current agent reads from the existing live checkout configured by `WEATHER_REPO_ROOT`; its own code can live in the separate `/opt/weather-monitor-agent` clone. It does not invoke the Python status CLI, watchdog, Spark, producer, Docker Compose lifecycle, or inference entrypoint.

## Topology confirmed from source

```text
Open-Meteo producer → run-scoped Kafka topic → Spark Structured Streaming
                   → Delta forecast/state tables → prospective validator state
                   → watchdog summaries and hourly Delta audit evidence
```

The Compose service allowlist is taken from `ops/azure_t2h/watchdog.py`: `broker`, `spark-master`, `spark-worker`, `live-hourly-producer-t2h`, and `streaming-inference-t2h-live`. The agent asks Docker for metadata filtered to the expected Compose project and selects only these service labels. It never requests container logs, environment variables, mounts, or Dockerfile command output.

## Source-of-truth matrix

| Metric / signal | Authoritative repository source | Mapping and limits |
|---|---|---|
| Active run ID | `data/runtime/prospective-live-t2h-168h-v1/active_run.json` | Must match the configured `WEATHER_RUN_ID`; the agent never follows a different active run. |
| Run request and frozen runtime contract | `.../<run_id>/start_request.json` | `run_id`, `cohort_protocol_id`, `expected_target_hours`, `expected_locations`, `expected_slots`, model/feature hashes, provider model and forecast horizon. |
| Cohort ID, target window and frozen contract | `.../<run_id>/cohort_manifest.json` | `cohort_id`, `cohort_start_target_time`, `cohort_end_target_time`, protocol, counts, model/feature hashes, provider, horizon. |
| Official cohort status | `.../<run_id>/cohort_status.json` | Written from validator state. The status file is the source for `status`, `microbatch_update_count`, `completed_target_hours`, `prospective_forecast_count`, `valid_evaluation_count`, pending/missing slots, duplicates, provenance, `restart_count`, and `last_update_at`. |
| Latest inference batch | `results/streaming-inference-t2h/<run_id>/spark_live/inference_batches.jsonl` | The agent reads a bounded tail and exports only a numeric/enum allowlist. Fields include batch ID, source/ready/forecast/rejected counts, duplicate conflicts, history gaps, invalid provenance, and `total_batch_seconds`. Current source does not record a per-row UTC completion timestamp, so the agent reports file mtime separately and leaves `latestBatchAt` null when absent. |
| Frozen model startup check | `.../spark_live/model_startup_validation.json` | `status` and `model_sha256` are compared with the frozen contract. Probe prediction and model bytes/path are not exported. |
| Persistence receipts | `.../spark_live/forecast_persistence_receipts.jsonl` | Receipt rows contain forecast records. The agent does **not** parse or export these records; it reads file metadata only and uses the watchdog's bounded summary count and `forecast_count_snapshot_audit` status. A direct receipt recount is unavailable without reading raw forecast rows. |
| Latest watchdog result and cohort heartbeat | `results/prospective-live-t2h/<run_id>/runtime/ops/monitor.jsonl` | Bounded tail of watchdog summary fields, errors/warnings and restart evidence. No watchdog function is invoked because it can write evidence, run Delta audit, or recover infrastructure. |
| Hourly audit and persistence summary | `.../runtime/ops/hourly_reports.jsonl` | Bounded tail. The agent reads `hourly_delta_audit`, receipt count, and reported forecast/evaluation counters. It carries recent PASS/FAIL counts and recent failed classifications; a later PASS does not erase earlier failures in the observation window. |
| Watchdog systemd state | `weather-t2h-watchdog.timer` and `weather-t2h-watchdog.service` | Read-only `systemctl show` properties. An oneshot watchdog service being `inactive` is not considered failure; `Result`/exit status are reported separately from timer `ActiveState`. |
| Host memory, uptime, load, CPU | Linux `/proc`-backed Node OS APIs | CPU is a 120 ms sample; memory and uptime are host metrics. Windows load average may be unavailable. |
| Disk | Filesystem containing configured `WEATHER_DATA_PATH` | Node `statfs` on that path, so the report measures the data filesystem rather than assuming `/`. |
| Docker health, restarts and resource usage | Docker CLI read-only metadata for the five allowed Compose services | Uses `execFile` with argument arrays and timeouts. Without Docker-socket access, reports `UNAVAILABLE`; no Docker group membership is added by the unit. |

### Metric semantics

- `microbatchUpdateCount` maps exactly to `microbatch_update_count`; it is not the 168 expected target hours.
- `completedTargetHours` maps to the validator's complete target-hour count across all locations.
- `forecastsPersisted` maps to `prospective_forecast_count`; `validEvaluations` maps to `valid_evaluation_count`.
- `pendingTarget` and `pendingReference` are surfaced as pending, not counted as evaluation failures.
- Gap, duplicate, rejected and provenance values are labeled as latest-batch or cohort-level according to their source. The agent does not invent a cumulative count.
- `0` is preserved as a measured zero. Missing, malformed, inaccessible and unsupported metrics remain `null` with collector warning codes. `UNKNOWN`/`STALE` describe the evidence state, not a numeric zero.
- The 90-minute cohort-stale classification is copied only from the existing watchdog warning `COHORT_UPDATE_STALE_OVER_90_MINUTES`; the agent introduces no new cohort-health rule.
- Audit failure history is retained in the agent's own snapshots and summarized from source history. Audit `FAIL` remains distinct from collector `PARTIAL`.

## Concurrent writes and read bounds

Validator JSON snapshots are read as single bounded files; JSONL sources are read using a bounded tail (2 MiB / 2,000 records maximum). A final unterminated JSONL line is treated as incomplete and excluded. Atomic replacement or append races produce a source warning; the agent never repairs source files.

The agent writes only to `MONITOR_OUTPUT_DIR` (`/var/lib/weather-monitor-agent` by default): atomic `latest.json`, UTC-dated JSONL history, and its process-lock file. Retention only deletes the agent's own dated history files. The path validator refuses an output directory inside the repository. No evidence path is a write target.

## Gaps that require Azure verification

The checked-in repository does not contain the official 2026-10-10 runtime artifacts. The following remain unverified until the VM is reachable:

1. Actual `/opt/weather-streaming/weather-streaming-bigdata` file permissions and which status files exist.
2. Runtime JSON/JSONL shapes and timestamps for the official run.
3. Effective systemd unit properties, watchdog timer state, container names/project labels, restart counts, health checks and Docker-socket permissions.
4. Node.js version, data filesystem capacity, agent-user read access, and the agent snapshot's actual resource cost.
5. A production dry-run against the pinned run and a real persisted snapshot.

Do not mark the phase complete until these checks are performed on the Azure VM without invoking any pipeline lifecycle operation.
