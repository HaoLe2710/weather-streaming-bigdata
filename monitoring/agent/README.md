# Weather Streaming Monitoring Agent

Node.js LTS oneshot collector for the T2H prospective pipeline. It reads the configured live checkout and writes metrics only under `MONITOR_OUTPUT_DIR`. No network ingestion, producer, Spark, Delta, model, watchdog, or Docker lifecycle operation is started.

## Requirements

- Node.js `>=20 <25`.
- Linux deployment uses `flock` from util-linux for an OS-managed, crash-released process lock.
- Docker and systemd access are optional collectors. If unavailable, the snapshot reports `UNAVAILABLE`/`PARTIAL`; it does not grant Docker access.

## Configure

Copy `.env.example` to a host-only environment file such as `/etc/weather-monitor-agent.env`. Replace paths/identifiers only for the intended run, keep that file outside Git, and keep `MONITOR_OUTPUT_DIR` outside the experiment repository. The current frozen protocol, model, feature list, provider and horizon are checked in `src/config.mjs`; environment overrides that differ fail configuration validation.

The Node CLI reads its environment from the caller. It does not parse shell files or expose environment contents.

## CLI

Run from this directory:

```sh
node src/cli.mjs --validate-config
node src/cli.mjs --show-paths
node src/cli.mjs --once --dry-run
node src/cli.mjs --once
npm test
```

`--once --dry-run` prints a validated snapshot and does not write the configured output directory. It exits nonzero when the pinned run/cohort identity or frozen contract cannot be verified. `--once` stores one snapshot; it does not loop. A valid but partial snapshot is still stored with explicit collector warnings.

## Collection and data rules

- Host data uses Node OS metrics and `statfs(WEATHER_DATA_PATH)`.
- Docker is queried using `execFile` argument arrays, fixed timeouts, expected Compose project label, and an exact service allowlist. No logs, secrets, or container environment are requested.
- Watchdog state is read with `systemctl show`; the watchdog itself is never invoked. An inactive completed oneshot is interpreted using its last `Result` and exit code.
- Cohort files are checked against configured run/cohort IDs and the frozen contract. No Python validation CLI is invoked.
- JSONL input is read as a bounded tail. Incomplete final writes are omitted; source files are never repaired.
- `forecast_persistence_receipts.jsonl` contains raw forecast rows. The agent reads only file metadata and the watchdog's aggregate receipt/audit summary.
- Missing or invalid values remain `null`; collector warning codes identify unavailable or malformed sources. Numeric zero is preserved.
- Output schema is versioned in `schema/monitoring-snapshot-v1.schema.json`; the serialized snapshot is bounded to 32 KiB.

## Storage and lock

`latest.json` is atomically replaced. `history/YYYY-MM-DD.jsonl` is append-only for that UTC date, with configurable retention (default 14 days). Retention removes only regular date-named files under this agent's own history directory. A process lock prevents overlapping snapshots; Linux uses util-linux `flock` held by a helper that exits when the collector exits, so the OS releases the lock after a crash. Non-Linux development uses an exclusive PID lock and reclaims it only after the owner PID is gone.

See [`../docs/MONITORING_CONTRACT_V1.md`](../docs/MONITORING_CONTRACT_V1.md), [`../docs/PHASE1_DISCOVERY.md`](../docs/PHASE1_DISCOVERY.md), and [`../docs/PHASE1_DEPLOYMENT.md`](../docs/PHASE1_DEPLOYMENT.md).
