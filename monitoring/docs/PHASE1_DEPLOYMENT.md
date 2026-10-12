# Phase 1 deployment and rollback

## Current state

The source is intended for a standalone Azure checkout at `/opt/weather-monitor-agent`, separate from `/opt/weather-streaming/weather-streaming-bigdata`. This document is an operator runbook, not evidence that Azure was changed. Azure SSH/runtime access was unavailable during this implementation; no production connection, install, daemon reload, timer activation, or collector run was performed.

The official run is pinned in the host environment file. Keep the live checkout, Compose overlays, Kafka, Spark, checkpoints, Delta tables, watchdog units and cohort evidence untouched.

## Prepare the separate checkout

From the Azure VM, clone the pushed feature branch into the dedicated path. If the path already exists, inspect its Git remote, branch and working tree first; never overwrite or reset it.

```sh
sudo install -d -o "$USER" -g "$(id -gn)" /opt/weather-monitor-agent
git clone --branch feat/weather-monitoring-agent-phase1 --single-branch \
  https://github.com/HaoLe2710/weather-streaming-bigdata.git \
  /opt/weather-monitor-agent
git -C /opt/weather-monitor-agent rev-parse HEAD
```

Compare the printed SHA with the pushed commit before continuing. Do not run `git pull` in the live checkout.

Verify Ubuntu's Node LTS and lock utility:

```sh
node --version
command -v node
command -v flock
```

The package supports Node `>=20 <25`. If Node is absent, stop and provision it separately through the approved VM software process; do not run a system-wide `apt upgrade`.

## Configure least-privilege reads

Create a non-login service identity without adding it to the `docker` group:

```sh
sudo useradd --system --user-group --no-create-home --home-dir /nonexistent \
  --shell /usr/sbin/nologin weather-monitor-agent
```

Create `/etc/weather-monitor-agent.env` from the host-specific values shown in `monitoring/agent/.env.example`. It must point `WEATHER_REPO_ROOT`, state/results/data roots at the existing live checkout, and use `/var/lib/weather-monitor-agent` for output. Keep the file root-owned (`root:root`, mode `0640`); it contains run identifiers and paths, never credentials.

Grant the service account read/traverse access only to the configured data path and the run-scoped runtime/evidence directories. If the VM uses ACLs, first inspect current ownership/ACLs, then grant the service identity `r-x` on the relevant directories and a default read ACL for future atomic evidence replacements. Do not change file contents or use recursive `chmod`/`chown`. If access cannot be granted narrowly, leave that collector `UNAVAILABLE` and stop before enablement.

Docker metadata is optional and the unit intentionally has no `docker` supplementary group. Do not add one to make the collector look healthy; doing so grants root-equivalent Docker access. If the operator later authorizes a narrower Docker metadata broker, implement that separately.

## Dry-run and install

First run a config check and inspect every resolved source path as the service identity. Then run a read-only sample under the same user and environment. One option is a transient systemd invocation with the host environment file:

```sh
sudo systemd-run --wait --pipe --collect \
  --property=User=weather-monitor-agent \
  --property=Group=weather-monitor-agent \
  --property=EnvironmentFile=/etc/weather-monitor-agent.env \
  --property=WorkingDirectory=/opt/weather-monitor-agent/monitoring/agent \
  /usr/bin/env node src/cli.mjs --validate-config

sudo systemd-run --wait --pipe --collect \
  --property=User=weather-monitor-agent \
  --property=Group=weather-monitor-agent \
  --property=EnvironmentFile=/etc/weather-monitor-agent.env \
  --property=WorkingDirectory=/opt/weather-monitor-agent/monitoring/agent \
  /usr/bin/env node src/cli.mjs --show-paths

sudo systemd-run --wait --pipe --collect \
  --property=User=weather-monitor-agent \
  --property=Group=weather-monitor-agent \
  --property=EnvironmentFile=/etc/weather-monitor-agent.env \
  --property=WorkingDirectory=/opt/weather-monitor-agent/monitoring/agent \
  /usr/bin/env node src/cli.mjs --once --dry-run
```

Review the full JSON: `cohort.identityValidation` and `frozenContractValidation` must be `PASS`, run/cohort IDs must match, the pipeline must remain observationally stable, and every missing source must have a reason. A Docker permission warning is expected under the least-privilege default. Any identity or contract mismatch is a hard stop. The dry-run does not write snapshots (the process lock is in the OS temporary directory).

After the operator verifies the dry-run against current runtime files, install only these new units:

```sh
sudo install -o root -g root -m 0644 \
  /opt/weather-monitor-agent/monitoring/deployment/systemd/weather-monitor-agent.service \
  /etc/systemd/system/weather-monitor-agent.service
sudo install -o root -g root -m 0644 \
  /opt/weather-monitor-agent/monitoring/deployment/systemd/weather-monitor-agent.timer \
  /etc/systemd/system/weather-monitor-agent.timer
sudo systemctl daemon-reload
sudo systemctl start weather-monitor-agent.service
sudo systemctl enable --now weather-monitor-agent.timer
```

The unit is a oneshot with a four-minute boot offset and five-minute cadence, separate from the existing watchdog timer. Verify two executions using only the new unit and output directory:

```sh
systemctl show weather-monitor-agent.timer --no-pager \
  --property=ActiveState --property=LastTriggerUSec --property=NextElapseUSecRealtime
systemctl show weather-monitor-agent.service --no-pager \
  --property=Result --property=ExecMainStatus --property=ExecMainStartTimestamp
sudo journalctl -u weather-monitor-agent.service -n 30 --no-pager
sudo cat /var/lib/weather-monitor-agent/latest.json
```

Re-check pipeline and watchdog state read-only before/after. Do not invoke `weather-t2h-watchdog.service` to test the Agent.

## Rollback

Rollback touches only the Agent's units and output:

```sh
sudo systemctl disable --now weather-monitor-agent.timer
sudo systemctl stop weather-monitor-agent.service
```

Leave the standalone checkout, `/var/lib/weather-monitor-agent` history and all Weather Streaming data/evidence in place. Do not remove the service account or output until a separate retention decision is made.

## Phase 2 readiness

V1 contains identifiers, aggregate progress and quality counts, frozen model identity, timestamps, Docker/systemd summaries and collector diagnostics. `snapshotId` is the ingestion idempotency key. A future Cloudflare Worker requires HTTPS, authenticated HMAC or equivalent rotating credentials, timestamp/replay checks, strict schema and size limits, idempotent writes, environment restrictions and bounded server retention. Network push remains disabled in this phase.
