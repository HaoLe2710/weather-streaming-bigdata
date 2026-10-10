#!/usr/bin/env python3
"""Read-only T2H cohort monitor with bounded infrastructure-only recovery."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Callable, Mapping, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from validation.azure_compose import (
    AzureComposeConfigurationError,
    azure_compose_environment,
    is_azure_runtime,
    validate_azure_compose_configuration,
)


PROTOCOL_ID = "T2H_LIVE_PROSPECTIVE_168H_V1"
EXPECTED_LOCATIONS = 63
EXPECTED_TARGET_HOURS = 168
EXPECTED_SLOTS = EXPECTED_LOCATIONS * EXPECTED_TARGET_HOURS
MODEL_SHA256 = "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a"
FEATURE_LIST_SHA256 = "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2"
PROVIDER_MODEL = "ecmwf_ifs"
SERVICES = (
    "broker",
    "spark-master",
    "spark-worker",
    "live-hourly-producer-t2h",
    "streaming-inference-t2h-live",
)
RESTART_BUDGET_PER_SERVICE_PER_DAY = 3
STALE_COHORT_SECONDS = 90 * 60
INFRA_RETRY_MARKERS = (
    "cannot connect to the docker daemon",
    "error during connect",
    "connection refused",
    "context deadline exceeded",
    "temporary failure in name resolution",
)


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _append_jsonl(path: Path, row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _latest(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    return rows[-1] if rows else None


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _compose_environment(
    repository_root: Path,
    run_id: str,
    runtime_configuration: Mapping[str, Any],
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    source = os.environ if environ is None else environ
    env = azure_compose_environment(
        repository_root,
        source,
        required=is_azure_runtime(repository_root, source),
    )
    env["WEATHER_INFERENCE_RUN_ID"] = run_id
    topic = runtime_configuration.get("input_topic")
    if isinstance(topic, str) and topic:
        env["WEATHER_PROSPECTIVE_INPUT_TOPIC"] = topic
    cache = runtime_configuration.get("producer_cache_path")
    if isinstance(cache, str) and cache:
        env["WEATHER_PROSPECTIVE_PRODUCER_CACHE_PATH"] = cache
    return env


def collect_container_states(
    repository_root: Path,
    run_id: str,
    runtime_configuration: Mapping[str, Any],
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, dict[str, Any]]:
    env = _compose_environment(repository_root, run_id, runtime_configuration)
    states: dict[str, dict[str, Any]] = {}
    for service in SERVICES:
        try:
            listed = runner(
                ["docker", "compose", "--profile", "t2h-live", "ps", "--all", "-q", service],
                cwd=repository_root,
                env=env,
                text=True,
                capture_output=True,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            states[service] = {
                "status": "unknown",
                "health": None,
                "restart_count": None,
                "container_count": 0,
                "error": f"{type(exc).__name__}: {exc}",
            }
            continue
        if listed.returncode != 0:
            states[service] = {
                "status": "unknown",
                "health": None,
                "restart_count": None,
                "container_count": 0,
                "error": listed.stderr[-1000:] or "docker compose ps failed",
            }
            continue
        ids = [item.strip() for item in listed.stdout.splitlines() if item.strip()]
        if not ids:
            states[service] = {
                "status": "missing",
                "health": None,
                "restart_count": None,
                "container_count": 0,
                "error": listed.stderr[-1000:] if listed.returncode else "no existing container",
            }
            continue
        try:
            inspected = runner(
                ["docker", "inspect", *ids],
                cwd=repository_root,
                env=env,
                text=True,
                capture_output=True,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            states[service] = {
                "status": "unknown",
                "health": None,
                "restart_count": None,
                "container_count": len(ids),
                "container_ids": ids,
                "error": f"{type(exc).__name__}: {exc}",
            }
            continue
        if inspected.returncode != 0:
            states[service] = {
                "status": "unknown",
                "health": None,
                "restart_count": None,
                "container_count": len(ids),
                "error": inspected.stderr[-1000:],
            }
            continue
        try:
            containers = json.loads(inspected.stdout)
        except json.JSONDecodeError:
            containers = []
        if not isinstance(containers, list) or not containers:
            states[service] = {
                "status": "unknown",
                "health": None,
                "restart_count": None,
                "container_count": len(ids),
                "error": "docker inspect returned invalid JSON",
            }
            continue
        statuses = [item.get("State", {}) for item in containers if isinstance(item, dict)]
        healths = [state.get("Health", {}).get("Status") for state in statuses]
        state_names = [str(state.get("Status", "unknown")) for state in statuses]
        states[service] = {
            "status": "running" if all(value == "running" for value in state_names) else ",".join(state_names),
            "health": "unhealthy" if "unhealthy" in healths else ("healthy" if all(value in (None, "healthy") for value in healths) else ",".join(str(value) for value in healths)),
            "restart_count": sum(int(item.get("RestartCount", 0)) for item in containers if isinstance(item, dict)),
            "container_count": len(containers),
            "container_ids": ids,
        }
    return states


def _data_findings(
    *,
    request: Mapping[str, Any] | None,
    manifest: Mapping[str, Any] | None,
    cohort_status: Mapping[str, Any] | None,
    inference_batches: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    producer_runtime: Mapping[str, Any] | None,
    now: datetime,
) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    if request is None:
        errors.append("START_REQUEST_MISSING")
    elif not request.get("run_id"):
        errors.append("START_REQUEST_RUN_ID_MISSING")
    contract = manifest or request
    if contract:
        if contract.get("cohort_protocol_id") != PROTOCOL_ID:
            errors.append("COHORT_PROTOCOL_MISMATCH")
        expected = {
            "expected_target_hours": EXPECTED_TARGET_HOURS,
            "expected_locations": EXPECTED_LOCATIONS,
            "expected_slots": EXPECTED_SLOTS,
            "model_sha256": MODEL_SHA256,
            "feature_list_sha256": FEATURE_LIST_SHA256,
            "provider_model": PROVIDER_MODEL,
            "forecast_horizon_hours": 2,
        }
        for name, value in expected.items():
            if contract.get(name) != value:
                errors.append(f"FROZEN_CONTRACT_MISMATCH:{name}")
    runtime_configuration = (request or {}).get("runtime_configuration", {}) if isinstance(request, Mapping) else {}
    if isinstance(runtime_configuration, Mapping) and runtime_configuration.get("bootstrap_required") is True:
        expected_topic = f"weather.hourly.observations.t2h.prospective.{(request or {}).get('run_id')}.v1"
        if runtime_configuration.get("input_topic") != expected_topic:
            errors.append("RUN_TOPIC_NOT_ISOLATED")
        if str((request or {}).get("run_id")) not in str(runtime_configuration.get("producer_cache_path", "")):
            errors.append("PRODUCER_CACHE_NOT_RUN_SCOPED")
    if cohort_status:
        if cohort_status.get("run_id") != (request or manifest or {}).get("run_id"):
            errors.append("COHORT_STATUS_RUN_ID_MISMATCH")
        if cohort_status.get("expected_slots") != EXPECTED_SLOTS:
            errors.append("COHORT_STATUS_EXPECTED_SLOT_COUNT_MISMATCH")
        if manifest and cohort_status.get("cohort_id") not in (None, manifest.get("cohort_id")):
            errors.append("COHORT_STATUS_COHORT_ID_MISMATCH")
        # Preserve the incident counter as collected evidence; never reset it here.
        if not isinstance(cohort_status.get("restart_count"), int):
            warnings.append("COHORT_RESTART_COUNT_MISSING")
        for metric in ("reference_conflicts", "reference_conflict_key_count", "invalid_provenance"):
            try:
                value = int(cohort_status.get(metric, 0) or 0)
            except (TypeError, ValueError):
                value = -1
            if value != 0:
                errors.append(f"COHORT_{metric.upper()}_NONZERO:{value}")
        duplicate_validation = cohort_status.get("duplicate_validation", {})
        if isinstance(duplicate_validation, Mapping):
            for metric in (
                "duplicate_forecast_ids",
                "duplicate_logical_forecasts",
                "duplicate_persistence_receipts",
                "duplicate_evaluation_ids",
            ):
                try:
                    value = int(duplicate_validation.get(metric, 0) or 0)
                except (TypeError, ValueError):
                    value = -1
                if value != 0:
                    errors.append(f"COHORT_{metric.upper()}_NONZERO:{value}")
        provenance_validation = cohort_status.get("provenance_validation", {})
        if isinstance(provenance_validation, Mapping):
            violations = provenance_validation.get("blocking_violations", [])
            if violations:
                errors.append("COHORT_PROVENANCE_BLOCKING_VIOLATIONS")
        last_update = _parse_timestamp(cohort_status.get("last_update_at"))
        if cohort_status.get("status") == "COLLECTING":
            if last_update is None:
                warnings.append("COHORT_UPDATE_TIMESTAMP_MISSING")
            elif (now - last_update).total_seconds() > STALE_COHORT_SECONDS:
                warnings.append("COHORT_UPDATE_STALE_OVER_90_MINUTES")

    for batch in inference_batches:
        batch_id = batch.get("batch_id")
        for metric in ("duplicate_conflict_keys", "gap_in_history_rows", "invalid_provenance_rows"):
            try:
                value = int(batch.get(metric, 0))
            except (TypeError, ValueError):
                value = -1
            if value != 0:
                errors.append(f"INFERENCE_{metric.upper()}_BATCH_{batch_id}:{value}")
    latest_batch = _latest(inference_batches)
    if latest_batch:
        try:
            source_rows = int(latest_batch.get("source_rows", 0) or 0)
            ready_rows = int(latest_batch.get("feature_ready_rows", 0) or 0)
            forecast_rows = int(latest_batch.get("new_forecast_rows", 0) or 0)
        except (TypeError, ValueError):
            errors.append("LATEST_INFERENCE_BATCH_METRICS_INVALID")
        else:
            if source_rows > 0 and ready_rows == 0 and forecast_rows == 0:
                errors.append("LATEST_BATCH_HAS_ZERO_READY_FEATURES_AND_FORECASTS")

    receipt_ids = [str(row.get("forecast_id")) for row in receipts if row.get("forecast_id")]
    duplicate_receipts = len(receipt_ids) - len(set(receipt_ids))
    if duplicate_receipts:
        errors.append(f"DUPLICATE_PERSISTENCE_RECEIPTS:{duplicate_receipts}")

    if producer_runtime:
        poll = _latest(producer_runtime.get("poll_results", [])) if isinstance(producer_runtime.get("poll_results"), list) else None
        if producer_runtime.get("status") not in ("PASS", None):
            warnings.append("PRODUCER_RUNTIME_STATUS_NOT_PASS")
        if poll:
            if poll.get("delivery_failures") or poll.get("producer_flush_remaining") not in (None, 0):
                errors.append("PRODUCER_KAFKA_DELIVERY_FAILURE")
            if poll.get("failed_locations"):
                warnings.append("PRODUCER_LOCATION_FETCH_FAILURE")
    return sorted(set(errors)), sorted(set(warnings))


def _delta_audit(
    repository_root: Path,
    run_id: str,
    runtime_configuration: Mapping[str, Any],
    *,
    expected_live_forecasts: int,
    audit_path: Path,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    env = _compose_environment(repository_root, run_id, runtime_configuration)
    container_audit_path = f"/opt/project/results/prospective-live-t2h/{run_id}/runtime/ops/{audit_path.name}"
    command = [
        "docker",
        "compose",
        "--profile",
        "t2h-live",
        "exec",
        "-T",
        "streaming-inference-t2h-live",
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "--packages",
        "io.delta:delta-spark_2.13:4.0.0",
        "--conf",
        "spark.jars.ivy=/tmp/weather-spark-ivy",
        "--conf",
        "spark.sql.session.timeZone=UTC",
        "/opt/project/spark/jobs/verify_t2h_forecast_delta.py",
        "--forecast-path",
        f"/opt/project/data/streaming/t2h_v1_1/{run_id}/forecasts",
        "--state-path",
        f"/opt/project/data/streaming/t2h_v1_1/{run_id}/state_live",
        "--output-json",
        container_audit_path,
        "--expected-live-forecasts",
        str(expected_live_forecasts),
    ]
    completed = runner(
        command,
        cwd=repository_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=900,
    )
    evidence = _read_json(audit_path)
    return {
        "status": "PASS" if completed.returncode == 0 and isinstance(evidence, Mapping) and evidence.get("status") == "PASS" else "FAIL",
        "returncode": completed.returncode,
        "evidence_path": str(audit_path),
        "checks": evidence.get("checks") if isinstance(evidence, Mapping) else None,
        "stdout_tail": completed.stdout[-2000:],
        "stderr_tail": completed.stderr[-2000:],
    }


def _restart_budget(actions: Sequence[Mapping[str, Any]], service: str, now: datetime) -> int:
    since = now - timedelta(hours=24)
    action_ids: set[str] = set()
    for action in actions:
        if action.get("service") != service or action.get("action") != "restart":
            continue
        timestamp = _parse_timestamp(action.get("requested_at"))
        if timestamp is not None and timestamp >= since:
            action_id = action.get("action_id")
            action_ids.add(str(action_id) if action_id else str(action.get("requested_at")))
    return len(action_ids)


def _run_with_infrastructure_retries(
    command: Sequence[str],
    *,
    repository_root: Path,
    env: Mapping[str, str],
    runner: Callable[..., subprocess.CompletedProcess[str]],
    sleep: Callable[[float], None],
) -> tuple[subprocess.CompletedProcess[str], int]:
    completed: subprocess.CompletedProcess[str] | None = None
    for attempt in range(3):
        try:
            completed = runner(
                list(command),
                cwd=repository_root,
                env=dict(env),
                text=True,
                capture_output=True,
                check=False,
                timeout=120,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            completed = subprocess.CompletedProcess(list(command), 1, "", f"{type(exc).__name__}: {exc}")
        message = f"{completed.stdout}\n{completed.stderr}".lower()
        transient = any(marker in message for marker in INFRA_RETRY_MARKERS)
        if completed.returncode == 0 or not transient or attempt == 2:
            return completed, attempt + 1
        sleep(2**attempt)
    assert completed is not None
    return completed, 3


def recover_infrastructure(
    repository_root: Path,
    run_id: str,
    runtime_configuration: Mapping[str, Any],
    containers: Mapping[str, Mapping[str, Any]],
    *,
    data_errors: Sequence[str],
    action_log: Path,
    now: datetime,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """Restart only existing unhealthy/stopped services, never after a data-contract error."""
    actions = _read_jsonl(action_log)
    env = _compose_environment(repository_root, run_id, runtime_configuration)
    records: list[dict[str, Any]] = []
    if data_errors:
        return records
    for service, state in containers.items():
        status = str(state.get("status", "unknown"))
        health = str(state.get("health") or "")
        if status == "running" and health not in {"unhealthy"}:
            continue
        if status == "missing":
            records.append({"service": service, "action": "none", "status": "MISSING_CONTAINER_REQUIRES_OPERATOR", "requested_at": now.isoformat()})
            continue
        if status not in {"running", "exited", "dead"}:
            records.append({"service": service, "action": "none", "status": "CONTAINER_STATE_UNVERIFIED", "requested_at": now.isoformat(), "error": state.get("error")})
            continue
        budget_used = _restart_budget(actions, service, now)
        if budget_used >= RESTART_BUDGET_PER_SERVICE_PER_DAY:
            records.append({"service": service, "action": "none", "status": "RESTART_BUDGET_EXCEEDED", "requested_at": now.isoformat()})
            continue
        operation = "start" if status != "running" else "restart"
        action_id = hashlib.sha256(f"{run_id}|{service}|{now.isoformat()}|{operation}".encode("utf-8")).hexdigest()
        record = {
            "action_id": action_id,
            "service": service,
            "action": "restart",
            "operation": operation,
            "status": "REQUESTED",
            "requested_at": now.isoformat(),
            "restart_count_before": state.get("restart_count"),
            "reason": f"container_status={status}; health={health or 'not-configured'}",
        }
        _append_jsonl(action_log, record)
        actions.append(record)
        completed, attempt_count = _run_with_infrastructure_retries(
            ["docker", "compose", "--profile", "t2h-live", operation, service],
            repository_root=repository_root,
            env=env,
            runner=runner,
            sleep=sleep,
        )
        record["status"] = "PASS" if completed.returncode == 0 else "FAIL"
        record["returncode"] = completed.returncode
        record["command_attempts"] = attempt_count
        record["stdout_tail"] = completed.stdout[-1000:]
        record["stderr_tail"] = completed.stderr[-1000:]
        _append_jsonl(action_log, record)
        records.append(record)
    return records


def _load_active_run(state_root: Path, requested_run_id: str | None) -> str | None:
    if requested_run_id:
        run_id = requested_run_id
    else:
        active = _read_json(state_root / "active_run.json")
        if not isinstance(active, Mapping):
            return None
        run_id = active.get("run_id")
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id) or run_id in {".", ".."}:
        return None
    return run_id


def _update_alert_evidence(ops_dir: Path, report: Mapping[str, Any]) -> None:
    alert_path = ops_dir / "alerts.jsonl"
    status_path = ops_dir / "alert_status.json"
    prior = _read_json(status_path)
    data_errors = list(report.get("data_errors") or [])
    infrastructure_errors = list(report.get("infrastructure_errors") or [])
    active = bool(data_errors or infrastructure_errors)
    fingerprint = hashlib.sha256(
        json.dumps(
            {
                "run_id": report.get("run_id"),
                "data_errors": sorted(data_errors),
                "infrastructure_errors": sorted(infrastructure_errors),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    was_active = isinstance(prior, Mapping) and prior.get("active") is True
    prior_fingerprint = prior.get("fingerprint") if isinstance(prior, Mapping) else None
    if active and (not was_active or prior_fingerprint != fingerprint):
        _append_jsonl(alert_path, {
            "event": "OPEN",
            "created_at": report.get("checked_at"),
            "run_id": report.get("run_id"),
            "fingerprint": fingerprint,
            "data_errors": sorted(data_errors),
            "infrastructure_errors": sorted(infrastructure_errors),
            "restart_count": report.get("restart_count"),
        })
    elif not active and was_active:
        _append_jsonl(alert_path, {
            "event": "RESOLVED",
            "created_at": report.get("checked_at"),
            "run_id": report.get("run_id"),
            "fingerprint": prior_fingerprint,
            "restart_count": report.get("restart_count"),
        })
    _atomic_json(status_path, {
        "active": active,
        "fingerprint": fingerprint if active else None,
        "updated_at": report.get("checked_at"),
    })


def monitor_once(
    repository_root: Path,
    state_root: Path,
    *,
    run_id: str | None = None,
    now: datetime | None = None,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    run_delta_audit: bool = True,
) -> dict[str, Any]:
    instant = now or datetime.now(timezone.utc)
    instant = instant.replace(tzinfo=timezone.utc) if instant.tzinfo is None or instant.utcoffset() is None else instant.astimezone(timezone.utc)
    selected_run_id = _load_active_run(state_root, run_id)
    if selected_run_id is None:
        return {"status": "NO_OFFICIAL_COHORT", "checked_at": instant.isoformat(), "cohort_started": False}

    run_state = state_root / selected_run_id
    request = _read_json(run_state / "start_request.json")
    manifest = _read_json(run_state / "cohort_manifest.json")
    cohort_status = _read_json(run_state / "cohort_status.json")
    if isinstance(cohort_status, Mapping) and cohort_status.get("status") == "FINALIZED":
        return {
            "status": "COHORT_FINALIZED",
            "checked_at": instant.isoformat(),
            "run_id": selected_run_id,
            "cohort_id": cohort_status.get("cohort_id"),
            "cohort_start_target_time": cohort_status.get("cohort_start_target_time"),
            "restart_count": cohort_status.get("restart_count"),
            "cohort_started": True,
            "monitoring_actions_taken": False,
        }
    runtime_configuration = request.get("runtime_configuration", {}) if isinstance(request, Mapping) else {}
    result_dir = repository_root / "results" / "prospective-live-t2h" / selected_run_id
    inference_dir = repository_root / "results" / "streaming-inference-t2h" / selected_run_id / "spark_live"
    batches = _read_jsonl(inference_dir / "inference_batches.jsonl")
    receipts = _read_jsonl(inference_dir / "forecast_persistence_receipts.jsonl")
    producer_runtime = _read_json(result_dir / "runtime" / "producer_runtime.json")
    containers = collect_container_states(repository_root, selected_run_id, runtime_configuration, runner=runner)
    errors, warnings = _data_findings(
        request=request if isinstance(request, Mapping) else None,
        manifest=manifest if isinstance(manifest, Mapping) else None,
        cohort_status=cohort_status if isinstance(cohort_status, Mapping) else None,
        inference_batches=batches,
        receipts=receipts,
        producer_runtime=producer_runtime if isinstance(producer_runtime, Mapping) else None,
        now=instant,
    )
    producer_runtime_age_seconds: float | None = None
    producer_summary_path = result_dir / "runtime" / "producer_runtime.json"
    if producer_runtime is None:
        if isinstance(cohort_status, Mapping) and cohort_status.get("status") == "COLLECTING":
            warnings.append("PRODUCER_RUNTIME_SUMMARY_MISSING")
            producer_state = containers.get("live-hourly-producer-t2h")
            if isinstance(producer_state, dict) and producer_state.get("status") == "running":
                producer_state["health"] = "unhealthy"
                producer_state["unhealthy_reason"] = "producer runtime summary missing"
    else:
        try:
            producer_runtime_age_seconds = max(
                0.0,
                instant.timestamp() - producer_summary_path.stat().st_mtime,
            )
        except OSError:
            producer_runtime_age_seconds = None
        if producer_runtime_age_seconds is not None and producer_runtime_age_seconds > 15 * 60:
            warnings.append("PRODUCER_RUNTIME_SUMMARY_STALE_OVER_15_MINUTES")
            producer_state = containers.get("live-hourly-producer-t2h")
            if isinstance(producer_state, dict) and producer_state.get("status") == "running":
                producer_state["health"] = "unhealthy"
                producer_state["unhealthy_reason"] = "producer runtime summary stale"
    if "COHORT_UPDATE_STALE_OVER_90_MINUTES" in warnings or "COHORT_UPDATE_TIMESTAMP_MISSING" in warnings:
        inference_state = containers.get("streaming-inference-t2h-live")
        if isinstance(inference_state, dict) and inference_state.get("status") == "running":
            inference_state["health"] = "unhealthy"
            inference_state["unhealthy_reason"] = "prospective cohort heartbeat stale"
    infra_errors = [
        f"CONTAINER_{service.upper().replace('-', '_')}_{state.get('status', 'unknown').upper()}"
        for service, state in containers.items()
        if state.get("status") != "running" or state.get("health") == "unhealthy"
    ]
    if "PRODUCER_RUNTIME_SUMMARY_MISSING" in warnings:
        infra_errors.append("PRODUCER_RUNTIME_SUMMARY_MISSING")
    if "PRODUCER_RUNTIME_SUMMARY_STALE_OVER_15_MINUTES" in warnings:
        infra_errors.append("PRODUCER_RUNTIME_SUMMARY_STALE_OVER_15_MINUTES")
    if "PRODUCER_LOCATION_FETCH_FAILURE" in warnings:
        infra_errors.append("PRODUCER_LOCATION_FETCH_FAILURE")
    if "PRODUCER_RUNTIME_STATUS_NOT_PASS" in warnings:
        infra_errors.append("PRODUCER_RUNTIME_STATUS_NOT_PASS")
    if "COHORT_UPDATE_STALE_OVER_90_MINUTES" in warnings:
        infra_errors.append("COHORT_UPDATE_STALE_OVER_90_MINUTES")
    if "COHORT_UPDATE_TIMESTAMP_MISSING" in warnings:
        infra_errors.append("COHORT_UPDATE_TIMESTAMP_MISSING")
    infra_errors = sorted(set(infra_errors))
    container_restart_counts = {
        service: state.get("restart_count") for service, state in containers.items()
    }
    report = {
        "checked_at": instant.isoformat(),
        "run_id": selected_run_id,
        "cohort_id": (manifest or {}).get("cohort_id") if isinstance(manifest, Mapping) else (cohort_status or {}).get("cohort_id") if isinstance(cohort_status, Mapping) else None,
        "cohort_status": (cohort_status or {}).get("status") if isinstance(cohort_status, Mapping) else "WAITING_FOR_VALID_CYCLE",
        "cohort_start_target_time": (manifest or {}).get("cohort_start_target_time") if isinstance(manifest, Mapping) else (cohort_status or {}).get("cohort_start_target_time") if isinstance(cohort_status, Mapping) else None,
        "cohort_end_target_time": (manifest or {}).get("cohort_end_target_time") if isinstance(manifest, Mapping) else (cohort_status or {}).get("cohort_end_target_time") if isinstance(cohort_status, Mapping) else None,
        "protocol_id": (manifest or request or {}).get("cohort_protocol_id") if isinstance(manifest or request, Mapping) else None,
        "expected_locations": EXPECTED_LOCATIONS,
        "expected_target_hours": EXPECTED_TARGET_HOURS,
        "expected_slots": EXPECTED_SLOTS,
        "model_sha256": MODEL_SHA256,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "provider_model": PROVIDER_MODEL,
        "forecast_horizon_hours": 2,
        "microbatch_update_count": (cohort_status or {}).get("microbatch_update_count") if isinstance(cohort_status, Mapping) else None,
        "last_update_at": (cohort_status or {}).get("last_update_at") if isinstance(cohort_status, Mapping) else None,
        "prospective_forecast_count": (cohort_status or {}).get("prospective_forecast_count") if isinstance(cohort_status, Mapping) else None,
        "valid_evaluation_count": (cohort_status or {}).get("valid_evaluation_count") if isinstance(cohort_status, Mapping) else None,
        "restart_count": (cohort_status or {}).get("restart_count") if isinstance(cohort_status, Mapping) else None,
        "container_restart_counts": container_restart_counts,
        "container_restart_count_total": sum(
            int(value) for value in container_restart_counts.values() if isinstance(value, int)
        ),
        "producer_runtime_age_seconds": producer_runtime_age_seconds,
        "latest_inference_batch": _latest(batches),
        "persistence_receipt_count": len(receipts),
        "container_states": containers,
        "data_errors": errors,
        "infrastructure_errors": infra_errors,
        "warnings": warnings,
        "status": "PASS" if not errors and not infra_errors else "FAIL",
    }

    ops_dir = result_dir / "runtime" / "ops"
    ops_dir.mkdir(parents=True, exist_ok=True)
    hourly_path = ops_dir / "hourly_reports.jsonl"
    prior_hourly = _read_jsonl(hourly_path)
    hour_key = instant.strftime("%Y-%m-%dT%H:00Z")
    previous_hourly = _latest(prior_hourly)
    hourly_entry_due = previous_hourly is None or previous_hourly.get("hour_key") != hour_key
    delta_audit_due = bool(
        run_delta_audit
        and manifest
        and isinstance(cohort_status, Mapping)
        and (hourly_entry_due or not previous_hourly.get("hourly_delta_audit"))
    )
    if hourly_entry_due or delta_audit_due:
        if delta_audit_due:
            audit_path = ops_dir / f"delta_audit_{instant.strftime('%Y%m%dT%H%M%SZ')}.json"
            audit = _delta_audit(
                repository_root,
                selected_run_id,
                runtime_configuration,
                expected_live_forecasts=int(cohort_status.get("prospective_forecast_count", 0) or 0),
                audit_path=audit_path,
                runner=runner,
            )
            report["hourly_delta_audit"] = audit
            if audit["status"] != "PASS":
                if audit.get("checks") is not None:
                    report["data_errors"] = sorted(set([*report["data_errors"], "HOURLY_DELTA_AUDIT_FAILED"]))
                else:
                    report["infrastructure_errors"] = sorted(set([*report["infrastructure_errors"], "HOURLY_DELTA_AUDIT_COULD_NOT_RUN"]))
                report["status"] = "FAIL"
        hourly = {
            "report_type": "HOURLY_DATA_AUDIT",
            "hour_key": hour_key,
            "checked_at": instant.isoformat(),
            "run_id": selected_run_id,
            "status": report["status"],
            "prospective_forecast_count": report["prospective_forecast_count"],
            "valid_evaluation_count": report["valid_evaluation_count"],
            "persistence_receipt_count": report["persistence_receipt_count"],
            "latest_inference_batch": report["latest_inference_batch"],
            "hourly_delta_audit": report.get("hourly_delta_audit"),
            "restart_count": report["restart_count"],
        }
        _append_jsonl(hourly_path, hourly)

    action_log = ops_dir / "restart_actions.jsonl"
    report["recovery_actions"] = recover_infrastructure(
        repository_root,
        selected_run_id,
        runtime_configuration,
        containers,
        data_errors=report["data_errors"],
        action_log=action_log,
        now=instant,
        runner=runner,
    )
    if report["recovery_actions"]:
        report["warnings"] = sorted(set([*report["warnings"], "INFRASTRUCTURE_RECOVERY_ACTION_RECORDED"]))

    monitor_log = ops_dir / "monitor.jsonl"
    prior_reports = _read_jsonl(monitor_log)
    _append_jsonl(monitor_log, report)
    date_key = instant.strftime("%Y-%m-%d")
    daily_log = ops_dir / "daily_reports.jsonl"
    prior_daily = _read_jsonl(daily_log)
    if not prior_daily or prior_daily[-1].get("date_key") != date_key:
        cutoff = instant - timedelta(hours=24)
        last_day = [
            row for row in [*prior_reports, report]
            if (timestamp := _parse_timestamp(row.get("checked_at"))) is not None and timestamp >= cutoff
        ]
        counts = Counter(str(row.get("status", "UNKNOWN")) for row in last_day)
        _append_jsonl(daily_log, {
            "report_type": "DAILY_SUMMARY",
            "date_key": date_key,
            "created_at": instant.isoformat(),
            "run_id": selected_run_id,
            "monitor_samples_last_24h": len(last_day),
            "status_counts_last_24h": dict(sorted(counts.items())),
            "latest_status": report["status"],
            "latest_forecast_count": report["prospective_forecast_count"],
            "latest_valid_evaluation_count": report["valid_evaluation_count"],
            "restart_count": report["restart_count"],
            "data_errors": report["data_errors"],
            "infrastructure_errors": report["infrastructure_errors"],
        })
    _update_alert_evidence(ops_dir, report)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor the active T2H 168-hour prospective cohort.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--state-root", type=Path, default=None)
    parser.add_argument("--run-id")
    parser.add_argument("--skip-delta-audit", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = args.repo_root.resolve()
    state_root = (args.state_root or repo_root / "data" / "runtime" / "prospective-live-t2h-168h-v1").resolve()
    try:
        compose_env = validate_azure_compose_configuration(repo_root, os.environ, runner=subprocess.run)
    except AzureComposeConfigurationError as exc:
        print(json.dumps({
            "status": "FAIL",
            "reason": "AZURE_COMPOSE_CONFIGURATION_INVALID",
            "detail": str(exc),
        }, ensure_ascii=False, sort_keys=True))
        return 2
    os.environ.update(compose_env)
    report = monitor_once(
        repo_root,
        state_root,
        run_id=args.run_id,
        run_delta_audit=not args.skip_delta_audit,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, allow_nan=False))
    return 1 if report.get("status") == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
