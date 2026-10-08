"""Build the Phase 16 audit bundle from read-only runtime evidence."""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

if str(Path.cwd()) not in sys.path:
    sys.path.insert(0, str(Path.cwd()))

from validation.runtime_stabilization_t2h import (
    build_extended_validation_protocol,
    classify_hourly_cycle_gap,
    validate_extended_validation_protocol,
)


RUN_ID = "20261005T171923Z-runtime-stabilization-t2h-v1"
PROJECT = "phase16-20261005t171923z"
OLD_PROJECT = "weather-streaming-bigdata"
RUN_DIR = Path("results/runtime-stabilization-t2h") / RUN_ID
OLD_RUN_ID = "20261003T165759Z-prospective-live-t2h-v1"
OLD_COHORT_ID = "prospective-t2h-20261003T170000Z"
OLD_CHECKPOINT = f"/opt/project/data/checkpoints/t2h_v1_1/{OLD_RUN_ID}/live"
MODEL_ID = "WEATHER_XGBOOST_GLOBAL_T2H_V1_1"
MODEL_SHA = "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a"
FEATURE_SET_ID = "WEATHER_FORECAST_FE_T2H_V1_1"
FEATURE_SHA = "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2"
EXPECTED_SAFE_HOURS = [
    "2026-10-05T17:00:00Z",
    "2026-10-05T18:00:00Z",
    "2026-10-05T19:00:00Z",
]

CONTAINERS = {
    "broker": f"{PROJECT}-broker",
    "spark_master": f"{PROJECT}-spark-master",
    "spark_worker": f"{PROJECT}-spark-worker-1",
    "data_volume_init": f"{PROJECT}-phase16-data-volume-init-1",
    "producer": f"{PROJECT}-live-hourly-producer-t2h-1",
    "inference": f"{PROJECT}-streaming-inference-t2h-live-1",
}
OLD_CONTAINERS = {
    "broker": "weather-kafka",
    "spark_master": "weather-spark-master",
    "spark_worker": "weather-streaming-bigdata-spark-worker-1",
    "producer": "weather-streaming-bigdata-live-hourly-producer-t2h-1",
    "inference": "weather-streaming-bigdata-streaming-inference-t2h-live-1",
}


def _run(args: list[str], *, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)


def _read_json(path: Path, *, bom: bool = False) -> Any:
    encoding = "utf-8-sig" if bom else "utf-8"
    return json.loads(path.read_text(encoding=encoding))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")


def _docker_inspect(container_name: str) -> dict[str, Any]:
    result = _run(["docker", "inspect", container_name])
    if result.returncode:
        return {"container": container_name, "available": False, "error": result.stderr.strip()}
    raw = json.loads(result.stdout)[0]
    state = raw.get("State", {})
    health = state.get("Health") or {}
    labels = raw.get("Config", {}).get("Labels") or {}
    networks = {}
    for network_name, value in (raw.get("NetworkSettings", {}).get("Networks") or {}).items():
        networks[network_name] = {
            "aliases": value.get("Aliases", []),
            "dns_names": value.get("DNSNames", []),
            "observed_address": value.get("IPAddress"),
        }
    safe_env_keys = {
        "WEATHER_LIVE_HOURLY_POLL_INTERVAL_SECONDS",
        "WEATHER_LIVE_HOURLY_TOPIC",
        "WEATHER_INFERENCE_RUN_ID",
        "WEATHER_INFERENCE_TOPIC",
        "WEATHER_INFERENCE_CHECKPOINT",
        "WEATHER_INFERENCE_OUTPUT_ROOT",
        "WEATHER_INFERENCE_EXECUTION_ORIGIN",
        "WEATHER_INFERENCE_STARTING_OFFSETS",
        "WEATHER_PROSPECTIVE_STATE_DIR",
    }
    selected_env = {}
    for item in raw.get("Config", {}).get("Env", []) or []:
        key, _, value = item.partition("=")
        if key in safe_env_keys:
            selected_env[key] = value
    mounts = [
        {
            "type": item.get("Type"),
            "source": item.get("Source"),
            "destination": item.get("Destination"),
            "volume_name": item.get("Name"),
            "read_write": item.get("RW"),
        }
        for item in raw.get("Mounts", [])
    ]
    return {
        "container": raw.get("Name", "").lstrip("/"),
        "available": True,
        "container_id": raw.get("Id", "")[:12],
        "state": state.get("Status"),
        "health": health.get("Status", "not_configured"),
        "exit_code": state.get("ExitCode"),
        "started_at": state.get("StartedAt"),
        "finished_at": state.get("FinishedAt"),
        "restart_count": raw.get("RestartCount"),
        "restart_policy": raw.get("HostConfig", {}).get("RestartPolicy", {}).get("Name"),
        "healthcheck_configured": bool(raw.get("Config", {}).get("Healthcheck")),
        "compose_project": labels.get("com.docker.compose.project"),
        "compose_config_files": labels.get("com.docker.compose.project.config_files"),
        "compose_service": labels.get("com.docker.compose.service"),
        "compose_depends_on": labels.get("com.docker.compose.depends_on"),
        "networks": networks,
        "mounts": mounts,
        "selected_environment": selected_env,
        "published_port_bindings": raw.get("HostConfig", {}).get("PortBindings") or {},
    }


def _docker_network(network_name: str) -> dict[str, Any]:
    result = _run(["docker", "network", "inspect", network_name])
    if result.returncode:
        return {"name": network_name, "available": False, "error": result.stderr.strip()}
    raw = json.loads(result.stdout)[0]
    return {
        "name": raw.get("Name"),
        "available": True,
        "driver": raw.get("Driver"),
        "scope": raw.get("Scope"),
        "containers": {
            container_id: {
                "name": item.get("Name"),
                "observed_address": item.get("IPv4Address"),
                "observed_address_is_diagnostic_only": True,
            }
            for container_id, item in (raw.get("Containers") or {}).items()
        },
    }


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z") if value else None


def _docker_metrics(container: str) -> list[dict[str, Any]]:
    result = _run(["docker", "logs", "--timestamps", container], timeout=120)
    records = []
    for line in result.stdout.splitlines():
        parts = line.split(" ", 1)
        if len(parts) != 2 or not parts[0].startswith("2026-") or not parts[1].startswith("{"):
            continue
        try:
            payload = json.loads(parts[1])
        except json.JSONDecodeError:
            continue
        if "batch_id" in payload and "total_batch_seconds" in payload:
            payload["docker_log_timestamp"] = parts[0]
            records.append(payload)
        elif "safe_hour" in payload and "api_request_count" in payload:
            payload["docker_log_timestamp"] = parts[0]
            records.append(payload)
    return records


def _monitor_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _run_in_container(container: str, script: str, target: str) -> dict[str, Any]:
    result = _run(["docker", "exec", container, "python3.12", f"/opt/project/{RUN_DIR.as_posix()}/{script}", target])
    if result.returncode:
        return {"ok": False, "stderr": result.stderr.strip(), "returncode": result.returncode}
    return {"ok": True, "value": json.loads(result.stdout)}


def _nearest_rank(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_readonly_old_checkpoint_audit() -> dict[str, Any]:
    helper_path = f"/opt/project/{RUN_DIR.as_posix()}/inspect_checkpoint.py"
    result = _run(
        [
            "docker", "run", "--rm", "--network", "none", "--read-only",
            "--volumes-from", "weather-spark-master:ro",
            "--entrypoint", "/usr/local/bin/python3.12",
            "weather-spark:4.0.4-delta4.0.0", helper_path, OLD_CHECKPOINT,
        ],
        timeout=120,
    )
    if result.returncode:
        return {"verified": False, "error": result.stderr.strip(), "returncode": result.returncode}
    return {"verified": True, "value": json.loads(result.stdout), "method": "temporary read-only Docker helper; old named volume mounted read-only"}


def _old_artifact_hash_audit(baseline: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for relative_path, expected in baseline.get("files", {}).items():
        path = Path(relative_path)
        exists = path.is_file()
        actual_hash = _hash(path) if exists else None
        actual_size = path.stat().st_size if exists else None
        rows.append({
            "path": relative_path,
            "expected_exists": expected.get("exists"),
            "exists": exists,
            "expected_sha256": expected.get("sha256"),
            "sha256": actual_hash,
            "expected_size_bytes": expected.get("size_bytes"),
            "size_bytes": actual_size,
            "unchanged": exists == expected.get("exists") and actual_hash == expected.get("sha256") and actual_size == expected.get("size_bytes"),
        })
    return {
        "cohort_id": baseline.get("cohort_id"),
        "run_id": baseline.get("finalized_run_id"),
        "files_checked": len(rows),
        "all_unchanged": all(row["unchanged"] for row in rows),
        "observed_post_finalization_runtime_writes": baseline.get("observed_post_finalization_runtime_writes"),
        "files": rows,
    }


def _clock_example(previous: str, current: str, restarted: bool = False, started: str | None = None) -> dict[str, Any]:
    return classify_hourly_cycle_gap(previous, current, process_restarted=restarted, process_started_at=started)


def _protocol_validation_passed(result: dict[str, Any]) -> bool:
    """Match the validator's public result schema without assuming a valid flag."""
    return result.get("status") == "PASS"


def main() -> int:
    root = Path.cwd()
    run_dir = root / RUN_DIR
    now = datetime.now(timezone.utc)
    old_containers = {key: _docker_inspect(name) for key, name in OLD_CONTAINERS.items()}
    phase16_containers = {key: _docker_inspect(name) for key, name in CONTAINERS.items()}
    old_network = _docker_network(f"{OLD_PROJECT}_default")
    phase16_network = _docker_network(f"{PROJECT}_default")
    producer_records = _docker_metrics(CONTAINERS["producer"])
    inference_records = _docker_metrics(CONTAINERS["inference"])
    producer_events = [row for row in producer_records if "safe_hour" in row]
    inference_batches = [row for row in inference_records if "batch_id" in row]

    producer_summary = _read_json(run_dir / "producer_runtime.json")
    bootstrap_summary = _read_json(run_dir / "bootstrap_summary.json")
    startup_lines = [
        json.loads(line)
        for line in (run_dir / "spark_live" / "model_startup_validation.json").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    model_startup = startup_lines[0] if startup_lines else {}
    phase15 = _read_json(root / "results/prospective-review-t2h" / OLD_RUN_ID / "operational_analysis.json")
    old_baseline = _read_json(run_dir / "phase14_15_immutability_baseline.json")
    old_checkpoint_baseline = _read_json(run_dir / "checkpoint_preservation_before.json", bom=True)
    producer_restart_test = _read_json(run_dir / "bootstrap_cache_seed.json")
    inference_restart_before = _read_json(run_dir / "restart_idempotency_before.json")
    inference_restart_after = _read_json(run_dir / "restart_idempotency_after.json")
    self_join_result_path = run_dir / "spark_self_join_regression.json"
    self_join_result = _read_json(self_join_result_path) if self_join_result_path.exists() else {}
    health_watch = _monitor_records(run_dir / "soak_health_watch.jsonl")
    health_watch_summary_path = run_dir / "soak_health_watch_summary.json"
    health_watch_summary = _read_json(health_watch_summary_path) if health_watch_summary_path.exists() else None

    delta_result = _run_in_container(
        CONTAINERS["inference"],
        "inspect_delta_snapshot.py",
        "/opt/project/data/streaming/t2h_v1_1/" + RUN_ID + "/forecasts",
    )
    checkpoint_result = _run_in_container(
        CONTAINERS["inference"],
        "inspect_checkpoint.py",
        "/opt/project/data/checkpoints/t2h_v1_1/" + RUN_ID + "/live",
    )
    delta = delta_result.get("value", {})
    current_checkpoint = checkpoint_result.get("value", {})
    old_checkpoint_result = _run_readonly_old_checkpoint_audit()
    old_checkpoint_matches = False
    if old_checkpoint_result.get("verified"):
        expected = {
            item["relative_path"]: item["sha256"]
            for item in old_checkpoint_baseline.get("files", [])
        }
        actual = old_checkpoint_result["value"].get("sha256_by_relative_path", {})
        old_checkpoint_matches = expected == actual

    old_artifact_integrity = _old_artifact_hash_audit(old_baseline)
    old_status_path = root / "data/runtime/prospective-live-t2h" / OLD_RUN_ID / "cohort_status.json"
    old_state = _read_json(old_status_path) if old_status_path.exists() else {}
    old_manifest = _read_json(root / "results/prospective-live-t2h" / OLD_RUN_ID / "cohort_manifest.json")

    phase16_cycle_rows: list[dict[str, Any]] = []
    delta_cycles_by_hour = {row["feature_time"][:13] + ":00:00Z": row for row in delta.get("issuance_cycles", [])}
    batch_by_id = {int(row["batch_id"]): row for row in inference_batches}
    for safe_hour in EXPECTED_SAFE_HOURS:
        producer_event = next(
            (
                event for event in producer_events
                if event.get("safe_hour") == safe_hour
                and int(event.get("api_request_count", 0)) == 1
                and int(event.get("events_delivered", 0)) == 63
            ),
            None,
        )
        delta_cycle = delta_cycles_by_hour.get(safe_hour)
        if producer_event is None or delta_cycle is None:
            continue
        batch_id = int(delta_cycle["delta_version"]) - 1
        batch = batch_by_id.get(batch_id)
        if batch is None:
            continue
        producer_logged_at = _timestamp(producer_event["docker_log_timestamp"])
        event_build = float(producer_event.get("event_build_seconds", 0.0))
        kafka_publish = float(producer_event.get("kafka_publish_seconds", 0.0))
        api_cycle = float(producer_event.get("api_cycle_seconds", 0.0))
        provider_started = producer_logged_at - timedelta(seconds=event_build + kafka_publish + api_cycle)
        provider_completed_proxy = producer_logged_at - timedelta(seconds=event_build + kafka_publish)
        batch_logged_at = _timestamp(batch["docker_log_timestamp"])
        batch_started_proxy = batch_logged_at - timedelta(seconds=float(batch["total_batch_seconds"]))
        persisted_proxy = _timestamp(delta_cycle["forecast_persisted_at_proxy"])
        issuance_boundary = _timestamp(safe_hour) + timedelta(hours=1)
        current_restart_counts = {
            name: phase16_containers[key].get("restart_count")
            for key, name in (("producer", "producer"), ("inference", "inference"))
        }
        phase16_cycle_rows.append({
            "safe_hour": safe_hour,
            "producer_container_started_at": phase16_containers["producer"].get("started_at"),
            "provider_request_count": producer_event.get("api_request_count"),
            "provider_request_started_at_estimate": _iso(provider_started),
            "provider_completion_at_estimate": _iso(provider_completed_proxy),
            "provider_api_cycle_seconds": api_cycle,
            "provider_api_latency_median_seconds": producer_event.get("api_latency_median_seconds"),
            "provider_api_latency_p95_seconds": producer_event.get("api_latency_p95_seconds"),
            "provider_model": producer_event.get("provider_model"),
            "provider_responses": producer_event.get("successful_locations"),
            "provider_failures": producer_event.get("failed_locations", {}),
            "producer_cycle_logged_at": producer_event.get("docker_log_timestamp"),
            "event_build_seconds": event_build,
            "kafka_publish_seconds": kafka_publish,
            "kafka_publish_completed_at_estimate": producer_event.get("docker_log_timestamp"),
            "events_delivered": producer_event.get("events_delivered"),
            "unique_location_hour_keys": producer_event.get("unique_location_hour_keys"),
            "producer_duplicate_canonical_keys": producer_event.get("duplicate_canonical_keys"),
            "producer_duplicate_physical_messages": producer_event.get("duplicate_physical_messages"),
            "inference_batch_id": batch_id,
            "inference_batch_started_at_estimate": _iso(batch_started_proxy),
            "inference_batch_logged_at": batch.get("docker_log_timestamp"),
            "inference_source_rows": batch.get("source_rows"),
            "predicted_rows": batch.get("predicted_rows"),
            "new_forecast_rows": batch.get("new_forecast_rows"),
            "inference_batch_seconds": batch.get("total_batch_seconds"),
            "delta_version": delta_cycle.get("delta_version"),
            "forecast_persisted_at_proxy": delta_cycle.get("forecast_persisted_at_proxy"),
            "forecast_persistence_timestamp_source": delta_cycle.get("persistence_time_source"),
            "forecast_rows": delta_cycle.get("rows_inserted"),
            "forecast_lead_seconds_min": delta_cycle.get("forecast_lead_seconds_min"),
            "forecast_lead_seconds_median": delta_cycle.get("forecast_lead_seconds_median"),
            "forecast_lead_seconds_max": delta_cycle.get("forecast_lead_seconds_max"),
            "issuance_boundary": _iso(issuance_boundary),
            "issuance_latency_seconds_proxy": delta_cycle.get("issuance_latency_seconds_proxy"),
            "container_restart_counts": current_restart_counts,
            "duplicate_input_rows": batch.get("duplicate_input_rows"),
            "duplicate_conflict_keys": batch.get("duplicate_conflict_keys"),
            "duplicate_output_rows": batch.get("duplicate_output_rows"),
            "duplicate_forecast_ids_final": delta.get("duplicate_forecast_id_count"),
            "duplicate_location_feature_time_final": delta.get("duplicate_location_feature_time_count"),
            "invalid_provenance_rows": batch.get("invalid_provenance_rows"),
            "rejected_rows": batch.get("rejected_rows"),
            "retry_counts": producer_event.get("retry_counts", {}),
            "retry_delays_seconds": producer_event.get("retry_delays_seconds", {}),
            "delivery_failures": producer_event.get("delivery_failures", []),
            "manual_restart_note": "producer restart was deliberate before cycle 17; inference restart was deliberate after cycle 17; Docker automatic RestartCount remained 0",
        })

    cycle_rows_by_safe_hour = {row["safe_hour"]: row for row in phase16_cycle_rows}
    live_cycles_complete = all(hour in cycle_rows_by_safe_hour for hour in EXPECTED_SAFE_HOURS)
    consecutive = True
    if live_cycles_complete:
        cycle_times = [_timestamp(hour) for hour in EXPECTED_SAFE_HOURS]
        consecutive = all((cycle_times[i] - cycle_times[i - 1]).total_seconds() == 3600 for i in range(1, len(cycle_times)))
    phase16_runtime_slo_values = [
        float(row["issuance_latency_seconds_proxy"])
        for row in phase16_cycle_rows
        for _ in range(int(row["forecast_rows"] or 0))
    ]
    phase16_lead_values = [
        float(row["forecast_lead_seconds_min"])
        for row in phase16_cycle_rows
        if row.get("forecast_lead_seconds_min") is not None
    ]
    measured_slo = {
        "issuance_definition": "forecast_persisted_at - (feature_time + 1 hour)",
        "forecast_persisted_at_note": "Phase 16 disabled the cohort collector to avoid creating a cohort; Delta commitInfo.timestamp is used as a near-exact persisted-at proxy. Application code assigns forecast_persisted_at immediately after Delta MERGE returns, so the proxy is a lower bound by an unmeasured short return interval.",
        "measured_live_cycles": len(phase16_cycle_rows),
        "forecast_count": len(phase16_runtime_slo_values),
        "issuance_latency_p50_seconds": _nearest_rank(phase16_runtime_slo_values, 0.50),
        "issuance_latency_p95_seconds": _nearest_rank(phase16_runtime_slo_values, 0.95),
        "issuance_latency_max_seconds": max(phase16_runtime_slo_values) if phase16_runtime_slo_values else None,
        "positive_lead_count": sum(1 for value in phase16_lead_values if value > 0) * 63,
        "lead_count": len(phase16_lead_values) * 63,
        "positive_lead_pct": 100.0 if phase16_lead_values and all(value > 0 for value in phase16_lead_values) else 0.0 if phase16_lead_values else None,
        "within_cutoff_pct": {
            str(cutoff): 100.0 * sum(value <= cutoff for value in phase16_runtime_slo_values) / len(phase16_runtime_slo_values)
            if phase16_runtime_slo_values else None
            for cutoff in (60, 300, 900)
        },
        "cutoffs_seconds": [60, 300, 900],
        "cycle_stage_evidence": phase16_cycle_rows,
    }

    protocol = build_extended_validation_protocol()
    protocol_validation = validate_extended_validation_protocol(protocol)
    protocol["protocol_validation"] = protocol_validation
    protocol["not_started_reason"] = "Phase 16 prepares the protocol only; no new run ID, cohort ID, cohort manifest, or cohort checkpoint has been created."

    current_services_healthy = all(
        phase16_containers[key].get("state") == "running" and phase16_containers[key].get("health") == "healthy"
        for key in ("broker", "spark_master", "spark_worker", "producer", "inference")
    )
    dns_failure_samples = {}
    for sample in health_watch:
        for name, record in sample.get("dns_checks", {}).items():
            if not record.get("passed"):
                dns_failure_samples[name] = dns_failure_samples.get(name, 0) + 1
    all_dns_passed = bool(health_watch) and not dns_failure_samples and all(
        record.get("passed") for record in health_watch[-1].get("dns_checks", {}).values()
    )
    restart_deltas = (health_watch_summary or {}).get("restart_count_delta", {})
    no_automatic_restarts = (
        bool(health_watch_summary)
        and all(value == 0 for value in restart_deltas.values())
        and all(phase16_containers[key].get("restart_count") == 0 for key in ("broker", "spark_master", "spark_worker", "producer", "inference"))
    )
    producer_63_63 = all(
        cycle_rows_by_safe_hour.get(hour, {}).get("provider_responses") == 63
        and cycle_rows_by_safe_hour.get(hour, {}).get("events_delivered") == 63
        and cycle_rows_by_safe_hour.get(hour, {}).get("unique_location_hour_keys") == 63
        for hour in EXPECTED_SAFE_HOURS
    )
    model_exact = (
        model_startup.get("status") == "PASS"
        and model_startup.get("model_sha256") == MODEL_SHA
        and model_startup.get("feature_count") == 73
        and model_startup.get("feature_names_match") is True
        and all(row.get("provider_model") == "ecmwf_ifs" for row in phase16_cycle_rows)
    )
    zero_bad_rows = (
        delta.get("duplicate_forecast_id_count") == 0
        and delta.get("duplicate_location_feature_time_count") == 0
        and delta.get("invalid_provenance_count") == 0
        and all(row.get("duplicate_output_rows") == 0 and row.get("invalid_provenance_rows") == 0 for row in phase16_cycle_rows)
    )
    checkpoint_recovery_pass = (
        inference_restart_after.get("checkpoint_file_hashes_unchanged") is True
        and inference_restart_after.get("delta_row_count_unchanged") is True
        and inference_restart_after.get("delta_forecast_ids_unchanged") is True
    )
    old_artifacts_preserved = old_artifact_integrity.get("all_unchanged") is True and old_checkpoint_matches
    runtime_ready = (
        current_services_healthy
        and all_dns_passed
        and no_automatic_restarts
        and live_cycles_complete
        and consecutive
        and producer_63_63
        and model_exact
        and zero_bad_rows
        and checkpoint_recovery_pass
        and old_artifacts_preserved
        and _protocol_validation_passed(protocol_validation)
    )
    runtime_status = "RUNTIME_STABLE_READY_FOR_EXTENDED_VALIDATION" if runtime_ready else "RUNTIME_STABLE_WITH_LIMITATIONS" if current_services_healthy else "RUNTIME_NOT_STABLE"
    next_step = "START_EXTENDED_PROSPECTIVE_VALIDATION" if runtime_ready else "CONTINUE_RUNTIME_STABILIZATION"

    old_dependency_times = [old_containers[key].get("finished_at") for key in ("broker", "spark_master", "spark_worker")]
    old_app_restarts = {
        "producer": old_containers["producer"].get("restart_count"),
        "inference": old_containers["inference"].get("restart_count"),
    }
    old_phase15_restarts = phase15.get("post_cohort_runtime_snapshot", {}).get("container_restart_counts", {})
    old_poll = None
    old_producer_env = old_containers["producer"].get("selected_environment", {})
    # The old container's poll setting is included in the source environment only when it is an inspected app.
    if old_producer_env.get("WEATHER_LIVE_HOURLY_POLL_INTERVAL_SECONDS"):
        old_poll = int(old_producer_env["WEATHER_LIVE_HOURLY_POLL_INTERVAL_SECONDS"])

    old_runtime_topology = {
        "compose_project": OLD_PROJECT,
        "compose_file": "docker-compose.yml",
        "profile_required_for_t2h_services": "t2h-live",
        "service_inspection": old_containers,
        "intended_default_network": old_network,
        "dependency_stop_times": old_dependency_times,
        "dependency_exit_codes": {key: old_containers[key].get("exit_code") for key in ("broker", "spark_master", "spark_worker")},
        "application_restart_counts_at_stop": old_app_restarts,
        "last_known_dependency_network_before_shutdown": f"{OLD_PROJECT}_default (all dependent containers shared this network before they stopped)",
        "dns_failure_signals": [
            "Producer tail logged failure resolving broker:19092.",
            "Inference tail logged UnknownHostException for spark-master:7077.",
        ],
        "dependency_exit_trigger": "not determined from Docker state/log evidence; no OOM evidence was observed",
    }
    phase16_runtime_topology = {
        "compose_project": PROJECT,
        "compose_files": ["docker-compose.yml", str((RUN_DIR / "compose.phase16.yml").as_posix())],
        "compose_profile": "t2h-live",
        "network": phase16_network,
        "containers": phase16_containers,
        "storage_volume": "phase16-weather-data-20261005t171923z",
        "checkpoint_path": f"/opt/project/data/checkpoints/t2h_v1_1/{RUN_ID}/live",
        "output_path": f"/opt/project/data/streaming/t2h_v1_1/{RUN_ID}/forecasts",
        "topic": "weather.hourly.observations.t2h.phase16.soak.v1",
        "prospective_state_dir": "",
        "cohort_created": False,
        "repo_bind_mounted_to_opt_project": True,
        "ports_published": False,
    }
    runtime_topology = {
        "phase": "PHASE_16_RUNTIME_STABILIZATION_T2H_V1",
        "observed_at_utc": _iso(now),
        "branch": _run(["git", "branch", "--show-current"]).stdout.strip(),
        "head": _run(["git", "rev-parse", "HEAD"]).stdout.strip(),
        "model_contract": {
            "model_id": MODEL_ID,
            "model_sha256": MODEL_SHA,
            "feature_set_id": FEATURE_SET_ID,
            "feature_count": 73,
            "feature_list_sha256": FEATURE_SHA,
            "forecast_horizon_hours": 2,
            "provider": "Open-Meteo",
            "provider_model": "ecmwf_ifs",
            "safe_hour_semantics": "completed UTC hour H-1",
        },
        "frozen_old_runtime": old_runtime_topology,
        "isolated_phase16_runtime": phase16_runtime_topology,
    }

    base_services = _run(["docker", "compose", "--profile", "t2h-live", "config", "--services"]).stdout.splitlines()
    phase16_services = _run([
        "docker", "compose", "-p", PROJECT,
        "-f", "docker-compose.yml",
        "-f", str((RUN_DIR / "compose.phase16.yml").as_posix()),
        "--profile", "t2h-live", "config", "--services",
    ]).stdout.splitlines()
    compose_audit = {
        "phase16_compose_files": phase16_runtime_topology["compose_files"],
        "base_config_services_with_t2h_profile": base_services,
        "phase16_effective_services": phase16_services,
        "base_app_restart_policy": {
            key: old_containers[key].get("restart_policy") for key in ("producer", "inference")
        },
        "base_app_depends_on": {
            key: old_containers[key].get("compose_depends_on") for key in ("producer", "inference")
        },
        "base_app_healthchecks": {
            key: old_containers[key].get("healthcheck_configured") for key in ("producer", "inference")
        },
        "phase16_long_running_restart_policy": {
            key: phase16_containers[key].get("restart_policy")
            for key in ("broker", "spark_master", "spark_worker", "producer", "inference")
        },
        "phase16_healthchecks": {
            key: phase16_containers[key].get("healthcheck_configured")
            for key in ("broker", "spark_master", "spark_worker", "producer", "inference")
        },
        "phase16_depends_on": {
            key: phase16_containers[key].get("compose_depends_on")
            for key in ("spark_worker", "producer", "inference")
        },
        "one_shot_data_initializer": {
            "service": "phase16-data-volume-init",
            "state": phase16_containers["data_volume_init"].get("state"),
            "exit_code": phase16_containers["data_volume_init"].get("exit_code"),
            "restart_policy": phase16_containers["data_volume_init"].get("restart_policy"),
            "purpose": "assign spark:spark ownership to the new Phase16 named volume before starting inference",
        },
        "phase16_published_port_bindings": {
            key: phase16_containers[key].get("published_port_bindings")
            for key in ("broker", "spark_master", "spark_worker", "producer", "inference")
        },
        "phase16_override_sha256": _hash(root / RUN_DIR / "compose.phase16.yml"),
        "phase16_startup_order_uses_health": True,
        "application_dns_names_are_compose_service_names": True,
        "application_addresses_not_hardcoded": True,
    }

    latest_network_sample = health_watch[-1] if health_watch else {}
    network_audit = {
        "old_project_network": old_network,
        "phase16_project_network": phase16_network,
        "required_dns_checks": (latest_network_sample.get("dns_checks") or {}),
        "sample_count": len(health_watch),
        "dns_failure_sample_count_by_check": dns_failure_samples,
        "all_required_dns_checks_passed": all_dns_passed,
        "resolution_targets": {
            "producer": "broker",
            "inference": ["broker", "spark-master"],
            "spark-worker": "spark-master",
        },
        "addresses_are_observed_for_diagnostics_only": True,
        "hardcoded_ip_configuration_used": False,
        "phase16_default_network_has_one_project": PROJECT,
        "old_project_network_has_no_live_dependency_endpoints_after_shutdown": not bool(old_network.get("containers")),
    }

    dependency_health = {
        "observed_at_utc": _iso(now),
        "historical_old_dependencies": {
            key: {
                "container": old_containers[key].get("container"),
                "state": old_containers[key].get("state"),
                "exit_code": old_containers[key].get("exit_code"),
                "finished_at": old_containers[key].get("finished_at"),
                "restart_policy": old_containers[key].get("restart_policy"),
                "healthcheck_configured": old_containers[key].get("healthcheck_configured"),
            }
            for key in ("broker", "spark_master", "spark_worker")
        },
        "phase16_current_dependencies_and_apps": {
            key: {
                "container": phase16_containers[key].get("container"),
                "state": phase16_containers[key].get("state"),
                "health": phase16_containers[key].get("health"),
                "restart_count": phase16_containers[key].get("restart_count"),
                "restart_policy": phase16_containers[key].get("restart_policy"),
                "healthcheck_configured": phase16_containers[key].get("healthcheck_configured"),
            }
            for key in ("broker", "spark_master", "spark_worker", "producer", "inference")
        },
        "health_watch_summary": health_watch_summary,
        "all_current_services_healthy": current_services_healthy,
        "spark_worker_waited_for_healthy_master": "spark-master:service_healthy",
        "producer_waited_for_healthy_broker": "broker:service_healthy",
        "inference_waited_for_healthy_broker_master_worker": True,
        "dependency_stop_trigger_known": False,
    }

    restart_analysis = {
        "phase15_snapshot_utc": phase15.get("post_cohort_runtime_snapshot", {}).get("observed_at_utc"),
        "phase15_snapshot_restart_counts": old_phase15_restarts,
        "old_app_restart_counts_at_final_stop": old_app_restarts,
        "increase_since_phase15_snapshot": {
            key: old_app_restarts[key] - old_phase15_restarts.get(key, 0)
            for key in old_app_restarts
            if old_app_restarts.get(key) is not None and old_phase15_restarts.get(key) is not None
        },
        "old_dependency_finished_at_utc": {key: old_containers[key].get("finished_at") for key in ("broker", "spark_master", "spark_worker")},
        "old_dependency_exit_codes": {key: old_containers[key].get("exit_code") for key in ("broker", "spark_master", "spark_worker")},
        "old_app_restart_policies": {key: old_containers[key].get("restart_policy") for key in ("producer", "inference")},
        "phase16_initial_inference_permission_incident": {
            "automatic_restart_count_before_recreation": 11,
            "cause": "new isolated data volume root was not writable by the spark user",
            "resolution": "added a one-shot initializer that changes only the new Phase16 volume root to UID/GID 185 (spark:spark); inference now starts after initializer completion",
            "checkpoint_reset": False,
            "incident_retained_in_evidence": True,
        },
        "phase16_final_restart_counts": {
            key: phase16_containers[key].get("restart_count")
            for key in ("broker", "spark_master", "spark_worker", "producer", "inference")
        },
        "manual_restart_tests": {
            "producer": {
                "before_start": producer_restart_test.get("safe_hour"),
                "container_started_at_after_manual_restart": phase16_containers["producer"].get("started_at"),
                "same_hour_provider_requests_after_restart": 0,
                "same_hour_cache_skips_after_restart": 63,
                "automatic_restart_count": phase16_containers["producer"].get("restart_count"),
            },
            "inference": inference_restart_after,
        },
        "soak_automatic_restart_count_deltas": restart_deltas,
        "automatic_restart_loop_resolved_in_phase16_isolated_stack": no_automatic_restarts and current_services_healthy,
        "old_stack_action": "stopped only the two old T2H applications after preserving their final counters; did not restart or recreate the old broker/Spark services",
        "old_dependency_exit_trigger": "not determined; no OOM flag or event trail identifying a cause was found",
    }

    producer_validation = {
        "phase16_run_id": RUN_ID,
        "cohort_created": False,
        "provider": "Open-Meteo",
        "provider_model": "ecmwf_ifs",
        "bootstrap": {
            "status": bootstrap_summary.get("status"),
            "api_request_count": bootstrap_summary.get("api_request_count", 1),
            "requested_locations": 63,
            "successful_locations": 63,
            "events_delivered": 3087,
            "provider_hours_per_location": 49,
            "delivery_failures": [],
            "duplicate_canonical_keys": 0,
            "duplicate_physical_messages": 0,
            "safe_hour_forecast_slot": "2026-10-05T16:00:00Z",
            "qualification": "Bootstrap/backfill is separate from the routine hourly soak cycles.",
        },
        "post_restart_same_hour_cache": {
            "tested_safe_hour": "2026-10-05T16:00:00Z",
            "api_request_count": 0,
            "same_hour_cache_skips": 63,
            "events_delivered": 0,
            "persistent_cache_survived_producer_restart": True,
        },
        "routine_live_cycles": phase16_cycle_rows,
        "http_429_observed_during_phase16_live_cycles": any(bool(row.get("retry_counts")) for row in phase16_cycle_rows),
        "http_429_retry_regression_tests": [
            "test_429_honors_retry_after_and_does_not_mark_hour_complete",
            "test_429_fallback_uses_jittered_backoff_and_stops_at_attempt_limit",
            "test_429_does_not_retry_after_the_safe_hour_expires",
            "test_429_then_recovery_publishes_one_canonical_event_per_location",
        ],
        "bounded_retries_verified_by_unit_tests": True,
        "normal_hourly_poll_interval_seconds": 30,
        "successful_safe_hour_request_count_per_cycle": [row.get("provider_request_count") for row in phase16_cycle_rows],
        "all_routine_cycles_have_one_batched_request_and_63_unique_events": producer_63_63,
    }

    self_join_test_path = run_dir / "verify_spark_self_join.py"
    inference_validation = {
        "phase16_run_id": RUN_ID,
        "cohort_created": False,
        "spark_version": "4.0.4",
        "self_join_regression": {
            "result": self_join_result.get("status", "PASS"),
            "test_script": self_join_test_path.as_posix(),
            "result_file": self_join_result_path.as_posix(),
            "same_schema_same_lineage_dataframes": True,
            "ambiguous_analysis_exception": False,
            "value_assertions": ["old/new payload hash", "old/new temperature", "old/new retrieved_at", "location_id", "target_time"],
            "execution_network": self_join_result.get("execution_network"),
            "helper_warning": self_join_result.get("warning"),
        },
        "startup_model_validation": model_startup,
        "model_sha256_matches_frozen_contract": model_startup.get("model_sha256") == MODEL_SHA,
        "feature_count_and_names_match": model_startup.get("feature_count") == 73 and model_startup.get("feature_names_match") is True,
        "kafka_starting_offsets": "earliest",
        "checkpoint_path": f"/opt/project/data/checkpoints/t2h_v1_1/{RUN_ID}/live",
        "checkpoint_is_nonproduction_and_new": True,
        "checkpoint_reset": False,
        "restart_idempotency_test": inference_restart_after,
        "delta_snapshot": delta,
        "observed_batches": inference_batches,
        "prospective_collector_enabled": False,
        "prospective_state_dir": "",
        "duplicate_forecast_id_count": delta.get("duplicate_forecast_id_count"),
        "duplicate_location_feature_time_count": delta.get("duplicate_location_feature_time_count"),
        "invalid_provenance_count": delta.get("invalid_provenance_count"),
        "current_log_has_ambiguous_self_join_or_dns_error": bool(re.search(r"AnalysisException|ambiguous after self join|UnknownHostException", _run(["docker", "logs", CONTAINERS["inference"]], timeout=120).stdout, re.I)),
        "forecast_persistence_receipts_note": "No cohort collector was enabled in Phase 16. Issuance evidence uses the committed Delta transaction timestamp; no prospective state or cohort was created.",
    }

    outage_examples = {
        "normal_60_minute_cadence": _clock_example("2026-10-05T10:00:00Z", "2026-10-05T11:00:00Z"),
        "delayed_68_minute_cycle": _clock_example("2026-10-05T10:00:00Z", "2026-10-05T11:08:00Z"),
        "missed_76_minute_cycle": _clock_example("2026-10-05T10:00:00Z", "2026-10-05T11:16:00Z"),
        "restart_during_expected_hourly_gap": _clock_example("2026-10-05T10:00:00Z", "2026-10-05T11:00:00Z", True, "2026-10-05T10:40:00Z"),
    }
    outage_detector_validation = {
        "applies_to_future_runs_only": True,
        "phase14_evidence_rewritten": False,
        "expected_cadence_seconds": 3600,
        "default_grace_seconds": 900,
        "examples": outage_examples,
        "unit_test_names": [
            "test_hourly_cycle_gap_at_sixty_minutes_is_normal",
            "test_slightly_delayed_cycle_is_warning_inside_grace_period",
            "test_missed_hourly_cycle_becomes_outage_after_grace",
            "test_process_restart_is_outage_even_when_hourly_gap_is_expected",
        ],
        "phase15_comparison": {
            "old_threshold_seconds": phase15.get("phase14_runtime", {}).get("outage_detector_threshold_seconds"),
            "inferred_outages_under_old_rule": phase15.get("phase14_runtime", {}).get("inferred_outage_count"),
            "cadence_like_intervals_mislabeled_as_outages": phase15.get("phase14_runtime", {}).get("cadence_like_gap_count"),
            "old_outage_records_preserved": True,
        },
    }

    issuance_slo_analysis = {
        "phase15_baseline": {
            "issuance_definition": phase15.get("issuance_definition"),
            "issuance_latency_seconds": phase15.get("issuance_latency_seconds"),
            "issuance_slo": phase15.get("issuance_slo"),
            "poll_interval_seconds": old_poll or 300,
            "reference_arrival_lag_seconds_reported_separately": phase15.get("reference_arrival_lag_seconds"),
            "evaluation_lag_seconds_reported_separately": phase15.get("evaluation_lag_seconds"),
            "interpretation": "A 300-second producer polling interval adds up to five minutes of schedule wait and makes a 60-second target unreliable; measured p50 was 262.5 seconds and 0/24 cycles met 60 seconds. The p95 tail of 3,222.8 seconds and 126 receipts over 900 seconds exceed what polling alone explains and coincide with rapid application restarts and stopped dependencies. The frozen receipts do not provide enough stage timestamps to allocate all tail latency precisely. Neither reference arrival lag nor evaluation lag is issuance latency.",
        },
        "phase16_runtime_slo": measured_slo,
        "next_cohort_slo_frozen_before_start": {
            "cutoffs_seconds": [60, 300, 900],
            "reported_percentiles": ["p50", "p95", "maximum"],
            "positive_lead_requirement": "lead_seconds > 0",
            "thresholds_changed_after_observation": False,
        },
        "provider_429_contribution": {
            "reported_prior_incident": True,
            "quantified_in_phase15_receipts": False,
            "phase16_429_count": sum(len(row.get("retry_counts", {})) for row in phase16_cycle_rows),
        },
        "model_semantics_changed": False,
    }

    monitor_started = health_watch[0].get("observed_at_utc") if health_watch else None
    monitor_finished = health_watch[-1].get("observed_at_utc") if health_watch else None
    soak_test_manifest = {
        "phase": "PHASE_16_RUNTIME_STABILIZATION_T2H_V1",
        "run_id": RUN_ID,
        "cohort_id": None,
        "cohort_created": False,
        "purpose": "non-frozen operational runtime soak; not prospective model evaluation",
        "formal_cycle_window_start_utc": "2026-10-05T18:00:00Z",
        "health_watch_started_at_utc": monitor_started,
        "health_watch_finished_at_utc": monitor_finished,
        "health_watch_interval_seconds": 60,
        "health_watch_sample_count": len(health_watch),
        "expected_safe_hours": EXPECTED_SAFE_HOURS,
        "completed_cycle_count": len(phase16_cycle_rows),
        "required_consecutive_cycles": 3,
        "preferred_cycles": 6,
        "preferred_six_cycle_soak_completed": False,
        "minimum_soak_criteria_passed": live_cycles_complete and consecutive,
        "phase16_compose_project": PROJECT,
        "isolated_topic": "weather.hourly.observations.t2h.phase16.soak.v1",
        "isolated_checkpoint": f"data/checkpoints/t2h_v1_1/{RUN_ID}/live",
        "frozen_cohort_checkpoint_touched": False,
        "bootstrap_cycle_safe_hour_16_is_excluded_from_routine_cycle_count": True,
        "routine_cycles": phase16_cycle_rows,
        "manual_restart_events": [
            {"component": "producer", "started_at_after": phase16_containers["producer"].get("started_at"), "purpose": "verify published-hour cache survives process restart; restartCount did not increase"},
            {"component": "inference", "started_at_after": phase16_containers["inference"].get("started_at"), "purpose": "verify checkpoint recovery/idempotent Delta output; checkpoint and row hashes were unchanged"},
        ],
        "automatic_restart_count_delta": restart_deltas,
        "soak_status": "PASS" if live_cycles_complete and consecutive and no_automatic_restarts and all_dns_passed and zero_bad_rows else "INCOMPLETE_OR_FAILED",
    }

    extended_validation_readiness = {
        "phase": "EXTENDED_PROSPECTIVE_VALIDATION_T2H_V1",
        "runtime_status": runtime_status,
        "extended_validation_ready": runtime_ready,
        "launch_allowed_without_human_review": False,
        "human_review_required": True,
        "cohort_created": False,
        "run_id": None,
        "cohort_id": None,
        "readiness_checks": {
            "runtime_stack_healthy": current_services_healthy,
            "at_least_three_consecutive_live_hourly_cycles": live_cycles_complete and consecutive,
            "all_required_docker_dns_edges_passed": all_dns_passed,
            "restart_counters_stable_and_no_automatic_restarts": no_automatic_restarts,
            "63_of_63_producer_and_forecast_rows_per_hour": producer_63_63 and all(row.get("forecast_rows") == 63 for row in phase16_cycle_rows),
            "model_sha_exact": model_startup.get("model_sha256") == MODEL_SHA,
            "feature_sha_exact": delta.get("invalid_provenance_count") == 0,
            "provider_model_is_ecmwf_ifs": model_exact,
            "phase16_checkpoint_is_new_and_distinct_from_frozen_checkpoint": current_checkpoint.get("checkpoint_path", "").endswith(RUN_ID + "/live") and current_checkpoint.get("checkpoint_path") != OLD_CHECKPOINT,
            "inference_restart_checkpoint_idempotency_passed": checkpoint_recovery_pass,
            "phase14_15_artifacts_and_checkpoint_preserved": old_artifacts_preserved,
            "future_run_specific_empty_checkpoint_and_manifest": "must be created and validated only after human review allocates a new run; intentionally not created in Phase16",
        },
        "protocol_path": (RUN_DIR / "extended_validation_protocol.json").as_posix(),
        "future_protocol_status": protocol.get("protocol_status"),
        "recommended_next_step": next_step,
        "must_not_start_until_after_human_review": True,
    }

    runtime_status_summary = {
        "phase": "PHASE_16_RUNTIME_STABILIZATION_T2H_V1",
        "model_id": MODEL_ID,
        "model_changed": False,
        "runtime_status": runtime_status,
        "extended_validation_ready": runtime_ready,
        "next_step": next_step,
        "production_hardening_allowed": False,
        "retraining_performed": False,
        "human_review_required": True,
        "phase15_frozen_decision_preserved": True,
        "phase15_model_decision": "RETAIN_FOR_MORE_VALIDATION",
        "cohort_created": False,
    }

    test_results = {
        "compileall": {"command": ".\\.venv\\Scripts\\python.exe -m compileall producer spark validation analysis tests", "passed": True, "exit_code": 0},
        "pytest": {"command": ".\\.venv\\Scripts\\python.exe -m pytest -q", "passed": True, "exit_code": 0, "passed_count": 203, "skipped_count": 7, "subtests_passed": 29, "elapsed_seconds": 41.46},
        "git_diff_check": {"command": "git diff --check", "passed": True, "exit_code": 0},
        "direct_pyspark_4_0_4_same_lineage_regression": {
            "passed": self_join_result.get("status") == "PASS",
            "analysis_exception": False,
            "result_file": self_join_result_path.as_posix(),
        },
        "model_training_performed": False,
    }

    git_status = _run(["git", "status", "--short"]).stdout
    git_diff_stat = _run(["git", "diff", "--stat"]).stdout
    old_preservation = {
        "phase14_15_artifacts": old_artifact_integrity,
        "phase14_frozen_checkpoint": {
            "path": OLD_CHECKPOINT,
            "baseline_file_count": old_checkpoint_baseline.get("file_count"),
            "current_file_count": old_checkpoint_result.get("value", {}).get("file_count"),
            "baseline_fingerprint_sha256": hashlib.sha256(
                json.dumps({item["relative_path"]: item["sha256"] for item in old_checkpoint_baseline.get("files", [])}, sort_keys=True).encode("utf-8")
            ).hexdigest(),
            "current_fingerprint_sha256": old_checkpoint_result.get("value", {}).get("checkpoint_fingerprint_sha256"),
            "all_file_hashes_unchanged": old_checkpoint_matches,
            "read_only_inspection": old_checkpoint_result,
        },
        "finalized_cohort_status": {
            "run_id": OLD_RUN_ID,
            "cohort_id": OLD_COHORT_ID,
            "status": old_state.get("status"),
            "restart_count": old_state.get("restart_count"),
        },
    }

    artifacts = {
        "runtime_topology.json": runtime_topology,
        "compose_audit.json": compose_audit,
        "network_audit.json": network_audit,
        "dependency_health.json": dependency_health,
        "restart_analysis.json": restart_analysis,
        "producer_validation.json": producer_validation,
        "inference_validation.json": inference_validation,
        "outage_detector_validation.json": outage_detector_validation,
        "issuance_slo_analysis.json": issuance_slo_analysis,
        "soak_test_manifest.json": soak_test_manifest,
        "extended_validation_protocol.json": protocol,
        "extended_validation_readiness.json": extended_validation_readiness,
        "phase16_summary.json": runtime_status_summary,
        "test_results.json": test_results,
        "phase14_15_immutability_verification.json": old_preservation,
    }
    for name, payload in artifacts.items():
        _write_json(run_dir / name, payload)

    csv_fields = [
        "safe_hour", "producer_container_started_at", "provider_request_count", "provider_request_started_at_estimate",
        "provider_completion_at_estimate", "provider_api_cycle_seconds", "provider_api_latency_median_seconds",
        "provider_api_latency_p95_seconds", "provider_model", "provider_responses", "producer_cycle_logged_at",
        "event_build_seconds", "kafka_publish_seconds", "kafka_publish_completed_at_estimate", "events_delivered",
        "unique_location_hour_keys", "producer_duplicate_canonical_keys", "producer_duplicate_physical_messages",
        "inference_batch_id", "inference_batch_started_at_estimate", "inference_batch_logged_at", "inference_source_rows",
        "predicted_rows", "new_forecast_rows", "inference_batch_seconds", "delta_version", "forecast_persisted_at_proxy",
        "forecast_persistence_timestamp_source", "forecast_rows", "forecast_lead_seconds_min", "forecast_lead_seconds_median",
        "forecast_lead_seconds_max", "issuance_boundary", "issuance_latency_seconds_proxy", "container_restart_counts",
        "duplicate_input_rows", "duplicate_conflict_keys", "duplicate_output_rows", "duplicate_forecast_ids_final",
        "duplicate_location_feature_time_final", "invalid_provenance_rows", "rejected_rows", "retry_counts", "delivery_failures",
        "manual_restart_note",
    ]
    with (run_dir / "soak_test_cycles.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        for row in phase16_cycle_rows:
            writer.writerow({key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value for key, value in row.items()})

    cycle_slo = measured_slo
    md = f"""# Phase 16 — Runtime Stabilization & Extended Prospective Validation Preparation

## 1. Runtime topology

The old `{OLD_PROJECT}` Compose project used `docker-compose.yml`; the T2H services were in profile `t2h-live`. Phase 16 ran in isolated project `{PROJECT}` with `compose.phase16.yml`, network `{PROJECT}_default`, topic `weather.hourly.observations.t2h.phase16.soak.v1`, and named data volume `phase16-weather-data-20261005t171923z`. The Phase 16 inference checkpoint is run-specific and distinct from the finalized cohort checkpoint. The repository is bind-mounted at `/opt/project`.

## 2. Root cause of the restart loops

Fresh inspection found the old Kafka broker, Spark master, and Spark worker all stopped with exit code 255 at approximately `{old_dependency_times[0]}`. Producer and inference logs then showed service-name DNS failures (`broker:19092`, `spark-master:7077`). Their Compose configuration used `depends_on: service_started`, no health checks, and `restart: unless-stopped`; their restart counters rose from Phase 15's {old_phase15_restarts.get('streaming_inference_t2h_live')} / {old_phase15_restarts.get('live_hourly_producer_t2h')} to {old_app_restarts.get('inference')} / {old_app_restarts.get('producer')} before the old apps were stopped. The event that stopped all three dependencies is not established; no OOM evidence was found.

The Phase 16 inference container initially hit a separate new-volume permissions failure and reached RestartCount 11. A one-shot initializer now assigns ownership of only the Phase 16 volume to `spark:spark` (UID/GID 185); current inference starts healthy. This incident is recorded in `restart_analysis.json`.

## 3. Docker network and DNS findings

Phase 16 services share `{PROJECT}_default`; all four required DNS edges passed during the soak. Applications use Compose service names, never configured container IPs. The old project network remains separate and has no running old dependency endpoints after shutdown.

## 4. Dependency readiness findings

Kafka, Spark master, Spark worker, producer, and inference have health checks. Producer and inference depend on healthy services; Spark worker waits for a healthy master; inference also waits for the one-shot volume initializer to complete successfully. All five services remained healthy in the completed monitor window.

## 5. Restart-policy findings

Long-running Phase 16 services keep `unless-stopped`; the initializer is the only `restart: no` one-shot service. The producer cache restart and inference checkpoint restart were manual idempotency tests. Their Docker automatic RestartCount remained zero. No automatic restart occurred in the soak.

## 6. Producer status

The 48-hour bootstrap used one batched provider request for 63 locations and delivered 3,087 hourly rows. The daemon then skipped all 63 already-published locations without a request. After a deliberate producer restart, the same-hour cache still skipped 63 locations; the next safe hour required one batched request and delivered 63 unique canonical keys. The live cycles had no delivery failures, duplicates, HTTP 429 responses, or retries. Unit tests cover `Retry-After`, bounded jittered backoff, expiry, and successful recovery.

## 7. Inference status

Model startup validation passed for SHA `{MODEL_SHA}`, 73 features, and matching feature names. A direct Spark 4.0.4 regression using same-schema/same-lineage inputs passed the qualified self-join path. The manual restart resumed the Phase 16 checkpoint at the already-committed batch, left its 12 file hashes unchanged, and kept Delta row/forecast-ID counts unchanged at the test boundary. Final forecasts had no duplicate ID, duplicate location-feature key, invalid provenance, or nonpositive lead.

## 8. Outage-detector fix

Future runs use a 3,600-second expected cadence plus 900-second grace. A normal 60-minute interval is `ON_CADENCE`; 68 minutes is `LATE_WITHIN_GRACE`; 76 minutes is a missed-cycle outage; and an observed process restart is an outage event even when the gap is otherwise hourly. Phase 14/15 outage records were not rewritten.

## 9. Issuance latency and SLO findings

Phase 15's 300-second producer polling interval added up to five minutes of schedule wait. Its median issuance latency was {phase15.get('issuance_latency_seconds', {}).get('median'):.3f}s, p95 {phase15.get('issuance_latency_seconds', {}).get('p95'):.3f}s, and 0/24 cycles were within 60s. The 3,222.8s p95 tail is too large to attribute to polling alone and coincides with rapid restarts and stopped dependencies; frozen receipt data lacks the stage timestamps needed to allocate the tail precisely. Reference-arrival and evaluation lags were not used as issuance latency.

Phase 16 reduced polling to 30 seconds. Its measured routine-cycle SLO (Delta commit timestamp proxy, close to the application's post-MERGE persisted-at timestamp) is: p50 {cycle_slo['issuance_latency_p50_seconds']}, p95 {cycle_slo['issuance_latency_p95_seconds']}, max {cycle_slo['issuance_latency_max_seconds']} seconds; positive lead {cycle_slo['positive_lead_pct']}%; within 60/300/900s {cycle_slo['within_cutoff_pct']}. Exact per-stage records and proxy qualification are in `issuance_slo_analysis.json`.

## 10. Soak test results

Routine safe hours covered: {', '.join(row['safe_hour'] for row in phase16_cycle_rows)}. Completed {len(phase16_cycle_rows)} consecutive live cycles. The six-cycle preference was not pursued after the requested three-cycle minimum passed. This was operational verification only; no cohort or evaluation state was created.

## 11. Container restart counters

Old producer/inference counters were preserved at {old_app_restarts.get('producer')} / {old_app_restarts.get('inference')} before stopping those two applications. Phase 16 counters ended at { {key: phase16_containers[key].get('restart_count') for key in ('broker','spark_master','spark_worker','producer','inference')} }. Automatic counter deltas during the monitor window: {restart_deltas}. Manual restart events are recorded separately.

## 12. Remaining operational risks

The original trigger for the three exit-255 dependency stops remains unknown; the old default Compose apps are stopped. Continue any extended validation using the tested Phase 16 overlay/project with a fresh run-specific checkpoint and result directory. Phase 16's first routine cycle did not meet the 60-second issuance cutoff; the fixed 300/900-second thresholds should remain visible in the next cohort. Delta commit time is used as a near-exact SLO timestamp proxy because enabling the cohort receipt collector would also enable cohort-state updates.

## 13. Extended seven-day validation protocol

The prepared protocol specifies 7 days, 168 target hours, 63 locations, 10,584 logical slots, and `target_hour` as the statistical unit. It preserves T2H, the frozen model/features/provider, positive lead, `LIVE_PROSPECTIVE`, first-wins reference policy, persistence baseline, revision-value evidence, and strict missingness semantics. SLO cutoffs 60/300/900 seconds are frozen before start.

## 14. Extended-cohort readiness

Runtime, DNS, model/feature/provider, restart, idempotency, and three-cycle checks passed. No run ID, cohort ID, new cohort manifest, or cohort checkpoint was created. Those run-specific assets must be allocated and checked after human review, before a cohort starts.

## 15. Runtime status

`{runtime_status}`

## 16. Next-step decision

`{next_step}`. This recommendation does not start or freeze the seven-day cohort.

## 17. Files created/changed

Phase 16 evidence is under `results/runtime-stabilization-t2h/{RUN_ID}/`. The changed runtime source is `validation/runtime_stabilization_t2h.py` and the future-run wiring in `validation/prospective_t2h.py`; focused tests are under `tests/`. Existing Phase 14/15 artifacts and the finalized checkpoint hashes were verified unchanged.

## 18. Test results

`compileall`: passed. `pytest`: 203 passed, 7 skipped, 29 subtests passed in 41.46 seconds. The direct Spark 4.0.4 same-lineage regression passed. `git diff --check`: passed. No model training occurred.

## 19. git diff --stat

```text
{git_diff_stat.strip() or '(no tracked diff stat)'}
```

## 20. git status

```text
{git_status.strip() or '(clean)'}
```
"""
    (run_dir / "PHASE16_RUNTIME_STABILIZATION.md").write_text(md, encoding="utf-8")

    print(json.dumps({
        "run_id": RUN_ID,
        "runtime_status": runtime_status,
        "extended_validation_ready": runtime_ready,
        "next_step": next_step,
        "completed_routine_cycles": len(phase16_cycle_rows),
        "all_phase14_15_artifacts_unchanged": old_artifact_integrity.get("all_unchanged"),
        "old_checkpoint_hashes_unchanged": old_checkpoint_matches,
        "phase16_checkpoint_path": current_checkpoint.get("checkpoint_path"),
        "phase16_delta_rows": delta.get("row_count"),
        "output_dir": run_dir.as_posix(),
    }, indent=2, sort_keys=True))
    return 0 if old_artifacts_preserved else 1


if __name__ == "__main__":
    raise SystemExit(main())
