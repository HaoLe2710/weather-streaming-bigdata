"""Pure configuration and aggregation helpers for scalability experiments."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
import re
from statistics import mean, median
from typing import Any, Iterable


_CONFIG_ID = re.compile(
    r"^p(?P<partitions>[1-9][0-9]*)-b(?P<bronze>[1-9][0-9]*)-"
    r"s(?P<silver>[1-9][0-9]*)-w(?P<workers>[1-9][0-9]*)$"
)


@dataclass(frozen=True)
class ScalabilityConfig:
    partitions: int
    bronze_cores: int
    silver_cores: int
    workers: int
    worker_cores_each: int = 4
    worker_memory_mb_each: int = 4096
    executor_cores: int = 1
    executor_memory_mb: int = 1024
    shuffle_partitions: int = 1
    trigger_interval: str = "1 second"

    @property
    def config_id(self) -> str:
        return build_config_id(
            self.partitions,
            self.bronze_cores,
            self.silver_cores,
            self.workers,
        )

    def validate(self) -> "ScalabilityConfig":
        validate_scalability_config(self)
        return self


def build_config_id(partitions: int, bronze_cores: int, silver_cores: int, workers: int) -> str:
    values = (partitions, bronze_cores, silver_cores, workers)
    if any(not isinstance(value, int) or isinstance(value, bool) or value <= 0 for value in values):
        raise ValueError("Config ID dimensions must be positive integers.")
    return f"p{partitions}-b{bronze_cores}-s{silver_cores}-w{workers}"


def parse_config_id(config_id: str) -> dict[str, int]:
    match = _CONFIG_ID.fullmatch(config_id)
    if not match:
        raise ValueError(
            "config_id must use p<partitions>-b<bronze>-s<silver>-w<workers>, "
            "for example p3-b2-s1-w1."
        )
    return {name: int(value) for name, value in match.groupdict().items()}


def validate_scalability_config(config: ScalabilityConfig) -> None:
    dimensions = {
        "partitions": config.partitions,
        "bronze_cores": config.bronze_cores,
        "silver_cores": config.silver_cores,
        "workers": config.workers,
        "worker_cores_each": config.worker_cores_each,
        "worker_memory_mb_each": config.worker_memory_mb_each,
        "executor_cores": config.executor_cores,
        "executor_memory_mb": config.executor_memory_mb,
        "shuffle_partitions": config.shuffle_partitions,
    }
    invalid = [
        f"{name}={value!r}"
        for name, value in dimensions.items()
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0
    ]
    if invalid:
        raise ValueError("Scalability dimensions must be positive integers: " + ", ".join(invalid))
    if config.bronze_cores > config.workers * config.worker_cores_each:
        raise ValueError("Bronze core cap exceeds total Spark worker cores.")
    if config.silver_cores > config.workers * config.worker_cores_each:
        raise ValueError("Silver core cap exceeds total Spark worker cores.")
    if config.bronze_cores + config.silver_cores > config.workers * config.worker_cores_each:
        raise ValueError("Combined application core caps exceed total Spark worker cores.")
    if not config.trigger_interval.strip():
        raise ValueError("trigger_interval must not be empty.")


def compute_scalability_metrics(
    *,
    baseline_rate: float | None,
    candidate_rate: float | None,
    baseline_cores: float | None,
    candidate_cores: float | None,
) -> dict[str, float | None]:
    speedup = (
        float(candidate_rate) / float(baseline_rate)
        if _positive(baseline_rate) and _number(candidate_rate)
        else None
    )
    core_multiplier = (
        float(candidate_cores) / float(baseline_cores)
        if _positive(baseline_cores) and _number(candidate_cores)
        else None
    )
    return {
        "speedup": speedup,
        "throughput_gain_percent": (speedup - 1) * 100 if speedup is not None else None,
        "compute_multiplier": core_multiplier,
        "scaling_efficiency": (
            speedup / core_multiplier
            if speedup is not None and _positive(core_multiplier)
            else None
        ),
        "candidate_rows_per_core": (
            float(candidate_rate) / float(candidate_cores)
            if _number(candidate_rate) and _positive(candidate_cores)
            else None
        ),
    }


def partition_distribution(
    records_by_partition: dict[str | int, int],
    partition_count: int,
) -> dict[str, Any]:
    if not isinstance(partition_count, int) or isinstance(partition_count, bool) or partition_count <= 0:
        raise ValueError("partition_count must be a positive integer.")
    counts = {str(index): 0 for index in range(partition_count)}
    for key, raw_count in records_by_partition.items():
        suffix = str(key).rsplit(":", 1)[-1]
        try:
            partition = str(int(suffix))
            count = int(raw_count)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid partition count entry: {key!r}={raw_count!r}.") from exc
        if partition not in counts or count < 0:
            raise ValueError(f"Partition {key!r} is outside the configured range or has a negative count.")
        counts[partition] = count
    values = list(counts.values())
    average = mean(values) if values else 0.0
    maximum = max(values, default=0)
    return {
        "records_per_partition": counts,
        "min_partition_records": min(values, default=0),
        "max_partition_records": maximum,
        "mean_partition_records": average,
        "partition_imbalance_ratio": maximum / average if average > 0 else None,
    }


def aggregate_scalability_runs(results: Iterable[dict[str, Any]]) -> dict[str, Any]:
    runs = list(results)
    if not runs:
        raise ValueError("At least one scalability run is required.")
    run_audits = [_scalability_run_audit(run) for run in runs]
    valid_runs = [run for run, audit in zip(runs, run_audits) if audit["valid_for_comparison"]]
    metric_runs = valid_runs
    fields = {
        "actual_rate": "actual_generated_msgs_sec",
        "bronze_processed_rate_avg": "avg_processed_rows_per_sec",
        "bronze_processed_rate_p50": "bronze_processed_rate_p50",
        "bronze_processed_rate_p95": "bronze_processed_rate_p95",
        "bronze_processed_rate_peak": "peak_processed_rows_per_sec",
        "silver_processed_rate_avg": "silver_avg_processed_rows_per_sec",
        "silver_processed_rate_p50": "silver_processed_rate_p50",
        "silver_processed_rate_p95": "silver_processed_rate_p95",
        "silver_processed_rate_peak": "silver_peak_processed_rows_per_sec",
        "startup_lag_peak": "startup_peak_kafka_to_bronze_lag",
        "steady_lag_avg": "steady_state_avg_kafka_to_bronze_lag",
        "steady_lag_p95": "steady_state_p95_kafka_to_bronze_lag",
        "steady_lag_peak": "steady_state_peak_kafka_to_bronze_lag",
        "lag_slope": "steady_state_lag_slope_records_per_sec",
        "latency_p50_ms": "replay_to_bronze_latency_p50_ms",
        "latency_p95_ms": "replay_to_bronze_latency_p95_ms",
        "latency_p99_ms": "replay_to_bronze_latency_p99_ms",
        "drain_seconds": "drain_seconds",
    }
    bronze_stage_rates = [_stage_rate_value(run, "bronze") for run in metric_runs]
    silver_stage_rates = [_stage_rate_value(run, "silver") for run in metric_runs]
    metrics: dict[str, dict[str, float | None]] = {}
    for label, key in fields.items():
        if label == "bronze_processed_rate_avg":
            values = bronze_stage_rates
        elif label == "silver_processed_rate_avg":
            values = silver_stage_rates
        else:
            values = [_number_value(run.get(key)) for run in metric_runs]
        numeric = [value for value in values if value is not None]
        metrics[label] = _statistics(numeric)

    pipeline_rates = [
        audit["pipeline_rate"]
        for audit in run_audits
        if audit["valid_for_comparison"] and audit["pipeline_rate"] is not None
    ]
    metrics["pipeline_rate"] = _statistics(pipeline_rates)

    worker_cpu = [
        _nested_number(run, "scalability_resource_metrics", "cluster_aggregate", "cpu_p95_percent")
        for run in metric_runs
    ]
    worker_memory = [
        _nested_number(run, "scalability_resource_metrics", "cluster_aggregate", "memory_p95_mb")
        for run in metric_runs
    ]
    allocated_cores = [
        _nested_number(run, "scalability_runtime_validation", "actual_allocated_cores_total")
        for run in metric_runs
    ]
    metrics["worker_cpu_p95_percent"] = _statistics([x for x in worker_cpu if x is not None])
    metrics["worker_memory_p95_mb"] = _statistics([x for x in worker_memory if x is not None])
    metrics["actual_allocated_cores"] = _statistics([x for x in allocated_cores if x is not None])
    resource_fields = {
        "worker_cpu_avg_percent": ("cluster_aggregate", "cpu_avg_percent"),
        "worker_cpu_peak_percent": ("cluster_aggregate", "cpu_peak_percent"),
        "worker_memory_avg_mb": ("cluster_aggregate", "memory_avg_mb"),
        "worker_memory_peak_mb": ("cluster_aggregate", "memory_peak_mb"),
        "kafka_cpu_avg_percent": ("kafka_broker", "cpu_avg_percent"),
        "kafka_cpu_p95_percent": ("kafka_broker", "cpu_p95_percent"),
        "kafka_cpu_peak_percent": ("kafka_broker", "cpu_peak_percent"),
        "kafka_memory_avg_mb": ("kafka_broker", "memory_avg_mb"),
        "kafka_memory_p95_mb": ("kafka_broker", "memory_p95_mb"),
        "kafka_memory_peak_mb": ("kafka_broker", "memory_peak_mb"),
    }
    for label, (section, field) in resource_fields.items():
        values = [
            _nested_number(run, "scalability_resource_metrics", section, field)
            for run in metric_runs
        ]
        metrics[label] = _statistics([value for value in values if value is not None])

    classifications = [run.get("capacity_classification") for run in runs]
    legacy_statuses = [audit["legacy_pipeline_status"] for audit in run_audits]
    if "LEGACY_PIPELINE_NOT_RECOMPUTABLE" in legacy_statuses:
        legacy_pipeline_status = "LEGACY_PIPELINE_NOT_RECOMPUTABLE"
    elif "RECOMPUTED_FROM_STAGE_RATES" in legacy_statuses:
        legacy_pipeline_status = "RECOMPUTED_FROM_STAGE_RATES"
    else:
        legacy_pipeline_status = "NO_LEGACY_PIPELINE_FIELD"
    aggregate: dict[str, Any] = {
        "run_count": len(runs),
        "valid_run_count": len(valid_runs),
        "reported_valid_run_count": sum(run.get("valid_for_comparison") is True for run in runs),
        "metrics_scope": "valid_runs" if valid_runs else "no_valid_comparison_runs",
        "run_ids": [run.get("run_id") for run in runs],
        "statuses": [run.get("status") for run in runs],
        "capacity_classifications": classifications,
        "per_run_pipeline_rate": run_audits,
        "pipeline_rate_definition": (
            "for each eligible run, minimum of that run's positive finite steady-state Bronze and Silver rates; "
            "experiment statistics aggregate those per-run pipeline rates"
        ),
        "legacy_pipeline_status": legacy_pipeline_status,
        "pipeline_rate_status": "NO_VALID_PIPELINE_RUNS",
        "metrics": metrics,
    }
    if not pipeline_rates:
        aggregate["pipeline_rate_unavailable_reason"] = (
            "No run met comparison eligibility with positive finite Bronze and Silver rates."
        )
        if legacy_pipeline_status == "LEGACY_PIPELINE_NOT_RECOMPUTABLE":
            aggregate["pipeline_rate_status"] = "LEGACY_PIPELINE_NOT_RECOMPUTABLE"
    else:
        aggregate["pipeline_rate_status"] = "RECOMPUTED_FROM_PER_RUN_STAGE_RATES"
    for label, summary in metrics.items():
        aggregate[f"{label}_mean"] = summary["mean"]
        aggregate[f"{label}_median"] = summary["median"]
        aggregate[f"{label}_min"] = summary["min"]
        aggregate[f"{label}_max"] = summary["max"]
    aggregate["latency_p95_mean"] = aggregate["latency_p95_ms_mean"]
    aggregate["drain_time_mean"] = aggregate["drain_seconds_mean"]
    aggregate["worker_cpu_p95_mean"] = aggregate["worker_cpu_p95_percent_mean"]
    aggregate["worker_memory_p95_mean_mb"] = aggregate["worker_memory_p95_mb_mean"]
    return aggregate


_REQUIRED_CORRECTNESS_CHECKS = (
    "bronze_matches_expected",
    "silver_matches_expected",
    "dlq_is_empty",
    "duplicate_event_groups_are_empty",
    "quality_violations_are_empty",
)


def _scalability_run_audit(run: dict[str, Any]) -> dict[str, Any]:
    bronze_rate = _stage_rate_value(run, "bronze")
    silver_rate = _stage_rate_value(run, "silver")
    reasons: list[str] = []
    status = str(run.get("status") or "").upper()

    if run.get("valid_for_comparison") is not True:
        reasons.append("valid_for_comparison is not true")
    if not status:
        reasons.append("run status is missing")
    invalid_statuses = {
        "FAILED",
        "INVALID_FOR_COMPARISON",
        "INVALID_CORRECTNESS",
        "FAILED_RESOURCE_LIMIT",
        "LOAD_GENERATOR_LIMITED",
    }
    if status in invalid_statuses or status.startswith(("FAILED_", "INVALID_")):
        reasons.append(f"run status is {status}")
    if (
        run.get("resource_safety_interrupted") is True
        or bool(run.get("resource_safety_interruption"))
        or run.get("broker_failure") is True
        or str(run.get("broker_status") or "").upper() in {"FAILED", "UNAVAILABLE"}
    ):
        reasons.append("resource safety interruption or broker failure was recorded")
    if any(
        run.get(key) is True
        for key in ("bronze_query_failed", "silver_query_failed", "query_failed")
    ):
        reasons.append("a streaming query failed")

    checks = run.get("correctness_checks")
    checks_complete = isinstance(checks, dict) and all(key in checks for key in _REQUIRED_CORRECTNESS_CHECKS)
    checks_failed = not isinstance(checks, dict)
    if isinstance(checks, dict):
        checks_failed = any(
            checks.get(key) is not True for key in _REQUIRED_CORRECTNESS_CHECKS
        ) or any(value is not True for value in checks.values())
    if run.get("correctness_passed") is not True or not checks_complete or checks_failed:
        reasons.append("correctness did not complete successfully")

    final_lag = run.get("final_source_lag")
    if final_lag is None:
        final_lag = run.get("final_kafka_to_bronze_lag")
    if not _number(final_lag) or float(final_lag) != 0:
        reasons.append("final source lag is missing or nonzero")

    return_codes = run.get("stream_process_return_codes")
    if not isinstance(return_codes, dict) or any(
        return_codes.get(stage) != 0 for stage in ("bronze", "silver")
    ):
        reasons.append("Bronze or Silver streaming query return status is missing or failed")

    allocation = run.get("scalability_runtime_validation")
    if not isinstance(allocation, dict) or allocation.get("passed") is not True:
        reasons.append("requested actual Spark allocation was not verified")

    if bronze_rate is None:
        reasons.append("Bronze steady-state rate is missing or invalid")
    if silver_rate is None:
        reasons.append("Silver steady-state rate is missing or invalid")

    legacy_pipeline_present = any(
        run.get(key) is not None for key in ("pipeline_sustainable_rate", "pipeline_rate")
    )
    if legacy_pipeline_present and (bronze_rate is None or silver_rate is None):
        legacy_status = "LEGACY_PIPELINE_NOT_RECOMPUTABLE"
    elif legacy_pipeline_present:
        legacy_status = "RECOMPUTED_FROM_STAGE_RATES"
    else:
        legacy_status = "NO_LEGACY_PIPELINE_FIELD"

    eligible = not reasons
    return {
        "run_id": run.get("run_id"),
        "status": run.get("status"),
        "valid_for_comparison": eligible,
        "exclusion_reasons": reasons,
        "bronze_rate": bronze_rate,
        "silver_rate": silver_rate,
        "pipeline_rate": min(bronze_rate, silver_rate) if eligible else None,
        "legacy_pipeline_status": legacy_status,
    }


def select_best_partition_config(config_summaries: dict[str, dict[str, Any]]) -> str:
    candidates = [
        (config_id, summary)
        for config_id, summary in config_summaries.items()
        if _all_repetitions_valid(summary)
        and _number(summary.get("pipeline_rate_median"))
        and all(value != "SATURATED" for value in summary.get("capacity_classifications", []))
    ]
    fallback = False
    if not candidates:
        candidates = [
            (config_id, summary)
            for config_id, summary in config_summaries.items()
            if _all_repetitions_valid(summary) and _number(summary.get("pipeline_rate_median"))
        ]
        fallback = True
    if not candidates:
        raise ValueError("No valid Kafka partition configuration is available for selection.")
    highest = max(float(summary["pipeline_rate_median"]) for _, summary in candidates)
    tied = [
        (config_id, summary)
        for config_id, summary in candidates
        if highest <= 0
        or (highest - float(summary["pipeline_rate_median"])) / highest <= 0.02
    ]
    selected = min(
        tied,
        key=lambda item: (
            _sort_missing(item[1].get("latency_p95_mean")),
            _sort_missing(item[1].get("steady_lag_p95_mean")),
            parse_config_id(item[0])["partitions"],
        ),
    )[0]
    config_summaries[selected]["selection_fallback_no_sustainable_config"] = fallback
    return selected


def select_best_core_config(config_summaries: dict[str, dict[str, Any]]) -> str:
    candidates = [
        (config_id, summary)
        for config_id, summary in config_summaries.items()
        if _all_repetitions_valid(summary)
        and _number(summary.get("pipeline_rate_median"))
        and all(value != "SATURATED" for value in summary.get("capacity_classifications", []))
    ]
    if not candidates:
        candidates = [
            (config_id, summary)
            for config_id, summary in config_summaries.items()
            if _all_repetitions_valid(summary) and _number(summary.get("pipeline_rate_median"))
        ]
    if not candidates:
        raise ValueError("No valid Spark core configuration is available for selection.")
    highest = max(float(summary["pipeline_rate_median"]) for _, summary in candidates)
    tied = [
        (config_id, summary)
        for config_id, summary in candidates
        if highest <= 0
        or (highest - float(summary["pipeline_rate_median"])) / highest <= 0.02
    ]
    return min(
        tied,
        key=lambda item: (
            _sort_missing(item[1].get("steady_lag_p95_mean")),
            _sort_missing(item[1].get("latency_p95_mean")),
            _sort_missing(item[1].get("drain_time_mean")),
            parse_config_id(item[0])["bronze"] + parse_config_id(item[0])["silver"],
            item[0],
        ),
    )[0]


def validate_runtime_allocation(
    observation: dict[str, Any],
    *,
    run_id: str,
    config: ScalabilityConfig,
) -> dict[str, Any]:
    expected_cores = {
        f"WeatherBronzeStreaming-{run_id}": config.bronze_cores,
        f"WeatherSilverStreaming-{run_id}": config.silver_cores,
    }
    apps = {str(app.get("name")): app for app in observation.get("apps", [])}
    errors: list[str] = []
    if observation.get("error"):
        errors.append(f"Spark Master observation failed: {observation['error']}")
    if observation.get("executor_error"):
        errors.append(f"Spark executor observation failed: {observation['executor_error']}")
    if set(apps) != set(expected_cores):
        errors.append("Expected exactly the run-scoped Bronze and Silver Spark applications.")
    if observation.get("other_active_apps"):
        errors.append("Unrelated Spark applications are active on the master.")

    registered_workers = observation.get("workers", [])
    workers = [worker for worker in registered_workers if worker.get("state") == "ALIVE"]
    if len(workers) != config.workers:
        errors.append(f"Expected {config.workers} Spark workers; observed {len(workers)}.")
    for worker in workers:
        if worker.get("cores_available") != config.worker_cores_each:
            errors.append(f"Spark worker {worker.get('id')} has an unexpected core count.")
        if worker.get("memory_available_mb") != config.worker_memory_mb_each:
            errors.append(f"Spark worker {worker.get('id')} has unexpected memory.")

    allocation_by_app: dict[str, int | None] = {}
    executor_count_by_app: dict[str, int] = {}
    executor_workers_by_app: dict[str, list[str]] = {}
    executor_details_by_app: dict[str, list[dict[str, Any]]] = {}
    executor_core_width_observed_by_app: dict[str, bool] = {}
    for app_name, expected in expected_cores.items():
        app = apps.get(app_name)
        if app is None:
            continue
        executors = app.get("executors") or []
        reported_cores = _number_value(app.get("actual_allocated_cores"))
        if reported_cores is None:
            reported_cores = _number_value(app.get("cores"))
        observed_executor_cores = [
            _number_value(executor.get("cores"))
            for executor in executors
            if _number_value(executor.get("cores")) is not None
        ]
        if reported_cores is not None:
            actual_cores: int | None = int(reported_cores)
        elif observed_executor_cores:
            actual_cores = int(sum(observed_executor_cores))
        else:
            actual_cores = None
        allocation_by_app[app_name] = actual_cores
        executor_count_by_app[app_name] = len(executors)
        worker_ids = sorted({str(executor.get("worker_id")) for executor in executors if executor.get("worker_id")})
        executor_workers_by_app[app_name] = worker_ids
        executor_details_by_app[app_name] = executors
        executor_core_width_observed_by_app[app_name] = len(observed_executor_cores) == len(executors) and bool(executors)
        if actual_cores is None:
            errors.append(f"{app_name} has no observed aggregate core allocation.")
        elif actual_cores != expected:
            errors.append(f"{app_name} requested {expected} cores but has {actual_cores} allocated.")
        if not executors:
            errors.append(f"{app_name} has no active executor evidence.")
        elif actual_cores is not None and actual_cores != len(executors) * config.executor_cores:
            errors.append(
                f"{app_name} has {actual_cores} aggregate cores across {len(executors)} executors; "
                f"expected {config.executor_cores} core(s) per executor."
            )
        for executor in executors:
            if executor.get("memory_mb") != config.executor_memory_mb:
                errors.append(f"{app_name} has an executor with unexpected memory.")
            if executor.get("cores") is not None and executor.get("cores") != config.executor_cores:
                errors.append(f"{app_name} has an executor with unexpected core width.")

    assigned_workers = sorted({
        worker_id
        for worker_ids in executor_workers_by_app.values()
        for worker_id in worker_ids
    })
    if config.workers > 1 and len(assigned_workers) < 2:
        errors.append("Executors did not run across both registered Spark workers.")
    total_allocated = sum(cores for cores in allocation_by_app.values() if cores is not None)
    return {
        "passed": not errors,
        "errors": errors,
        "expected_cores_by_app": expected_cores,
        "actual_allocated_cores_by_app": allocation_by_app,
        "actual_allocated_cores_total": total_allocated,
        "executor_count_by_app": executor_count_by_app,
        "executor_core_width_observed_by_app": executor_core_width_observed_by_app,
        "executor_workers_by_app": executor_workers_by_app,
        "executor_details_by_app": executor_details_by_app,
        "worker_count": len(workers),
        "worker_ids": [worker.get("id") for worker in workers],
        "registered_worker_records": registered_workers,
        "dead_worker_ids": [worker.get("id") for worker in registered_workers if worker.get("state") != "ALIVE"],
        "assigned_worker_ids": assigned_workers,
        "unrelated_active_app_count": len(observation.get("other_active_apps", [])),
    }


def summarize_progress_percentiles(
    rows: Iterable[dict[str, Any]],
    *,
    steady_state_start: str | None,
    generation_end_time: str | None,
) -> dict[str, dict[str, Any]]:
    start = _parse_time(steady_state_start)
    end = _parse_time(generation_end_time)
    selected: dict[str, list[dict[str, Any]]] = {"bronze": [], "silver": []}
    for row in rows:
        if row.get("event_type") != "progress":
            continue
        stage = str(row.get("stage") or "").lower()
        progress = row.get("progress") or {}
        if stage not in selected:
            continue
        if stage == "silver" and "dlq" in str(progress.get("name") or "").lower():
            continue
        captured = _parse_time(row.get("captured_at_utc") or progress.get("timestamp"))
        if captured is None or (start is not None and captured < start) or (end is not None and captured > end):
            continue
        selected[stage].append(progress)

    summary: dict[str, dict[str, Any]] = {}
    for stage, progress_rows in selected.items():
        rates = [
            float(row["processedRowsPerSecond"])
            for row in progress_rows
            if _number(row.get("processedRowsPerSecond"))
        ]
        durations = []
        for row in progress_rows:
            duration = (row.get("durationMs") or {}).get("triggerExecution")
            if _number(duration):
                durations.append(float(duration))
        summary[stage] = {
            "sample_count": len(progress_rows),
            "processed_rate_p50": _quantile(rates, 0.50),
            "processed_rate_p95": _quantile(rates, 0.95),
            "batch_duration_avg_ms": mean(durations) if durations else None,
            "batch_duration_p95_ms": _quantile(durations, 0.95),
            "batch_duration_max_ms": max(durations) if durations else None,
        }
    return summary


def scalability_resource_metrics(samples: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(samples)
    worker_groups: dict[str, list[dict[str, Any]]] = {}
    broker_rows: list[dict[str, Any]] = []
    for row in rows:
        service = row.get("service")
        if service == "spark_worker":
            name = str(row.get("container_name") or row.get("container") or "worker")
            worker_groups.setdefault(name, []).append(row)
        elif service == "kafka_broker":
            broker_rows.append(row)
    per_worker = {name: _resource_stats(group) for name, group in worker_groups.items()}
    worker_by_time: dict[str, list[dict[str, Any]]] = {}
    for group in worker_groups.values():
        for row in group:
            worker_by_time.setdefault(str(row.get("timestamp_utc")), []).append(row)
    aggregate_rows = []
    for timestamp, group in worker_by_time.items():
        cpus = [float(row["cpu_percent"]) for row in group if _number(row.get("cpu_percent"))]
        memory = [float(row["memory_usage_mb"]) for row in group if _number(row.get("memory_usage_mb"))]
        aggregate_rows.append({
            "timestamp_utc": timestamp,
            "cpu_percent": sum(cpus) if cpus else None,
            "memory_usage_mb": sum(memory) if memory else None,
        })
    return {
        "per_worker": per_worker,
        "cluster_aggregate": _resource_stats(aggregate_rows),
        "kafka_broker": _resource_stats(broker_rows),
        "cluster_worker_count": len(per_worker),
        "cluster_aggregate_cpu_definition": "sum of per-worker Docker CPU percent at each sample timestamp",
        "cluster_aggregate_memory_definition": "sum of per-worker resident memory at each sample timestamp",
    }


def _resource_stats(rows: list[dict[str, Any]]) -> dict[str, float | int | None]:
    cpu = [float(row["cpu_percent"]) for row in rows if _number(row.get("cpu_percent"))]
    memory = [float(row["memory_usage_mb"]) for row in rows if _number(row.get("memory_usage_mb"))]
    return {
        "sample_count": len(rows),
        "cpu_avg_percent": mean(cpu) if cpu else None,
        "cpu_p50_percent": _quantile(cpu, 0.50),
        "cpu_p95_percent": _quantile(cpu, 0.95),
        "cpu_peak_percent": max(cpu) if cpu else None,
        "memory_avg_mb": mean(memory) if memory else None,
        "memory_p50_mb": _quantile(memory, 0.50),
        "memory_p95_mb": _quantile(memory, 0.95),
        "memory_peak_mb": max(memory) if memory else None,
    }


def _statistics(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"mean": None, "median": None, "min": None, "max": None}
    return {
        "mean": mean(values),
        "median": median(values),
        "min": min(values),
        "max": max(values),
    }


def _nested_number(value: dict[str, Any], *path: str) -> float | None:
    current: Any = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return _number_value(current)


def _stage_rate_value(run: dict[str, Any], stage: str) -> float | None:
    keys = {
        "bronze": ("bronze_processed_rate_avg", "avg_processed_rows_per_sec"),
        "silver": ("silver_processed_rate_avg", "silver_avg_processed_rows_per_sec"),
    }
    canonical_key, legacy_key = keys[stage]
    raw_value = run[canonical_key] if canonical_key in run else run.get(legacy_key)
    value = _number_value(raw_value)
    return value if _positive(value) else None


def _number_value(value: Any) -> float | None:
    return float(value) if _number(value) else None


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _positive(value: Any) -> bool:
    return _number(value) and float(value) > 0


def _sort_missing(value: Any) -> float:
    return float(value) if _number(value) else math.inf


def _all_repetitions_valid(summary: dict[str, Any]) -> bool:
    valid = int(summary.get("valid_run_count") or 0)
    total = int(summary.get("run_count", valid) or 0)
    return valid > 0 and valid == total


def _quantile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone() if parsed.tzinfo else parsed
    except ValueError:
        return None
