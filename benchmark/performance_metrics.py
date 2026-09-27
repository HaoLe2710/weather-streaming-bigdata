"""Pure helpers for throughput benchmark telemetry and steady-state analysis."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
from typing import Any, Iterable


DEFAULT_WARMUP_SECONDS = 15.0
DEFAULT_WARMUP_MIN_BATCHES = 3


def percentile(values: Iterable[float], quantile: float) -> float | None:
    """Return a linearly interpolated percentile (Hyndman-Fan type 7)."""
    if not 0.0 <= quantile <= 1.0:
        raise ValueError("quantile must be between 0 and 1.")
    ordered = sorted(float(value) for value in values if _is_number(value))
    if not ordered:
        return None
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _is_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _capture_time(row: dict[str, Any]) -> datetime | None:
    progress = row.get("progress")
    return utc_time(
        row.get("captured_at_utc")
        or (progress.get("timestamp") if isinstance(progress, dict) else None)
    )


def _progress_stage(row: dict[str, Any]) -> str | None:
    progress = row.get("progress")
    if row.get("event_type") != "progress" or not isinstance(progress, dict):
        return None
    stage = str(row.get("stage", "")).lower()
    name = str(progress.get("name") or "").lower()
    if stage == "bronze":
        return "bronze"
    if stage == "silver" and name.endswith("-silver"):
        return "silver"
    return None


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source = Path(path)
    if not source.exists():
        return rows
    with source.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at {source}:{line_number}: {exc}") from exc
            if isinstance(row, dict):
                rows.append(row)
    return rows


def parse_kafka_offsets(value: Any, expected_topic: str | None = None) -> dict[str, int]:
    """Decode Spark Kafka offset JSON to topic:partition -> next offset."""
    if isinstance(value, str):
        if not value or value == "null":
            return {}
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {}
    if not isinstance(value, dict):
        return {}

    offsets: dict[str, int] = {}
    for topic, partitions in value.items():
        if expected_topic is not None and topic != expected_topic:
            continue
        if not isinstance(partitions, dict):
            continue
        for partition, offset in partitions.items():
            try:
                numeric_offset = int(offset)
                numeric_partition = int(partition)
            except (TypeError, ValueError):
                continue
            if numeric_offset >= 0 and numeric_partition >= 0:
                offsets[f"{topic}:{numeric_partition}"] = numeric_offset
    return offsets


def kafka_source_offsets(
    progress: dict[str, Any],
    topic: str,
    offset_field: str = "endOffset",
) -> dict[str, int]:
    for source in progress.get("sources", []):
        if not isinstance(source, dict):
            continue
        offsets = parse_kafka_offsets(source.get(offset_field), topic)
        if offsets:
            return offsets
    return {}


def latest_spark_end_offsets(
    rows: Iterable[dict[str, Any]], topic: str
) -> dict[str, int]:
    candidates: list[tuple[int, str, dict[str, int]]] = []
    for row in rows:
        progress = row.get("progress")
        if row.get("event_type") != "progress" or not isinstance(progress, dict):
            continue
        offsets = kafka_source_offsets(progress, topic, "endOffset")
        if not offsets:
            continue
        try:
            batch_id = int(progress.get("batchId", -1))
        except (TypeError, ValueError):
            batch_id = -1
        candidates.append((batch_id, str(progress.get("timestamp", "")), offsets))
    return max(candidates, key=lambda item: (item[0], item[1]))[2] if candidates else {}


def select_steady_state_window(
    rows: Iterable[dict[str, Any]],
    *,
    warmup_start_time: str | None = None,
    generation_start_time: str | None = None,
    generation_end_time: str | None = None,
    minimum_warmup_seconds: float = DEFAULT_WARMUP_SECONDS,
    minimum_completed_batches: int = DEFAULT_WARMUP_MIN_BATCHES,
) -> dict[str, Any]:
    """Choose a deterministic steady-state window for the production interval.

    The window starts after both a minimum elapsed warm-up and the minimum
    number of completed progress batches for each main query (Bronze and
    Silver). Kafka lag and progress samples are then clipped to the replay
    interval, so the post-replay drain cannot make an overloaded rate appear
    steady.
    """
    if minimum_warmup_seconds < 0:
        raise ValueError("minimum_warmup_seconds must be zero or greater.")
    if minimum_completed_batches < 1:
        raise ValueError("minimum_completed_batches must be at least one.")

    copied = [dict(row) for row in rows]
    generation_start = utc_time(generation_start_time)
    generation_end = utc_time(generation_end_time)
    explicit_start = utc_time(warmup_start_time)
    query_start_times = [
        _capture_time(row)
        for row in copied
        if row.get("event_type") == "query_started"
        and row.get("stage") in {"bronze", "silver"}
    ]
    query_start_times = [value for value in query_start_times if value is not None]
    warmup_start = (
        explicit_start
        or (min(query_start_times) if query_start_times else None)
        or generation_start
    )

    by_stage_and_query: dict[str, dict[str, dict[int, datetime]]] = {
        "bronze": {},
        "silver": {},
    }
    for row in copied:
        stage = _progress_stage(row)
        progress = row.get("progress")
        captured = _capture_time(row)
        if stage is None or not isinstance(progress, dict) or captured is None:
            continue
        try:
            batch_id = int(progress.get("batchId", -1))
        except (TypeError, ValueError):
            continue
        if batch_id < 0:
            continue
        query_name = str(
            progress.get("name") or row.get("query_id") or row.get("query_run_id") or stage
        )
        by_stage_and_query[stage].setdefault(query_name, {})[batch_id] = captured

    batch_cutoffs: dict[str, str | None] = {"bronze": None, "silver": None}
    batch_cutoff_times: list[datetime] = []
    batches_ready = True
    for stage in ("bronze", "silver"):
        query_groups = by_stage_and_query[stage]
        if not query_groups:
            batches_ready = False
            continue
        query_cutoffs: list[datetime] = []
        for batches in query_groups.values():
            ordered = sorted(batches.items())
            if len(ordered) < minimum_completed_batches:
                batches_ready = False
                continue
            query_cutoffs.append(ordered[minimum_completed_batches - 1][1])
        if query_cutoffs:
            cutoff = max(query_cutoffs)
            batch_cutoffs[stage] = _format_time(cutoff)
            batch_cutoff_times.append(cutoff)

    steady_start: datetime | None = None
    if warmup_start is not None and batches_ready and len(batch_cutoff_times) == 2:
        thresholds = [
            warmup_start + timedelta(seconds=minimum_warmup_seconds),
            *batch_cutoff_times,
        ]
        if generation_start is not None:
            thresholds.append(generation_start)
        steady_start = max(thresholds)

    progress_counts = {"bronze": 0, "silver": 0}
    if steady_start is not None:
        for row in copied:
            stage = _progress_stage(row)
            captured = _capture_time(row)
            if (
                stage is not None
                and captured is not None
                and captured >= steady_start
                and (generation_end is None or captured <= generation_end)
            ):
                progress_counts[stage] += 1

    policy = {
        "name": "elapsed_time_and_completed_batches",
        "minimum_warmup_seconds": float(minimum_warmup_seconds),
        "minimum_completed_batches_per_query": int(minimum_completed_batches),
        "steady_state_start_rule": (
            "Later of warmup_start plus minimum_warmup_seconds, the minimum "
            "completed-batch cutoff for each main query, and replay start."
        ),
        "steady_state_end_rule": "Simulator generation_end_time; post-replay drain is excluded.",
    }
    return {
        "warmup_policy": policy,
        "warmup_start": _format_time(warmup_start),
        "steady_state_start": _format_time(steady_start),
        "generation_start_time": _format_time(generation_start),
        "generation_end_time": _format_time(generation_end),
        "batch_cutoffs": batch_cutoffs,
        "steady_state_sample_count": progress_counts["bronze"] + progress_counts["silver"],
        "steady_state_progress_sample_counts": progress_counts,
        "steady_state_ready": steady_start is not None,
    }


def summarize_progress(
    rows: Iterable[dict[str, Any]],
    *,
    steady_state_start: str | None = None,
    generation_end_time: str | None = None,
) -> dict[str, Any]:
    """Summarize main Bronze and Silver query progress separately."""
    by_stage: dict[str, list[dict[str, Any]]] = {"bronze": [], "silver": []}
    totals: dict[str, int] = {"bronze": 0, "silver": 0}
    start = utc_time(steady_state_start)
    end = utc_time(generation_end_time)
    explicit_window = steady_state_start is not None or generation_end_time is not None

    for row in rows:
        progress = row.get("progress")
        stage = _progress_stage(row)
        if stage is None or not isinstance(progress, dict):
            continue
        try:
            count = int(progress.get("numInputRows") or 0)
            if count > 0:
                totals[stage] += count
        except (TypeError, ValueError):
            pass

        if explicit_window:
            captured = _capture_time(row)
            included = (
                start is not None
                and captured is not None
                and captured >= start
                and (end is None or captured <= end)
            )
        else:
            included = not row.get("warmup_excluded", False)
        if included:
            by_stage[stage].append(progress)

    summaries: dict[str, dict[str, Any]] = {}
    for stage, progress_rows in by_stage.items():
        inputs = [
            float(row["inputRowsPerSecond"])
            for row in progress_rows
            if _is_number(row.get("inputRowsPerSecond"))
        ]
        processed = [
            float(row["processedRowsPerSecond"])
            for row in progress_rows
            if _is_number(row.get("processedRowsPerSecond"))
        ]
        durations = [
            float(row["durationMs"]["triggerExecution"])
            for row in progress_rows
            if isinstance(row.get("durationMs"), dict)
            and _is_number(row["durationMs"].get("triggerExecution"))
        ]
        duration_components: dict[str, dict[str, float | None]] = {}
        component_names = sorted({
            name
            for row in progress_rows
            for name in (row.get("durationMs") or {})
        })
        for name in component_names:
            values = [
                float(row["durationMs"][name])
                for row in progress_rows
                if isinstance(row.get("durationMs"), dict)
                and _is_number(row["durationMs"].get(name))
            ]
            duration_components[name] = {
                "avg_ms": _mean(values),
                "p95_ms": percentile(values, 0.95),
                "max_ms": max(values) if values else None,
            }
        summaries[stage] = {
            "input_records": totals[stage],
            "steady_state_sample_count": len(progress_rows),
            "avg_input_rows_per_sec": _mean(inputs),
            "peak_input_rows_per_sec": max(inputs) if inputs else None,
            "p50_processed_rows_per_sec": percentile(processed, 0.50),
            "avg_processed_rows_per_sec": _mean(processed),
            "p95_processed_rows_per_sec": percentile(processed, 0.95),
            "peak_processed_rows_per_sec": max(processed) if processed else None,
            "avg_batch_duration_ms": _mean(durations),
            "p95_batch_duration_ms": percentile(durations, 0.95),
            "max_batch_duration_ms": max(durations) if durations else None,
            "duration_components": duration_components,
        }
    return {
        "bronze_input_records": totals["bronze"],
        "silver_input_records": totals["silver"],
        "bronze": summaries["bronze"],
        "silver": summaries["silver"],
    }


def _lag_value(row: dict[str, Any]) -> int | None:
    value = row.get("kafka_to_bronze_lag_records")
    if not _is_number(value):
        value = row.get("lag_records")
    return int(value) if _is_number(value) else None


def linear_regression_slope(points: Iterable[tuple[float, float]]) -> float | None:
    """Return least-squares y-per-x slope, or None when fewer than two x values exist."""
    pairs = [
        (float(x), float(y))
        for x, y in points
        if _is_number(x) and _is_number(y)
    ]
    if len(pairs) < 2:
        return None
    mean_x = sum(x for x, _ in pairs) / len(pairs)
    mean_y = sum(y for _, y in pairs) / len(pairs)
    denominator = sum((x - mean_x) ** 2 for x, _ in pairs)
    if denominator == 0:
        return None
    return sum((x - mean_x) * (y - mean_y) for x, y in pairs) / denominator


def summarize_kafka_to_bronze_lag(
    samples: Iterable[dict[str, Any]],
    *,
    generation_start_time: str | None,
    generation_end_time: str | None,
    steady_state_start: str | None,
) -> dict[str, Any]:
    """Separate replay startup lag, steady-state source lag, and final source lag."""
    generation_start = utc_time(generation_start_time)
    generation_end = utc_time(generation_end_time)
    steady_start = utc_time(steady_state_start)
    valid: list[tuple[datetime, int]] = []
    for row in samples:
        timestamp = utc_time(row.get("timestamp_utc"))
        lag = _lag_value(row)
        if timestamp is not None and lag is not None:
            valid.append((timestamp, lag))
    valid.sort(key=lambda item: item[0])

    in_replay = [
        (timestamp, lag)
        for timestamp, lag in valid
        if (generation_start is None or timestamp >= generation_start)
        and (generation_end is None or timestamp <= generation_end)
    ]
    startup = [
        lag for timestamp, lag in in_replay
        if steady_start is None or timestamp < steady_start
    ]
    steady = [
        (timestamp, lag) for timestamp, lag in in_replay
        if steady_start is not None and timestamp >= steady_start
    ]
    steady_lags = [lag for _, lag in steady]
    first_steady_time = steady[0][0] if steady else None
    slope = linear_regression_slope(
        [
            ((timestamp - first_steady_time).total_seconds(), float(lag))
            for timestamp, lag in steady
        ]
    ) if first_steady_time is not None else None
    return {
        "startup_peak_kafka_to_bronze_lag": max(startup) if startup else None,
        "steady_state_peak_kafka_to_bronze_lag": max(steady_lags) if steady_lags else None,
        "steady_state_avg_kafka_to_bronze_lag": _mean(
            [float(value) for value in steady_lags]
        ),
        "steady_state_p95_kafka_to_bronze_lag": percentile(
            [float(value) for value in steady_lags], 0.95
        ),
        "steady_state_final_kafka_to_bronze_lag": steady_lags[-1] if steady_lags else None,
        "steady_state_lag_slope_records_per_sec": slope,
        "steady_state_lag_sample_count": len(steady_lags),
        "production_peak_kafka_to_bronze_lag": (
            max(lag for _, lag in in_replay) if in_replay else None
        ),
        "final_source_lag": valid[-1][1] if valid else None,
        "kafka_to_bronze_lag_sample_count": len(valid),
    }


def lag_peak(samples: Iterable[dict[str, Any]]) -> tuple[int | None, int | None]:
    """Legacy peak/final helper that reads both current and legacy lag keys."""
    rows = list(samples)
    lags = [value for row in rows if (value := _lag_value(row)) is not None]
    return max(lags) if lags else None, lags[-1] if lags else None


def resource_statistics(
    samples: Iterable[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Aggregate Docker CPU and memory samples for each measured container."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in samples:
        container = row.get("container")
        if container:
            grouped.setdefault(str(container), []).append(row)

    result: dict[str, dict[str, Any]] = {}
    for container, rows in grouped.items():
        cpu = [
            float(row["cpu_percent"])
            for row in rows
            if _is_number(row.get("cpu_percent"))
        ]
        memory = [
            float(row["memory_usage_mb"])
            for row in rows
            if _is_number(row.get("memory_usage_mb"))
        ]
        first_value = lambda key, fallback=None: next(
            (row.get(key, fallback) for row in rows if row.get(key, fallback) is not None),
            fallback,
        )
        result[container] = {
            "sample_count": len(rows),
            "cpu_avg_percent": _mean(cpu),
            "cpu_p50_percent": percentile(cpu, 0.50),
            "cpu_p95_percent": percentile(cpu, 0.95),
            "cpu_peak_percent": max(cpu) if cpu else None,
            "memory_avg_mb": _mean(memory),
            "memory_p50_mb": percentile(memory, 0.50),
            "memory_p95_mb": percentile(memory, 0.95),
            "memory_peak_mb": max(memory) if memory else None,
            "host_logical_cpu_count": first_value("host_logical_cpu_count"),
            "container_cpu_quota_cores": first_value("container_cpu_quota_cores"),
            "container_cpu_quota_us": first_value("container_cpu_quota_us"),
            "container_cpu_period_us": first_value("container_cpu_period_us"),
            "container_cpuset": first_value("container_cpuset"),
            "container_memory_limit_mb": first_value(
                "container_memory_limit_mb", first_value("configured_memory_limit_mb")
            ),
        }
    return result


def resource_peaks(samples: Iterable[dict[str, Any]]) -> dict[str, dict[str, float | None]]:
    """Legacy peak-only resource summary."""
    return {
        container: {
            "peak_cpu_percent": values.get("cpu_peak_percent"),
            "peak_memory_mb": values.get("memory_peak_mb"),
        }
        for container, values in resource_statistics(samples).items()
    }


def replay_to_bronze_latency_metrics(
    *sources: dict[str, Any] | None,
) -> dict[str, Any]:
    """Read the renamed latency fields, falling back to legacy latency aliases."""
    aliases = {
        "count": ("replay_to_bronze_latency_count", "latency_count"),
        "min_ms": ("replay_to_bronze_latency_min_ms", "latency_min_ms"),
        "avg_ms": ("replay_to_bronze_latency_avg_ms", "latency_avg_ms"),
        "p50_ms": ("replay_to_bronze_latency_p50_ms", "latency_p50_ms"),
        "p95_ms": ("replay_to_bronze_latency_p95_ms", "latency_p95_ms"),
        "p99_ms": ("replay_to_bronze_latency_p99_ms", "latency_p99_ms"),
        "max_ms": ("replay_to_bronze_latency_max_ms", "latency_max_ms"),
        "definition": (
            "replay_to_bronze_latency_definition",
            "latency_definition",
        ),
        "percentile_method": (
            "replay_to_bronze_latency_percentile_method",
            "latency_percentile_method",
        ),
    }
    result = {}
    for output_key, key_pair in aliases.items():
        result[output_key] = None
        for source in sources:
            if not isinstance(source, dict):
                continue
            value = next(
                (source[key] for key in key_pair if key in source),
                None,
            )
            if value is not None:
                result[output_key] = value
                break
    return result


def progress_completion_time(
    rows: Iterable[dict[str, Any]],
    *,
    stage: str,
    expected_records: int | None,
) -> str | None:
    """Find the first progress completion whose cumulative stage input reaches target."""
    if expected_records is None or expected_records <= 0:
        return None
    candidates = []
    for row in rows:
        if _progress_stage(row) != stage:
            continue
        progress = row.get("progress")
        captured = _capture_time(row)
        if not isinstance(progress, dict) or captured is None:
            continue
        try:
            batch_id = int(progress.get("batchId", -1))
            count = int(progress.get("numInputRows") or 0)
        except (TypeError, ValueError):
            continue
        if batch_id >= 0 and count >= 0:
            candidates.append((captured, batch_id, count))
    candidates.sort(key=lambda item: (item[0], item[1]))
    total = 0
    for captured, _, count in candidates:
        total += count
        if total >= expected_records:
            return _format_time(captured)
    return None


def classify_capacity(
    *,
    failed: bool,
    pipeline_completed: bool,
    final_source_lag: int | None,
    actual_rate: float | None,
    pipeline_sustainable_rate: float | None,
    steady_state_lag_slope_records_per_sec: float | None,
    pipeline_drain_seconds: float | None,
    generation_duration_seconds: float | None,
) -> tuple[str, str]:
    """Classify capacity from sustained lag growth, bottleneck rate, and drain."""
    if failed:
        return "FAILED", "A stream, checker, or required steady-state measurement failed."
    if final_source_lag is None:
        return "FAILED", "No valid Kafka-to-Bronze source-lag sample was captured."
    if not pipeline_completed or final_source_lag > 0:
        return (
            "SATURATED",
            "The pipeline did not finish with all expected Silver records and zero final Kafka-to-Bronze source lag.",
        )
    if (
        not _is_number(actual_rate)
        or actual_rate <= 0
        or not _is_number(pipeline_sustainable_rate)
        or pipeline_sustainable_rate <= 0
        or not _is_number(steady_state_lag_slope_records_per_sec)
    ):
        return "FAILED", "Steady-state throughput or lag trend evidence is incomplete."

    rate_ratio = float(pipeline_sustainable_rate) / float(actual_rate)
    slope = float(steady_state_lag_slope_records_per_sec)
    saturation_slope = max(20.0, float(actual_rate) * 0.10)
    stable_slope = max(10.0, float(actual_rate) * 0.02)
    drain_limit = max(
        15.0,
        float(generation_duration_seconds or 0.0) * 0.25,
    )

    if slope > saturation_slope and rate_ratio < 0.90:
        return (
            "SATURATED",
            "Steady-state Kafka-to-Bronze lag grew by more than 10% of offered rate while the pipeline bottleneck processed under 90% of offered rate.",
        )
    if (
        slope <= stable_slope
        and rate_ratio >= 0.95
        and pipeline_drain_seconds is not None
        and pipeline_drain_seconds <= drain_limit
    ):
        return (
            "UNDER_CAPACITY",
            "The pipeline completed; steady-state lag was stable or declining, the bottleneck kept at least 95% of offered rate, and drain time stayed within policy.",
        )

    reasons = []
    if slope > stable_slope:
        reasons.append("steady-state source lag accumulated")
    if rate_ratio < 0.95:
        reasons.append("the pipeline bottleneck was below 95% of offered rate")
    if pipeline_drain_seconds is None or pipeline_drain_seconds > drain_limit:
        reasons.append("pipeline drain exceeded policy or could not be timed")
    detail = "; ".join(reasons) if reasons else "available headroom was marginal"
    return "NEAR_CAPACITY", f"The run drained, but {detail}."


def utc_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_time(value: datetime | None) -> str | None:
    return value.isoformat(timespec="milliseconds") if value is not None else None


def production_peak_lag(
    samples: Iterable[dict[str, Any]], start_time: str | None, end_time: str | None
) -> int | None:
    """Legacy helper using the current Kafka-to-Bronze lag field when present."""
    start = utc_time(start_time)
    end = utc_time(end_time)
    values = []
    for row in samples:
        timestamp = utc_time(row.get("timestamp_utc"))
        lag = _lag_value(row)
        if timestamp is None or lag is None:
            continue
        if start is not None and timestamp < start:
            continue
        if end is not None and timestamp > end:
            continue
        values.append(lag)
    return max(values) if values else None


def _metric_values(
    results: list[dict[str, Any]],
    key: str,
    legacy_key: str | None = None,
) -> list[float]:
    values = []
    for result in results:
        value = result.get(key)
        if value is None and legacy_key:
            value = result.get(legacy_key)
        if _is_number(value):
            values.append(float(value))
    return values


def aggregate_repetitions(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate repetitions while accepting the legacy result field names."""
    if not results:
        raise ValueError("At least one run result is required.")
    requested_rates = {
        result.get("requested_rate", result.get("requested_rate_msgs_sec"))
        for result in results
    }
    if len(requested_rates) != 1:
        raise ValueError("All repetitions in one aggregate must use the same rate.")

    average_fields = {
        "actual_rate": "actual_generated_msgs_sec",
        "bronze_processed_rate_avg": "avg_processed_rows_per_sec",
        "bronze_processed_rate_p50": None,
        "bronze_processed_rate_p95": None,
        "bronze_processed_rate_peak": "peak_processed_rows_per_sec",
        "silver_processed_rate_avg": "silver_avg_processed_rows_per_sec",
        "silver_processed_rate_p50": None,
        "silver_processed_rate_p95": None,
        "silver_processed_rate_peak": "silver_peak_processed_rows_per_sec",
        "startup_peak_kafka_to_bronze_lag": "max_kafka_lag_during_production",
        "steady_state_peak_kafka_to_bronze_lag": None,
        "steady_state_avg_kafka_to_bronze_lag": None,
        "steady_state_p95_kafka_to_bronze_lag": None,
        "steady_state_lag_slope_records_per_sec": None,
        "bronze_batch_p95_ms": "p95_batch_duration_ms",
        "silver_batch_p95_ms": None,
        "pipeline_drain_seconds": "drain_seconds",
        "replay_to_bronze_latency_p50_ms": "latency_p50_ms",
        "replay_to_bronze_latency_p95_ms": "latency_p95_ms",
        "replay_to_bronze_latency_p99_ms": "latency_p99_ms",
        "worker_cpu_avg": None,
        "worker_cpu_p50": None,
        "worker_cpu_p95": None,
        "worker_cpu_peak": "spark_worker_peak_cpu_percent",
        "worker_memory_avg_mb": None,
        "worker_memory_p50_mb": None,
        "worker_memory_p95_mb": None,
        "worker_memory_peak_mb": "spark_worker_peak_memory_mb",
        "broker_cpu_avg": None,
        "broker_cpu_p50": None,
        "broker_cpu_p95": None,
        "broker_cpu_peak": "broker_peak_cpu_percent",
        "broker_memory_avg_mb": None,
        "broker_memory_p50_mb": None,
        "broker_memory_p95_mb": None,
        "broker_memory_peak_mb": "broker_peak_memory_mb",
    }
    aggregate: dict[str, Any] = {
        "requested_rate": next(iter(requested_rates)),
        "run_count": len(results),
        "run_ids": [result.get("run_id") for result in results],
    }
    for field, legacy_key in average_fields.items():
        values = _metric_values(results, field, legacy_key)
        aggregate[field] = _mean(values)

    bronze_rate = aggregate["bronze_processed_rate_avg"]
    silver_rate = aggregate["silver_processed_rate_avg"]
    aggregate["pipeline_sustainable_rate"] = (
        min(bronze_rate, silver_rate)
        if _is_number(bronze_rate) and _is_number(silver_rate)
        else None
    )
    classifications = [
        result.get("capacity_classification", result.get("status"))
        for result in results
    ]
    aggregate["capacity_classifications"] = classifications
    if "FAILED" in classifications:
        aggregate["capacity_classification"] = "FAILED"
    elif "SATURATED" in classifications:
        aggregate["capacity_classification"] = "SATURATED"
    elif "NEAR_CAPACITY" in classifications:
        aggregate["capacity_classification"] = "NEAR_CAPACITY"
    elif classifications and all(value == "UNDER_CAPACITY" for value in classifications):
        aggregate["capacity_classification"] = "UNDER_CAPACITY"
    else:
        aggregate["capacity_classification"] = "MIXED"

    # Legacy aliases keep readers of the previous summary contract working.
    aggregate["processed_rate_mean"] = aggregate["bronze_processed_rate_avg"]
    aggregate["latency_p50_mean_ms"] = aggregate["replay_to_bronze_latency_p50_ms"]
    aggregate["latency_p95_mean_ms"] = aggregate["replay_to_bronze_latency_p95_ms"]
    aggregate["latency_p99_mean_ms"] = aggregate["replay_to_bronze_latency_p99_ms"]
    aggregate["batch_duration_mean_ms"] = _mean(
        _metric_values(results, "avg_batch_duration_ms")
    )
    aggregate["max_lag_mean"] = _mean(_metric_values(results, "max_kafka_lag"))
    return aggregate


def _container_resource_environment(
    manifest: dict[str, Any],
    resource_summaries: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    baseline = manifest.get("throughput_baseline") or manifest.get("fixed_infrastructure") or {}
    limits = baseline.get("docker_container_limits") or {}
    aliases = {
        "worker": "weather-spark-worker",
        "broker": "weather-kafka",
    }
    quota = {}
    quota_us = {}
    period_us = {}
    cpuset = {}
    memory_limit = {}
    recorded = {}
    for role, name in aliases.items():
        item = limits.get(name) if isinstance(limits, dict) else None
        item = item if isinstance(item, dict) else {}
        stats = resource_summaries.get(name, {})
        quota_value = item.get(
            "configured_cpu_limit_cores",
            item.get("container_cpu_quota_cores", stats.get("container_cpu_quota_cores")),
        )
        cpuset_value = item.get(
            "configured_cpuset_cpus",
            item.get("container_cpuset", stats.get("container_cpuset")),
        )
        memory_value = item.get(
            "configured_memory_limit_mb",
            item.get("container_memory_limit_mb", stats.get("container_memory_limit_mb")),
        )
        quota[role] = quota_value
        quota_us[role] = item.get(
            "configured_cpu_quota_us", stats.get("container_cpu_quota_us")
        )
        period_us[role] = item.get(
            "configured_cpu_period_us", stats.get("container_cpu_period_us")
        )
        cpuset[role] = cpuset_value or None
        memory_limit[role] = memory_value
        recorded[role] = {
            "cpu_quota": (
                "recorded"
                if any(key in item for key in (
                    "configured_cpu_limit_cores", "configured_cpu_quota_us",
                    "configured_cpu_period_us", "container_cpu_quota_cores",
                ))
                else "not_recorded"
            ),
            "cpuset": (
                "recorded"
                if "configured_cpuset_cpus" in item or "container_cpuset" in item
                else "not_recorded"
            ),
            "memory_limit": (
                "recorded"
                if "configured_memory_limit_mb" in item or "container_memory_limit_mb" in item
                else "not_recorded"
            ),
        }
    host_count = baseline.get("host_logical_cpu_count")
    if host_count is None:
        host_count = next(
            (
                values.get("host_logical_cpu_count")
                for values in resource_summaries.values()
                if values.get("host_logical_cpu_count") is not None
            ),
            None,
        )
    return {
        "host_logical_cpu_count": host_count,
        "container_cpu_quota": quota,
        "container_cpu_quota_us": quota_us,
        "container_cpu_period_us": period_us,
        "container_cpuset": cpuset,
        "container_memory_limit_mb": memory_limit,
        "container_limit_recording": recorded,
    }


def analyze_run_artifacts(
    *,
    progress_rows: Iterable[dict[str, Any]],
    lag_samples: Iterable[dict[str, Any]],
    resource_samples: Iterable[dict[str, Any]],
    simulator_summary: dict[str, Any],
    delta_metrics: dict[str, Any] | None,
    manifest: dict[str, Any] | None = None,
    legacy_result: dict[str, Any] | None = None,
    warmup_start_time: str | None = None,
    minimum_warmup_seconds: float = DEFAULT_WARMUP_SECONDS,
    minimum_completed_batches: int = DEFAULT_WARMUP_MIN_BATCHES,
    drain_complete: bool | None = None,
    stream_process_return_codes: dict[str, Any] | None = None,
    failure_reason: str | None = None,
    metrics_error: str | None = None,
) -> dict[str, Any]:
    """Build the schema-v2 result analysis from run-scoped raw telemetry."""
    rows = list(progress_rows)
    lags = list(lag_samples)
    resources = list(resource_samples)
    delta = delta_metrics if isinstance(delta_metrics, dict) else {}
    manifest = manifest if isinstance(manifest, dict) else {}
    legacy = legacy_result if isinstance(legacy_result, dict) else {}
    simulator = simulator_summary if isinstance(simulator_summary, dict) else {}
    if drain_complete is None:
        drain_complete = bool(legacy.get("drain_complete"))
    return_codes = stream_process_return_codes or legacy.get("stream_process_return_codes") or {}

    window = select_steady_state_window(
        rows,
        warmup_start_time=warmup_start_time,
        generation_start_time=simulator.get("generation_start_time"),
        generation_end_time=simulator.get("generation_end_time"),
        minimum_warmup_seconds=minimum_warmup_seconds,
        minimum_completed_batches=minimum_completed_batches,
    )
    progress = summarize_progress(
        rows,
        steady_state_start=window["steady_state_start"],
        generation_end_time=simulator.get("generation_end_time"),
    )
    lag_summary = summarize_kafka_to_bronze_lag(
        lags,
        generation_start_time=simulator.get("generation_start_time"),
        generation_end_time=simulator.get("generation_end_time"),
        steady_state_start=window["steady_state_start"],
    )
    resources_by_container = resource_statistics(resources)
    worker = resources_by_container.get("weather-spark-worker", {})
    broker = resources_by_container.get("weather-kafka", {})

    produced_value = simulator.get("kafka_messages", legacy.get("produced_messages"))
    expected = int(produced_value) if _is_number(produced_value) else None
    bronze_value = delta.get("bronze_records", legacy.get("bronze_records"))
    silver_value = delta.get(
        "silver_records",
        legacy.get("silver_records", legacy.get("silver_output_records")),
    )
    bronze_records = int(bronze_value) if _is_number(bronze_value) else None
    silver_records = int(silver_value) if _is_number(silver_value) else None
    bronze_progress = progress["bronze"]["input_records"]
    silver_progress = progress["silver"]["input_records"]

    bronze_rate = progress["bronze"]["avg_processed_rows_per_sec"]
    silver_rate = progress["silver"]["avg_processed_rows_per_sec"]
    pipeline_rate = (
        min(bronze_rate, silver_rate)
        if _is_number(bronze_rate) and _is_number(silver_rate)
        else None
    )
    bottleneck = (
        "bronze" if _is_number(bronze_rate) and _is_number(silver_rate) and bronze_rate <= silver_rate
        else "silver" if _is_number(bronze_rate) and _is_number(silver_rate)
        else None
    )

    bronze_completion = progress_completion_time(
        rows, stage="bronze", expected_records=expected
    )
    silver_completion = progress_completion_time(
        rows, stage="silver", expected_records=expected
    )
    completion_times = [
        utc_time(value)
        for value in (bronze_completion, silver_completion)
        if value is not None
    ]
    pipeline_completion = _format_time(max(completion_times)) if completion_times else None
    generation_end = utc_time(simulator.get("generation_end_time"))
    completion_time = utc_time(pipeline_completion)
    pipeline_drain = (
        max(0.0, (completion_time - generation_end).total_seconds())
        if completion_time is not None and generation_end is not None
        else legacy.get("drain_seconds")
    )
    generation_start = utc_time(simulator.get("generation_start_time"))
    generation_duration = (
        (generation_end - generation_start).total_seconds()
        if generation_start is not None and generation_end is not None
        else simulator.get("generation_elapsed_seconds", legacy.get("generation_elapsed_seconds"))
    )

    latency = replay_to_bronze_latency_metrics(delta, legacy)
    expected_counts_match = (
        expected is not None
        and bronze_records == expected
        and silver_records == expected
    )
    final_source_lag = lag_summary["final_source_lag"]
    query_exceptions = [
        row for row in rows
        if row.get("event_type") == "query_terminated" and row.get("exception")
    ]
    queries_clean = (
        not query_exceptions
        and not any(value not in (0, None) for value in return_codes.values())
        and not failure_reason
    )
    progress_complete = (
        expected is not None
        and bronze_progress >= expected
        and silver_progress >= expected
    )
    pipeline_completed = bool(
        drain_complete
        and final_source_lag == 0
        and expected_counts_match
        and progress_complete
        and queries_clean
    )

    faults = [
        simulator.get(key)
        for key in (
            "duplicates_generated",
            "invalid_generated",
            "late_generated",
            "out_of_order_generated",
        )
        if _is_number(simulator.get(key))
    ]
    unexpected_faults = any(value != 0 for value in faults)
    resource_complete = all(
        _is_number(container.get(key))
        for container in (worker, broker)
        for key in (
            "cpu_avg_percent", "cpu_p95_percent", "cpu_peak_percent",
            "memory_avg_mb", "memory_p95_mb", "memory_peak_mb",
        )
    )
    latency_complete = _is_number(latency.get("count")) and (
        expected is not None and int(latency["count"]) >= expected
    )
    steady_samples = window["steady_state_sample_count"]
    steady_lag_samples = lag_summary["steady_state_lag_sample_count"]
    steady_evidence_complete = (
        window["steady_state_ready"]
        and window["steady_state_progress_sample_counts"]["bronze"] >= 1
        and window["steady_state_progress_sample_counts"]["silver"] >= 1
        and steady_lag_samples >= 2
        and lag_summary["steady_state_lag_slope_records_per_sec"] is not None
        and _is_number(pipeline_rate)
    )
    process_failed = bool(
        not queries_clean
        or any(value not in (0, None) for value in return_codes.values())
    )
    data_loss_after_source_drain = bool(
        expected is not None
        and final_source_lag == 0
        and (not expected_counts_match or not progress_complete)
    )
    result_failed = bool(
        failure_reason
        or metrics_error
        or process_failed
        or unexpected_faults
        or data_loss_after_source_drain
        or (drain_complete and expected is not None and not expected_counts_match)
        or (drain_complete and expected is not None and not progress_complete)
        or not resource_complete
        or not latency_complete
        or not steady_evidence_complete
        or final_source_lag is None
    )
    actual_rate = simulator.get(
        "actual_generated_msgs_sec",
        legacy.get("actual_rate", legacy.get("actual_generated_msgs_sec")),
    )
    if not _is_number(actual_rate):
        actual_rate = None
    capacity, capacity_reason = classify_capacity(
        failed=result_failed,
        pipeline_completed=pipeline_completed,
        final_source_lag=final_source_lag,
        actual_rate=actual_rate,
        pipeline_sustainable_rate=pipeline_rate,
        steady_state_lag_slope_records_per_sec=(
            lag_summary["steady_state_lag_slope_records_per_sec"]
        ),
        pipeline_drain_seconds=(
            float(pipeline_drain) if _is_number(pipeline_drain) else None
        ),
        generation_duration_seconds=(
            float(generation_duration) if _is_number(generation_duration) else None
        ),
    )

    resource_environment = _container_resource_environment(manifest, resources_by_container)
    requested_rate = legacy.get("requested_rate", legacy.get("requested_rate_msgs_sec"))
    if requested_rate is None:
        requested_rate = simulator.get("requested_replay_rate")
    run_id = simulator.get("run_id", legacy.get("run_id"))
    replay_latency = {
        "count": latency.get("count"),
        "min_ms": latency.get("min_ms"),
        "avg_ms": latency.get("avg_ms"),
        "p50_ms": latency.get("p50_ms"),
        "p95_ms": latency.get("p95_ms"),
        "p99_ms": latency.get("p99_ms"),
        "max_ms": latency.get("max_ms"),
        "definition": latency.get("definition"),
        "percentile_method": latency.get("percentile_method"),
    }
    result: dict[str, Any] = {
        "analysis_schema_version": 2,
        "run_id": run_id,
        "requested_rate": requested_rate,
        "actual_rate": actual_rate,
        "produced_messages": expected,
        "bronze_records": bronze_records,
        "silver_records": silver_records,
        "bronze_input_records": bronze_progress,
        "silver_input_records": silver_progress,
        "bronze_processed_rate_avg": progress["bronze"]["avg_processed_rows_per_sec"],
        "bronze_processed_rate_p50": progress["bronze"]["p50_processed_rows_per_sec"],
        "bronze_processed_rate_p95": progress["bronze"]["p95_processed_rows_per_sec"],
        "bronze_processed_rate_peak": progress["bronze"]["peak_processed_rows_per_sec"],
        "bronze_avg_input_rows_per_sec": progress["bronze"]["avg_input_rows_per_sec"],
        "bronze_batch_avg_ms": progress["bronze"]["avg_batch_duration_ms"],
        "bronze_batch_p95_ms": progress["bronze"]["p95_batch_duration_ms"],
        "bronze_batch_max_ms": progress["bronze"]["max_batch_duration_ms"],
        "bronze_steady_state_batches": progress["bronze"]["steady_state_sample_count"],
        "silver_processed_rate_avg": progress["silver"]["avg_processed_rows_per_sec"],
        "silver_processed_rate_p50": progress["silver"]["p50_processed_rows_per_sec"],
        "silver_processed_rate_p95": progress["silver"]["p95_processed_rows_per_sec"],
        "silver_processed_rate_peak": progress["silver"]["peak_processed_rows_per_sec"],
        "silver_avg_input_rows_per_sec": progress["silver"]["avg_input_rows_per_sec"],
        "silver_batch_avg_ms": progress["silver"]["avg_batch_duration_ms"],
        "silver_batch_p95_ms": progress["silver"]["p95_batch_duration_ms"],
        "silver_batch_max_ms": progress["silver"]["max_batch_duration_ms"],
        "silver_steady_state_batches": progress["silver"]["steady_state_sample_count"],
        "pipeline_sustainable_rate": pipeline_rate,
        "pipeline_bottleneck_stage": bottleneck,
        **lag_summary,
        "max_kafka_to_bronze_lag": lag_summary["production_peak_kafka_to_bronze_lag"],
        "final_kafka_to_bronze_lag": final_source_lag,
        "steady_state_sample_count": {
            "bronze_progress": window["steady_state_progress_sample_counts"]["bronze"],
            "silver_progress": window["steady_state_progress_sample_counts"]["silver"],
            "kafka_to_bronze_lag": steady_lag_samples,
        },
        "steady_state_progress_sample_count": steady_samples,
        "warmup_policy": window["warmup_policy"],
        "warmup_start": window["warmup_start"],
        "steady_state_start": window["steady_state_start"],
        "steady_state_end": window["generation_end_time"],
        "bronze_completion_time": bronze_completion,
        "silver_completion_time": silver_completion,
        "pipeline_completion_time": pipeline_completion,
        "pipeline_drain_seconds": pipeline_drain,
        "replay_duration_seconds": generation_duration,
        "replay_to_bronze_latency_ms": replay_latency,
        "replay_to_bronze_latency_count": latency.get("count"),
        "replay_to_bronze_latency_min_ms": latency.get("min_ms"),
        "replay_to_bronze_latency_avg_ms": latency.get("avg_ms"),
        "replay_to_bronze_latency_p50_ms": latency.get("p50_ms"),
        "replay_to_bronze_latency_p95_ms": latency.get("p95_ms"),
        "replay_to_bronze_latency_p99_ms": latency.get("p99_ms"),
        "replay_to_bronze_latency_max_ms": latency.get("max_ms"),
        "replay_to_bronze_latency_definition": latency.get("definition"),
        "replay_to_bronze_latency_percentile_method": latency.get("percentile_method"),
        "worker_cpu_avg": worker.get("cpu_avg_percent"),
        "worker_cpu_p50": worker.get("cpu_p50_percent"),
        "worker_cpu_p95": worker.get("cpu_p95_percent"),
        "worker_cpu_peak": worker.get("cpu_peak_percent"),
        "worker_memory_avg_mb": worker.get("memory_avg_mb"),
        "worker_memory_p50_mb": worker.get("memory_p50_mb"),
        "worker_memory_p95_mb": worker.get("memory_p95_mb"),
        "worker_memory_peak_mb": worker.get("memory_peak_mb"),
        "broker_cpu_avg": broker.get("cpu_avg_percent"),
        "broker_cpu_p50": broker.get("cpu_p50_percent"),
        "broker_cpu_p95": broker.get("cpu_p95_percent"),
        "broker_cpu_peak": broker.get("cpu_peak_percent"),
        "broker_memory_avg_mb": broker.get("memory_avg_mb"),
        "broker_memory_p50_mb": broker.get("memory_p50_mb"),
        "broker_memory_p95_mb": broker.get("memory_p95_mb"),
        "broker_memory_peak_mb": broker.get("memory_peak_mb"),
        "resource_statistics": resources_by_container,
        **resource_environment,
        "final_source_lag": final_source_lag,
        "pipeline_completed": pipeline_completed,
        "pipeline_completion_check": {
            "drain_complete_from_progress": bool(drain_complete),
            "final_kafka_to_bronze_source_lag_zero": final_source_lag == 0,
            "bronze_count_matches_produced": expected is not None and bronze_records == expected,
            "silver_count_matches_produced": expected is not None and silver_records == expected,
            "bronze_progress_reached_expected": expected is not None and bronze_progress >= expected,
            "silver_progress_reached_expected": expected is not None and silver_progress >= expected,
            "stream_queries_completed_cleanly": queries_clean,
        },
        "steady_state_measurements_complete": steady_evidence_complete,
        "resource_measurements_complete": resource_complete,
        "latency_measurements_complete": latency_complete,
        "capacity_classification": capacity,
        "capacity_reason": capacity_reason,
    }
    return result
