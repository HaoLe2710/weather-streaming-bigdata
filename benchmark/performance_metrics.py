"""Pure helpers for the throughput benchmark's persisted metrics."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any, Iterable


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
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


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
    """Decode Spark Kafka offset JSON to ``topic:partition`` -> next offset."""
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


def add_warmup_markers(
    rows: Iterable[dict[str, Any]], warmup_batches: int
) -> list[dict[str, Any]]:
    if warmup_batches < 0:
        raise ValueError("warmup_batches must be zero or greater.")
    copied = [dict(row) for row in rows]
    batches: dict[tuple[str, str], list[tuple[int, int]]] = {}
    for index, row in enumerate(copied):
        progress = row.get("progress")
        if row.get("event_type") != "progress" or not isinstance(progress, dict):
            continue
        try:
            batch_id = int(progress.get("batchId", -1))
        except (TypeError, ValueError):
            batch_id = -1
        key = (str(row.get("stage", "")), str(progress.get("name") or progress.get("queryId", "")))
        batches.setdefault(key, []).append((batch_id, index))

    warmup_indices: set[int] = set()
    for entries in batches.values():
        for _, index in sorted(entries)[:warmup_batches]:
            warmup_indices.add(index)
    for index, row in enumerate(copied):
        if row.get("event_type") == "progress":
            row["warmup_excluded"] = index in warmup_indices
    return copied


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def summarize_progress(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Summarize non-warm-up Bronze and Silver query progress events."""
    by_stage: dict[str, list[dict[str, Any]]] = {"bronze": [], "silver": []}
    totals: dict[str, int] = {"bronze": 0, "silver": 0}
    for row in rows:
        progress = row.get("progress")
        if row.get("event_type") != "progress" or not isinstance(progress, dict):
            continue
        stage = str(row.get("stage", ""))
        name = str(progress.get("name") or "").lower()
        if stage == "bronze":
            key = "bronze"
        elif stage == "silver" and name.endswith("-silver"):
            key = "silver"
        else:
            continue
        try:
            totals[key] += int(progress.get("numInputRows") or 0)
        except (TypeError, ValueError):
            pass
        if not row.get("warmup_excluded", False):
            by_stage[key].append(progress)

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
                "mean_ms": _mean(values),
                "p95_ms": percentile(values, 0.95),
                "max_ms": max(values) if values else None,
            }
        summaries[stage] = {
            "input_records": totals[stage],
            "included_batches": len(progress_rows),
            "avg_input_rows_per_sec": _mean(inputs),
            "peak_input_rows_per_sec": max(inputs) if inputs else None,
            "avg_processed_rows_per_sec": _mean(processed),
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


def lag_peak(samples: Iterable[dict[str, Any]]) -> tuple[int | None, int | None]:
    rows = list(samples)
    lags = [int(row["lag_records"]) for row in rows if _is_number(row.get("lag_records"))]
    last_lag = rows[-1].get("lag_records") if rows else None
    return (
        max(lags) if lags else None,
        int(last_lag) if _is_number(last_lag) else None,
    )


def resource_peaks(samples: Iterable[dict[str, Any]]) -> dict[str, dict[str, float | None]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in samples:
        container = row.get("container")
        if container:
            grouped.setdefault(str(container), []).append(row)
    result: dict[str, dict[str, float | None]] = {}
    for container, rows in grouped.items():
        cpu = [float(row["cpu_percent"]) for row in rows if _is_number(row.get("cpu_percent"))]
        memory = [
            float(row["memory_usage_mb"])
            for row in rows
            if _is_number(row.get("memory_usage_mb"))
        ]
        result[container] = {
            "peak_cpu_percent": max(cpu) if cpu else None,
            "peak_memory_mb": max(memory) if memory else None,
        }
    return result


def classify_capacity(
    *,
    failed: bool,
    all_records_processed: bool,
    final_lag: int | None,
    production_peak_lag: int | None,
    requested_rate: int,
    trigger_interval_seconds: float,
) -> tuple[str, str]:
    """Classify a run using a documented two-trigger backlog allowance."""
    if failed:
        return "FAILED", "A streaming process or run step failed."
    if not all_records_processed or final_lag is None or final_lag > 0:
        return "SATURATED", "Produced records were not fully processed with zero final Kafka lag."
    near_zero_limit = max(1, math.ceil(requested_rate * trigger_interval_seconds * 2))
    if production_peak_lag is not None and production_peak_lag <= near_zero_limit:
        return (
            "UNDER_CAPACITY",
            "The run drained completely and peak replay-time backlog stayed within two trigger intervals.",
        )
    return (
        "NEAR_CAPACITY",
        "The run drained completely, but replay-time backlog exceeded the two-trigger-interval allowance.",
    )


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


def production_peak_lag(
    samples: Iterable[dict[str, Any]], start_time: str | None, end_time: str | None
) -> int | None:
    start = utc_time(start_time)
    end = utc_time(end_time)
    values: list[int] = []
    for row in samples:
        timestamp = utc_time(row.get("timestamp_utc"))
        lag = row.get("lag_records")
        if timestamp is None or not _is_number(lag):
            continue
        if start is not None and timestamp < start:
            continue
        if end is not None and timestamp > end:
            continue
        values.append(int(lag))
    return max(values) if values else None


def aggregate_repetitions(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        raise ValueError("At least one run result is required.")
    requested_rates = {result.get("requested_rate_msgs_sec") for result in results}
    if len(requested_rates) != 1:
        raise ValueError("All repetitions in one aggregate must use the same rate.")
    processed_rates = [
        float(result["avg_processed_rows_per_sec"])
        for result in results
        if _is_number(result.get("avg_processed_rows_per_sec"))
    ]
    batch_means = [
        float(result["avg_batch_duration_ms"])
        for result in results
        if _is_number(result.get("avg_batch_duration_ms"))
    ]
    max_lags = [
        float(result["max_kafka_lag"])
        for result in results
        if _is_number(result.get("max_kafka_lag"))
    ]
    summary = {
        "requested_rate_msgs_sec": next(iter(requested_rates)),
        "run_count": len(results),
        "run_ids": [result.get("run_id") for result in results],
        "processed_rate_mean": _mean(processed_rates),
        "processed_rate_min": min(processed_rates) if processed_rates else None,
        "processed_rate_max": max(processed_rates) if processed_rates else None,
        "latency_p50_mean_ms": _mean([
            float(result["latency_p50_ms"])
            for result in results
            if _is_number(result.get("latency_p50_ms"))
        ]),
        "latency_p95_mean_ms": _mean([
            float(result["latency_p95_ms"])
            for result in results
            if _is_number(result.get("latency_p95_ms"))
        ]),
        "latency_p99_mean_ms": _mean([
            float(result["latency_p99_ms"])
            for result in results
            if _is_number(result.get("latency_p99_ms"))
        ]),
        "batch_duration_mean_ms": _mean(batch_means),
        "max_lag_mean": _mean(max_lags),
        "capacity_classifications": [result.get("capacity_classification") for result in results],
    }
    return summary
