from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

from .metrics import calculate_coverage, calculate_metrics


WINDOWS_HOURS = (24, 168)


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        moment = value
    else:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        moment = datetime.fromisoformat(text)
    if moment.tzinfo is None or moment.utcoffset() is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _scope_keys(row: Mapping[str, Any]) -> tuple[str, str]:
    return (str(row.get("reference_source") or "UNKNOWN_SOURCE"), str(row.get("evaluation_mode") or "UNCLASSIFIED"))


def _target_hour_key(row: Mapping[str, Any]) -> datetime:
    target_time = _utc(row["target_time"])
    if target_time.minute or target_time.second or target_time.microsecond:
        raise ValueError("evaluation target_time must be an exact UTC hour")
    return target_time


def _expected_reference_stats(rows: list[Mapping[str, Any]], expected: int) -> dict[str, Any]:
    coverage = calculate_coverage(rows)
    received = int(coverage["received_references"])
    return {
        **coverage,
        "expected_references": expected,
        "received_references": received,
        "missing_references": max(0, expected - received),
        "reference_coverage_pct": None if expected == 0 else min(100.0, received / expected * 100.0),
    }


def build_hourly_metrics(
    evaluation_rows: Iterable[Mapping[str, Any]],
    *,
    expected_locations: int = 63,
    minimum_sample: int = 1,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, datetime], list[Mapping[str, Any]]] = defaultdict(list)
    for row in evaluation_rows:
        if row.get("target_time") is None:
            continue
        groups[(*_scope_keys(row), _target_hour_key(row))].append(row)
    result: list[dict[str, Any]] = []
    for (source, mode, target_time), rows in sorted(groups.items(), key=lambda item: item[0]):
        metrics = calculate_metrics(rows, minimum_sample=minimum_sample)
        coverage = _expected_reference_stats(rows, expected_locations)
        location_count = len({str(row.get("location_id")) for row in rows if row.get("status") == "EVALUATED"})
        result.append(
            {
                "reference_source": source,
                "evaluation_mode": mode,
                "target_time": target_time,
                "window_hours": 1,
                "location_id": None,
                "metric_scope": "GLOBAL",
                **metrics,
                **coverage,
                "expected_sample_count": expected_locations,
                "expected_location_count": expected_locations,
                "location_count": location_count,
                "window_complete": metrics["sample_count"] == expected_locations and location_count == expected_locations,
            }
        )
    return result


def _rolling_for_group(
    rows: list[Mapping[str, Any]],
    *,
    window_hours: int,
    expected_locations: int,
    by_location: bool,
    minimum_sample: int,
    location_ids: tuple[str, ...],
) -> list[dict[str, Any]]:
    bucket: dict[datetime, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        bucket[_target_hour_key(row)].append(row)
    hours = sorted(bucket)
    output: list[dict[str, Any]] = []
    left = 0
    for end_index, end_time in enumerate(hours):
        start_time = end_time - timedelta(hours=window_hours - 1)
        while left <= end_index and hours[left] < start_time:
            left += 1
        window_rows = [row for instant in hours[left : end_index + 1] for row in bucket[instant]]
        if by_location:
            location_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
            for row in window_rows:
                location_groups[str(row.get("location_id"))].append(row)
            locations = location_ids or tuple(sorted(location_groups))
            for location_id in locations:
                location_rows = location_groups.get(location_id, [])
                evaluated = [row for row in location_rows if row.get("status") == "EVALUATED"]
                metrics = calculate_metrics(evaluated, minimum_sample=minimum_sample)
                expected = window_hours
                coverage = _expected_reference_stats(location_rows, expected)
                output.append(
                    {
                        "reference_source": str(rows[0].get("reference_source") or "UNKNOWN_SOURCE"),
                        "evaluation_mode": str(rows[0].get("evaluation_mode") or "UNCLASSIFIED"),
                        "target_time": end_time,
                        "window_hours": window_hours,
                        "location_id": location_id,
                        "metric_scope": "LOCATION",
                        **metrics,
                        **coverage,
                        "expected_sample_count": expected,
                        "expected_location_count": 1,
                        "location_count": int(bool(evaluated)),
                        "window_complete": metrics["sample_count"] == expected,
                    }
                )
        else:
            evaluated = [row for row in window_rows if row.get("status") == "EVALUATED"]
            metrics = calculate_metrics(evaluated, minimum_sample=minimum_sample)
            expected = window_hours * expected_locations
            location_count = len({str(row.get("location_id")) for row in evaluated})
            coverage = _expected_reference_stats(window_rows, expected)
            output.append(
                {
                    "reference_source": str(rows[0].get("reference_source") or "UNKNOWN_SOURCE"),
                    "evaluation_mode": str(rows[0].get("evaluation_mode") or "UNCLASSIFIED"),
                    "target_time": end_time,
                    "window_hours": window_hours,
                    "location_id": None,
                    "metric_scope": "GLOBAL",
                    **metrics,
                    **coverage,
                    "expected_sample_count": expected,
                    "expected_location_count": expected_locations,
                    "location_count": location_count,
                    "window_complete": metrics["sample_count"] == expected and location_count == expected_locations,
                }
            )
    return output


def build_rolling_metrics(
    evaluation_rows: Iterable[Mapping[str, Any]],
    *,
    expected_locations: int = 63,
    minimum_samples: Mapping[int, int] | None = None,
    windows: tuple[int, ...] = WINDOWS_HOURS,
    location_ids: Iterable[str] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in evaluation_rows:
        if row.get("target_time") is not None:
            grouped[_scope_keys(row)].append(row)

    min_per_location = dict(minimum_samples or {24: 24, 168: 168})
    known_locations = tuple(sorted(str(location_id) for location_id in (location_ids or ())))
    global_rows: list[dict[str, Any]] = []
    location_rows: list[dict[str, Any]] = []
    for (_source, _mode), group_rows in sorted(grouped.items()):
        observed_locations = tuple(sorted({str(row.get("location_id")) for row in group_rows if row.get("location_id") is not None}))
        group_location_ids = known_locations or observed_locations
        for window_hours in windows:
            per_location_threshold = int(min_per_location.get(window_hours, window_hours))
            global_rows.extend(
                _rolling_for_group(
                    group_rows,
                    window_hours=window_hours,
                    expected_locations=expected_locations,
                    by_location=False,
                    minimum_sample=per_location_threshold * expected_locations,
                    location_ids=group_location_ids,
                )
            )
            location_rows.extend(
                _rolling_for_group(
                    group_rows,
                    window_hours=window_hours,
                    expected_locations=expected_locations,
                    by_location=True,
                    minimum_sample=per_location_threshold,
                    location_ids=group_location_ids,
                )
            )
    return {"rolling_global_metrics": global_rows, "rolling_location_metrics": location_rows}
