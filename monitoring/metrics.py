from __future__ import annotations

import math
from typing import Any, Iterable, Mapping, Sequence


METRIC_VALUE_FIELDS = (
    "model_error_c",
    "persistence_error_c",
    "reference_temperature_c",
    "model_prediction_temperature_c",
    "persistence_prediction_temperature_c",
)


def _finite_values(rows: Iterable[Mapping[str, Any]], field: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(field)
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            values.append(number)
    return values


def _percentile(sorted_values: Sequence[float], fraction: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (len(sorted_values) - 1) * fraction
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return float(sorted_values[lower])
    weight = rank - lower
    return float(sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight)


def distribution_stats(values: Iterable[Any]) -> dict[str, float | int | None]:
    sample = _finite_values(({"value": value} for value in values), "value")
    sample.sort()
    if not sample:
        return {"count": 0, "mean": None, "std": None, "min": None, "p05": None, "p50": None, "p95": None, "max": None}
    mean = math.fsum(sample) / len(sample)
    variance = math.fsum((value - mean) ** 2 for value in sample) / len(sample)
    return {
        "count": len(sample),
        "mean": mean,
        "std": math.sqrt(variance),
        "min": sample[0],
        "p05": _percentile(sample, 0.05),
        "p50": _percentile(sample, 0.50),
        "p95": _percentile(sample, 0.95),
        "max": sample[-1],
    }


def _r_squared(predictions: Sequence[float], references: Sequence[float]) -> float | None:
    if len(references) < 2:
        return None
    mean_reference = math.fsum(references) / len(references)
    total = math.fsum((value - mean_reference) ** 2 for value in references)
    if total == 0.0 or not math.isfinite(total):
        return None
    residual = math.fsum((actual - predicted) ** 2 for predicted, actual in zip(predictions, references, strict=True))
    score = 1.0 - residual / total
    return score if math.isfinite(score) else None


def _error_metrics(errors: Sequence[float], predictions: Sequence[float], references: Sequence[float]) -> dict[str, float | None]:
    if not errors:
        return {"mae": None, "rmse": None, "bias": None, "r2": None}
    count = len(errors)
    return {
        "mae": math.fsum(abs(value) for value in errors) / count,
        "rmse": math.sqrt(math.fsum(value * value for value in errors) / count),
        "bias": math.fsum(errors) / count,
        "r2": _r_squared(predictions, references),
    }


def _usable_evaluations(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    usable: list[Mapping[str, Any]] = []
    for row in rows:
        if row.get("status", "EVALUATED") != "EVALUATED":
            continue
        try:
            numbers = [float(row[field]) for field in METRIC_VALUE_FIELDS]
        except (KeyError, TypeError, ValueError):
            continue
        if all(math.isfinite(value) for value in numbers):
            usable.append(row)
    return usable


def quality_status(model_mae: float | None, persistence_mae: float | None, sample_count: int, minimum_sample: int) -> str:
    if sample_count < minimum_sample or model_mae is None or persistence_mae is None:
        return "INSUFFICIENT_SAMPLE"
    if model_mae < persistence_mae:
        return "MODEL_BEATS_PERSISTENCE"
    if model_mae > persistence_mae:
        return "MODEL_TRAILS_PERSISTENCE"
    return "MODEL_EQUALS_PERSISTENCE"


def _scalar_location_metrics(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    model_errors = [float(row["model_error_c"]) for row in rows]
    persistence_errors = [float(row["persistence_error_c"]) for row in rows]
    count = len(rows)
    model_mae = math.fsum(abs(value) for value in model_errors) / count
    persistence_mae = math.fsum(abs(value) for value in persistence_errors) / count
    return {
        "sample_count": count,
        "model_mae": model_mae,
        "model_rmse": math.sqrt(math.fsum(value * value for value in model_errors) / count),
        "model_bias": math.fsum(model_errors) / count,
        "persistence_mae": persistence_mae,
        "persistence_rmse": math.sqrt(math.fsum(value * value for value in persistence_errors) / count),
        "persistence_bias": math.fsum(persistence_errors) / count,
        "mae_skill": None if persistence_mae == 0.0 else 1.0 - model_mae / persistence_mae,
    }


def calculate_metrics(rows: Iterable[Mapping[str, Any]], *, minimum_sample: int = 1) -> dict[str, Any]:
    sample = _usable_evaluations(rows)
    model_errors = [float(row["model_error_c"]) for row in sample]
    persistence_errors = [float(row["persistence_error_c"]) for row in sample]
    references = [float(row["reference_temperature_c"]) for row in sample]
    model_predictions = [float(row["model_prediction_temperature_c"]) for row in sample]
    persistence_predictions = [float(row["persistence_prediction_temperature_c"]) for row in sample]
    model = _error_metrics(model_errors, model_predictions, references)
    persistence = _error_metrics(persistence_errors, persistence_predictions, references)
    model_mae = model["mae"]
    persistence_mae = persistence["mae"]
    model_rmse = model["rmse"]
    persistence_rmse = persistence["rmse"]
    mae_improvement = None if model_mae is None or persistence_mae is None else persistence_mae - model_mae
    rmse_improvement = None if model_rmse is None or persistence_rmse is None else persistence_rmse - model_rmse
    mae_improvement_pct = None if mae_improvement is None or persistence_mae in (None, 0.0) else mae_improvement / persistence_mae * 100.0
    rmse_improvement_pct = None if rmse_improvement is None or persistence_rmse in (None, 0.0) else rmse_improvement / persistence_rmse * 100.0
    mae_skill = None if model_mae is None or persistence_mae in (None, 0.0) else 1.0 - model_mae / persistence_mae

    by_location: dict[str, list[Mapping[str, Any]]] = {}
    for row in sample:
        by_location.setdefault(str(row.get("location_id") or ""), []).append(row)
    per_location = {location_id: _scalar_location_metrics(location_rows) for location_id, location_rows in sorted(by_location.items())}
    location_mae = [metrics["model_mae"] for metrics in per_location.values()]
    beats = sum(1 for metrics in per_location.values() if metrics["model_mae"] < metrics["persistence_mae"])
    ge = sum(1 for metrics in per_location.values() if metrics["model_mae"] >= metrics["persistence_mae"])

    distributions = {
        "temperature_c": distribution_stats(references),
        "reference_temperature_c": distribution_stats(references),
        "humidity_pct": distribution_stats(_finite_values(sample, "humidity_pct")),
        "pressure_hpa": distribution_stats(_finite_values(sample, "pressure_hpa")),
        "precipitation_mm": distribution_stats(_finite_values(sample, "precipitation_mm")),
        "wind_speed_kmh": distribution_stats(_finite_values(sample, "wind_speed_kmh")),
        "wind_gust_kmh": distribution_stats(_finite_values(sample, "wind_gust_kmh")),
        "weather_code": distribution_stats(_finite_values(sample, "weather_code")),
        "model_prediction_temperature_c": distribution_stats(model_predictions),
        "persistence_prediction_temperature_c": distribution_stats(persistence_predictions),
        "model_error_c": distribution_stats(model_errors),
        "persistence_error_c": distribution_stats(persistence_errors),
    }
    return {
        "sample_count": len(sample),
        "model_mae": model_mae,
        "model_rmse": model_rmse,
        "model_bias": model["bias"],
        "model_r2": model["r2"],
        "persistence_mae": persistence_mae,
        "persistence_rmse": persistence_rmse,
        "persistence_bias": persistence["bias"],
        "persistence_r2": persistence["r2"],
        "mae_improvement_c": mae_improvement,
        "mae_improvement_pct": mae_improvement_pct,
        "rmse_improvement_c": rmse_improvement,
        "rmse_improvement_pct": rmse_improvement_pct,
        "mae_skill": mae_skill,
        "micro_global_mae": model_mae,
        "macro_location_mae": math.fsum(location_mae) / len(location_mae) if location_mae else None,
        "locations_evaluated": len(by_location),
        "locations_model_beats_persistence": beats,
        "locations_model_equals_or_trails_persistence": ge,
        "locations_model_mae_ge_persistence": ge,
        "quality_status": quality_status(model_mae, persistence_mae, len(sample), minimum_sample),
        "per_location": per_location,
        "distributions": distributions,
        "reference_arrival_lag_seconds": distribution_stats(_finite_values(sample, "reference_arrival_lag_seconds")),
        "evaluation_lag_seconds": distribution_stats(_finite_values(sample, "evaluation_lag_seconds")),
    }


def calculate_coverage(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    sample = list(rows)
    target_passed = sum(
        1
        for row in sample
        if bool(row.get("target_time_passed", row.get("status") != "PENDING_TARGET_TIME"))
    )
    evaluated = sum(1 for row in sample if row.get("status") == "EVALUATED")
    pending_reference = sum(1 for row in sample if row.get("status") in {"PENDING_REFERENCE", "PENDING_BASELINE"})
    pending_target = sum(1 for row in sample if row.get("status") == "PENDING_TARGET_TIME")
    invalid = sum(1 for row in sample if row.get("status") == "INVALID_PROVENANCE")
    conflict = sum(1 for row in sample if row.get("status") == "REFERENCE_CONFLICT")
    expected_references = len(sample)
    received_references = sum(1 for row in sample if bool(row.get("target_reference_received")))
    return {
        "forecasts_total": len(sample),
        "forecasts_target_time_passed": target_passed,
        "evaluated_count": evaluated,
        "pending_target_count": pending_target,
        "pending_reference_count": pending_reference,
        "invalid_count": invalid,
        "reference_conflict_count": conflict,
        "expected_references": expected_references,
        "received_references": received_references,
        "missing_references": max(0, expected_references - received_references),
        "reference_coverage_pct": None if expected_references == 0 else received_references / expected_references * 100.0,
        "evaluation_coverage_pct": None if target_passed == 0 else evaluated / target_passed * 100.0,
    }


def metric_record(
    rows: Iterable[Mapping[str, Any]],
    *,
    expected_sample_count: int,
    expected_location_count: int | None = None,
    minimum_sample: int = 1,
) -> dict[str, Any]:
    sample = list(rows)
    evaluated = [row for row in sample if row.get("status") == "EVALUATED"]
    result = calculate_metrics(evaluated, minimum_sample=minimum_sample)
    result.update(calculate_coverage(sample))
    result["expected_sample_count"] = int(expected_sample_count)
    result["window_complete"] = result["sample_count"] == expected_sample_count
    if expected_location_count is not None:
        result["expected_location_count"] = int(expected_location_count)
        result["location_count"] = len({str(row.get("location_id")) for row in evaluated})
        result["window_complete"] = result["window_complete"] and result["location_count"] == expected_location_count
    return result
