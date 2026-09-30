"""Shared split, per-location, and artifact evaluation routines."""

from __future__ import annotations

from collections import Counter
from typing import Any, Sequence

import numpy as np

from ml.config import EXPECTED_LOCATION_COUNT, EXPECTED_ROWS_PER_TEST_LOCATION
from ml.metrics import compare_metrics, regression_metrics


def validate_test_coverage(
    location_ids: Sequence[str], event_times: np.ndarray, target_times: np.ndarray
) -> dict[str, Any]:
    locations = np.asarray(location_ids, dtype=object)
    if locations.size != len(event_times) or locations.size != len(target_times):
        raise ValueError("Test location and time metadata lengths differ")
    counts = Counter(str(location_id) for location_id in locations)
    if len(counts) != EXPECTED_LOCATION_COUNT:
        raise ValueError(f"TEST has {len(counts)} locations; expected {EXPECTED_LOCATION_COUNT}")
    wrong_counts = {location_id: count for location_id, count in counts.items() if count != EXPECTED_ROWS_PER_TEST_LOCATION}
    if wrong_counts:
        raise ValueError(f"TEST rows/location differ from {EXPECTED_ROWS_PER_TEST_LOCATION}: {wrong_counts}")

    events = np.asarray(event_times, dtype="datetime64[ns]")
    targets = np.asarray(target_times, dtype="datetime64[ns]")
    if not np.all(targets == events + np.timedelta64(1, "h")):
        raise ValueError("TEST target_time is not exactly event_time + 1 hour")
    target_years = targets.astype("datetime64[Y]").astype(np.int64) + 1970
    if np.any(target_years != 2025):
        raise ValueError("TEST includes target times outside 2025 UTC")
    return {
        "location_count": len(counts),
        "rows_per_location": EXPECTED_ROWS_PER_TEST_LOCATION,
        "rows_by_location": dict(sorted(counts.items())),
        "target_period": "2025 UTC",
        "target_time_alignment": "target_time = event_time + 1 hour",
    }


def evaluate_per_location(
    y_true: Any,
    persistence_prediction: Any,
    xgboost_prediction: Any,
    location_ids: Sequence[str],
) -> list[dict[str, Any]]:
    actual = np.asarray(y_true, dtype=np.float64).reshape(-1)
    baseline = np.asarray(persistence_prediction, dtype=np.float64).reshape(-1)
    model = np.asarray(xgboost_prediction, dtype=np.float64).reshape(-1)
    locations = np.asarray(location_ids, dtype=object).reshape(-1)
    if not (actual.size == baseline.size == model.size == locations.size):
        raise ValueError("Per-location evaluation arrays have different lengths")

    rows: list[dict[str, Any]] = []
    for location_id in sorted(set(str(value) for value in locations)):
        mask = locations == location_id
        baseline_metrics = regression_metrics(actual[mask], baseline[mask])
        model_metrics = regression_metrics(actual[mask], model[mask])
        comparison = compare_metrics(baseline_metrics, model_metrics)
        rows.append(
            {
                "location_id": location_id,
                "n": int(mask.sum()),
                "persistence_mae": baseline_metrics["mae"],
                "persistence_rmse": baseline_metrics["rmse"],
                "persistence_r2": baseline_metrics["r2"],
                "persistence_bias": baseline_metrics["mean_error"],
                "xgboost_mae": model_metrics["mae"],
                "xgboost_rmse": model_metrics["rmse"],
                "xgboost_r2": model_metrics["r2"],
                "xgboost_bias": model_metrics["mean_error"],
                "mae_absolute_improvement": comparison["mae_absolute_improvement"],
                "mae_percentage_improvement": comparison["mae_percentage_improvement"],
                "rmse_absolute_improvement": comparison["rmse_absolute_improvement"],
                "rmse_percentage_improvement": comparison["rmse_percentage_improvement"],
            }
        )
    return rows


def summarize_location_results(rows: Sequence[dict[str, Any]], tolerance: float = 1e-12) -> dict[str, Any]:
    improved = [row for row in rows if float(row["mae_absolute_improvement"]) > tolerance]
    worse = [row for row in rows if float(row["mae_absolute_improvement"]) < -tolerance]
    tied = [row for row in rows if abs(float(row["mae_absolute_improvement"])) <= tolerance]
    worst_absolute = sorted(rows, key=lambda row: (-float(row["xgboost_mae"]), row["location_id"]))[:10]
    worst_regression = sorted(rows, key=lambda row: (float(row["mae_absolute_improvement"]), row["location_id"]))[:10]
    return {
        "locations_total": len(rows),
        "locations_improved": len(improved),
        "locations_worse": len(worse),
        "locations_tied": len(tied),
        "improved_location_ids": [row["location_id"] for row in improved],
        "worse_location_ids": [row["location_id"] for row in worse],
        "worst_10_xgboost_mae": worst_absolute,
        "worst_10_regression_vs_persistence": worst_regression,
    }


def write_test_predictions_parquet(
    output_path: str,
    *,
    location_ids: Sequence[str],
    event_times: np.ndarray,
    target_times: np.ndarray,
    actual: np.ndarray,
    persistence_prediction: np.ndarray,
    xgboost_prediction: np.ndarray,
) -> None:
    """Write the full diagnostic prediction table; callers must use Drive paths."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - exercised in Colab
        raise RuntimeError("PyArrow is required to write test_predictions.parquet") from exc

    n = len(actual)
    vectors = [location_ids, event_times, target_times, persistence_prediction, xgboost_prediction]
    if any(len(values) != n for values in vectors):
        raise ValueError("Prediction artifact columns have different lengths")
    actual64 = np.asarray(actual, dtype=np.float64)
    persistence64 = np.asarray(persistence_prediction, dtype=np.float64)
    model64 = np.asarray(xgboost_prediction, dtype=np.float64)
    table = pa.table(
        {
            "location_id": pa.array([str(value) for value in location_ids], type=pa.string()),
            "event_time": pa.array(np.asarray(event_times, dtype="datetime64[ns]"), type=pa.timestamp("ns")),
            "target_time": pa.array(np.asarray(target_times, dtype="datetime64[ns]"), type=pa.timestamp("ns")),
            "actual_temperature_c": pa.array(actual64, type=pa.float64()),
            "persistence_prediction_c": pa.array(persistence64, type=pa.float64()),
            "xgboost_prediction_c": pa.array(model64, type=pa.float64()),
            "persistence_error_c": pa.array(persistence64 - actual64, type=pa.float64()),
            "xgboost_error_c": pa.array(model64 - actual64, type=pa.float64()),
        }
    )
    pq.write_table(table, output_path, compression="snappy")
