"""Shared regression metric definitions used by every evaluation path."""

from __future__ import annotations

import math
from typing import Any

import numpy as np


def regression_metrics(y_true: Any, y_pred: Any) -> dict[str, float | int | None]:
    """Return MAE, RMSE, R², and mean prediction error (prediction - actual)."""

    actual = np.asarray(y_true, dtype=np.float64).reshape(-1)
    predicted = np.asarray(y_pred, dtype=np.float64).reshape(-1)
    if actual.shape != predicted.shape:
        raise ValueError(f"Actual and predicted shapes differ: {actual.shape} != {predicted.shape}")
    if actual.size == 0:
        raise ValueError("Cannot calculate metrics for an empty input")
    if not np.isfinite(actual).all() or not np.isfinite(predicted).all():
        raise ValueError("Metric inputs must contain only finite values")

    error = predicted - actual
    residual_sum_squares = float(np.dot(error, error))
    centered = actual - float(np.mean(actual))
    total_sum_squares = float(np.dot(centered, centered))
    r2 = 1.0 - residual_sum_squares / total_sum_squares if total_sum_squares > 0 else None
    return {
        "n": int(actual.size),
        "mae": float(np.mean(np.abs(error))),
        "rmse": float(math.sqrt(residual_sum_squares / actual.size)),
        "r2": float(r2) if r2 is not None else None,
        "mean_error": float(np.mean(error)),
    }


def compare_metrics(
    baseline: dict[str, float | int | None], model: dict[str, float | int | None]
) -> dict[str, float | int | None]:
    """Calculate positive gains when the model improves over the baseline."""

    baseline_mae = float(baseline["mae"])
    baseline_rmse = float(baseline["rmse"])
    mae_absolute = baseline_mae - float(model["mae"])
    rmse_absolute = baseline_rmse - float(model["rmse"])
    return {
        "n": int(model["n"]),
        "mae_absolute_improvement": mae_absolute,
        "mae_percentage_improvement": 100.0 * mae_absolute / baseline_mae
        if baseline_mae != 0
        else None,
        "rmse_absolute_improvement": rmse_absolute,
        "rmse_percentage_improvement": 100.0 * rmse_absolute / baseline_rmse
        if baseline_rmse != 0
        else None,
    }
