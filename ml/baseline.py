"""Official persistence baseline: predict the current temperature unchanged."""

from __future__ import annotations

from typing import Sequence

import numpy as np

from ml.config import PERSISTENCE_FEATURE


def persistence_predictions(features: np.ndarray, feature_names: Sequence[str]) -> np.ndarray:
    """Return temperature_c(t) in the original feature order."""

    if features.ndim != 2 or features.shape[1] != len(feature_names):
        raise ValueError("Feature matrix width does not match the stored feature list")
    try:
        temperature_index = feature_names.index(PERSISTENCE_FEATURE)
    except ValueError as exc:
        raise ValueError(f"Required persistence input {PERSISTENCE_FEATURE!r} is missing") from exc
    predictions = np.asarray(features[:, temperature_index], dtype=np.float64)
    if not np.isfinite(predictions).all():
        raise ValueError("Persistence input contains null, NaN, or infinite values")
    return predictions
