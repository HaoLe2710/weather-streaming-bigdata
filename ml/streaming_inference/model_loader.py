from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import threading
from typing import Sequence

from .contract import (
    FEATURE_COUNT,
    MODEL_BYTES,
    MODEL_SHA256,
    XGBOOST_VERSION,
    repository_root,
    sha256_file,
)


@dataclass
class _CachedBooster:
    path: str
    size: int
    mtime_ns: int
    booster: object
    loads: int


_MODEL_CACHE: dict[tuple[str, str], _CachedBooster] = {}
_CACHE_LOCK = threading.Lock()


def default_model_path() -> Path:
    configured = os.getenv("WEATHER_FORECAST_MODEL_PATH")
    if configured:
        return Path(configured)
    return repository_root() / "data" / "models" / "weather_forecast_xgboost_v1" / "weather_forecast_xgboost_v1.json"


def load_verified_booster(
    model_path: str | Path | None = None,
    *,
    expected_sha256: str | None = None,
):
    configured_sha = os.getenv("WEATHER_FORECAST_MODEL_SHA256", MODEL_SHA256)
    if configured_sha != MODEL_SHA256:
        raise ValueError(
            f"configured model SHA must match frozen V1 contract: expected {MODEL_SHA256}, found {configured_sha}"
        )
    expected_sha256 = expected_sha256 or configured_sha
    path = Path(model_path) if model_path is not None else default_model_path()
    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"frozen XGBoost model not found: {path}")
    stat = path.stat()
    key = (str(path), expected_sha256)
    with _CACHE_LOCK:
        cached = _MODEL_CACHE.get(key)
        if cached is not None and cached.size == stat.st_size and cached.mtime_ns == stat.st_mtime_ns:
            return cached.booster

        if stat.st_size != MODEL_BYTES:
            raise ValueError(f"model artifact must be {MODEL_BYTES} bytes, found {stat.st_size}")
        actual_sha = sha256_file(path)
        if actual_sha != expected_sha256:
            raise ValueError(f"model SHA-256 mismatch: expected {expected_sha256}, found {actual_sha}")

        import xgboost as xgb

        if xgb.__version__ != XGBOOST_VERSION:
            raise RuntimeError(f"XGBoost {XGBOOST_VERSION} is required, found {xgb.__version__}")
        booster = xgb.Booster()
        booster.load_model(str(path))
        booster.set_param({"device": "cpu", "nthread": max(1, int(os.getenv("WEATHER_INFERENCE_MODEL_THREADS", "1")))})
        if booster.num_features() != FEATURE_COUNT:
            raise ValueError(f"model expects {booster.num_features()} features; contract requires {FEATURE_COUNT}")

        loads = 1 if cached is None else cached.loads + 1
        _MODEL_CACHE[key] = _CachedBooster(str(path), stat.st_size, stat.st_mtime_ns, booster, loads)
        return booster


def predict_feature_matrix(
    rows: Sequence[Sequence[float]],
    feature_names: Sequence[str],
    *,
    model_path: str | Path | None = None,
):
    """Predict a vectorized batch after enforcing the frozen ordered 73-column contract."""
    from ml.streaming_inference.contract import load_feature_contract

    contract = load_feature_contract()
    if tuple(feature_names) != contract.feature_names:
        raise ValueError("prediction feature names/order differ from the frozen model feature list")
    import numpy as np
    import xgboost as xgb

    matrix = np.asarray(rows, dtype=np.float32)
    if matrix.ndim != 2 or matrix.shape[1] != FEATURE_COUNT:
        raise ValueError(f"prediction input must have shape (n, {FEATURE_COUNT}), got {matrix.shape}")
    if not np.isfinite(matrix).all():
        raise ValueError("prediction input contains null, NaN, +Inf, or -Inf")
    booster = load_verified_booster(model_path)
    predictions = booster.predict(xgb.DMatrix(matrix, feature_names=list(feature_names)))
    if not np.isfinite(predictions).all():
        raise ValueError("XGBoost returned a non-finite prediction")
    return predictions


def model_load_count(model_path: str | Path | None = None) -> int:
    path = (Path(model_path) if model_path is not None else default_model_path()).resolve()
    cache = _MODEL_CACHE.get((str(path), MODEL_SHA256))
    return 0 if cache is None else cache.loads
