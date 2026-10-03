from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import threading
import time
from typing import Sequence

from .t2h_contract import (
    FEATURE_COUNT,
    MODEL_BYTES,
    MODEL_SHA256,
    XGBOOST_VERSION,
    default_model_path,
    load_t2h_feature_contract,
    validate_t2h_configuration,
)
from .contract import sha256_file


@dataclass
class _CachedT2HBooster:
    size: int
    mtime_ns: int
    booster: object
    loads: int


_MODEL_CACHE: dict[tuple[str, str], _CachedT2HBooster] = {}
_CACHE_LOCK = threading.Lock()


def load_verified_t2h_booster(model_path: str | Path | None = None):
    validate_t2h_configuration()
    path = Path(model_path) if model_path is not None else default_model_path()
    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"canonical T2H model artifact is missing: {path}")
    stat = path.stat()
    key = (str(path), MODEL_SHA256)
    with _CACHE_LOCK:
        cached = _MODEL_CACHE.get(key)
        if cached and cached.size == stat.st_size and cached.mtime_ns == stat.st_mtime_ns:
            return cached.booster
        if stat.st_size != MODEL_BYTES:
            raise ValueError(f"canonical T2H model must be {MODEL_BYTES} bytes, found {stat.st_size}")
        actual_sha = sha256_file(path)
        if actual_sha != MODEL_SHA256:
            raise ValueError(f"canonical T2H model SHA-256 mismatch: expected {MODEL_SHA256}, found {actual_sha}")

        import xgboost as xgb

        if xgb.__version__ != XGBOOST_VERSION:
            raise RuntimeError(f"XGBoost {XGBOOST_VERSION} is required for the frozen T2H model, found {xgb.__version__}")
        contract = load_t2h_feature_contract()
        booster = xgb.Booster()
        booster.load_model(str(path))
        booster.set_param({"device": "cpu", "nthread": max(1, int(os.getenv("WEATHER_INFERENCE_MODEL_THREADS", "1")))})
        if booster.num_features() != FEATURE_COUNT:
            raise ValueError(f"canonical T2H model expects {booster.num_features()} features; contract requires {FEATURE_COUNT}")
        if tuple(booster.feature_names or ()) != contract.feature_names:
            raise ValueError("canonical T2H model feature names/order differ from the frozen 73-feature list")

        loads = 1 if cached is None else cached.loads + 1
        _MODEL_CACHE[key] = _CachedT2HBooster(stat.st_size, stat.st_mtime_ns, booster, loads)
        return booster


def predict_t2h_feature_matrix(rows: Sequence[Sequence[float]], feature_names: Sequence[str]):
    import numpy as np
    import xgboost as xgb

    contract = load_t2h_feature_contract()
    if tuple(feature_names) != contract.feature_names:
        raise ValueError("T2H prediction feature names/order differ from the frozen canonical list")
    matrix = np.asarray(rows, dtype=np.float32)
    if matrix.ndim != 2 or matrix.shape[1] != FEATURE_COUNT:
        raise ValueError(f"T2H prediction input must have shape (n, {FEATURE_COUNT}), got {matrix.shape}")
    if not np.isfinite(matrix).all():
        raise ValueError("T2H prediction input contains null, NaN, +Inf, or -Inf")
    predictions = load_verified_t2h_booster().predict(xgb.DMatrix(matrix, feature_names=list(feature_names)))
    if not np.isfinite(predictions).all():
        raise ValueError("canonical T2H model returned a non-finite prediction")
    return predictions


def t2h_model_load_count(model_path: str | Path | None = None) -> int:
    path = (Path(model_path) if model_path is not None else default_model_path()).resolve()
    cached = _MODEL_CACHE.get((str(path), MODEL_SHA256))
    return 0 if cached is None else cached.loads


def validate_t2h_model(model_path: str | Path | None = None) -> dict[str, object]:
    import numpy as np
    import xgboost as xgb

    contract = load_t2h_feature_contract()
    started = time.perf_counter()
    path = (Path(model_path) if model_path is not None else default_model_path()).resolve()
    booster = load_verified_t2h_booster(path)
    probe = np.zeros((1, FEATURE_COUNT), dtype=np.float32)
    prediction = booster.predict(xgb.DMatrix(probe, feature_names=list(contract.feature_names)))
    if prediction.shape != (1,) or not np.isfinite(prediction).all():
        raise ValueError("canonical T2H model CPU startup probe did not return one finite prediction")
    return {
        "status": "PASS",
        "model_path": str(path),
        "model_bytes": path.stat().st_size,
        "model_sha256": MODEL_SHA256,
        "feature_count": FEATURE_COUNT,
        "feature_names_match": tuple(booster.feature_names or ()) == contract.feature_names,
        "xgboost_version": xgb.__version__,
        "device": "cpu",
        "load_and_probe_seconds": time.perf_counter() - started,
        "probe_prediction_temperature_c": float(prediction[0]),
    }
