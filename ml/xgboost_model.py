"""Reproducible XGBoost candidate selection and final fitting helpers."""

from __future__ import annotations

import gc
import re
import shutil
import subprocess
import time
from typing import Any, Sequence

import numpy as np

from ml.artifacts import persist_candidate_results, read_json
from ml.config import (
    CPU_CANDIDATES,
    EARLY_STOPPING_ROUNDS,
    GPU_CANDIDATES,
    MAX_BOOST_ROUNDS,
    RANDOM_SEED,
    XGBOOST_TREE_METHOD,
)
from ml.data_loader import SplitArrays
from ml.metrics import regression_metrics


def detect_accelerator() -> dict[str, Any]:
    """Probe both NVIDIA visibility and a tiny actual XGBoost CUDA training run."""

    nvidia_smi = shutil.which("nvidia-smi")
    gpu_names: list[str] = []
    nvidia_smi_error: str | None = None
    if nvidia_smi:
        try:
            completed = subprocess.run(
                [nvidia_smi, "--query-gpu=name", "--format=csv,noheader"],
                check=False,
                capture_output=True,
                text=True,
                timeout=15,
            )
            if completed.returncode == 0:
                gpu_names = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
            else:
                nvidia_smi_error = (completed.stderr or completed.stdout).strip()[:500]
        except (OSError, subprocess.TimeoutExpired) as exc:
            nvidia_smi_error = str(exc)[:500]
    else:
        nvidia_smi_error = "nvidia-smi not found"

    try:
        import xgboost as xgb
    except ImportError as exc:  # pragma: no cover - dependency installed by notebook
        raise RuntimeError("Install requirements-ml.txt before probing XGBoost") from exc

    gpu_probe_error: str | None = None
    gpu_usable = False
    if gpu_names:
        probe_x = np.asarray([[0.0], [1.0], [2.0], [3.0]], dtype=np.float32)
        probe_y = np.asarray([0.0, 1.0, 2.0, 3.0], dtype=np.float32)
        try:
            probe_matrix = xgb.DMatrix(probe_x, label=probe_y, feature_names=["probe"])
            xgb.train(
                {
                    "objective": "reg:squarederror",
                    "eval_metric": "mae",
                    "tree_method": "hist",
                    "device": "cuda",
                    "seed": RANDOM_SEED,
                    "verbosity": 0,
                },
                probe_matrix,
                num_boost_round=2,
                verbose_eval=False,
            )
            gpu_usable = True
        except Exception as exc:  # XGBoostError is not a stable public import across releases
            gpu_probe_error = f"{type(exc).__name__}: {str(exc)[:500]}"
    elif gpu_names:
        gpu_probe_error = "No NVIDIA device was returned by nvidia-smi"
    else:
        gpu_probe_error = "GPU probe skipped because nvidia-smi did not report a device"

    return {
        "device": "cuda" if gpu_usable else "cpu",
        "accelerator_type": "NVIDIA GPU" if gpu_usable else "CPU",
        "gpu_names": gpu_names,
        "nvidia_smi_available": nvidia_smi is not None,
        "nvidia_smi_error": nvidia_smi_error,
        "xgboost_gpu_probe_passed": gpu_usable,
        "xgboost_gpu_probe_error": gpu_probe_error,
        "xgboost_version": xgb.__version__,
    }


def candidate_profiles(device: str) -> list[dict[str, Any]]:
    if device not in {"cpu", "cuda"}:
        raise ValueError(f"Unsupported XGBoost device: {device}")
    source = GPU_CANDIDATES if device == "cuda" else CPU_CANDIDATES
    prefix = "gpu" if device == "cuda" else "cpu"
    return [
        {**candidate, "candidate_id": re.sub(r"^(gpu|cpu)_", f"{prefix}_", candidate["candidate_id"])}
        for candidate in source
    ]


def _xgb_params(
    candidate: dict[str, Any],
    device: str,
    *,
    nthread: int | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "objective": "reg:squarederror",
        "eval_metric": "mae",
        "tree_method": XGBOOST_TREE_METHOD,
        "device": device,
        "seed": RANDOM_SEED,
        "verbosity": 0,
        "max_depth": int(candidate["max_depth"]),
        "eta": float(candidate["learning_rate"]),
        "min_child_weight": float(candidate["min_child_weight"]),
        "subsample": float(candidate["subsample"]),
        "colsample_bytree": float(candidate["colsample_bytree"]),
        "lambda": float(candidate["reg_lambda"]),
        "alpha": float(candidate["reg_alpha"]),
    }
    if nthread is not None and nthread > 0:
        params["nthread"] = int(nthread)
    return params


def _looks_like_gpu_resource_error(error: BaseException) -> bool:
    message = f"{type(error).__name__}: {error}".lower()
    return any(token in message for token in ("out of memory", "cuda_error_memory", "bad_alloc", "resource exhausted"))


def _to_dmatrix(data: SplitArrays, feature_names: Sequence[str], xgb: Any, nthread: int | None) -> Any:
    return xgb.DMatrix(
        data.features,
        label=data.target,
        feature_names=list(feature_names),
        nthread=nthread,
    )


def _train_with_device_fallback(
    xgb: Any,
    params: dict[str, Any],
    dtrain: Any,
    dvalidation: Any,
    *,
    max_boost_rounds: int,
    early_stopping_rounds: int,
) -> tuple[Any, str, str | None, float]:
    attempt_params = dict(params)
    requested_device = str(attempt_params["device"])
    started = time.perf_counter()
    try:
        booster = xgb.train(
            attempt_params,
            dtrain,
            num_boost_round=max_boost_rounds,
            evals=[(dvalidation, "validation")],
            early_stopping_rounds=early_stopping_rounds,
            verbose_eval=False,
        )
        return booster, requested_device, None, time.perf_counter() - started
    except Exception as exc:
        if requested_device != "cuda" or not _looks_like_gpu_resource_error(exc):
            raise
        elapsed_before_fallback = time.perf_counter() - started
        gc.collect()
        attempt_params["device"] = "cpu"
        retry_started = time.perf_counter()
        booster = xgb.train(
            attempt_params,
            dtrain,
            num_boost_round=max_boost_rounds,
            evals=[(dvalidation, "validation")],
            early_stopping_rounds=early_stopping_rounds,
            verbose_eval=False,
        )
        return (
            booster,
            "cpu",
            f"CUDA resource failure; retried on CPU ({type(exc).__name__}: {str(exc)[:300]})",
            elapsed_before_fallback + time.perf_counter() - retry_started,
        )


def run_candidate_search(
    train_data: SplitArrays,
    validation_data: SplitArrays,
    feature_names: Sequence[str],
    *,
    device: str,
    checkpoint_directory: str,
    nthread: int | None = None,
    candidate_configs: Sequence[dict[str, Any]] | None = None,
    max_boost_rounds: int = MAX_BOOST_ROUNDS,
    early_stopping_rounds: int = EARLY_STOPPING_ROUNDS,
) -> list[dict[str, Any]]:
    """Run/resume a deterministic candidate set and persist after every attempt."""

    import xgboost as xgb

    configs = list(candidate_configs or candidate_profiles(device))
    if not configs:
        raise ValueError("Candidate search requires at least one configuration")

    checkpoint_path = f"{checkpoint_directory}/candidate_results.json"
    try:
        records = read_json(checkpoint_path)
        if not isinstance(records, list):
            raise ValueError("candidate_results.json must contain a list")
    except FileNotFoundError:
        records = []

    dtrain = _to_dmatrix(train_data, feature_names, xgb, nthread)
    dvalidation = _to_dmatrix(validation_data, feature_names, xgb, nthread)
    for candidate in configs:
        candidate_id = candidate["candidate_id"]
        successful = [
            record
            for record in records
            if record.get("candidate_id") == candidate_id and record.get("status") == "SUCCESS"
        ]
        if successful:
            if successful[-1].get("params") != candidate:
                raise ValueError(f"Saved candidate {candidate_id} has a different parameter contract")
            continue

        attempt = 1 + sum(record.get("candidate_id") == candidate_id for record in records)
        params = _xgb_params(candidate, device, nthread=nthread)
        started = time.perf_counter()
        try:
            booster, actual_device, fallback_reason, training_seconds = _train_with_device_fallback(
                xgb,
                params,
                dtrain,
                dvalidation,
                max_boost_rounds=max_boost_rounds,
                early_stopping_rounds=early_stopping_rounds,
            )
            best_iteration = int(booster.best_iteration)
            boosting_rounds = best_iteration + 1
            predictions = booster.predict(dvalidation, iteration_range=(0, boosting_rounds))
            metrics = regression_metrics(validation_data.target, predictions)
            record = {
                "candidate_id": candidate_id,
                "attempt": attempt,
                "params": candidate,
                "train_rows": train_data.row_count,
                "validation_rows": validation_data.row_count,
                "best_iteration": best_iteration,
                "best_boost_rounds": boosting_rounds,
                "validation_metrics": metrics,
                "training_time_seconds": float(training_seconds),
                "elapsed_seconds_including_fallback": float(time.perf_counter() - started),
                "device": actual_device,
                "device_fallback_reason": fallback_reason,
                "status": "SUCCESS",
                "error": None,
            }
        except Exception as exc:
            record = {
                "candidate_id": candidate_id,
                "attempt": attempt,
                "params": candidate,
                "train_rows": train_data.row_count,
                "validation_rows": validation_data.row_count,
                "best_iteration": None,
                "best_boost_rounds": None,
                "validation_metrics": None,
                "training_time_seconds": float(time.perf_counter() - started),
                "device": device,
                "device_fallback_reason": None,
                "status": "FAILED",
                "error": f"{type(exc).__name__}: {str(exc)[:1000]}",
            }
        finally:
            if "booster" in locals():
                del booster
            gc.collect()
        records.append(record)
        persist_candidate_results(checkpoint_directory, records)
    return records


def select_candidate(records: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Rank successful candidates by validation MAE, RMSE, model size proxy, then time."""

    successful = [
        record
        for record in records
        if record.get("status") == "SUCCESS" and record.get("validation_metrics")
    ]
    if not successful:
        raise RuntimeError("No candidate completed successfully")

    def rank(record: dict[str, Any]) -> tuple[float, float, int, float, str]:
        metrics = record["validation_metrics"]
        params = record["params"]
        rounds = int(record["best_boost_rounds"])
        size_proxy = rounds * int(params["max_depth"])
        return (
            float(metrics["mae"]),
            float(metrics["rmse"]),
            size_proxy,
            float(record["training_time_seconds"]),
            str(record["candidate_id"]),
        )

    ranked = sorted(successful, key=rank)
    winner = ranked[0]
    return {
        "candidate_id": winner["candidate_id"],
        "params": winner["params"],
        "best_iteration": winner["best_iteration"],
        "best_boost_rounds": winner["best_boost_rounds"],
        "validation_metrics": winner["validation_metrics"],
        "training_time_seconds": winner["training_time_seconds"],
        "device": winner["device"],
        "successful_candidate_count": len(successful),
        "selection_metric": "validation_mae",
        "tie_break_order": ["validation_rmse", "best_boost_rounds*max_depth", "training_time_seconds"],
        "selection_reason": "Lowest full-validation MAE; ties are resolved by lower RMSE, smaller model-size proxy, then lower training time.",
    }


def fit_final_model(
    training_data: SplitArrays,
    feature_names: Sequence[str],
    selected: dict[str, Any],
    *,
    device: str,
    nthread: int | None = None,
) -> tuple[Any, str, str | None, float]:
    """Fit the frozen parameters for exactly best_iteration + 1 rounds."""

    import xgboost as xgb

    if training_data.row_count != selected.get("final_training_rows", training_data.row_count):
        raise ValueError("Final fit row count differs from the frozen selection record")
    rounds = int(selected["best_iteration"]) + 1
    if rounds < 1:
        raise ValueError("Final training requires at least one boosting round")
    dtrain = _to_dmatrix(training_data, feature_names, xgb, nthread)
    params = _xgb_params(selected["params"], device, nthread=nthread)
    started = time.perf_counter()
    try:
        booster = xgb.train(params, dtrain, num_boost_round=rounds, verbose_eval=False)
        return booster, device, None, time.perf_counter() - started
    except Exception as exc:
        if device != "cuda" or not _looks_like_gpu_resource_error(exc):
            raise
        del dtrain
        gc.collect()
        dtrain = _to_dmatrix(training_data, feature_names, xgb, nthread)
        params["device"] = "cpu"
        retry_started = time.perf_counter()
        booster = xgb.train(params, dtrain, num_boost_round=rounds, verbose_eval=False)
        return (
            booster,
            "cpu",
            f"CUDA resource failure; final fit retried on CPU ({type(exc).__name__}: {str(exc)[:300]})",
            time.perf_counter() - started,
        )


def feature_importance_records(booster: Any, feature_names: Sequence[str]) -> list[dict[str, Any]]:
    gains = booster.get_score(importance_type="gain")
    weights = booster.get_score(importance_type="weight")
    rows = [
        {
            "feature": name,
            "gain": float(gains.get(name, 0.0)),
            "weight": int(weights.get(name, 0)),
        }
        for name in feature_names
    ]
    return sorted(rows, key=lambda row: (-row["gain"], -row["weight"], row["feature"]))
