"""Controlled train/freeze/one-read TEST run for the V1.1 T2H model."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gc
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess
import time
from typing import Any, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from historical.forecast_t2h_v1_1 import DEFAULT_ARTIFACT_ROOT, MODEL_ID, SOURCE_CONTRACT_ID
from ml.config import EARLY_STOPPING_ROUNDS, MAX_BOOST_ROUNDS, RANDOM_SEED
from ml.metrics import compare_metrics, regression_metrics
from ml.xgboost_model import (
    candidate_profiles,
    detect_accelerator,
    feature_importance_records,
    fit_final_model,
    run_candidate_search,
    select_candidate,
)
from ml.forecast_t2h_v1_1 import (
    DATASET_PATH,
    FEATURE_SET_ID,
    MODEL_FEATURE_COLUMNS,
    TARGET_COLUMN,
    _atomic_write_json,
    _sha256,
    feature_list_sha256,
    load_split_arrays,
    persistence_predictions,
    validate_dataset_manifest,
    write_persistence_validation,
    write_distribution_comparison,
    write_source_alignment,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def validate_model_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """Validate the frozen model's ID, exact 73-feature digest, and T2H target contract."""
    expected_digest = feature_list_sha256()
    errors: list[str] = []
    checks = {
        "model_id": manifest.get("model_id") == MODEL_ID,
        "feature_set_id": manifest.get("feature_set_id") == FEATURE_SET_ID,
        "source_contract_id": manifest.get("source_contract_id") == SOURCE_CONTRACT_ID,
        "target_column": manifest.get("target_column") == TARGET_COLUMN,
        "target_offset_seconds": manifest.get("target_offset_seconds") == 7200,
        "feature_count": manifest.get("feature_count") == len(MODEL_FEATURE_COLUMNS) == 73,
        "feature_names": manifest.get("feature_names") == list(MODEL_FEATURE_COLUMNS),
        "feature_list_sha256": manifest.get("feature_list_sha256") == expected_digest,
        "serialization_format": manifest.get("serialization_format") == "XGBoost JSON",
        "model_sha256": isinstance(manifest.get("model_sha256"), str)
        and len(manifest["model_sha256"]) == 64
        and all(character in "0123456789abcdef" for character in manifest["model_sha256"]),
    }
    errors.extend(name for name, passed in checks.items() if not passed)
    return {
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "checks": checks,
        "model_id": manifest.get("model_id"),
        "feature_count": manifest.get("feature_count"),
        "feature_list_sha256": manifest.get("feature_list_sha256"),
        "target_offset_seconds": manifest.get("target_offset_seconds"),
    }


def _candidate_configs(device: str) -> list[dict[str, Any]]:
    configs = candidate_profiles(device)
    if device == "cpu":
        configs.append({
            "candidate_id": "cpu_d6_lr003_regularized",
            "max_depth": 6,
            "learning_rate": 0.03,
            "min_child_weight": 2.0,
            "subsample": 0.85,
            "colsample_bytree": 0.80,
            "reg_lambda": 3.0,
            "reg_alpha": 0.1,
        })
    return configs


def _dependency_versions() -> dict[str, str]:
    import numpy
    import pandas
    import pyarrow
    import sklearn
    import xgboost

    return {
        "python": platform.python_version(),
        "numpy": numpy.__version__,
        "pandas": pandas.__version__,
        "pyarrow": pyarrow.__version__,
        "xgboost": xgboost.__version__,
        "scikit_learn": sklearn.__version__,
        "httpx": importlib.metadata.version("httpx"),
    }


def _safe_device_probe() -> dict[str, Any]:
    try:
        return detect_accelerator()
    except Exception as exc:
        return {
            "device": "cpu",
            "accelerator_type": "CPU",
            "gpu_names": [],
            "nvidia_smi_available": False,
            "nvidia_smi_error": None,
            "xgboost_gpu_probe_passed": False,
            "xgboost_gpu_probe_error": f"{type(exc).__name__}: {str(exc)[:500]}",
        }


def _dump_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False, lineterminator="\n")


def _test_metrics_by_location(
    location_ids: np.ndarray,
    actual: np.ndarray,
    predicted: np.ndarray,
    persistence: np.ndarray,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    records: list[dict[str, Any]] = []
    for location_id in sorted(set(location_ids.astype(str))):
        selected = location_ids.astype(str) == location_id
        model_metrics = regression_metrics(actual[selected], predicted[selected])
        baseline_metrics = regression_metrics(actual[selected], persistence[selected])
        difference = float(model_metrics["mae"]) - float(baseline_metrics["mae"])
        records.append({
            "location_id": location_id,
            "sample_count": int(selected.sum()),
            "xgboost_mae": model_metrics["mae"],
            "persistence_mae": baseline_metrics["mae"],
            "mae_skill": 1.0 - float(model_metrics["mae"]) / float(baseline_metrics["mae"])
            if float(baseline_metrics["mae"]) != 0
            else None,
            "xgboost_bias": model_metrics["mean_error"],
            "xgboost_rmse": model_metrics["rmse"],
            "xgboost_r2": model_metrics["r2"],
            "persistence_rmse": baseline_metrics["rmse"],
            "mae_comparison": "better" if difference < -1e-12 else "equal" if abs(difference) <= 1e-12 else "worse",
        })
    counts = {
        "locations_better": sum(row["mae_comparison"] == "better" for row in records),
        "locations_equal": sum(row["mae_comparison"] == "equal" for row in records),
        "locations_worse": sum(row["mae_comparison"] == "worse" for row in records),
    }
    return records, counts


def _temporal_diagnostics(
    feature_times: np.ndarray,
    actual: np.ndarray,
    predicted: np.ndarray,
    persistence: np.ndarray,
) -> dict[str, Any]:
    timestamps = pd.to_datetime(feature_times, utc=True)
    frame = pd.DataFrame({
        "feature_time": timestamps,
        "actual": actual.astype(np.float64),
        "prediction": predicted.astype(np.float64),
        "persistence": persistence.astype(np.float64),
    })
    frame["error"] = frame["prediction"] - frame["actual"]
    frame["hour_utc"] = frame["feature_time"].dt.hour
    frame["month_utc"] = frame["feature_time"].dt.month
    frame["season_year"] = frame["feature_time"].dt.year
    frame["abs_error"] = frame["error"].abs()
    frame["squared_error"] = frame["error"] ** 2
    frame["persistence_abs_error"] = (frame["persistence"] - frame["actual"]).abs()

    def aggregate(grouped: Any) -> dict[str, Any]:
        results = {}
        for key, group in grouped:
            results[str(key)] = {
                "rows": int(len(group)),
                "mae": float(group["abs_error"].mean()),
                "rmse": float(np.sqrt(group["squared_error"].mean())),
                "bias": float(group["error"].mean()),
                "persistence_mae": float(group["persistence_abs_error"].mean()),
            }
        return results

    return {
        "split": "TEST",
        "grouped_by_target_time_rule": "Target time is feature_time + 2h; group keys use the corresponding feature_time UTC calendar dimensions.",
        "by_utc_hour": aggregate(frame.groupby("hour_utc", sort=True)),
        "by_utc_month": aggregate(frame.groupby("month_utc", sort=True)),
        "by_year": aggregate(frame.groupby("season_year", sort=True)),
    }


def _write_test_predictions(
    artifact_root: Path,
    location_ids: np.ndarray,
    feature_times: np.ndarray,
    target_times: np.ndarray,
    actual: np.ndarray,
    predicted: np.ndarray,
    persistence: np.ndarray,
) -> Path:
    table = pa.table({
        "location_id": pa.array(location_ids.astype(str)),
        "feature_time": pa.array(pd.to_datetime(feature_times, utc=True).to_pydatetime(), type=pa.timestamp("us", tz="UTC")),
        "target_time": pa.array(pd.to_datetime(target_times, utc=True).to_pydatetime(), type=pa.timestamp("us", tz="UTC")),
        "target_temperature_2h": pa.array(actual.astype(np.float32)),
        "xgboost_prediction": pa.array(predicted.astype(np.float32)),
        "persistence_prediction": pa.array(persistence.astype(np.float32)),
        "xgboost_error": pa.array((predicted.astype(np.float64) - actual.astype(np.float64)).astype(np.float32)),
    })
    path = artifact_root / "test_predictions.parquet"
    temp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, temp, compression="snappy", use_dictionary=True)
    os.replace(temp, path)
    return path


def _reload_prediction_difference(
    booster: Any,
    reloaded: Any,
    features: np.ndarray,
    feature_names: Sequence[str],
    *,
    device: str,
    best_rounds: int,
) -> float:
    """Compare serialization on one predictor device to avoid CPU/GPU drift."""
    import xgboost as xgb

    booster.set_param({"device": device})
    reloaded.set_param({"device": device})
    matrix = xgb.DMatrix(features, feature_names=list(feature_names))
    original_predictions = booster.predict(matrix, iteration_range=(0, best_rounds))
    reloaded_predictions = reloaded.predict(matrix, iteration_range=(0, best_rounds))
    differences = np.abs(
        original_predictions.astype(np.float64) - reloaded_predictions.astype(np.float64)
    )
    return float(differences.max(initial=0.0))


def train_and_evaluate(
    *,
    dataset_root: str | Path = DATASET_PATH,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    model_path: str | Path | None = None,
    nthread: int | None = None,
    max_boost_rounds: int = 1500,
    early_stopping_rounds: int = 80,
) -> dict[str, Any]:
    """Run VALIDATION-only selection, freeze, final fit, then open TEST once."""
    root = Path(artifact_root)
    root.mkdir(parents=True, exist_ok=True)
    run_started_at = _utc_now()
    run_id = root.name
    default_model = root / f"weather_forecast_xgboost_t2h_v1_1_{run_id}.json"
    saved_model_path = Path(model_path) if model_path else default_model
    saved_model_path.parent.mkdir(parents=True, exist_ok=True)

    freeze_path = root / "freeze_manifest.json"
    test_metrics_path = root / "test_metrics.json"
    if test_metrics_path.exists():
        raise RuntimeError("TEST evaluation already exists for this run; refusing to read TEST a second time")
    prior_freeze = json.loads(freeze_path.read_text(encoding="utf-8")) if freeze_path.exists() else None
    if prior_freeze and prior_freeze.get("test_first_read_time"):
        raise RuntimeError("freeze manifest records that TEST was already opened; refusing a second read")
    prior_winner_path = root / "winner_selection.json"
    prior_winner_selection = (
        json.loads(prior_winner_path.read_text(encoding="utf-8"))
        if prior_freeze and prior_winner_path.is_file()
        else None
    )
    if prior_freeze:
        candidate_results_path = root / "candidate_results.json"
        if not prior_winner_selection or not candidate_results_path.is_file():
            raise RuntimeError("frozen run is missing its candidate selection or candidate results")
        if _sha256(candidate_results_path) != prior_freeze.get("candidate_results_sha256"):
            raise RuntimeError("candidate results changed after the persisted freeze point")
        if prior_winner_selection.get("candidate_id") != prior_freeze.get("winner_candidate_id"):
            raise RuntimeError("saved candidate selection differs from the persisted freeze point")

    dataset_validation = validate_dataset_manifest(dataset_root, root)
    if dataset_validation["status"] != "PASS":
        raise ValueError("dataset manifest validation did not pass")

    live_probe_path = root / "live_forecast_probe.json"
    if not live_probe_path.is_file():
        raise FileNotFoundError("fresh live_forecast_probe.json is required before T2H candidate search")
    source_alignment = write_source_alignment(root, live_probe_path)
    distribution_comparison = write_distribution_comparison(dataset_root, live_probe_path, root)
    if source_alignment["status"] != "PASS_WITH_LIMITATIONS":
        raise ValueError("source alignment report did not preserve its required limitations")
    if distribution_comparison["status"] != "DESCRIPTIVE_ONLY":
        raise ValueError("live distribution comparison did not pass its descriptive-only contract")

    baseline = write_persistence_validation(dataset_root, root)
    train, feature_names = load_split_arrays(dataset_root, "TRAIN")
    validation, validation_feature_names = load_split_arrays(dataset_root, "VALIDATION")
    if feature_names != validation_feature_names or feature_names != list(MODEL_FEATURE_COLUMNS):
        raise ValueError("TRAIN, VALIDATION, and frozen feature contracts differ")

    device_info = _safe_device_probe()
    device = str(device_info["device"])
    if nthread is None:
        nthread = max(1, min(8, os.cpu_count() or 1))
    candidates = _candidate_configs(device)
    if len(candidates) < 4 or len(candidates) > 6:
        raise AssertionError("the V1.1 candidate family must contain 4-6 candidates")

    candidate_started = time.perf_counter()
    candidate_records = run_candidate_search(
        train,
        validation,
        feature_names,
        device=device,
        checkpoint_directory=str(root),
        nthread=nthread,
        candidate_configs=candidates,
        max_boost_rounds=max_boost_rounds,
        early_stopping_rounds=early_stopping_rounds,
    )
    candidate_elapsed = time.perf_counter() - candidate_started
    candidate_search_elapsed = (
        float(prior_winner_selection.get("candidate_search_elapsed_seconds", candidate_elapsed))
        if prior_winner_selection
        else candidate_elapsed
    )
    successful = [record for record in candidate_records if record.get("status") == "SUCCESS"]
    if len(successful) < 4:
        raise RuntimeError(f"at least four successful candidates are required; got {len(successful)}")

    selected = select_candidate(candidate_records)
    winner_metrics = selected["validation_metrics"]
    baseline_metrics = baseline["metrics"]
    validation_comparison = compare_metrics(baseline_metrics, winner_metrics)
    winner_selection = {
        **selected,
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "feature_list_sha256": json.loads((root / "feature_contract.json").read_text(encoding="utf-8"))["feature_list_sha256"],
        "selection_metric": "validation MAE",
        "selection_reason": "Lowest full VALIDATION MAE among the controlled successful candidates; TEST is not loaded or scored before freeze.",
        "validation_baseline_comparison": validation_comparison,
        "beats_persistence_mae": float(winner_metrics["mae"]) < float(baseline_metrics["mae"]),
        "beats_persistence_rmse": float(winner_metrics["rmse"]) < float(baseline_metrics["rmse"]),
        "candidate_search_elapsed_seconds": candidate_search_elapsed,
        "candidate_search_resumed_from_checkpoint": bool(prior_winner_selection),
        "candidate_search_seconds_this_invocation": candidate_elapsed,
        "successful_candidate_count": len(successful),
        "failed_candidate_count": len(candidate_records) - len(successful),
        "test_read_before_freeze": False,
    }
    if prior_freeze and prior_freeze.get("winner_candidate_id") != selected["candidate_id"]:
        raise RuntimeError("resumed selection differs from the previously frozen winner")
    _atomic_write_json(root / "winner_selection.json", winner_selection)

    freeze_time = prior_freeze["freeze_time"] if prior_freeze else _utc_now()
    freeze_manifest = {
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "feature_list_sha256": winner_selection["feature_list_sha256"],
        "winner_candidate_id": selected["candidate_id"],
        "frozen_parameters": selected["params"],
        "final_rounds": int(selected["best_iteration"]) + 1,
        "validation_selection_metric": "mae",
        "validation_metrics": winner_metrics,
        "configuration_frozen": True,
        "freeze_time": freeze_time,
        "test_first_read_time": None,
        "test_read_count": 0,
        "test_read_before_freeze": False,
        "dataset_root": str(Path(dataset_root)),
        "test_partition": "split=TEST",
        "frozen_before_trainer_test_partition_load": True,
        "candidate_results_sha256": _sha256(root / "candidate_results.json"),
    }
    _atomic_write_json(freeze_path, freeze_manifest)

    validation_sample_x = validation.features[: min(25_000, validation.row_count)].copy()
    validation_sample_y = validation.target[: len(validation_sample_x)].copy()
    final_features = np.concatenate([train.features, validation.features], axis=0)
    final_targets = np.concatenate([train.target, validation.target], axis=0)
    final_training_rows = int(final_targets.size)
    final_training = type(train)(features=final_features, target=final_targets)
    selected_for_final = {**selected, "final_training_rows": final_training_rows}
    del train, validation, final_features, final_targets
    gc.collect()

    final_fit_started = time.perf_counter()
    booster, final_device, fallback_reason, final_fit_seconds = fit_final_model(
        final_training,
        feature_names,
        selected_for_final,
        device=str(selected["device"]),
        nthread=nthread,
    )
    final_fit_elapsed = time.perf_counter() - final_fit_started
    del final_training
    gc.collect()

    booster.save_model(str(saved_model_path))
    model_sha = _sha256(saved_model_path)
    model_size = saved_model_path.stat().st_size

    import xgboost as xgb

    reloaded = xgb.Booster()
    reloaded.load_model(str(saved_model_path))
    feature_count = len(feature_names)
    reload_max_abs_difference = _reload_prediction_difference(
        booster,
        reloaded,
        validation_sample_x,
        feature_names,
        device=final_device,
        best_rounds=int(selected["best_iteration"]) + 1,
    )
    reload_tolerance = 1e-6
    reload_parity = {
        "status": "PASS" if reload_max_abs_difference <= reload_tolerance else "FAIL",
        "split": "VALIDATION",
        "parity_device": final_device,
        "parity_definition": "Both in-memory and JSON-reloaded boosters use the final-fit device before comparing identical VALIDATION rows.",
        "sample_rows": int(len(validation_sample_x)),
        "max_absolute_prediction_difference": reload_max_abs_difference,
        "tolerance": reload_tolerance,
        "feature_count": feature_count,
        "feature_names_match": list(reloaded.feature_names or []) == feature_names,
        "serialized_model_sha256": model_sha,
    }
    _atomic_write_json(root / "reload_parity.json", reload_parity)
    if reload_parity["status"] != "PASS" or not reload_parity["feature_names_match"]:
        raise RuntimeError("serialized XGBoost JSON failed validation-sample reload parity")

    runtime = {
        "run_id": run_id,
        "started_at_utc": run_started_at,
        "completed_at_utc": _utc_now(),
        "versions": _dependency_versions(),
        "device_probe": device_info,
        "actual_candidate_device": selected["device"],
        "actual_final_device": final_device,
        "final_device_fallback_reason": fallback_reason,
        "cpu_count": os.cpu_count(),
        "nthread": nthread,
        "candidate_search_seconds": candidate_elapsed,
        "final_fit_seconds": final_fit_seconds,
        "final_fit_elapsed_seconds_including_data_release": final_fit_elapsed,
        "configured_max_boost_rounds": max_boost_rounds,
        "early_stopping_rounds": early_stopping_rounds,
        "random_seed": RANDOM_SEED,
        "memory_note": "TEST is excluded from all candidate and final-fit data structures until the persisted freeze point.",
    }
    _atomic_write_json(root / "runtime_summary.json", runtime)

    model_manifest = {
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "source_contract_id": SOURCE_CONTRACT_ID,
        "target_column": TARGET_COLUMN,
        "target_offset_seconds": 7200,
        "feature_count": feature_count,
        "feature_list_sha256": winner_selection["feature_list_sha256"],
        "feature_names": feature_names,
        "winner_candidate_id": selected["candidate_id"],
        "parameters": selected["params"],
        "best_iteration_from_validation": selected["best_iteration"],
        "final_boost_rounds": int(selected["best_iteration"]) + 1,
        "final_training_rows_train_plus_validation": final_training_rows,
        "final_fit_seconds": final_fit_seconds,
        "device": final_device,
        "model_file": str(saved_model_path),
        "model_file_size_bytes": model_size,
        "model_sha256": model_sha,
        "serialization_format": "XGBoost JSON",
        "validation_metrics": winner_metrics,
        "test_metrics_path": "test_metrics.json",
        "freeze_time": freeze_time,
        "reload_parity_path": "reload_parity.json",
    }
    model_manifest_validation = validate_model_manifest(model_manifest)
    _atomic_write_json(root / "model_manifest_validation.json", model_manifest_validation)
    if model_manifest_validation["status"] != "PASS":
        raise RuntimeError(f"model manifest validation failed: {model_manifest_validation['errors']}")
    _atomic_write_json(root / "model_manifest.json", model_manifest)

    freeze_manifest.update({
        "model_file": str(saved_model_path),
        "model_file_size_bytes": model_size,
        "model_sha256": model_sha,
        "final_fit_complete": True,
        "reload_parity_status": reload_parity["status"],
    })
    _atomic_write_json(freeze_path, freeze_manifest)

    # This timestamp is written immediately before the sole TEST data load.
    test_first_read_time = _utc_now()
    if test_first_read_time <= freeze_time:
        raise RuntimeError("freeze_time must be earlier than TEST_first_read_time")
    freeze_manifest.update({
        "test_first_read_time": test_first_read_time,
        "test_read_count": 1,
        "test_read_action": "load_split_arrays(dataset_root, 'TEST')",
        "test_first_read_after_freeze": True,
    })
    _atomic_write_json(freeze_path, freeze_manifest)
    test, test_feature_names = load_split_arrays(dataset_root, "TEST")
    if test_feature_names != feature_names:
        raise ValueError("TEST feature list differs from the frozen model")
    if test.row_count != len(test.location_id) or test.row_count != len(test.event_time) or test.row_count != len(test.target_time):
        raise ValueError("TEST metadata arrays are not row aligned")
    if not np.all((pd.to_datetime(test.target_time, utc=True) - pd.to_datetime(test.event_time, utc=True)).total_seconds() == 7200):
        raise ValueError("TEST target_time is not exactly feature_time + 2h")

    dtest = xgb.DMatrix(test.features, feature_names=feature_names, nthread=nthread)
    xgb_predictions = reloaded.predict(dtest, iteration_range=(0, int(selected["best_iteration"]) + 1))
    persistence = persistence_predictions(test.features, feature_names)
    xgb_metrics = regression_metrics(test.target, xgb_predictions)
    persistence_metrics = regression_metrics(test.target, persistence)
    improvement = compare_metrics(persistence_metrics, xgb_metrics)
    test_result = {
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "split": "TEST",
        "test_read_count": 1,
        "test_first_read_time": test_first_read_time,
        "freeze_time": freeze_time,
        "freeze_before_test_read": freeze_time < test_first_read_time,
        "rows": test.row_count,
        "locations": int(len(set(test.location_id.astype(str)))),
        "xgboost": {**xgb_metrics, "bias": xgb_metrics["mean_error"]},
        "persistence": {**persistence_metrics, "bias": persistence_metrics["mean_error"]},
        "improvement_vs_persistence": {
            **improvement,
            "mae_skill": 1.0 - float(xgb_metrics["mae"]) / float(persistence_metrics["mae"])
            if float(persistence_metrics["mae"]) != 0
            else None,
        },
        "beats_persistence_mae": float(xgb_metrics["mae"]) < float(persistence_metrics["mae"]),
        "beats_persistence_rmse": float(xgb_metrics["rmse"]) < float(persistence_metrics["rmse"]),
        "model_sha256": model_sha,
        "feature_list_sha256": winner_selection["feature_list_sha256"],
    }
    _atomic_write_json(test_metrics_path, test_result)

    location_results, location_counts = _test_metrics_by_location(
        test.location_id,
        test.target,
        xgb_predictions,
        persistence,
    )
    _dump_csv(root / "per_location_metrics.csv", location_results)
    _atomic_write_json(root / "per_location_metrics.json", {
        "split": "TEST",
        "location_count": len(location_results),
        **location_counts,
        "locations": location_results,
    })
    _atomic_write_json(root / "temporal_diagnostics.json", _temporal_diagnostics(
        test.event_time,
        test.target,
        xgb_predictions,
        persistence,
    ))
    predictions_path = _write_test_predictions(
        root,
        test.location_id,
        test.event_time,
        test.target_time,
        test.target,
        xgb_predictions,
        persistence,
    )

    feature_importance = feature_importance_records(reloaded, feature_names)
    _atomic_write_json(root / "feature_importance.json", {
        "model_id": MODEL_ID,
        "split_used_for_model_fit": "TRAIN+VALIDATION",
        "test_used_for_feature_importance": False,
        "features": feature_importance,
    })

    freeze_manifest.update({
        "test_metrics_written": True,
        "test_predictions_path": predictions_path.name,
        "test_evaluation_complete_at_utc": _utc_now(),
    })
    _atomic_write_json(freeze_path, freeze_manifest)
    return test_result


def write_checksums(artifact_root: str | Path, model_path: str | Path) -> dict[str, Any]:
    root = Path(artifact_root)
    model = Path(model_path)
    model_resolved = model.resolve()
    files = []
    for path in sorted(root.rglob("*")):
        if (
            path.is_file()
            and path.name != "checksums.json"
            and not path.name.endswith(".tmp")
            and path.resolve() != model_resolved
        ):
            files.append({
                "path": path.relative_to(root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            })
    if model.is_file():
        try:
            model_report_path = model.relative_to(root).as_posix()
        except ValueError:
            model_report_path = str(model)
        files.append({"path": model_report_path, "bytes": model.stat().st_size, "sha256": _sha256(model), "kind": "model_artifact"})
    report = {"algorithm": "SHA-256", "files": files}
    _atomic_write_json(root / "checksums.json", report)
    return report


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-root", type=Path, default=DATASET_PATH)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--model-path", type=Path)
    parser.add_argument("--nthread", type=int)
    parser.add_argument("--max-boost-rounds", type=int, default=1500)
    parser.add_argument("--early-stopping-rounds", type=int, default=80)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    result = train_and_evaluate(
        dataset_root=args.dataset_root,
        artifact_root=args.artifact_root,
        model_path=args.model_path,
        nthread=args.nthread,
        max_boost_rounds=args.max_boost_rounds,
        early_stopping_rounds=args.early_stopping_rounds,
    )
    final_model = args.model_path or args.artifact_root / f"weather_forecast_xgboost_t2h_v1_1_{args.artifact_root.name}.json"
    checksums = write_checksums(args.artifact_root, final_model)
    print(json.dumps({"test_metrics": result, "checksums_file_count": len(checksums["files"])}, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by notebook/script entry point
    raise SystemExit(main())
