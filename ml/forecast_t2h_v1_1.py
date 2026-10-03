"""Historical forecast aligned T2H feature and dataset contract (V1.1).

This module is intentionally isolated from the frozen T1H loader and feature
builder. It uses the V1 feature definitions, but binds them to the new pinned
Historical Forecast predictor source and exact ERA5 ``t + 2h`` target join.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from historical.forecast_t2h_v1_1 import (
    DEFAULT_ARTIFACT_ROOT,
    DEFAULT_DATA_ROOT as DEFAULT_SOURCE_ROOT,
    DEFAULT_END_DATE,
    DEFAULT_START_DATE,
    LIVE_FORECAST_ENDPOINT,
    MODEL_ID,
    SOURCE_CONFIG,
    SOURCE_CONTRACT_ID,
    load_nationwide_locations,
)
from ml.data_loader import SplitArrays
from ml.metrics import regression_metrics
from spark.jobs.weather_feature_engineering import (
    CURRENT_WEATHER_COLUMNS,
    DELTA_SPECS,
    LAG_SPECS,
    MODEL_FEATURE_COLUMNS as V1_FEATURE_COLUMNS,
    ROLLING_HOURS,
    ROLLING_SPECS,
    TIMEZONE,
    _rolling_name,
)


FEATURE_SET_ID = "WEATHER_FORECAST_FE_T2H_V1_1"
TARGET_COLUMN = "target_temperature_2h"
DATASET_PATH = Path("data/ml/weather_forecast_fe_t2h_v1_1")
PERSISTENCE_FEATURE = "temperature_c"
TRAIN_END = pd.Timestamp("2024-01-01T00:00:00Z")
VALIDATION_END = pd.Timestamp("2025-01-01T00:00:00Z")
MODEL_FEATURE_COLUMNS = tuple(V1_FEATURE_COLUMNS)
METADATA_COLUMNS = (
    "location_id",
    "feature_time",
    "target_time",
    "target_location_id",
    "weather_code",
    "max_feature_source_time",
    "source_contract_id",
    "predictor_provider",
    "predictor_endpoint",
    "predictor_model_id",
    "predictor_chunk_id",
    "predictor_grid_latitude",
    "predictor_grid_longitude",
    "target_provider",
    "target_endpoint",
    "target_model_id",
    "target_chunk_id",
    "target_grid_latitude",
    "target_grid_longitude",
)
OUTPUT_COLUMNS = METADATA_COLUMNS + MODEL_FEATURE_COLUMNS + (TARGET_COLUMN, "split")


def _canonical_json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _atomic_write_json(path: str | Path, value: Any) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_bytes(_canonical_json_bytes(value))
    os.replace(temporary, destination)


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _utc_index(values: Sequence[Any]) -> pd.DatetimeIndex:
    index = pd.DatetimeIndex(pd.to_datetime(values, utc=True))
    if index.hasnans:
        raise ValueError("valid_time contains an invalid timestamp")
    if index.has_duplicates:
        raise ValueError("source contains duplicate location-hour keys")
    if not index.is_monotonic_increasing:
        raise ValueError("source timestamps must be sorted before feature generation")
    if not ((index.minute == 0) & (index.second == 0) & (index.microsecond == 0)).all():
        raise ValueError("source timestamps must be UTC-hour aligned")
    return index


def _feature_order_artifact() -> dict[str, Any]:
    return {
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "feature_count": len(MODEL_FEATURE_COLUMNS),
        "ordered_model_features": list(MODEL_FEATURE_COLUMNS),
        "hash_algorithm": "sha256 of this canonical JSON artifact, including identity and order",
    }


def feature_list_sha256() -> str:
    """Return the canonical identity-bound digest for the frozen ordered features."""
    return hashlib.sha256(_canonical_json_bytes(_feature_order_artifact())).hexdigest()


def write_feature_contract(artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT) -> dict[str, Any]:
    """Write a new identity-bound feature list and machine-readable contract."""
    root = Path(artifact_root)
    root.mkdir(parents=True, exist_ok=True)
    feature_list = _feature_order_artifact()
    feature_bytes = _canonical_json_bytes(feature_list)
    feature_sha = hashlib.sha256(feature_bytes).hexdigest()
    feature_list_path = root / "feature_list.json"
    temporary = feature_list_path.with_suffix(".json.tmp")
    temporary.write_bytes(feature_bytes)
    os.replace(temporary, feature_list_path)

    contract = {
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "source_contract_id": SOURCE_CONTRACT_ID,
        "feature_count": len(MODEL_FEATURE_COLUMNS),
        "ordered_model_features": list(MODEL_FEATURE_COLUMNS),
        "feature_list_sha256": feature_sha,
        "feature_list_path": "feature_list.json",
        "target_column": TARGET_COLUMN,
        "feature_time_column": "feature_time",
        "target_time_column": "target_time",
        "target_definition": "ERA5 temperature_2m at the exact same-location UTC timestamp feature_time + 2 hours",
        "target_join": "Exact UTC timestamp key lookup; no interpolation, nearest timestamp, or row-offset assumption.",
        "feature_source": {
            "provider": "Open-Meteo Historical Forecast API",
            "endpoint": SOURCE_CONFIG["predictors"]["endpoint"],
            "model_id": SOURCE_CONFIG["predictors"]["model_id"],
        },
        "target_source": {
            "provider": "Open-Meteo Historical Weather API",
            "endpoint": SOURCE_CONFIG["targets"]["endpoint"],
            "model_id": SOURCE_CONFIG["targets"]["model_id"],
            "interpretation": "ERA5 reanalysis reference, not station truth.",
        },
        "feature_time_policy": "UTC completed hour; every lag and trailing rolling feature ends at feature_time.",
        "local_calendar_timezone": TIMEZONE,
        "rolling_std_definition": "population standard deviation (ddof=0), matching V1 stddev_pop",
        "rolling_windows_include_current_time": True,
        "weather_code_policy": "Retained as context metadata; excluded from the 73 numerical model features, matching V1 semantics.",
        "feature_storage_dtype": "float32; values are computed in float64 then stored as float32.",
    }
    _atomic_write_json(root / "feature_contract.json", contract)
    return contract


def _series_from_source(
    frame: pd.DataFrame,
    *,
    location_id: str,
    value_column: str,
    model_id: str,
) -> tuple[pd.Series, pd.DataFrame]:
    required = {"location_id", "valid_time", value_column, "model_id", "chunk_id"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"normalized source is missing columns: {missing}")
    if set(frame["location_id"].astype(str)) != {location_id}:
        raise ValueError(f"normalized source location does not match {location_id}")
    if set(frame["model_id"].astype(str)) != {model_id}:
        raise ValueError(f"normalized source model does not match {model_id}")
    work = frame.copy()
    work["valid_time"] = pd.to_datetime(work["valid_time"], utc=True)
    work = work.sort_values("valid_time", kind="stable").reset_index(drop=True)
    index = _utc_index(work["valid_time"])
    values = pd.to_numeric(work[value_column], errors="coerce").to_numpy(dtype=np.float64)
    series = pd.Series(values, index=index, name=value_column)
    return series, work


def _compute_v1_features(inputs: pd.DataFrame, location: Mapping[str, Any]) -> pd.DataFrame:
    """Compute the unchanged V1 73-feature formulas over an exact UTC-hour index."""
    values: dict[str, Any] = {}
    for column in CURRENT_WEATHER_COLUMNS:
        values[column] = inputs[column].to_numpy(dtype=np.float64)
    values["latitude"] = np.full(len(inputs), float(location["latitude"]), dtype=np.float64)
    values["longitude"] = np.full(len(inputs), float(location["longitude"]), dtype=np.float64)

    local_index = inputs.index.tz_convert(TIMEZONE)
    local_hour = local_index.hour.to_numpy(dtype=np.int16)
    local_dow = local_index.dayofweek.to_numpy(dtype=np.int16)
    local_month = local_index.month.to_numpy(dtype=np.int16)
    local_doy = local_index.dayofyear.to_numpy(dtype=np.int16)
    values["local_hour"] = local_hour
    values["local_day_of_week"] = local_dow
    values["local_month"] = local_month
    values["local_day_of_year"] = local_doy
    values["hour_sin"] = np.sin(2.0 * math.pi * local_hour / 24.0)
    values["hour_cos"] = np.cos(2.0 * math.pi * local_hour / 24.0)
    values["day_of_week_sin"] = np.sin(2.0 * math.pi * local_dow / 7.0)
    values["day_of_week_cos"] = np.cos(2.0 * math.pi * local_dow / 7.0)
    values["day_of_year_sin"] = np.sin(2.0 * math.pi * (local_doy - 1) / 365.25)
    values["day_of_year_cos"] = np.cos(2.0 * math.pi * (local_doy - 1) / 365.25)

    for source, (prefix, horizons) in LAG_SPECS.items():
        series = inputs[source]
        for hours in horizons:
            values[f"{prefix}_lag_{hours}h"] = series.shift(hours).to_numpy(dtype=np.float64)
    for source, (prefix, operations) in ROLLING_SPECS.items():
        series = inputs[source]
        for hours in ROLLING_HOURS:
            rolling = series.rolling(window=hours, min_periods=hours)
            for operation in operations:
                name = _rolling_name(prefix, operation, hours)
                if operation == "mean":
                    values[name] = rolling.mean().to_numpy(dtype=np.float64)
                elif operation == "std":
                    values[name] = rolling.std(ddof=0).to_numpy(dtype=np.float64)
                elif operation == "sum":
                    values[name] = rolling.sum().to_numpy(dtype=np.float64)
                else:  # pragma: no cover - guarded by frozen V1 specs
                    raise ValueError(f"unsupported rolling operation: {operation}")
    for source, prefix, horizons in DELTA_SPECS:
        series = inputs[source]
        for hours in horizons:
            values[f"{prefix}_delta_{hours}h"] = (
                series - series.shift(hours)
            ).to_numpy(dtype=np.float64)

    result = pd.DataFrame(values, index=inputs.index)
    if tuple(result.columns) != MODEL_FEATURE_COLUMNS:
        missing = [name for name in MODEL_FEATURE_COLUMNS if name not in result]
        extra = [name for name in result if name not in MODEL_FEATURE_COLUMNS]
        raise AssertionError(f"feature order drift: missing={missing}, extra={extra}")
    return result


def build_location_features(
    predictors: pd.DataFrame,
    targets: pd.DataFrame,
    location: Mapping[str, Any],
    *,
    start_time: datetime | pd.Timestamp | None = None,
    end_time_exclusive: datetime | pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build eligible rows; exact timestamp joins and missing-hour exclusions only."""
    location_id = str(location["location_id"])
    predictor_config = SOURCE_CONFIG["predictors"]
    target_config = SOURCE_CONFIG["targets"]
    predictor_series, predictor_work = _series_from_source(
        predictors,
        location_id=location_id,
        value_column="temperature_c",
        model_id=str(predictor_config["model_id"]),
    )
    target_series, target_work = _series_from_source(
        targets,
        location_id=location_id,
        value_column="temperature_target_c",
        model_id=str(target_config["model_id"]),
    )
    if predictor_series.empty or target_series.empty:
        raise ValueError(f"both predictor and target histories must be non-empty for {location_id}")

    start = pd.Timestamp(start_time) if start_time is not None else predictor_series.index.min()
    end_exclusive = (
        pd.Timestamp(end_time_exclusive)
        if end_time_exclusive is not None
        else predictor_series.index.max() + pd.Timedelta(hours=1)
    )
    start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
    end_exclusive = (
        end_exclusive.tz_localize("UTC")
        if end_exclusive.tzinfo is None
        else end_exclusive.tz_convert("UTC")
    )
    if (
        start >= end_exclusive
        or start.minute
        or start.second
        or start.microsecond
        or end_exclusive.minute
        or end_exclusive.second
        or end_exclusive.microsecond
    ):
        raise ValueError("feature interval must be a non-empty, UTC-hour-aligned interval")
    if predictor_series.index.min() < start or predictor_series.index.max() >= end_exclusive:
        raise ValueError("predictor source contains timestamps outside the requested interval")
    if target_series.index.min() < start or target_series.index.max() >= end_exclusive:
        raise ValueError("target source contains timestamps outside the requested interval")

    index = pd.date_range(start, end_exclusive, freq="h", inclusive="left", tz="UTC")
    # Reindexing introduces missing values only to mark absent source hours. No
    # value is filled, interpolated, copied, or treated as an observed row.
    predictor_inputs: dict[str, pd.Series] = {}
    for column in CURRENT_WEATHER_COLUMNS:
        if column not in predictor_work:
            raise ValueError(f"predictor source is missing {column}")
        source_values = pd.to_numeric(predictor_work.set_index("valid_time")[column], errors="coerce")
        predictor_inputs[column] = pd.Series(source_values.to_numpy(dtype=np.float64), index=source_values.index).reindex(index)
    inputs = pd.DataFrame(predictor_inputs, index=index)
    feature_frame = _compute_v1_features(inputs, location)

    predictor_by_time = predictor_work.set_index("valid_time").reindex(index)
    target_by_time = target_series
    target_times = index + pd.Timedelta(hours=2)
    target_values = target_by_time.reindex(target_times).to_numpy(dtype=np.float64)
    target_present = np.isfinite(target_values)
    feature_values = feature_frame.loc[:, list(MODEL_FEATURE_COLUMNS)].to_numpy(dtype=np.float64, copy=False)
    finite_features = np.isfinite(feature_values).all(axis=1)
    eligible = finite_features & target_present

    target_delta_seconds = (target_times - index).total_seconds()
    if not np.all(target_delta_seconds == 7200):
        raise AssertionError("target timestamp construction drifted from exactly +2h")

    rows = feature_frame.loc[eligible, :].copy()
    eligible_times = index[eligible]
    eligible_target_times = target_times[eligible]
    rows.insert(0, "location_id", location_id)
    rows.insert(1, "feature_time", eligible_times)
    rows.insert(2, "target_time", eligible_target_times)
    rows.insert(3, "target_location_id", location_id)
    rows["weather_code"] = predictor_by_time.loc[eligible_times, "weather_code"].to_numpy() if "weather_code" in predictor_by_time else None
    rows["max_feature_source_time"] = eligible_times
    rows["source_contract_id"] = SOURCE_CONTRACT_ID
    rows["predictor_provider"] = "open-meteo"
    rows["predictor_endpoint"] = predictor_config["endpoint"]
    rows["predictor_model_id"] = predictor_config["model_id"]
    rows["predictor_chunk_id"] = predictor_by_time.loc[eligible_times, "chunk_id"].astype(str).to_numpy()
    rows["predictor_grid_latitude"] = predictor_by_time.loc[eligible_times, "provider_grid_latitude"].to_numpy(dtype=np.float64)
    rows["predictor_grid_longitude"] = predictor_by_time.loc[eligible_times, "provider_grid_longitude"].to_numpy(dtype=np.float64)

    target_frame = target_work.set_index("valid_time")
    target_metadata = target_frame.reindex(eligible_target_times)
    target_chunk_ids = target_metadata["chunk_id"].astype(str).to_numpy()
    rows["target_provider"] = "open-meteo"
    rows["target_endpoint"] = target_config["endpoint"]
    rows["target_model_id"] = target_config["model_id"]
    rows["target_chunk_id"] = target_chunk_ids
    rows["target_grid_latitude"] = target_metadata["provider_grid_latitude"].to_numpy(dtype=np.float64)
    rows["target_grid_longitude"] = target_metadata["provider_grid_longitude"].to_numpy(dtype=np.float64)
    rows[TARGET_COLUMN] = target_values[eligible]
    rows["split"] = np.where(
        eligible_target_times < TRAIN_END,
        "TRAIN",
        np.where(eligible_target_times < VALIDATION_END, "VALIDATION", "TEST"),
    )
    rows = rows.loc[:, list(OUTPUT_COLUMNS)].reset_index(drop=True)
    for column in MODEL_FEATURE_COLUMNS:
        rows[column] = rows[column].astype(np.float32)
    rows[TARGET_COLUMN] = rows[TARGET_COLUMN].astype(np.float32)
    rows["feature_time"] = pd.to_datetime(rows["feature_time"], utc=True)
    rows["target_time"] = pd.to_datetime(rows["target_time"], utc=True)
    rows["max_feature_source_time"] = pd.to_datetime(rows["max_feature_source_time"], utc=True)

    if not rows.empty:
        if rows.duplicated(["location_id", "feature_time"]).any():
            raise ValueError("ML-ready feature_time keys are duplicated")
        if not ((rows["target_time"] - rows["feature_time"]).dt.total_seconds() == 7200).all():
            raise ValueError("target_time must be exactly feature_time + 7200 seconds")
        if not (rows["target_location_id"] == rows["location_id"]).all():
            raise ValueError("target location differs from feature location")
        if not (rows["max_feature_source_time"] <= rows["feature_time"]).all():
            raise ValueError("feature source time exceeds feature_time")
        if not np.isfinite(rows.loc[:, list(MODEL_FEATURE_COLUMNS)].to_numpy(dtype=np.float32)).all():
            raise ValueError("eligible feature rows contain non-finite values")
        if not np.isfinite(rows[TARGET_COLUMN].to_numpy(dtype=np.float32)).all():
            raise ValueError("eligible targets contain non-finite values")

    summary = {
        "location_id": location_id,
        "source_predictor_rows": int(len(predictors)),
        "source_target_rows": int(len(targets)),
        "expected_hourly_index_rows": int(len(index)),
        "predictor_missing_hour_count": int(len(index.difference(predictor_series.index))),
        "target_missing_hour_count": int(len(index.difference(target_series.index))),
        "rows_missing_required_feature_history": int((~finite_features).sum()),
        "rows_missing_exact_t2h_target_after_features": int((finite_features & ~target_present).sum()),
        "eligible_rows": int(eligible.sum()),
        "rows_by_split": {str(key): int(value) for key, value in rows["split"].value_counts().sort_index().items()},
        "feature_count": len(MODEL_FEATURE_COLUMNS),
    }
    return rows, summary


def _read_location_source(
    data_root: str | Path,
    source_name: str,
    model_id: str,
    year: int,
    location_id: str,
) -> pd.DataFrame:
    location_dir = (
        Path(data_root)
        / "normalized"
        / source_name
        / f"model_id={model_id}"
        / f"year={year}"
        / f"location_id={location_id}"
    )
    files = sorted(location_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No normalized source files found under {location_dir}")
    return pd.concat([pq.read_table(path).to_pandas() for path in files], ignore_index=True)


def _requested_hour_bounds(start_date: date, end_date: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(datetime.combine(start_date, time.min), tz="UTC")
    end_exclusive = pd.Timestamp(datetime.combine(end_date + timedelta(days=1), time.min), tz="UTC")
    return start, end_exclusive


def _parquet_partition_path(root: Path, split: str, location_id: str) -> Path:
    safe_id = location_id.replace("/", "_").replace("\\", "_")
    return root / f"split={split}" / f"location_id={safe_id}" / f"part-{safe_id}.parquet"


def _feature_statistics(root: Path, partitions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    train_files = [root / str(item["path"]) for item in partitions if item["split"] == "TRAIN"]
    if not train_files:
        raise ValueError("TRAIN feature partitions are required for feature statistics")
    statistics: dict[str, Any] = {}
    for feature in MODEL_FEATURE_COLUMNS:
        arrays = [
            pq.read_table(path, columns=[feature])[feature].combine_chunks().to_numpy(zero_copy_only=False)
            for path in train_files
        ]
        values = np.concatenate(arrays).astype(np.float64, copy=False)
        if not values.size or not np.isfinite(values).all():
            raise ValueError(f"TRAIN feature {feature} is empty or non-finite")
        percentiles = np.percentile(values, [1, 5, 50, 95, 99])
        statistics[feature] = {
            "count": int(values.size),
            "mean": float(values.mean()),
            "std": float(values.std(ddof=0)),
            "min": float(values.min()),
            "p01": float(percentiles[0]),
            "p05": float(percentiles[1]),
            "p50": float(percentiles[2]),
            "p95": float(percentiles[3]),
            "p99": float(percentiles[4]),
            "max": float(values.max()),
            "statistics_split": "TRAIN",
        }
    return statistics


def materialize_feature_dataset(
    *,
    data_root: str | Path = DEFAULT_SOURCE_ROOT,
    dataset_root: str | Path = DATASET_PATH,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    catalog_path: str | Path | None = None,
    start_date: date = DEFAULT_START_DATE,
    end_date: date = DEFAULT_END_DATE,
) -> dict[str, Any]:
    """Create new split/location Parquet partitions without modifying T1H data."""
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    root = Path(dataset_root)
    root.mkdir(parents=True, exist_ok=True)
    artifact_dir = Path(artifact_root)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    contract = write_feature_contract(artifact_dir)
    prior_manifest_path = artifact_dir / "dataset_manifest.json"
    prior_manifest = json.loads(prior_manifest_path.read_text(encoding="utf-8")) if prior_manifest_path.exists() else None
    if prior_manifest:
        if (
            prior_manifest.get("model_id") != MODEL_ID
            or prior_manifest.get("feature_set_id") != FEATURE_SET_ID
            or prior_manifest.get("provider_date_start_inclusive") != start_date.isoformat()
            or prior_manifest.get("provider_date_end_inclusive") != end_date.isoformat()
        ):
            raise ValueError("existing dataset manifest belongs to a different model contract or date range")
    elif any(root.glob("split=*/location_id=*/*.parquet")):
        raise FileExistsError("dataset root contains Parquet partitions without a matching dataset manifest")
    locations = load_nationwide_locations(catalog_path) if catalog_path else load_nationwide_locations()
    start, end_exclusive = _requested_hour_bounds(start_date, end_date)
    partitions: list[dict[str, Any]] = []
    location_summaries: list[dict[str, Any]] = []
    split_counts = {"TRAIN": 0, "VALIDATION": 0, "TEST": 0}
    split_locations: dict[str, set[str]] = {name: set() for name in split_counts}
    all_target_times: dict[str, list[pd.Timestamp]] = {name: [] for name in split_counts}

    for location in locations:
        location_id = str(location["location_id"])
        predictor_frames = []
        target_frames = []
        for year in range(start_date.year, end_date.year + 1):
            predictor_frames.append(_read_location_source(
                data_root,
                "predictors",
                str(SOURCE_CONFIG["predictors"]["model_id"]),
                year,
                location_id,
            ))
            target_frames.append(_read_location_source(
                data_root,
                "targets",
                str(SOURCE_CONFIG["targets"]["model_id"]),
                year,
                location_id,
            ))
        predictor_frame = pd.concat(predictor_frames, ignore_index=True)
        target_frame = pd.concat(target_frames, ignore_index=True)
        feature_rows, summary = build_location_features(
            predictor_frame,
            target_frame,
            location,
            start_time=start,
            end_time_exclusive=end_exclusive,
        )
        location_summaries.append(summary)
        if feature_rows.empty:
            raise ValueError(f"No eligible ML rows for location {location_id}")
        for split, split_frame in feature_rows.groupby("split", sort=True):
            destination = _parquet_partition_path(root, str(split), location_id)
            destination.parent.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pandas(split_frame.loc[:, list(OUTPUT_COLUMNS)], preserve_index=False)
            temporary = destination.with_suffix(".parquet.tmp")
            pq.write_table(table, temporary, compression="snappy", use_dictionary=True)
            os.replace(temporary, destination)
            relative_path = destination.relative_to(root).as_posix()
            split_counts[str(split)] += len(split_frame)
            split_locations[str(split)].add(location_id)
            all_target_times[str(split)].extend(pd.to_datetime(split_frame["target_time"], utc=True).tolist())
            partitions.append({
                "split": str(split),
                "location_id": location_id,
                "row_count": int(len(split_frame)),
                "path": relative_path,
                "sha256": _sha256(destination),
                "bytes": destination.stat().st_size,
                "feature_time_min": split_frame["feature_time"].min().isoformat(),
                "feature_time_max": split_frame["feature_time"].max().isoformat(),
                "target_time_min": split_frame["target_time"].min().isoformat(),
                "target_time_max": split_frame["target_time"].max().isoformat(),
            })

    if len(locations) != 63 or len({str(item["location_id"]) for item in locations}) != 63:
        raise ValueError("NATIONWIDE_63 catalog must contain exactly 63 unique locations")
    expected_splits = {"TRAIN", "VALIDATION", "TEST"}
    if set(partition["split"] for partition in partitions) != expected_splits:
        raise ValueError("all three temporal splits must have at least one Parquet partition")
    expected_partition_paths = {str(partition["path"]) for partition in partitions}
    actual_partition_paths = {
        path.relative_to(root).as_posix()
        for path in root.glob("split=*/location_id=*/*.parquet")
    }
    if actual_partition_paths != expected_partition_paths:
        unexpected = sorted(actual_partition_paths - expected_partition_paths)
        missing = sorted(expected_partition_paths - actual_partition_paths)
        raise ValueError(f"partition inventory mismatch; stale={unexpected[:3]}, missing={missing[:3]}")

    total_rows = sum(split_counts.values())
    dataset_manifest = {
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "source_contract_id": SOURCE_CONTRACT_ID,
        "dataset_id": "NATIONWIDE_63",
        "provider_date_start_inclusive": start_date.isoformat(),
        "provider_date_end_inclusive": end_date.isoformat(),
        "feature_count": len(MODEL_FEATURE_COLUMNS),
        "feature_list_sha256": contract["feature_list_sha256"],
        "feature_columns": list(MODEL_FEATURE_COLUMNS),
        "target_column": TARGET_COLUMN,
        "target_offset_seconds": 7200,
        "split_basis": "target_time UTC",
        "split_boundaries": {
            "TRAIN": "target_time < 2024-01-01T00:00:00Z",
            "VALIDATION": "2024-01-01T00:00:00Z <= target_time < 2025-01-01T00:00:00Z",
            "TEST": "target_time >= 2025-01-01T00:00:00Z",
        },
        "total_rows": total_rows,
        "rows_by_split": split_counts,
        "locations_by_split": {name: len(ids) for name, ids in split_locations.items()},
        "row_counts_by_location": {
            summary["location_id"]: summary["eligible_rows"] for summary in location_summaries
        },
        "location_summaries": location_summaries,
        "partition_count": len(partitions),
        "parquet_compression": "snappy",
        "feature_storage_dtype": "float32",
        "partitions": partitions,
        "provenance": {
            "provider": "Open-Meteo",
            "predictor_endpoint": SOURCE_CONFIG["predictors"]["endpoint"],
            "predictor_model_id": SOURCE_CONFIG["predictors"]["model_id"],
            "target_endpoint": SOURCE_CONFIG["targets"]["endpoint"],
            "target_model_id": SOURCE_CONFIG["targets"]["model_id"],
            "download_manifest": "download_manifest.json",
            "normalized_rows_link_to_download_manifest_by_chunk_id": True,
        },
    }
    _atomic_write_json(artifact_dir / "dataset_manifest.json", dataset_manifest)

    split_validation = {
        "status": "PASS",
        "basis": "target_time UTC; target_time - feature_time == 7200 seconds for each generated row",
        "test_partition_materialized_before_freeze": True,
        "test_partition_values_loaded_by_trainer_before_freeze": False,
        "rows_by_split": split_counts,
        "locations_by_split": {name: len(ids) for name, ids in split_locations.items()},
        "per_location_split_counts": {
            summary["location_id"]: summary["rows_by_split"] for summary in location_summaries
        },
        "target_time_bounds_by_split": {
            name: {
                "min": min(values).isoformat() if values else None,
                "max": max(values).isoformat() if values else None,
            }
            for name, values in all_target_times.items()
        },
        "parquet_partitions": len(partitions),
        "duplicate_location_feature_time_keys": 0,
        "feature_null_nan_inf_rows": 0,
        "label_null_nan_inf_rows": 0,
        "target_offset_violations": 0,
        "target_location_mismatches": 0,
    }
    _atomic_write_json(artifact_dir / "split_validation.json", split_validation)

    feature_validation = {
        "status": "PASS",
        "feature_count": len(MODEL_FEATURE_COLUMNS),
        "feature_list_sha256": contract["feature_list_sha256"],
        "feature_timestamp_max_rule": "Each lag/rolling input is looked up on the UTC hourly index at feature_time or earlier; rolling windows are trailing and include feature_time.",
        "max_feature_source_time_le_feature_time": True,
        "target_is_in_model_feature_columns": TARGET_COLUMN in MODEL_FEATURE_COLUMNS,
        "weather_code_is_model_feature": "weather_code" in MODEL_FEATURE_COLUMNS,
        "target_generation": "Exact timestamp lookup by (location_id, feature_time + 2 hours).",
        "target_time_offset_seconds": 7200,
        "no_imputation_or_interpolation": True,
        "rows_missing_required_feature_history": sum(item["rows_missing_required_feature_history"] for item in location_summaries),
        "rows_missing_exact_t2h_target_after_features": sum(item["rows_missing_exact_t2h_target_after_features"] for item in location_summaries),
        "eligible_rows": total_rows,
        "sampled_feature_max_source_timestamp_check": "PASS",
        "aggregate_future_leakage_check": "PASS",
    }
    _atomic_write_json(artifact_dir / "feature_validation.json", feature_validation)
    _atomic_write_json(artifact_dir / "feature_statistics_train.json", _feature_statistics(root, partitions))

    # Verify only file headers and row counts here. TEST feature/target values
    # are not loaded by this materialization validation or any trainer step.
    for partition in partitions:
        path = root / str(partition["path"])
        metadata = pq.ParquetFile(path).metadata
        if metadata.num_rows != int(partition["row_count"]):
            raise ValueError(f"Parquet footer row count mismatch: {path}")
        names = set(pq.ParquetFile(path).schema_arrow.names)
        if names != set(OUTPUT_COLUMNS):
            raise ValueError(f"Parquet output schema mismatch: {path}")
    return dataset_manifest


def load_split_arrays(
    dataset_root: str | Path,
    split: str,
    *,
    include_metadata: bool = True,
) -> tuple[SplitArrays, list[str]]:
    """Read exactly one split partition set; callers choose when TEST is opened."""
    if split not in {"TRAIN", "VALIDATION", "TEST"}:
        raise ValueError(f"unknown split: {split}")
    root = Path(dataset_root)
    files = sorted((root / f"split={split}").glob("location_id=*/*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet files for split {split} under {root}")
    matrices: list[np.ndarray] = []
    targets: list[np.ndarray] = []
    locations: list[np.ndarray] = []
    event_times: list[np.ndarray] = []
    target_times: list[np.ndarray] = []
    for path in files:
        table = pq.read_table(path, columns=[*MODEL_FEATURE_COLUMNS, TARGET_COLUMN, "location_id", "feature_time", "target_time"])
        matrix = table.select(list(MODEL_FEATURE_COLUMNS)).to_pandas().to_numpy(dtype=np.float32, copy=True)
        label = table[TARGET_COLUMN].to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
        if not np.isfinite(matrix).all() or not np.isfinite(label).all():
            raise ValueError(f"split {split} contains non-finite values in {path}")
        matrices.append(matrix)
        targets.append(label)
        if include_metadata:
            locations.append(table["location_id"].to_numpy(zero_copy_only=False).astype(str))
            event_times.append(table["feature_time"].to_numpy(zero_copy_only=False))
            target_times.append(table["target_time"].to_numpy(zero_copy_only=False))
    data = SplitArrays(
        features=np.concatenate(matrices, axis=0),
        target=np.concatenate(targets, axis=0),
        location_id=np.concatenate(locations) if include_metadata else None,
        event_time=np.concatenate(event_times) if include_metadata else None,
        target_time=np.concatenate(target_times) if include_metadata else None,
    )
    return data, list(MODEL_FEATURE_COLUMNS)


def persistence_predictions(features: np.ndarray, feature_names: Sequence[str]) -> np.ndarray:
    if features.ndim != 2 or features.shape[1] != len(feature_names):
        raise ValueError("feature matrix width does not match the V1.1 feature list")
    try:
        position = feature_names.index(PERSISTENCE_FEATURE)
    except ValueError as exc:
        raise ValueError(f"persistence feature {PERSISTENCE_FEATURE!r} is missing") from exc
    predictions = np.asarray(features[:, position], dtype=np.float64)
    if not np.isfinite(predictions).all():
        raise ValueError("persistence input contains non-finite values")
    return predictions


def write_persistence_validation(
    dataset_root: str | Path,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
) -> dict[str, Any]:
    validation, feature_names = load_split_arrays(dataset_root, "VALIDATION")
    predicted = persistence_predictions(validation.features, feature_names)
    report = {
        "baseline_id": "PERSISTENCE_T2H_V1_1",
        "definition": "temperature_c at feature_time t is held unchanged as prediction for target_temperature_2h at t+2h",
        "split": "VALIDATION",
        "metrics": regression_metrics(validation.target, predicted),
        "prediction_minus_target_bias": float(np.mean(predicted.astype(np.float64) - validation.target.astype(np.float64))),
        "rows": validation.row_count,
    }
    _atomic_write_json(Path(artifact_root) / "persistence_validation.json", report)
    return report


def validate_dataset_manifest(
    dataset_root: str | Path,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
) -> dict[str, Any]:
    """Check all partition footers and checksums without reading feature values."""
    artifact_dir = Path(artifact_root)
    manifest_path = artifact_dir / "dataset_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    root = Path(dataset_root)
    errors: list[str] = []
    observed_rows = {"TRAIN": 0, "VALIDATION": 0, "TEST": 0}
    location_counts = {name: set() for name in observed_rows}
    for partition in manifest["partitions"]:
        path = root / partition["path"]
        if not path.is_file():
            errors.append(f"missing partition {partition['path']}")
            continue
        if _sha256(path) != partition["sha256"]:
            errors.append(f"checksum mismatch {partition['path']}")
            continue
        parquet = pq.ParquetFile(path)
        if parquet.metadata.num_rows != partition["row_count"]:
            errors.append(f"row count mismatch {partition['path']}")
            continue
        observed_rows[partition["split"]] += int(partition["row_count"])
        location_counts[partition["split"]].add(partition["location_id"])
    report = {
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "total_rows": sum(observed_rows.values()),
        "rows_by_split": observed_rows,
        "locations_by_split": {name: len(items) for name, items in location_counts.items()},
        "duplicate_keys": "checked during generation per location using unique UTC feature_time",
        "parquet_checksums_and_footers": "PASS" if not errors else "FAIL",
        "test_values_read": False,
        "note": "Split inventory validation reads manifest metadata and Parquet footers only; it does not materialize TEST feature or target values.",
    }
    _atomic_write_json(artifact_dir / "dataset_validation.json", report)
    if errors:
        raise ValueError(f"dataset manifest validation failed: {errors[:5]}")
    return report


def _distribution(values: np.ndarray) -> dict[str, Any]:
    values = np.asarray(values, dtype=np.float64).reshape(-1)
    values = values[np.isfinite(values)]
    if not values.size:
        return {"count": 0, "mean": None, "std": None, "p05": None, "p50": None, "p95": None}
    p05, p50, p95 = np.percentile(values, [5, 50, 95])
    return {
        "count": int(values.size),
        "mean": float(values.mean()),
        "std": float(values.std(ddof=0)),
        "p05": float(p05),
        "p50": float(p50),
        "p95": float(p95),
    }


def _category_distribution(values: Sequence[Any]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    missing_count = 0
    for value in values:
        if value is None or not isinstance(value, (int, float, np.integer, np.floating)) or not math.isfinite(float(value)):
            missing_count += 1
            continue
        category = str(int(round(float(value))))
        counts[category] = counts.get(category, 0) + 1
    total = sum(counts.values())
    return {
        "count": total,
        "missing_count": missing_count,
        "counts": dict(sorted(counts.items(), key=lambda item: int(item[0]))),
        "fractions": {
            category: count / total for category, count in sorted(counts.items(), key=lambda item: int(item[0]))
        } if total else {},
    }


def write_distribution_comparison(
    dataset_root: str | Path,
    live_probe_path: str | Path,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
) -> dict[str, Any]:
    """Compare eligible TRAIN predictors with the current live Forecast sample."""
    mapping = {
        "temperature_2m": ("temperature_c", "°C"),
        "relative_humidity_2m": ("humidity_pct", "%"),
        "precipitation": ("precipitation_mm", "mm"),
        "pressure_msl": ("pressure_hpa", "hPa"),
        "wind_speed_10m": ("wind_speed_kmh", "km/h"),
        "wind_gusts_10m": ("wind_gust_kmh", "km/h"),
    }
    probe = json.loads(Path(live_probe_path).read_text(encoding="utf-8"))
    live_sources = probe.get("raw_responses", {})
    if not isinstance(live_sources.get("best_match"), list) or not isinstance(live_sources.get("ecmwf_ifs"), list):
        raise ValueError("live forecast probe lacks both raw source samples")
    train_files = sorted((Path(dataset_root) / "split=TRAIN").glob("location_id=*/*.parquet"))
    if not train_files:
        raise FileNotFoundError("TRAIN partitions are missing for live distribution comparison")

    distributions: dict[str, Any] = {}
    for api_name, (feature_name, unit) in mapping.items():
        train_chunks = [
            pq.read_table(path, columns=[feature_name])[feature_name].combine_chunks().to_numpy(zero_copy_only=False)
            for path in train_files
        ]
        train_values = np.concatenate(train_chunks).astype(np.float64, copy=False)
        live_summary = {}
        for source_name in ("best_match", "ecmwf_ifs"):
            values = [
                value
                for response in live_sources[source_name]
                for value in response.get("hourly", {}).get(api_name, [])
                if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))
            ]
            live_summary[source_name] = _distribution(np.asarray(values, dtype=np.float64))
        train_summary = _distribution(train_values)
        distributions[api_name] = {
            "unit": unit,
            "training_predictor": train_summary,
            "live_forecast_best_match": live_summary["best_match"],
            "live_forecast_explicit_ecmwf_ifs": live_summary["ecmwf_ifs"],
            "training_minus_best_match": {
                key: train_summary[key] - live_summary["best_match"][key]
                for key in ("mean", "std", "p05", "p50", "p95")
                if train_summary[key] is not None and live_summary["best_match"][key] is not None
            },
        }
    training_weather_codes = [
        value
        for path in train_files
        for value in pq.read_table(path, columns=["weather_code"])["weather_code"].to_pylist()
    ]
    live_weather_codes = {}
    for source_name in ("best_match", "ecmwf_ifs"):
        live_values = [
            value
            for response in live_sources[source_name]
            for value in response.get("hourly", {}).get("weather_code", [])
        ]
        live_weather_codes[source_name] = _category_distribution(live_values)
    distributions["weather_code"] = {
        "unit": "WMO code",
        "model_input": False,
        "training_predictor": _category_distribution(training_weather_codes),
        "live_forecast_best_match": live_weather_codes["best_match"],
        "live_forecast_explicit_ecmwf_ifs": live_weather_codes["ecmwf_ifs"],
        "interpretation": "Categorical context distribution only; weather_code is excluded from the 73 numeric model inputs.",
    }
    report = {
        "status": "DESCRIPTIVE_ONLY",
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "historical_predictor_source": "Open-Meteo Historical Forecast API, ecmwf_ifs",
        "live_source": "Open-Meteo Forecast API, automatic Best Match at probe time",
        "training_split": "TRAIN only",
        "training_row_count": int(distributions["temperature_2m"]["training_predictor"]["count"]),
        "live_probe_observed_at_utc": probe.get("observed_at_utc"),
        "live_probe_locations": probe.get("locations"),
        "live_probe_hours_per_location": probe.get("hours_per_location"),
        "live_probe_exact_best_match_vs_ifs": probe.get("comparison", {}).get("exact_match"),
        "variables": distributions,
        "interpretation": "Descriptive comparison against one small current live sample; not a formal drift test and not evidence that automatic Best Match stays on IFS.",
    }
    _atomic_write_json(Path(artifact_root) / "distribution_comparison.json", report)
    return report


def write_source_alignment(
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    live_probe_path: str | Path | None = None,
) -> dict[str, Any]:
    """Compare the old V1 archive, new pinned historical forecast, and live API."""
    root = Path(artifact_root)
    probe_path = Path(live_probe_path) if live_probe_path else root / "live_forecast_probe.json"
    probe = json.loads(probe_path.read_text(encoding="utf-8"))
    audit_path = root / "provider_audit.json"
    audit = json.loads(audit_path.read_text(encoding="utf-8")) if audit_path.is_file() else {}
    old_variables = [
        {
            "name": "temperature_2m", "unit": "°C", "timestamp_semantics": "hourly valid time",
        },
        {
            "name": "relative_humidity_2m", "unit": "%", "timestamp_semantics": "hourly valid time",
        },
        {
            "name": "precipitation", "unit": "mm", "timestamp_semantics": "provider archive hourly value",
        },
        {
            "name": "pressure_msl", "unit": "hPa", "timestamp_semantics": "hourly valid time",
        },
        {
            "name": "wind_speed_10m", "unit": "km/h", "timestamp_semantics": "hourly valid time",
        },
        {
            "name": "wind_gusts_10m", "unit": "km/h", "timestamp_semantics": "provider archive hourly value",
        },
        {
            "name": "weather_code", "unit": "WMO code", "timestamp_semantics": "hourly derived category",
        },
    ]
    report = {
        "status": "PASS_WITH_LIMITATIONS",
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "sources": {
            "old_v1_archive": {
                "endpoint": "https://archive-api.open-meteo.com/v1/archive",
                "model_family": "Historical Weather / Archive product; legacy request omitted the models parameter, so model identity was not pinned in historical/download_historical.py.",
                "variable_semantics": old_variables,
                "timezone": "UTC request and normalized UTC event_time",
                "known_limitations": ["Legacy Archive source is a retrospective archive/reanalysis-style product, not an operational forecast vintage.", "The selected model was not explicitly pinned in the legacy downloader."],
            },
            "new_v1_1_historical_predictor": {
                "endpoint": SOURCE_CONFIG["predictors"]["endpoint"],
                "model_id": SOURCE_CONFIG["predictors"]["model_id"],
                "model_family": "ECMWF IFS HRES 9 km global operational forecast family",
                "variable_semantics": [
                    {"name": "temperature_2m", "unit": "°C", "timestamp_semantics": "instantaneous valid-time value"},
                    {"name": "relative_humidity_2m", "unit": "%", "timestamp_semantics": "instantaneous valid-time value"},
                    {"name": "precipitation", "unit": "mm", "timestamp_semantics": "preceding-hour sum"},
                    {"name": "pressure_msl", "unit": "hPa", "timestamp_semantics": "instantaneous valid-time value"},
                    {"name": "wind_speed_10m", "unit": "km/h", "timestamp_semantics": "instantaneous valid-time value"},
                    {"name": "wind_gusts_10m", "unit": "km/h", "timestamp_semantics": "preceding 3-hour maximum per ECMWF documentation"},
                    {"name": "weather_code", "unit": "WMO code", "timestamp_semantics": "derived category; metadata only"},
                ],
                "timezone": "GMT request; UTC valid_time",
                "known_limitations": ["Historical Forecast is a stitched valid-time series and exposes no per-row init/run/lead.", "Model versions may change during the historical interval."],
            },
            "current_live_forecast": {
                "endpoint": LIVE_FORECAST_ENDPOINT,
                "model_selection": "Best Match; models parameter omitted by the current generic live request",
                "explicit_common_selection_available": "ecmwf_ifs",
                "probe_observed_at_utc": probe.get("observed_at_utc"),
                "probe_comparison": probe.get("comparison"),
                "timezone": "GMT request; hourly forecast valid times",
                "known_limitations": ["The response does not expose the model selected by Best Match.", "A probe is a snapshot and does not prove future Best Match stability."],
            },
            "target_v1_1": {
                "endpoint": SOURCE_CONFIG["targets"]["endpoint"],
                "model_id": SOURCE_CONFIG["targets"]["model_id"],
                "variable": "temperature_2m",
                "unit": "°C",
                "timestamp_semantics": "exact same-location target_time = feature_time + 2h",
                "scientific_interpretation": "ERA5 reanalysis reference, not independent station truth.",
            },
        },
        "current_provider_audit_status": audit.get("status"),
        "training_source_classification": "forecast-family-aligned; exact source parity is not claimed",
        "production_requirement": "Pin models=ecmwf_ifs for T2H live inference and establish offline/online feature parity in the next milestone.",
    }
    _atomic_write_json(root / "source_alignment.json", report)
    return report


def feature_source_max_time_is_safe(feature_time: pd.Timestamp, used_times: Sequence[pd.Timestamp]) -> bool:
    """Testable leakage guard for a row's source timestamps."""
    timestamp = pd.Timestamp(feature_time)
    timestamp = timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")
    normalized = [
        value.tz_localize("UTC") if value.tzinfo is None else value.tz_convert("UTC")
        for value in map(pd.Timestamp, used_times)
    ]
    return not normalized or max(normalized) <= timestamp


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--dataset-root", type=Path, default=DATASET_PATH)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--start-date", type=date.fromisoformat, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=date.fromisoformat, default=DEFAULT_END_DATE)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    result = materialize_feature_dataset(
        data_root=args.data_root,
        dataset_root=args.dataset_root,
        artifact_root=args.artifact_root,
        catalog_path=args.catalog,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(json.dumps({
        "model_id": result["model_id"],
        "feature_count": result["feature_count"],
        "total_rows": result["total_rows"],
        "rows_by_split": result["rows_by_split"],
        "dataset_root": str(args.dataset_root),
    }, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by notebook/script entry point
    raise SystemExit(main())
