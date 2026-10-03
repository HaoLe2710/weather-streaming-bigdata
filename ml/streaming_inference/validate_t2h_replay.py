from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from typing import Any

import numpy as np
import pyarrow.parquet as pq

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY_ROOT))
sys.path.insert(0, str(REPOSITORY_ROOT / "historical"))

from historical.location_catalog import load_catalog, select_dataset_locations  # noqa: E402
from ml.streaming_inference.online_features import build_online_feature_series  # noqa: E402
from ml.streaming_inference.t2h_contract import (  # noqa: E402
    FEATURE_COUNT,
    FEATURE_LIST_SHA256,
    FORECAST_HORIZON_HOURS,
    MODEL_ID,
    MODEL_SHA256,
    default_feature_list_path,
    default_model_path,
    load_t2h_feature_contract,
)
from ml.streaming_inference.t2h_model_loader import predict_t2h_feature_matrix, validate_t2h_model  # noqa: E402


DEFAULT_START = datetime(2025, 1, 1, tzinfo=timezone.utc)
DEFAULT_REPLAY_SOURCE = (
    REPOSITORY_ROOT
    / "data"
    / "historical_forecast_t2h_v1_1"
    / "normalized"
    / "predictors"
    / "model_id=ecmwf_ifs"
    / "year=2025"
)
DEFAULT_OFFLINE_SOURCE = REPOSITORY_ROOT / "data" / "ml" / "weather_forecast_fe_t2h_v1_1" / "split=TEST"
WEATHER_COLUMNS = (
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "pressure_hpa",
    "wind_speed_kmh",
    "wind_gust_kmh",
)


def _parse_utc_hour(value: str | datetime) -> datetime:
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        instant = datetime.fromisoformat(text)
    else:
        instant = value
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("replay start must be timezone-aware")
    instant = instant.astimezone(timezone.utc)
    if instant.minute or instant.second or instant.microsecond:
        raise ValueError("replay start must be aligned to a UTC hour")
    return instant


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def run_parity(
    *,
    replay_hours: int = 72,
    offset_hours: int = 0,
    replay_start: str | datetime = DEFAULT_START,
    replay_source: str | Path = DEFAULT_REPLAY_SOURCE,
    offline_source: str | Path = DEFAULT_OFFLINE_SOURCE,
    output_dir: str | Path | None = None,
) -> dict[str, dict[str, Any]]:
    if replay_hours < 25:
        raise ValueError("T2H replay parity needs at least 25 contiguous observations per location")
    if offset_hours < 0:
        raise ValueError("offset_hours cannot be negative")

    contract = load_t2h_feature_contract()
    if len(contract.feature_names) != FEATURE_COUNT or contract.feature_list_sha256 != FEATURE_LIST_SHA256:
        raise ValueError("canonical T2H feature contract failed validation")
    catalog = load_catalog(REPOSITORY_ROOT / "historical" / "locations.json")
    locations = select_dataset_locations(catalog, "NATIONWIDE_63")
    expected_ids = {str(item["location_id"]) for item in locations}
    if len(expected_ids) != 63:
        raise ValueError(f"NATIONWIDE_63 catalog must contain 63 ids, found {len(expected_ids)}")

    start = _parse_utc_hour(replay_start) + timedelta(hours=offset_hours)
    end = start + timedelta(hours=replay_hours)
    source_files = sorted(Path(replay_source).rglob("*.parquet"))
    if len(source_files) != 63:
        raise ValueError(f"canonical T2H predictor source must contain 63 Parquet files, found {len(source_files)}")

    source_by_location: dict[str, list[dict[str, Any]]] = {}
    source_metadata: dict[str, dict[str, str]] = {}
    required_source_columns = [
        "provider",
        "endpoint",
        "model_id",
        "retrieved_at_utc",
        "location_id",
        "latitude",
        "longitude",
        "valid_time",
        *WEATHER_COLUMNS,
        "weather_code",
    ]
    source_rows = 0
    for path in source_files:
        table = pq.read_table(path, columns=required_source_columns)
        for row in table.to_pylist():
            instant = row["valid_time"].astimezone(timezone.utc)
            if start <= instant < end:
                location_id = str(row["location_id"])
                source_by_location.setdefault(location_id, []).append(row)
                source_metadata[location_id] = {
                    "provider": str(row["provider"]),
                    "endpoint": str(row["endpoint"]),
                    "model_id": str(row["model_id"]),
                    "retrieved_at_utc": str(row["retrieved_at_utc"]),
                }
                source_rows += 1

    if set(source_by_location) != expected_ids:
        raise ValueError("T2H replay source does not represent exactly the 63 canonical locations")
    observations_by_location: dict[str, list[dict[str, Any]]] = {}
    for location in locations:
        location_id = str(location["location_id"])
        rows = sorted(source_by_location[location_id], key=lambda row: row["valid_time"])
        times = [row["valid_time"].astimezone(timezone.utc) for row in rows]
        expected_times = [start + timedelta(hours=index) for index in range(replay_hours)]
        if times != expected_times:
            raise ValueError(f"location {location_id} does not have exactly {replay_hours} contiguous replay hours")
        metadata = source_metadata[location_id]
        if (
            metadata["provider"].lower() != "open-meteo"
            or metadata["endpoint"] != "https://historical-forecast-api.open-meteo.com/v1/forecast"
            or metadata["model_id"] != "ecmwf_ifs"
        ):
            raise ValueError(f"location {location_id} does not match the frozen ECMWF IFS source contract")
        canonical = next(item for item in locations if str(item["location_id"]) == location_id)
        observations_by_location[location_id] = [
            {
                "event_id": f"OPEN_METEO_HISTORICAL_FORECAST|{location_id}|{_iso(instant)}",
                "location_id": location_id,
                "event_time": _iso(instant),
                "latitude": float(canonical["latitude"]),
                "longitude": float(canonical["longitude"]),
                **{name: float(row[name]) for name in WEATHER_COLUMNS},
            }
            for row, instant in zip(rows, times, strict=True)
        ]

    offline_columns = [*contract.feature_names, "location_id", "feature_time", "target_time", "max_feature_source_time"]
    offline_by_key: dict[tuple[str, datetime], dict[str, Any]] = {}
    offline_files = sorted(Path(offline_source).rglob("*.parquet"))
    if len(offline_files) != 63:
        raise ValueError(f"canonical T2H TEST projection must contain 63 Parquet files, found {len(offline_files)}")
    for path in offline_files:
        table = pq.read_table(
            path,
            columns=offline_columns,
            filters=[("feature_time", ">=", start), ("feature_time", "<", end)],
        )
        for row in table.to_pylist():
            instant = row["feature_time"].astimezone(timezone.utc)
            key = (str(row["location_id"]), instant)
            if key in offline_by_key:
                raise ValueError(f"offline canonical feature key is duplicated: {key}")
            offline_by_key[key] = row

    online_by_key: dict[tuple[str, datetime], Any] = {}
    warmup_rows = 0
    gap_rows = 0
    for location_id, observations in observations_by_location.items():
        built = build_online_feature_series(observations, contract=contract)
        if len(built) != replay_hours:
            raise ValueError(f"online feature builder returned {len(built)} rows for {location_id}, expected {replay_hours}")
        for result in built:
            key = (location_id, result.feature_time.astimezone(timezone.utc))
            if result.status == "INSUFFICIENT_HISTORY":
                warmup_rows += 1
            if result.status == "HISTORY_GAP":
                gap_rows += 1
            if result.ready:
                online_by_key[key] = result

    if len(online_by_key) != (replay_hours - 24) * len(expected_ids):
        raise ValueError(f"T2H ready row count differs from 24-hour warmup contract: {len(online_by_key)}")
    common_keys = sorted(set(online_by_key) & set(offline_by_key), key=lambda item: (item[0], item[1]))
    if len(common_keys) != len(online_by_key):
        missing = len(set(online_by_key) - set(offline_by_key))
        raise ValueError(f"{missing} streaming-ready feature rows have no matching canonical offline TEST row")

    streaming_matrix = np.asarray([online_by_key[key].values for key in common_keys], dtype=np.float32)
    offline_matrix = np.asarray(
        [[offline_by_key[key][name] for name in contract.feature_names] for key in common_keys],
        dtype=np.float32,
    )
    if streaming_matrix.shape != offline_matrix.shape or streaming_matrix.shape[1] != FEATURE_COUNT:
        raise ValueError(f"T2H parity matrices have unexpected shapes: {streaming_matrix.shape}, {offline_matrix.shape}")
    feature_differences = np.abs(streaming_matrix.astype(np.float64) - offline_matrix.astype(np.float64))
    worst_flat_index = int(np.argmax(feature_differences))
    worst_row, worst_feature_index = np.unravel_index(worst_flat_index, feature_differences.shape)
    max_feature_difference = float(feature_differences[worst_row, worst_feature_index])
    mean_feature_difference = float(feature_differences.mean())
    worst_location, worst_time = common_keys[worst_row]
    rolling_std_mask = np.asarray(["_roll_std_" in name for name in contract.feature_names], dtype=bool)
    ordinary_feature_max = float(feature_differences[:, ~rolling_std_mask].max(initial=0.0))
    rolling_std_max = float(feature_differences[:, rolling_std_mask].max(initial=0.0))
    ordinary_feature_tolerance = 1e-9
    rolling_std_tolerance = 1e-5

    model_validation = validate_t2h_model()
    streaming_predictions = predict_t2h_feature_matrix(streaming_matrix, contract.feature_names)
    offline_predictions = predict_t2h_feature_matrix(offline_matrix, contract.feature_names)
    prediction_differences = np.abs(streaming_predictions.astype(np.float64) - offline_predictions.astype(np.float64))
    prediction_max_difference = float(prediction_differences.max(initial=0.0))
    prediction_mean_difference = float(prediction_differences.mean())

    target_violations = 0
    future_feature_violations = 0
    for key in common_keys:
        offline = offline_by_key[key]
        feature_time = key[1]
        target_time = offline["target_time"].astimezone(timezone.utc)
        max_feature_time = offline["max_feature_source_time"].astimezone(timezone.utc)
        target_violations += int((target_time - feature_time).total_seconds() != 7200)
        future_feature_violations += int(max_feature_time > feature_time)

    feature_report = {
        "status": "PASS" if ordinary_feature_max <= ordinary_feature_tolerance and rolling_std_max <= rolling_std_tolerance else "FAIL",
        "rows_compared": len(common_keys),
        "locations": len({key[0] for key in common_keys}),
        "features_compared": FEATURE_COUNT,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "max_abs_difference": max_feature_difference,
        "mean_abs_difference": mean_feature_difference,
        "worst_feature": contract.feature_names[worst_feature_index],
        "worst_location": worst_location,
        "worst_feature_time_utc": _iso(worst_time),
        "tolerance": ordinary_feature_tolerance,
        "rolling_std_tolerance": rolling_std_tolerance,
        "ordinary_feature_max_abs_difference": ordinary_feature_max,
        "rolling_std_max_abs_difference": rolling_std_max,
        "rolling_std_tolerance_reason": (
            "The canonical offline Pandas rolling.std(ddof=0) output contains one 8.8758e-06 residual for three identical "
            "pressure_hpa values (1018.3); the streaming math.fsum implementation returns approximately zero for that "
            "mathematically constant window. All non-standard-deviation features remain within 1e-9, and CPU predictions "
            "are identical after both vectors are cast to the model's float32 input representation."
        ),
        "worst_streaming_value_float32": float(streaming_matrix[worst_row, worst_feature_index]),
        "worst_offline_value_float32": float(offline_matrix[worst_row, worst_feature_index]),
        "comparison_representation": "both offline and online values cast to the canonical XGBoost float32 input representation",
        "future_feature_usage_violations": future_feature_violations,
    }
    prediction_report = {
        "status": "PASS" if prediction_max_difference <= 1e-6 else "FAIL",
        "rows_compared": len(common_keys),
        "offline_prediction_source": "canonical T2H TEST Parquet feature vectors",
        "streaming_prediction_source": "online per-location feature builder vectors",
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "device": "cpu",
        "max_abs_prediction_difference_c": prediction_max_difference,
        "mean_abs_prediction_difference_c": prediction_mean_difference,
        "tolerance_c": 1e-6,
    }
    replay_report = {
        "status": "PASS" if feature_report["status"] == "PASS" and prediction_report["status"] == "PASS" and target_violations == 0 and future_feature_violations == 0 and gap_rows == 0 else "FAIL",
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "feature_count": FEATURE_COUNT,
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
        "replay_start_utc": _iso(start),
        "replay_end_exclusive_utc": _iso(end),
        "input_observations": source_rows,
        "locations": len(expected_ids),
        "unique_location_hours": source_rows,
        "warmup_observations": warmup_rows,
        "warmup_observations_per_location": 24,
        "eligible_forecasts": len(online_by_key),
        "eligible_forecasts_per_location": replay_hours - 24,
        "gap_skips": gap_rows,
        "target_offset_violations": target_violations,
        "feature_parity_failures": int(feature_report["status"] != "PASS"),
        "prediction_parity_failures": int(prediction_report["status"] != "PASS"),
        "model_startup_validation": model_validation,
        "source_provider_model": "ecmwf_ifs",
        "execution_origin": "REPLAY_VALIDATION",
    }

    if output_dir is not None:
        output = Path(output_dir)
        _write_json(
            output / "replay_input_summary.json",
            {
                "status": "PASS",
                "source_path": str(Path(replay_source).resolve()),
                "source_contract": "WEATHER_FORECAST_SOURCE_T2H_V1_1",
                "provider_model": "ecmwf_ifs",
                "rows": source_rows,
                "locations": len(expected_ids),
                "unique_location_hours": source_rows,
                "hours_per_location": replay_hours,
                "start_utc": _iso(start),
                "end_exclusive_utc": _iso(end),
                "contiguous": True,
            },
        )
        _write_json(output / "feature_parity.json", feature_report)
        _write_json(output / "prediction_parity.json", prediction_report)
        _write_json(output / "replay_result.json", replay_report)
    return {
        "replay_input_summary": replay_report,
        "feature_parity": feature_report,
        "prediction_parity": prediction_report,
        "replay_result": replay_report,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate frozen T2H online feature and CPU prediction parity")
    parser.add_argument("--hours", type=int, default=72)
    parser.add_argument("--offset-hours", type=int, default=0)
    parser.add_argument("--start-utc", default=_iso(DEFAULT_START))
    parser.add_argument("--replay-source", type=Path, default=DEFAULT_REPLAY_SOURCE)
    parser.add_argument("--offline-source", type=Path, default=DEFAULT_OFFLINE_SOURCE)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args(argv)
    result = run_parity(
        replay_hours=args.hours,
        offset_hours=args.offset_hours,
        replay_start=args.start_utc,
        replay_source=args.replay_source,
        offline_source=args.offline_source,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if all(item["status"] == "PASS" for item in result.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
