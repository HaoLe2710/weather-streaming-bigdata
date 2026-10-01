from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any

from ml.artifacts import atomic_write_json
from ml.streaming_inference.contract import load_feature_contract, repository_root
from ml.streaming_inference.model_loader import predict_feature_matrix
from ml.streaming_inference.online_features import build_online_feature_series


def _sample_training_rows(root: Path, location_ids: list[str], start: datetime, end: datetime, columns: list[str]) -> dict[str, list[dict[str, Any]]]:
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("PyArrow is required to scan committed TRAIN feature Parquet for parity") from exc

    training_dir = root / "data" / "ml" / "weather_forecast_fe_v1" / "split=TRAIN"
    files = sorted(training_dir.glob("part-*.parquet"))
    if not files:
        raise FileNotFoundError(f"TRAIN feature Parquet is unavailable at {training_dir}")
    locations = pa.array(location_ids, type=pa.string())
    lower = pa.scalar(start.replace(tzinfo=None), type=pa.timestamp("ns"))
    upper = pa.scalar(end.replace(tzinfo=None), type=pa.timestamp("ns"))
    result = {location_id: [] for location_id in location_ids}
    for path in files:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(columns=columns, batch_size=32_768):
            table = pa.Table.from_batches([batch])
            event_time = table["event_time"]
            mask = pc.and_kleene(
                pc.and_kleene(
                    pc.is_in(table["location_id"], value_set=locations),
                    pc.greater_equal(event_time, lower),
                ),
                pc.less(event_time, upper),
            )
            selected = table.filter(mask).to_pydict()
            for index, location_id in enumerate(selected.get("location_id", [])):
                row = {column: selected[column][index] for column in columns}
                stamp = row["event_time"]
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=timezone.utc)
                row["event_time"] = stamp
                row["location_id"] = location_id
                result[location_id].append(row)
    for rows in result.values():
        rows.sort(key=lambda item: item["event_time"])
    return result


def validate_feature_and_prediction_parity(
    root: str | Path | None = None,
    *,
    model_path: str | Path | None = None,
    output_path: str | Path | None = None,
    feature_output_path: str | Path | None = None,
    prediction_output_path: str | Path | None = None,
    location_count: int = 3,
    target_hours: int = 72,
) -> dict[str, Any]:
    import numpy as np

    base = Path(root) if root is not None else repository_root()
    contract = load_feature_contract(base)
    model_manifest = base / "results" / "modeling" / "20261001T171122Z-xgb-v1" / "training_manifest.json"
    frozen = json.loads(model_manifest.read_text(encoding="utf-8"))
    location_ids = list(frozen["location_summary"]["improved_location_ids"][:location_count])
    if location_count < 1 or not location_ids:
        raise ValueError("location_count must select at least one canonical location")

    target_start = datetime(2023, 4, 1, tzinfo=timezone.utc)
    target_end = target_start + timedelta(hours=target_hours)
    source_start = target_start - timedelta(hours=24)
    columns = list(dict.fromkeys(["location_id", "event_time", *contract.feature_names]))
    rows_by_location = _sample_training_rows(base, location_ids, source_start, target_end, columns)

    offline_vectors: list[list[float]] = []
    online_vectors: list[list[float]] = []
    compared_times: list[datetime] = []
    for location_id in location_ids:
        rows = rows_by_location[location_id]
        expected_source_rows = 24 + target_hours
        if len(rows) != expected_source_rows:
            raise AssertionError(
                f"{location_id} has {len(rows)} TRAIN rows in parity interval; expected {expected_source_rows}"
            )
        online = build_online_feature_series(rows, contract=contract)
        offline_by_time = {row["event_time"]: row for row in rows}
        for result in online:
            if not (target_start <= result.feature_time < target_end):
                continue
            if not result.ready:
                raise AssertionError(f"online builder returned {result.status} for {location_id} at {result.feature_time}")
            reference = offline_by_time[result.feature_time]
            online_vectors.append(list(result.values))
            offline_vectors.append([float(reference[name]) for name in contract.feature_names])
            compared_times.append(result.feature_time)

    if not offline_vectors:
        raise AssertionError("no feature parity rows were selected")
    offline = np.asarray(offline_vectors, dtype=np.float64)
    online = np.asarray(online_vectors, dtype=np.float64)
    absolute = np.abs(online - offline)
    max_feature_diff = float(absolute.max(initial=0.0))
    max_feature_row, max_feature_col = np.unravel_index(int(absolute.argmax()), absolute.shape)

    online_predictions = predict_feature_matrix(online_vectors, contract.feature_names, model_path=model_path)
    offline_predictions = predict_feature_matrix(offline_vectors, contract.feature_names, model_path=model_path)
    prediction_diff = np.abs(online_predictions - offline_predictions)
    max_prediction_diff = float(prediction_diff.max(initial=0.0))
    mismatch_mask = absolute > 1e-8
    max_by_feature = {
        name: float(absolute[:, index].max(initial=0.0))
        for index, name in enumerate(contract.feature_names)
    }

    feature_pass = max_feature_diff <= 1e-8
    prediction_pass = max_prediction_diff <= 1e-4
    result = {
        "status": "PASS" if feature_pass and prediction_pass else "FAIL",
        "source": "committed WEATHER_FORECAST_FE_V1 Parquet split=TRAIN, projected without target columns",
        "test_split_read": False,
        "target_temperature_column_read": False,
        "feature_set_id": contract.feature_set_id,
        "feature_count": len(contract.feature_names),
        "feature_list_sha256": contract.feature_list_sha256,
        "compared_rows": len(offline_vectors),
        "source_rows_including_24h_warmup": sum(len(value) for value in rows_by_location.values()),
        "locations": location_ids,
        "max_feature_absolute_difference": max_feature_diff,
        "max_absolute_difference_by_feature": max_by_feature,
        "feature_mismatch_count": int(mismatch_mask.sum()),
        "max_feature_difference_name": contract.feature_names[max_feature_col],
        "max_feature_difference_location": location_ids[max_feature_row // target_hours],
        "max_feature_difference_time": compared_times[max_feature_row].strftime("%Y-%m-%dT%H:%M:%SZ"),
        "feature_tolerance": 1e-8,
        "feature_parity_pass": feature_pass,
        "prediction_rows": int(prediction_diff.size),
        "max_prediction_absolute_difference_c": max_prediction_diff,
        "prediction_tolerance_c": 1e-4,
        "prediction_parity_pass": prediction_pass,
        "model_id": contract.model_id,
        "model_sha256": contract.model_sha256,
    }
    if output_path is not None:
        atomic_write_json(output_path, result)
    feature_report = {
        key: result[key]
        for key in (
            "status",
            "source",
            "test_split_read",
            "target_temperature_column_read",
            "feature_set_id",
            "feature_count",
            "feature_list_sha256",
            "compared_rows",
            "source_rows_including_24h_warmup",
            "locations",
            "max_feature_absolute_difference",
            "max_absolute_difference_by_feature",
            "feature_mismatch_count",
            "max_feature_difference_name",
            "max_feature_difference_location",
            "max_feature_difference_time",
            "feature_tolerance",
            "feature_parity_pass",
        )
    }
    feature_report["status"] = "PASS" if feature_pass else "FAIL"
    prediction_report = {
        "status": "PASS" if prediction_pass else "FAIL",
        "model_id": contract.model_id,
        "model_sha256": contract.model_sha256,
        "feature_set_id": contract.feature_set_id,
        "feature_list_sha256": contract.feature_list_sha256,
        "compared_rows": int(prediction_diff.size),
        "locations": location_ids,
        "max_prediction_absolute_difference_c": max_prediction_diff,
        "prediction_tolerance_c": 1e-4,
        "prediction_parity_pass": prediction_pass,
    }
    if feature_output_path is not None:
        atomic_write_json(feature_output_path, feature_report)
    if prediction_output_path is not None:
        atomic_write_json(prediction_output_path, prediction_report)
    if result["status"] != "PASS":
        raise AssertionError(f"feature/prediction parity failed: {result}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare online features and predictions with frozen TRAIN FE V1 rows.")
    parser.add_argument("--root", type=Path, default=repository_root())
    parser.add_argument("--model-path", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--feature-output", type=Path)
    parser.add_argument("--prediction-output", type=Path)
    parser.add_argument("--locations", type=int, default=3)
    parser.add_argument("--hours", type=int, default=72)
    args = parser.parse_args()
    result = validate_feature_and_prediction_parity(
        args.root,
        model_path=args.model_path,
        output_path=args.output,
        feature_output_path=args.feature_output,
        prediction_output_path=args.prediction_output,
        location_count=args.locations,
        target_hours=args.hours,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
