from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import sys
from typing import Any


PROJECT_ROOT = Path("/opt/project")
sys.path.insert(0, str(PROJECT_ROOT / "historical"))

from historical.location_catalog import load_catalog, select_dataset_locations  # noqa: E402
from ml.artifacts import atomic_write_json  # noqa: E402
from ml.streaming_inference.contract import (  # noqa: E402
    FEATURE_COUNT,
    FEATURE_LIST_SHA256,
    FEATURE_SET_ID,
    MODEL_ID,
    MODEL_SHA256,
)
from ml.streaming_inference.online_features import HISTORY_HOURS  # noqa: E402


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def validate_outputs(
    *,
    forecast_path: Path,
    state_path: Path,
    output_path: Path,
    source_observations: int,
    replay_hours: int,
    schema_output_path: Path | None = None,
    expected_location_ids: list[str] | None = None,
    expected_forecast_rows: int | None = None,
    batch_metrics_path: Path | None = None,
) -> dict[str, Any]:
    from pyspark.sql import SparkSession, functions as F

    spark = (
        SparkSession.builder.appName("check-weather-streaming-inference-v1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        forecasts = spark.read.format("delta").load(str(forecast_path))
        state = spark.read.format("delta").load(str(state_path))
        forecast_count = forecasts.count()
        unique_id_count = forecasts.select("forecast_id").distinct().count()
        duplicate_key_count = (
            forecasts.groupBy("model_id", "location_id", "feature_time")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        canonical_locations = select_dataset_locations(load_catalog(), "NATIONWIDE_63")
        canonical_ids = {str(item["location_id"]) for item in canonical_locations}
        expected_locations = set(expected_location_ids) if expected_location_ids else canonical_ids
        if not expected_locations.issubset(canonical_ids):
            raise ValueError(f"requested validation contains non-canonical location IDs: {sorted(expected_locations - canonical_ids)}")
        output_locations = {str(row[0]) for row in forecasts.select("location_id").distinct().collect()}
        missing_locations = sorted(expected_locations - output_locations)
        unknown_locations = sorted(output_locations - expected_locations)
        invalid = forecasts.filter(
            F.col("forecast_id").isNull()
            | F.col("location_id").isNull()
            | F.col("feature_time").isNull()
            | F.col("target_time").isNull()
            | F.col("prediction_temperature_c").isNull()
            | F.isnan("prediction_temperature_c")
            | (F.abs(F.col("prediction_temperature_c")) >= F.lit(float("inf")))
            | (F.unix_timestamp("target_time") - F.unix_timestamp("feature_time") != 3600)
            | (F.col("model_id") != F.lit(MODEL_ID))
            | (F.col("model_sha256") != F.lit(MODEL_SHA256))
            | (F.col("feature_set_id") != F.lit(FEATURE_SET_ID))
            | (F.col("feature_list_sha256") != F.lit(FEATURE_LIST_SHA256))
        ).count()
        forecast_stats = forecasts.agg(
            F.min("feature_time").alias("feature_time_min"),
            F.max("feature_time").alias("feature_time_max"),
            F.min("target_time").alias("target_time_min"),
            F.max("target_time").alias("target_time_max"),
            F.min("prediction_temperature_c").alias("min"),
            F.max("prediction_temperature_c").alias("max"),
            F.avg("prediction_temperature_c").alias("mean"),
            F.stddev_pop("prediction_temperature_c").alias("std"),
        ).first()
        state_count = state.count()
        state_unique_count = state.select("location_id", "event_time").distinct().count()
        state_per_location = [int(row["count"]) for row in state.groupBy("location_id").count().collect()]
        forecast_schema = [
            {
                "field": field.name,
                "type": field.dataType.simpleString(),
                "nullable": field.nullable,
            }
            for field in forecasts.schema.fields
        ]

        derived_expected_forecasts = max(0, replay_hours - HISTORY_HOURS) * len(expected_locations)
        expected_forecasts = expected_forecast_rows if expected_forecast_rows is not None else derived_expected_forecasts
        batches = _read_jsonl(batch_metrics_path) if batch_metrics_path else []
        warmup_skips = sum(int(item.get("insufficient_history_rows", 0)) for item in batches)
        gap_skips = sum(int(item.get("history_gap_rows", 0)) for item in batches)
        duplicate_input = sum(int(item.get("duplicate_input_rows", 0)) for item in batches)
        duplicate_output = sum(int(item.get("duplicate_output_rows", 0)) for item in batches)
        observed_loads = sum(int(item.get("model_load_count_across_observed_workers", 0)) for item in batches)
        runtime_seconds = sum(float(item.get("total_batch_seconds", 0.0)) for item in batches)
        prediction_medians = [
            float(item.get("prediction_seconds_median_per_location", 0.0))
            for item in batches
            if int(item.get("predicted_rows", 0)) > 0
        ]
        prediction_p95s = [
            float(item.get("prediction_seconds_p95_per_location", 0.0))
            for item in batches
            if int(item.get("predicted_rows", 0)) > 0
        ]
        prediction_maxima = [
            float(item.get("prediction_seconds_max_per_location", 0.0))
            for item in batches
            if int(item.get("predicted_rows", 0)) > 0
        ]
        batch_durations = [float(item.get("total_batch_seconds", 0.0)) for item in batches]

        def _duration_summary(values: list[float]) -> dict[str, float]:
            if not values:
                return {"median": 0.0, "p95": 0.0, "max": 0.0}
            ordered = sorted(values)
            p95_index = int((len(ordered) - 1) * 0.95 + 0.5)
            return {"median": statistics.median(ordered), "p95": ordered[p95_index], "max": ordered[-1]}

        if prediction_medians:
            prediction_summary = {
                "median_seconds_per_location": statistics.median(prediction_medians),
                "p95_seconds_per_location": statistics.median(prediction_p95s),
                "max_seconds_per_location": max(prediction_maxima),
            }
        else:
            prediction_summary = {
                "median_seconds_per_location": 0.0,
                "p95_seconds_per_location": 0.0,
                "max_seconds_per_location": 0.0,
            }

        checks = {
            "forecast_count_matches_expected": forecast_count == expected_forecasts,
            "forecast_id_unique": forecast_count == unique_id_count,
            "model_location_time_unique": duplicate_key_count == 0,
            "target_time_is_feature_time_plus_one_hour": invalid == 0,
            "all_expected_locations_produce_forecasts": len(output_locations) == len(expected_locations),
            "no_unknown_locations": not unknown_locations,
            "state_observation_key_unique": state_count == state_unique_count,
            "state_retained_within_49_hours": max(state_per_location, default=0) <= 49,
        }
        report = {
            "status": "PASS" if all(checks.values()) else "FAIL",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "forecast_path": str(forecast_path),
            "state_path": str(state_path),
            "source_observations": source_observations,
            "replay_hours": replay_hours,
            "required_prior_history_hours": HISTORY_HOURS,
            "expected_forecasts": expected_forecasts,
            "forecast_rows": forecast_count,
            "warmup_skipped_rows": warmup_skips,
            "history_gap_skipped_rows": gap_skips,
            "duplicate_input_rows": duplicate_input,
            "duplicate_forecast_id_count": forecast_count - unique_id_count,
            "duplicate_model_location_time_count": duplicate_key_count,
            "duplicate_output_rows_on_replay": duplicate_output,
            "invalid_forecast_rows": invalid,
            "locations_expected": len(expected_locations),
            "locations_with_forecasts": len(output_locations),
            "missing_locations": missing_locations,
            "unknown_locations": unknown_locations,
            "model_id": MODEL_ID,
            "model_sha256": MODEL_SHA256,
            "feature_set_id": FEATURE_SET_ID,
            "feature_count": FEATURE_COUNT,
            "feature_list_sha256": FEATURE_LIST_SHA256,
            "forecast_schema": forecast_schema,
            "forecast_temperature_c": {
                "min": forecast_stats["min"],
                "max": forecast_stats["max"],
                "mean": forecast_stats["mean"],
                "std": forecast_stats["std"],
            },
            "feature_time_min_utc": forecast_stats["feature_time_min"],
            "feature_time_max_utc": forecast_stats["feature_time_max"],
            "target_time_min_utc": forecast_stats["target_time_min"],
            "target_time_max_utc": forecast_stats["target_time_max"],
            "state_observation_rows": state_count,
            "state_unique_observation_keys": state_unique_count,
            "state_rows_per_location": {
                "min": min(state_per_location, default=0),
                "max": max(state_per_location, default=0),
            },
            "runtime_seconds_from_batches": runtime_seconds,
            "full_microbatch_runtime_seconds": _duration_summary(batch_durations),
            "prediction_latency": prediction_summary,
            "model_load_count_from_batches": observed_loads,
            "checks": checks,
        }
        atomic_write_json(output_path, report)
        if schema_output_path is not None:
            atomic_write_json(
                schema_output_path,
                {
                    "table_path": str(forecast_path),
                    "fields": forecast_schema,
                    "feature_time_target_time_invariant": "target_time = feature_time + 1 hour",
                    "idempotency_key": "forecast_id",
                },
            )
        print(json.dumps(report, indent=2, default=str))
        if report["status"] != "PASS":
            raise AssertionError(f"streaming output validation failed: {report}")
        return report
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the V1 forecast and bounded state Delta tables")
    parser.add_argument("--forecast-path", type=Path, default=Path("/opt/project/data/streaming/weather_forecast_xgboost_v1/forecasts"))
    parser.add_argument("--state-path", type=Path, default=Path("/opt/project/data/streaming/weather_forecast_xgboost_v1/state_hourly_observations"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--schema-output", type=Path)
    parser.add_argument("--source-observations", type=int, required=True)
    parser.add_argument("--replay-hours", type=int, required=True)
    parser.add_argument("--expected-location-ids", help="comma-separated canonical subset; defaults to NATIONWIDE_63")
    parser.add_argument("--expected-forecast-rows", type=int)
    parser.add_argument("--batch-metrics", type=Path)
    args = parser.parse_args()
    validate_outputs(
        forecast_path=args.forecast_path,
        state_path=args.state_path,
        output_path=args.output,
        schema_output_path=args.schema_output,
        source_observations=args.source_observations,
        replay_hours=args.replay_hours,
        expected_location_ids=args.expected_location_ids.split(",") if args.expected_location_ids else None,
        expected_forecast_rows=args.expected_forecast_rows,
        batch_metrics_path=args.batch_metrics,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
