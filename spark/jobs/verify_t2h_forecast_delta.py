from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    if hasattr(value, "item"):
        return value.item()
    return value


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession, functions as F

    parser = argparse.ArgumentParser(description="Read-only audit of T2H forecast and state Delta tables")
    parser.add_argument("--forecast-path", required=True)
    parser.add_argument("--state-path")
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--expected-replay-forecasts", type=int, default=None)
    parser.add_argument("--expected-live-forecasts", type=int, default=None)
    args = parser.parse_args(argv)

    spark = (
        SparkSession.builder.appName("verify-weather-forecast-t2h-v1-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        forecasts = spark.read.format("delta").load(args.forecast_path).cache()
        total_rows = forecasts.count()
        duplicate_ids = total_rows - forecasts.select("forecast_id").distinct().count()
        duplicate_logical_keys = (
            forecasts.groupBy("model_id", "location_id", "feature_time", "target_time")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        origin_counts = {row["execution_origin"]: int(row["count"]) for row in forecasts.groupBy("execution_origin").count().collect()}
        location_counts = {
            str(row["location_id"]): int(row["count"])
            for row in forecasts.filter(F.col("execution_origin") == "REPLAY_VALIDATION").groupBy("location_id").count().collect()
        }
        live_location_counts = {
            str(row["location_id"]): int(row["count"])
            for row in forecasts.filter(F.col("execution_origin") == "LIVE_PROSPECTIVE").groupBy("location_id").count().collect()
        }
        invalid_target_offsets = forecasts.filter(
            (F.col("target_time").cast("long") - F.col("feature_time").cast("long")) != 7200
        ).count()
        invalid_contract_rows = forecasts.filter(
            (F.col("model_id") != "WEATHER_XGBOOST_GLOBAL_T2H_V1_1")
            | (F.col("model_sha256") != "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a")
            | (F.col("feature_set_id") != "WEATHER_FORECAST_FE_T2H_V1_1")
            | (F.col("feature_list_sha256") != "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2")
            | (F.col("feature_count") != 73)
            | (F.col("forecast_horizon_hours") != 2)
            | (F.col("provider_model") != "ecmwf_ifs")
        ).count()
        invalid_provider_rows = forecasts.filter(
            (
                (F.col("execution_origin") == "LIVE_PROSPECTIVE")
                & (
                    (F.col("provider") != "Open-Meteo")
                    | (F.col("provider_endpoint") != "https://api.open-meteo.com/v1/forecast")
                    | (F.col("source") != "OPEN_METEO_LIVE_HOURLY")
                )
            )
            | (
                (F.col("execution_origin") == "REPLAY_VALIDATION")
                & (
                    (F.col("provider_endpoint") != "https://historical-forecast-api.open-meteo.com/v1/forecast")
                    | (F.col("source") != "OPEN_METEO_HISTORICAL_FORECAST")
                )
            )
        ).count()
        invalid_live_leads = forecasts.filter(
            (F.col("execution_origin") == "LIVE_PROSPECTIVE")
            & (
                F.col("forecast_lead_seconds").isNull()
                | (F.col("forecast_lead_seconds") <= 0)
                | (
                    F.abs(
                        F.col("forecast_lead_seconds")
                        - (F.col("target_time").cast("double") - F.col("inference_time").cast("double"))
                    )
                    > 0.001
                )
            )
        ).count()
        lead_stats_row = (
            forecasts.filter(F.col("execution_origin") == "LIVE_PROSPECTIVE")
            .agg(
                F.count(F.lit(1)).alias("count"),
                F.min("forecast_lead_seconds").alias("min"),
                F.avg("forecast_lead_seconds").alias("mean"),
                F.expr("percentile_approx(forecast_lead_seconds, 0.5, 10000)").alias("median"),
                F.expr("percentile_approx(forecast_lead_seconds, 0.95, 10000)").alias("p95"),
                F.max("forecast_lead_seconds").alias("max"),
            )
            .first()
        )
        lead_stats = {name: _json_value(lead_stats_row[name]) for name in ("count", "min", "mean", "median", "p95", "max")}
        sample = forecasts.orderBy("execution_origin", "location_id", "feature_time").limit(1).first()
        sample_record = None if sample is None else {name: _json_value(value) for name, value in sample.asDict().items()}

        state_report = None
        if args.state_path:
            states = spark.read.format("delta").load(args.state_path).cache()
            state_rows = states.count()
            state_locations = {row["location_id"]: int(row["count"]) for row in states.groupBy("location_id").count().collect()}
            state_duplicate_keys = state_rows - states.select("location_id", "event_time").distinct().count()
            state_report = {
                "rows": state_rows,
                "locations": len(state_locations),
                "rows_per_location_values": sorted(set(state_locations.values())),
                "duplicate_location_hours": state_duplicate_keys,
            }

        checks = {
            "forecast_id_duplicates_zero": duplicate_ids == 0,
            "logical_forecast_duplicates_zero": duplicate_logical_keys == 0,
            "target_offset_violations_zero": invalid_target_offsets == 0,
            "contract_violations_zero": invalid_contract_rows == 0,
            "provider_contract_violations_zero": invalid_provider_rows == 0,
            "live_nonpositive_leads_zero": invalid_live_leads == 0,
            "replay_location_coverage_valid": origin_counts.get("REPLAY_VALIDATION", 0) == 0 or len(location_counts) == 63,
            "replay_rows_match_expected": args.expected_replay_forecasts is None or origin_counts.get("REPLAY_VALIDATION", 0) == args.expected_replay_forecasts,
            "live_rows_match_expected": args.expected_live_forecasts is None or origin_counts.get("LIVE_PROSPECTIVE", 0) == args.expected_live_forecasts,
        }
        if state_report is not None:
            checks["state_has_63_locations"] = state_report["locations"] == 63
            checks["state_history_is_49_rows_per_location"] = state_report["rows_per_location_values"] == [49]
            checks["state_duplicate_location_hours_zero"] = state_report["duplicate_location_hours"] == 0
        report = {
            "status": "PASS" if all(checks.values()) else "FAIL",
            "forecast_path": args.forecast_path,
            "total_forecast_rows": total_rows,
            "origin_counts": origin_counts,
            "replay_location_count": len(location_counts),
            "replay_rows_per_location_values": sorted(set(location_counts.values())),
            "live_location_count": len(live_location_counts),
            "live_rows_per_location_values": sorted(set(live_location_counts.values())),
            "forecast_id_duplicates": duplicate_ids,
            "logical_forecast_duplicates": duplicate_logical_keys,
            "target_offset_violations": invalid_target_offsets,
            "contract_violations": invalid_contract_rows,
            "provider_contract_violations": invalid_provider_rows,
            "nonpositive_live_leads": invalid_live_leads,
            "live_forecast_lead_seconds": lead_stats,
            "state": state_report,
            "checks": checks,
            "sample_forecast": sample_record,
        }
        output = Path(args.output_json)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False))
        return 0 if report["status"] == "PASS" else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
