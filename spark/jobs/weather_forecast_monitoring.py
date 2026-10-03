from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Mapping
import uuid


PROJECT_ROOT = Path(os.getenv("WEATHER_MONITORING_PROJECT_ROOT", "/opt/project"))
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "historical"))

from monitoring.evaluation_contract import (  # noqa: E402
    EVALUATION_VERSION,
    LIVE_REFERENCE_SOURCE,
    format_utc_hour,
    observation_key,
    parse_utc_timestamp,
    stable_id,
)
from monitoring.forecast_evaluator import evaluate_forecasts, evaluation_schema  # noqa: E402
from monitoring.hourly_archive import (  # noqa: E402
    archive_schema,
    canonicalize_batch,
    normalize_observation,
    revision_record,
    revision_schema,
)
from monitoring.live_reference_backfill import fetch_live_reference_backfill  # noqa: E402
from monitoring.metrics import calculate_coverage  # noqa: E402
from monitoring.rolling_metrics import build_hourly_metrics, build_rolling_metrics  # noqa: E402


DEFAULT_ROOT = Path(os.getenv("WEATHER_MONITORING_OUTPUT_ROOT", "/opt/project/data/streaming/weather_forecast_evaluation_v1"))
DEFAULT_ARCHIVE_PATH = Path(
    os.getenv(
        "WEATHER_MONITORING_ARCHIVE_PATH",
        "/opt/project/data/streaming/weather_hourly_observations_v1/observations",
    )
)
DEFAULT_FORECAST_PATH = Path(
    os.getenv(
        "WEATHER_MONITORING_FORECAST_PATH",
        "/opt/project/data/streaming/weather_forecast_xgboost_v1/forecasts",
    )
)
DEFAULT_CHECKPOINT_PATH = Path(
    os.getenv("WEATHER_MONITORING_CHECKPOINT_PATH", "/opt/project/data/checkpoints/weather_forecast_monitoring_v1")
)
DEFAULT_TOPIC = os.getenv("WEATHER_MONITORING_TOPIC", "weather.hourly.observations.v1")
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("WEATHER_MONITORING_BOOTSTRAP_SERVERS", "broker:19092")
DEFAULT_CATALOG_PATH = Path(os.getenv("WEATHER_MONITORING_CATALOG_PATH", "/opt/project/historical/locations.json"))
DEFAULT_RUN_ID = os.getenv("WEATHER_MONITORING_RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-forecast-monitoring-v1"))
METRIC_VERSION = "FORECAST_METRICS_V1"
DEFAULT_REFERENCE_SOURCE = os.getenv("WEATHER_MONITORING_REFERENCE_SOURCE", "all")
DEFAULT_EVALUATION_MODE = os.getenv("WEATHER_MONITORING_EVALUATION_MODE", "all")

TIME_FIELDS = frozenset(
    {
        "event_time",
        "ingestion_time",
        "first_archived_at",
        "feature_time",
        "target_time",
        "forecast_inference_time",
        "reference_ingestion_time",
        "evaluation_time",
        "first_ingestion_time",
        "later_ingestion_time",
        "detected_at",
        "target_time",
        "computed_at",
    }
)


def _spark_timestamp(value: Any):
    if value is None:
        return None
    parsed = parse_utc_timestamp(value, assume_naive_utc=True)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _spark_row(record: Mapping[str, Any], schema) -> dict[str, Any]:
    converted: dict[str, Any] = {}
    for field in schema.fieldNames():
        value = record.get(field)
        if field in TIME_FIELDS and value is not None:
            value = _spark_timestamp(value)
        converted[field] = value
    return converted


def _json_default(value: Any):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if value.tzinfo else value.isoformat() + "Z"
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _append_json_line(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(dict(value), sort_keys=True, ensure_ascii=False, default=_json_default) + "\n")


def _ensure_delta(spark, path: Path, schema) -> None:
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, str(path)):
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    spark.createDataFrame([], schema).write.format("delta").mode("errorifexists").save(str(path))


def _merge_insert_only(spark, path: Path, rows: list[dict[str, Any]], schema, condition: str) -> None:
    if not rows:
        return
    from delta.tables import DeltaTable

    source = spark.createDataFrame([_spark_row(row, schema) for row in rows], schema)
    DeltaTable.forPath(spark, str(path)).alias("target").merge(source.alias("source"), condition).whenNotMatchedInsertAll().execute()


def _merge_evaluations(spark, path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    from delta.tables import DeltaTable

    schema = evaluation_schema()
    source = spark.createDataFrame([_spark_row(row, schema) for row in rows], schema)
    (
        DeltaTable.forPath(spark, str(path))
        .alias("target")
        .merge(source.alias("source"), "target.forecast_id = source.forecast_id AND target.evaluation_version = source.evaluation_version")
        .whenMatchedUpdateAll(condition="target.status <> 'EVALUATED'")
        .whenNotMatchedInsertAll()
        .execute()
    )


def _metric_schema():
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    fields = [
        ("metric_id", StringType(), False),
        ("metric_version", StringType(), False),
        ("reference_source", StringType(), False),
        ("evaluation_mode", StringType(), False),
        ("metric_scope", StringType(), False),
        ("window_hours", IntegerType(), False),
        ("target_time", TimestampType(), False),
        ("location_id", StringType(), True),
        ("sample_count", LongType(), False),
        ("expected_sample_count", LongType(), False),
        ("expected_references", LongType(), True),
        ("received_references", LongType(), True),
        ("missing_references", LongType(), True),
        ("reference_coverage_pct", DoubleType(), True),
        ("window_complete", BooleanType(), False),
        ("location_count", IntegerType(), True),
        ("expected_location_count", IntegerType(), True),
        ("forecasts_total", LongType(), True),
        ("forecasts_target_time_passed", LongType(), True),
        ("evaluated_count", LongType(), True),
        ("pending_target_count", LongType(), True),
        ("pending_reference_count", LongType(), True),
        ("invalid_count", LongType(), True),
        ("reference_conflict_count", LongType(), True),
        ("evaluation_coverage_pct", DoubleType(), True),
        ("model_mae", DoubleType(), True),
        ("model_rmse", DoubleType(), True),
        ("model_bias", DoubleType(), True),
        ("model_r2", DoubleType(), True),
        ("persistence_mae", DoubleType(), True),
        ("persistence_rmse", DoubleType(), True),
        ("persistence_bias", DoubleType(), True),
        ("persistence_r2", DoubleType(), True),
        ("mae_improvement_c", DoubleType(), True),
        ("mae_improvement_pct", DoubleType(), True),
        ("rmse_improvement_c", DoubleType(), True),
        ("rmse_improvement_pct", DoubleType(), True),
        ("mae_skill", DoubleType(), True),
        ("micro_global_mae", DoubleType(), True),
        ("macro_location_mae", DoubleType(), True),
        ("locations_evaluated", IntegerType(), True),
        ("locations_model_beats_persistence", IntegerType(), True),
        ("locations_model_equals_or_trails_persistence", IntegerType(), True),
        ("quality_status", StringType(), True),
        ("reference_arrival_lag_median_seconds", DoubleType(), True),
        ("reference_arrival_lag_p95_seconds", DoubleType(), True),
        ("reference_arrival_lag_max_seconds", DoubleType(), True),
        ("evaluation_lag_median_seconds", DoubleType(), True),
        ("evaluation_lag_p95_seconds", DoubleType(), True),
        ("evaluation_lag_max_seconds", DoubleType(), True),
        ("metrics_json", StringType(), False),
        ("distribution_json", StringType(), False),
        ("computed_at", TimestampType(), False),
    ]
    return StructType([StructField(name, kind, nullable) for name, kind, nullable in fields])


def _distribution_lag_fields(record: Mapping[str, Any], key: str) -> dict[str, Any]:
    lag = record.get(key) or {}
    prefix = "reference_arrival_lag" if key == "reference_arrival_lag_seconds" else "evaluation_lag"
    return {
        f"{prefix}_median_seconds": lag.get("p50"),
        f"{prefix}_p95_seconds": lag.get("p95"),
        f"{prefix}_max_seconds": lag.get("max"),
    }


def _flatten_metric(record: Mapping[str, Any], *, computed_at: datetime) -> dict[str, Any]:
    source = str(record.get("reference_source") or "UNKNOWN_SOURCE")
    mode = str(record.get("evaluation_mode") or "UNCLASSIFIED")
    scope = str(record.get("metric_scope") or "GLOBAL")
    window = int(record.get("window_hours") or 1)
    target_time = record["target_time"]
    location_id = record.get("location_id")
    row = {
        "metric_id": stable_id("FORECAST_METRIC", METRIC_VERSION, source, mode, scope, window, target_time, location_id),
        "metric_version": METRIC_VERSION,
        "reference_source": source,
        "evaluation_mode": mode,
        "metric_scope": scope,
        "window_hours": window,
        "target_time": target_time,
        "location_id": location_id,
        "sample_count": int(record.get("sample_count") or 0),
        "expected_sample_count": int(record.get("expected_sample_count") or 0),
        "expected_references": record.get("expected_references"),
        "received_references": record.get("received_references"),
        "missing_references": record.get("missing_references"),
        "reference_coverage_pct": record.get("reference_coverage_pct"),
        "window_complete": bool(record.get("window_complete")),
        "location_count": record.get("location_count"),
        "expected_location_count": record.get("expected_location_count"),
        "forecasts_total": record.get("forecasts_total"),
        "forecasts_target_time_passed": record.get("forecasts_target_time_passed"),
        "evaluated_count": record.get("evaluated_count"),
        "pending_target_count": record.get("pending_target_count"),
        "pending_reference_count": record.get("pending_reference_count"),
        "invalid_count": record.get("invalid_count"),
        "reference_conflict_count": record.get("reference_conflict_count"),
        "evaluation_coverage_pct": record.get("evaluation_coverage_pct"),
        "model_mae": record.get("model_mae"),
        "model_rmse": record.get("model_rmse"),
        "model_bias": record.get("model_bias"),
        "model_r2": record.get("model_r2"),
        "persistence_mae": record.get("persistence_mae"),
        "persistence_rmse": record.get("persistence_rmse"),
        "persistence_bias": record.get("persistence_bias"),
        "persistence_r2": record.get("persistence_r2"),
        "mae_improvement_c": record.get("mae_improvement_c"),
        "mae_improvement_pct": record.get("mae_improvement_pct"),
        "rmse_improvement_c": record.get("rmse_improvement_c"),
        "rmse_improvement_pct": record.get("rmse_improvement_pct"),
        "mae_skill": record.get("mae_skill"),
        "micro_global_mae": record.get("micro_global_mae"),
        "macro_location_mae": record.get("macro_location_mae"),
        "locations_evaluated": record.get("locations_evaluated"),
        "locations_model_beats_persistence": record.get("locations_model_beats_persistence"),
        "locations_model_equals_or_trails_persistence": record.get("locations_model_equals_or_trails_persistence"),
        "quality_status": record.get("quality_status"),
        **_distribution_lag_fields(record, "reference_arrival_lag_seconds"),
        **_distribution_lag_fields(record, "evaluation_lag_seconds"),
        "metrics_json": json.dumps(dict(record), sort_keys=True, default=_json_default, separators=(",", ":")),
        "distribution_json": json.dumps(record.get("distributions", {}), sort_keys=True, separators=(",", ":")),
        "computed_at": computed_at,
    }
    return row


def _write_metrics(spark, rows: list[dict[str, Any]], path: Path, *, computed_at: datetime) -> None:
    if not rows:
        return
    schema = _metric_schema()
    flattened = [_flatten_metric(row, computed_at=computed_at) for row in rows]
    from delta.tables import DeltaTable

    source = spark.createDataFrame([_spark_row(row, schema) for row in flattened], schema)
    (
        DeltaTable.forPath(spark, str(path))
        .alias("target")
        .merge(source.alias("source"), "target.metric_id = source.metric_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _read_json_records(spark, path: Path) -> list[dict[str, Any]]:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, str(path)):
        return []
    return [json.loads(value) for value in spark.read.format("delta").load(str(path)).toJSON().collect()]


def _read_existing_archive_for_keys(spark, path: Path, rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    if not rows:
        return {}
    from pyspark.sql import functions as F

    schema = archive_schema()
    incoming = spark.createDataFrame([_spark_row(row, schema) for row in rows], schema)
    key_columns = ["source", "location_id", "event_time"]
    existing = (
        spark.read.format("delta")
        .load(str(path))
        .join(F.broadcast(incoming.select(*key_columns).dropDuplicates()), key_columns, "inner")
        .toJSON()
        .collect()
    )
    result: dict[tuple[str, str, str], dict[str, Any]] = {}
    for value in existing:
        row = json.loads(value)
        key = observation_key(row["source"], row["location_id"], row["event_time"], assume_naive_utc=True)
        result[key] = row
    return result


def _rejection_schema():
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("rejection_id", StringType(), False),
            StructField("event_id", StringType(), True),
            StructField("source", StringType(), True),
            StructField("location_id", StringType(), True),
            StructField("reason", StringType(), False),
            StructField("raw_json", StringType(), True),
            StructField("rejected_at", TimestampType(), False),
        ]
    )


def _parse_batch_records(batch_df) -> tuple[list[dict[str, Any]], int]:
    values: list[dict[str, Any]] = []
    malformed = 0
    for row in batch_df.select("value").collect():
        raw = row["value"]
        if raw is None:
            malformed += 1
            continue
        if isinstance(raw, (bytes, bytearray, memoryview)):
            raw = bytes(raw).decode("utf-8", errors="replace")
        try:
            record = json.loads(str(raw))
        except json.JSONDecodeError:
            malformed += 1
            continue
        if not isinstance(record, dict):
            malformed += 1
            continue
        values.append(record)
    return values, malformed


def _process_observations(
    spark,
    observations: list[dict[str, Any]],
    *,
    config: argparse.Namespace,
    known_locations: set[str],
    malformed_count: int = 0,
    archived_at: datetime | None = None,
) -> dict[str, int]:
    from datetime import timezone

    now = archived_at or datetime.now(timezone.utc)
    batch = canonicalize_batch(observations, known_locations, archived_at=now)
    incoming = batch["observations"]
    existing_by_key = _read_existing_archive_for_keys(spark, config.archive_path, incoming)
    new_rows: list[dict[str, Any]] = []
    conflicts = list(batch["conflicts"])
    duplicates = int(batch["duplicate_count"])
    for row in incoming:
        key = observation_key(row["source"], row["location_id"], row["event_time"])
        existing = existing_by_key.get(key)
        if existing is None:
            new_rows.append(row)
        elif str(existing.get("reference_payload_sha256")) == str(row.get("reference_payload_sha256")):
            duplicates += 1
        else:
            conflicts.append(
                revision_record(
                    existing,
                    row,
                    detected_at=now,
                    assume_naive_utc=True,
                )
            )

    _merge_insert_only(
        spark,
        config.archive_path,
        new_rows,
        archive_schema(),
        "target.source = source.source AND target.location_id = source.location_id AND target.event_time = source.event_time",
    )
    _merge_insert_only(
        spark,
        config.revisions_path,
        conflicts,
        revision_schema(),
        "target.revision_id = source.revision_id",
    )

    rejected = list(batch["rejected"])
    if malformed_count:
        rejected.extend({"event_id": None, "reason": "MALFORMED_KAFKA_JSON", "raw": None} for _ in range(malformed_count))
    rejected_rows = []
    for item in rejected:
        raw = item.get("raw")
        event = raw if isinstance(raw, dict) else {}
        reason = str(item.get("reason") or "INVALID_OBSERVATION")
        event_id = item.get("event_id") or event.get("event_id")
        rejected_rows.append(
            {
                "rejection_id": stable_id("OBSERVATION_REJECTION", event_id or "MALFORMED", reason),
                "event_id": None if event_id is None else str(event_id),
                "source": event.get("source"),
                "location_id": event.get("location_id"),
                "reason": reason,
                "raw_json": None if raw is None else json.dumps(raw, sort_keys=True, default=_json_default),
                "rejected_at": now,
            }
        )
    _merge_insert_only(
        spark,
        config.rejections_path,
        rejected_rows,
        _rejection_schema(),
        "target.rejection_id = source.rejection_id",
    )
    return {
        "source_observation_rows": len(observations) + malformed_count,
        "canonical_observation_rows_added": len(new_rows),
        "duplicate_observation_rows": duplicates,
        "reference_conflict_rows": len(conflicts),
        "invalid_observation_rows": len(rejected),
    }


def _catalog_locations(path: Path) -> list[dict[str, Any]]:
    from location_catalog import DATASET_NATIONWIDE_63, load_catalog, select_dataset_locations

    locations = select_dataset_locations(load_catalog(path), DATASET_NATIONWIDE_63)
    if len(locations) != 63:
        raise ValueError(f"NATIONWIDE_63 catalog must have exactly 63 locations, found {len(locations)}")
    return locations


def _paths(args: argparse.Namespace) -> None:
    args.run_results_dir = args.results_root / args.run_id
    args.archive_path = args.archive_path or DEFAULT_ARCHIVE_PATH
    args.evaluations_path = args.output_root / "evaluations"
    args.hourly_metrics_path = args.output_root / "hourly_metrics"
    args.rolling_global_path = args.output_root / "rolling_global_metrics"
    args.rolling_location_path = args.output_root / "rolling_location_metrics"
    args.revisions_path = args.output_root / "reference_revisions"
    args.rejections_path = args.output_root / "rejected_observations"


def _read_forecast_records(spark, path: Path) -> list[dict[str, Any]]:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, str(path)):
        return []
    return [json.loads(value) for value in spark.read.format("delta").load(str(path)).toJSON().collect()]


def _read_revision_keys(spark, path: Path) -> set[tuple[str, str, str]]:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, str(path)):
        return set()
    rows = spark.read.format("delta").load(str(path)).select("source", "location_id", "event_time").dropDuplicates().toJSON().collect()
    return {
        observation_key(row["source"], row["location_id"], row["event_time"], assume_naive_utc=True)
        for row in (json.loads(value) for value in rows)
    }


def _read_existing_evaluations(spark, path: Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    rows = _read_json_records(spark, path)
    return {str(row["forecast_id"]): row for row in rows if row.get("forecast_id")}, rows


def _evaluation_duplicate_count(spark, path: Path) -> int:
    from pyspark.sql import functions as F

    counts = (
        spark.read.format("delta")
        .load(str(path))
        .groupBy("evaluation_id", "evaluation_version")
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicates"))
        .first()
    )
    return 0 if counts is None or counts["duplicates"] is None else int(counts["duplicates"])


def _metric_duplicate_count(spark, paths: list[Path]) -> int:
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    duplicate_total = 0
    for path in paths:
        if not DeltaTable.isDeltaTable(spark, str(path)):
            continue
        counts = (
            spark.read.format("delta")
            .load(str(path))
            .groupBy("metric_id")
            .count()
            .filter(F.col("count") > 1)
            .select(F.sum(F.col("count") - F.lit(1)).alias("duplicates"))
            .first()
        )
        if counts is not None and counts["duplicates"] is not None:
            duplicate_total += int(counts["duplicates"])
    return duplicate_total


def _upsert_all_metrics(
    spark,
    evaluation_rows: list[dict[str, Any]],
    config: argparse.Namespace,
    now: datetime,
    *,
    location_ids: set[str],
) -> dict[str, int]:
    hourly = build_hourly_metrics(
        evaluation_rows,
        expected_locations=config.expected_locations,
        minimum_sample=1,
    )
    rolling = build_rolling_metrics(
        evaluation_rows,
        expected_locations=config.expected_locations,
        minimum_samples={24: config.minimum_samples_24h, 168: config.minimum_samples_7d},
        location_ids=location_ids,
    )
    _write_metrics(spark, hourly, config.hourly_metrics_path, computed_at=now)
    _write_metrics(spark, rolling["rolling_global_metrics"], config.rolling_global_path, computed_at=now)
    _write_metrics(spark, rolling["rolling_location_metrics"], config.rolling_location_path, computed_at=now)
    return {
        "hourly_metric_rows_recomputed": len(hourly),
        "rolling_global_metric_rows_recomputed": len(rolling["rolling_global_metrics"]),
        "rolling_location_metric_rows_recomputed": len(rolling["rolling_location_metrics"]),
        "metric_duplicate_rows": _metric_duplicate_count(
            spark,
            [config.hourly_metrics_path, config.rolling_global_path, config.rolling_location_path],
        ),
    }


def _run_evaluation(spark, config: argparse.Namespace, known_locations: set[str], *, now: datetime) -> dict[str, Any]:
    started = time.perf_counter()
    forecasts = _read_forecast_records(spark, config.forecast_path)
    archive_rows = _read_json_records(spark, config.archive_path)
    conflicted_keys = _read_revision_keys(spark, config.revisions_path)
    existing_map, existing_rows = _read_existing_evaluations(spark, config.evaluations_path)
    read_seconds = time.perf_counter() - started

    if config.reference_source != "all":
        source_archive = [row for row in archive_rows if str(row.get("source") or "") == config.reference_source]
        source_feature_keys = {
            (
                str(row.get("event_id") or ""),
                str(row.get("location_id") or ""),
                format_utc_hour(row.get("event_time"), assume_naive_utc=True),
            )
            for row in source_archive
        }
        before_filter_count = len(forecasts)

        def forecast_feature_key(row: Mapping[str, Any]):
            try:
                return (
                    str(row.get("source_event_id") or ""),
                    str(row.get("location_id") or ""),
                    format_utc_hour(row.get("feature_time"), assume_naive_utc=True),
                )
            except (TypeError, ValueError):
                return None

        forecasts = [
            row
            for row in forecasts
            if forecast_feature_key(row) in source_feature_keys
        ]
        archive_rows = source_archive
        conflicted_keys = {key for key in conflicted_keys if key[0] == config.reference_source}
        filtered_forecast_count = before_filter_count - len(forecasts)
    else:
        filtered_forecast_count = 0

    join_started = time.perf_counter()
    result = evaluate_forecasts(
        forecasts,
        archive_rows,
        evaluation_time=now,
        known_location_ids=known_locations,
        conflicted_reference_keys=conflicted_keys,
        existing_evaluations=existing_map,
        cohort_id=config.cohort_id,
        assume_naive_utc=True,
    )
    before_ids = {str(row.get("forecast_id")) for row in existing_rows}
    evaluation_rows = result["evaluations"]
    if config.evaluation_mode != "all":
        evaluation_rows = [row for row in evaluation_rows if row.get("evaluation_mode") == config.evaluation_mode]
    new_evaluation_rows = [row for row in evaluation_rows if str(row.get("forecast_id")) not in before_ids]
    updated_evaluation_rows = [row for row in evaluation_rows if str(row.get("forecast_id")) in before_ids and existing_map.get(str(row.get("forecast_id")), {}).get("status") != row.get("status")]
    join_seconds = time.perf_counter() - join_started
    evaluation_started = time.perf_counter()
    _merge_evaluations(spark, config.evaluations_path, evaluation_rows)
    persisted = _read_json_records(spark, config.evaluations_path)
    duplicate_evaluations = _evaluation_duplicate_count(spark, config.evaluations_path)
    evaluation_seconds = time.perf_counter() - evaluation_started
    aggregate_started = time.perf_counter()
    metric_counts = _upsert_all_metrics(spark, persisted, config, now, location_ids=known_locations)
    aggregation_seconds = time.perf_counter() - aggregate_started
    coverage = calculate_coverage(persisted)
    return {
        "forecast_rows": len(forecasts),
        "forecasts_scanned": len(forecasts),
        "forecasts_filtered_by_reference_source": filtered_forecast_count,
        "forecast_duplicate_rows": result["duplicate_forecast_count"],
        "forecast_ready_count": sum(1 for row in evaluation_rows if row.get("status") == "READY"),
        "evaluation_rows_inserted": len(new_evaluation_rows),
        "evaluation_rows_updated": len(updated_evaluation_rows),
        "evaluation_rows_already_existing": max(0, len(evaluation_rows) - len(new_evaluation_rows) - len(updated_evaluation_rows)),
        "evaluation_rows_total": len(persisted),
        "evaluation_duplicate_rows": duplicate_evaluations,
        "reference_rows_scanned": len(archive_rows),
        "reference_conflict_keys": len(conflicted_keys),
        "reference_duplicate_rows_seen": result["duplicate_observation_count"],
        "invalid_observation_rows_seen": result["invalid_observation_count"],
        "read_seconds": read_seconds,
        "join_seconds": join_seconds,
        "evaluation_seconds": evaluation_seconds,
        "evaluation_rows_per_second": None if evaluation_seconds <= 0 else len(new_evaluation_rows) / evaluation_seconds,
        **coverage,
        **metric_counts,
        "aggregation_seconds": aggregation_seconds,
    }


def _resource_snapshot() -> dict[str, Any]:
    result: dict[str, Any] = {"cpu_seconds": None, "peak_rss_bytes": None}
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        result["cpu_seconds"] = float(usage.ru_utime + usage.ru_stime)
        peak = int(usage.ru_maxrss)
        result["peak_rss_bytes"] = peak if os.name == "nt" else peak * 1024
    except (ImportError, OSError, AttributeError):
        pass
    return result


def _archive_kafka_once(spark, config: argparse.Namespace, known_locations: set[str]) -> dict[str, int]:
    from pyspark.sql import functions as F

    archive_started = time.perf_counter()
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("startingOffsets", os.getenv("WEATHER_MONITORING_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "true")
    )
    if config.max_offsets_per_trigger > 0:
        kafka = kafka.option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
    rows = kafka.load().select(F.col("value"))
    totals = {
        "source_observation_rows": 0,
        "canonical_observation_rows_added": 0,
        "duplicate_observation_rows": 0,
        "reference_conflict_rows": 0,
        "invalid_observation_rows": 0,
    }

    def process_batch(batch_df, batch_id: int) -> None:
        batch_started = time.perf_counter()
        records, malformed = _parse_batch_records(batch_df)
        batch_metrics = _process_observations(
            spark,
            records,
            config=config,
            known_locations=known_locations,
            malformed_count=malformed,
        )
        for name, value in batch_metrics.items():
            totals[name] += int(value)
        print(
            json.dumps(
                {
                    "event": "weather_forecast_monitoring_archive_batch",
                    "batch_id": int(batch_id),
                    **batch_metrics,
                    "archive_seconds": time.perf_counter() - batch_started,
                },
                sort_keys=True,
            ),
            flush=True,
        )

    query = (
        rows.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", str(config.checkpoint_path))
        .queryName("weatherForecastMonitoringV1")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    totals["archive_duration_seconds"] = time.perf_counter() - archive_started
    return totals


def _fetch_and_archive_backfill(spark, config: argparse.Namespace, known_locations: set[str]) -> dict[str, int]:
    from location_catalog import DATASET_NATIONWIDE_63, load_catalog, select_dataset_locations

    locations = select_dataset_locations(load_catalog(config.catalog_path), DATASET_NATIONWIDE_63)
    events = fetch_live_reference_backfill(
        config.reference_backfill_target_time,
        locations,
        endpoint=config.forecast_api_endpoint,
        timeout_seconds=config.backfill_timeout_seconds,
    )
    return _process_observations(spark, events, config=config, known_locations=known_locations)


def _ensure_tables(spark, config: argparse.Namespace) -> None:
    _ensure_delta(spark, config.archive_path, archive_schema())
    _ensure_delta(spark, config.revisions_path, revision_schema())
    _ensure_delta(spark, config.rejections_path, _rejection_schema())
    _ensure_delta(spark, config.evaluations_path, evaluation_schema())
    _ensure_delta(spark, config.hourly_metrics_path, _metric_schema())
    _ensure_delta(spark, config.rolling_global_path, _metric_schema())
    _ensure_delta(spark, config.rolling_location_path, _metric_schema())


def _run_cycle(
    spark,
    config: argparse.Namespace,
    known_locations: set[str],
    *,
    archive: Mapping[str, Any] | None = None,
    cycle_started_at: datetime | None = None,
    cycle_started_perf: float | None = None,
) -> dict[str, Any]:
    started_clock = cycle_started_at or datetime.now(timezone.utc)
    started = cycle_started_perf if cycle_started_perf is not None else time.perf_counter()
    evaluation_clock = datetime.now(timezone.utc)
    evaluation = _run_evaluation(spark, config, known_locations, now=evaluation_clock)
    ended_clock = datetime.now(timezone.utc)
    resources = _resource_snapshot()
    cycle = {
        "cycle_id": str(uuid.uuid4()),
        "mode": config.mode,
        "cohort_id": config.cohort_id,
        "reference_source_filter": config.reference_source,
        "evaluation_mode_filter": config.evaluation_mode,
        "cycle_started_at": started_clock,
        "cycle_completed_at": ended_clock,
        "evaluation_started_at": evaluation_clock,
        "cycle_seconds": time.perf_counter() - started,
        "reference_topic": config.topic,
        "forecast_path": str(config.forecast_path),
        "archive_path": str(config.archive_path),
        "evaluation_path": str(config.evaluations_path),
        **dict(archive or {}),
        **evaluation,
        "resources": resources,
    }
    _append_json_line(config.run_results_dir / "monitoring_cycles.jsonl", cycle)
    print(json.dumps(cycle, sort_keys=True, default=_json_default), flush=True)
    return cycle


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delayed, source-safe V1 weather forecast monitoring")
    parser.add_argument("--mode", choices=("once", "daemon", "backfill"), default=os.getenv("WEATHER_MONITORING_MODE", "once"))
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--forecast-path", type=Path, default=DEFAULT_FORECAST_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--archive-path", type=Path, default=DEFAULT_ARCHIVE_PATH)
    parser.add_argument("--checkpoint-path", type=Path, default=DEFAULT_CHECKPOINT_PATH)
    parser.add_argument("--catalog-path", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument("--results-root", type=Path, default=Path(os.getenv("WEATHER_MONITORING_RESULTS_DIR", "/opt/project/results/forecast-monitoring")))
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--cohort-id", default=os.getenv("WEATHER_MONITORING_COHORT_ID"))
    parser.add_argument("--reference-source", default=DEFAULT_REFERENCE_SOURCE)
    parser.add_argument(
        "--evaluation-mode",
        choices=("all", "LIVE_PROSPECTIVE", "LIVE_SOURCE_BACKFILL", "REPLAY"),
        default=DEFAULT_EVALUATION_MODE,
    )
    parser.add_argument("--interval-seconds", type=int, default=int(os.getenv("WEATHER_MONITORING_INTERVAL_SECONDS", "300")))
    parser.add_argument("--max-offsets-per-trigger", type=int, default=int(os.getenv("WEATHER_MONITORING_MAX_OFFSETS_PER_TRIGGER", "100000")))
    parser.add_argument("--expected-locations", type=int, default=63)
    parser.add_argument("--minimum-samples-24h", type=int, default=24)
    parser.add_argument("--minimum-samples-7d", type=int, default=168)
    parser.add_argument("--reference-backfill-target-time", default=None)
    parser.add_argument("--forecast-api-endpoint", default=os.getenv("WEATHER_MONITORING_FORECAST_API_ENDPOINT", "https://api.open-meteo.com/v1/forecast"))
    parser.add_argument("--backfill-timeout-seconds", type=float, default=60.0)
    args = parser.parse_args(argv)
    args.revisions_path = args.output_root / "reference_revisions"
    args.rejections_path = args.output_root / "rejected_observations"
    args.evaluations_path = args.output_root / "evaluations"
    args.hourly_metrics_path = args.output_root / "hourly_metrics"
    args.rolling_global_path = args.output_root / "rolling_global_metrics"
    args.rolling_location_path = args.output_root / "rolling_location_metrics"
    args.run_results_dir = args.results_root / args.run_id
    if args.interval_seconds < 30:
        parser.error("interval-seconds must be at least 30 seconds")
    if args.max_offsets_per_trigger < 0:
        parser.error("max-offsets-per-trigger cannot be negative")
    if args.expected_locations <= 0 or args.minimum_samples_24h <= 0 or args.minimum_samples_7d <= 0:
        parser.error("expected location and minimum sample counts must be positive")
    if args.reference_backfill_target_time and args.mode != "backfill":
        parser.error("reference-backfill-target-time is allowed only with --mode backfill")
    if not args.reference_source.strip():
        parser.error("reference-source must be 'all' or a non-empty source name")
    if args.reference_backfill_target_time and args.reference_source not in {"all", LIVE_REFERENCE_SOURCE}:
        parser.error("reference backfill always fetches OPEN_METEO_LIVE_HOURLY references")
    if args.reference_backfill_target_time:
        from monitoring.evaluation_contract import parse_utc_hour

        args.reference_backfill_target_time = parse_utc_hour(args.reference_backfill_target_time)
    return args


def _spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("weather-forecast-monitoring-v1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("WEATHER_MONITORING_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )


def main(argv: list[str] | None = None) -> int:
    from location_catalog import DATASET_NATIONWIDE_63, load_catalog, select_dataset_locations

    config = _arguments(argv)
    catalog = select_dataset_locations(load_catalog(config.catalog_path), DATASET_NATIONWIDE_63)
    known_locations = {str(row["location_id"]) for row in catalog}
    if len(known_locations) != config.expected_locations:
        raise ValueError(f"canonical location catalog has {len(known_locations)} IDs; expected {config.expected_locations}")

    spark = _spark_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        _ensure_tables(spark, config)
        if config.mode in {"once", "daemon"}:
            while True:
                cycle_started_perf = time.perf_counter()
                cycle_started_at = datetime.now(timezone.utc)
                archive_stats = _archive_kafka_once(spark, config, known_locations)
                _run_cycle(
                    spark,
                    config,
                    known_locations,
                    archive=archive_stats,
                    cycle_started_at=cycle_started_at,
                    cycle_started_perf=cycle_started_perf,
                )
                if config.mode == "once":
                    break
                remaining = max(0.0, config.interval_seconds - (time.perf_counter() - cycle_started_perf))
                time.sleep(remaining)
        else:
            cycle_started_perf = time.perf_counter()
            cycle_started_at = datetime.now(timezone.utc)
            archive_stats = {
                "source_observation_rows": 0,
                "canonical_observation_rows_added": 0,
                "duplicate_observation_rows": 0,
                "reference_conflict_rows": 0,
                "invalid_observation_rows": 0,
                "archive_duration_seconds": 0.0,
            }
            if config.reference_backfill_target_time is not None:
                archive_started = time.perf_counter()
                archive_stats = _fetch_and_archive_backfill(spark, config, known_locations)
                archive_stats["archive_duration_seconds"] = time.perf_counter() - archive_started
            _run_cycle(
                spark,
                config,
                known_locations,
                archive=archive_stats,
                cycle_started_at=cycle_started_at,
                cycle_started_perf=cycle_started_perf,
            )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
