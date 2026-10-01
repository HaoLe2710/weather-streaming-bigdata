from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from typing import Any


PROJECT_ROOT = Path("/opt/project")
sys.path.insert(0, str(PROJECT_ROOT / "historical"))

from location_catalog import load_catalog, select_dataset_locations  # noqa: E402
from ml.streaming_inference.contract import (  # noqa: E402
    FEATURE_LIST_SHA256,
    FEATURE_SET_ID,
    MODEL_ID,
    MODEL_SHA256,
    load_feature_contract,
    sha256_file,
)
from ml.streaming_inference.model_loader import default_model_path  # noqa: E402
from ml.streaming_inference.online_features import (  # noqa: E402
    build_online_feature_series,
    make_forecast_record,
    parse_utc_hour,
)


INPUT_TOPIC = os.getenv("WEATHER_INFERENCE_TOPIC", "weather.hourly.observations.v1")
BOOTSTRAP_SERVERS = os.getenv("WEATHER_INFERENCE_BOOTSTRAP_SERVERS", "broker:19092")
SOURCE_DELTA_PATH = os.getenv(
    "WEATHER_INFERENCE_REPLAY_SOURCE",
    "/opt/project/data/historical/weather_hourly_vn63",
)
OUTPUT_ROOT = Path(os.getenv("WEATHER_INFERENCE_OUTPUT_ROOT", "/opt/project/data/streaming/weather_forecast_xgboost_v1"))
FORECAST_PATH = Path(os.getenv("WEATHER_INFERENCE_FORECAST_PATH", str(OUTPUT_ROOT / "forecasts")))
STATE_PATH = Path(os.getenv("WEATHER_INFERENCE_STATE_PATH", str(OUTPUT_ROOT / "state_hourly_observations")))
REJECTION_PATH = Path(os.getenv("WEATHER_INFERENCE_REJECTION_PATH", str(OUTPUT_ROOT / "rejected_observations")))
CHECKPOINT_PATH = Path(
    os.getenv(
        "WEATHER_INFERENCE_CHECKPOINT",
        "/opt/project/data/checkpoints/weather_forecast_xgboost_v1",
    )
)
RUN_ID = os.getenv("WEATHER_INFERENCE_RUN_ID", "local-streaming-inference-v1")
RUN_RESULTS = Path(os.getenv("WEATHER_INFERENCE_RESULTS_DIR", f"/opt/project/results/streaming-inference/{RUN_ID}"))
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("WEATHER_INFERENCE_MAX_OFFSETS_PER_TRIGGER", "100000"))
HISTORY_RETAIN_HOURS = int(os.getenv("WEATHER_INFERENCE_HISTORY_RETAIN_HOURS", "48"))
FORECAST_HORIZON_HOURS = 1
CATALOG_PATH = Path(os.getenv("WEATHER_INFERENCE_CATALOG_PATH", "/opt/project/historical/locations.json"))

WEATHER_COLUMNS = (
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "pressure_hpa",
    "wind_speed_kmh",
    "wind_gust_kmh",
)
STATE_COLUMNS = (
    "location_id",
    "event_time",
    "event_id",
    "city",
    "latitude",
    "longitude",
    *WEATHER_COLUMNS,
    "weather_code",
    "source",
    "payload_hash",
)
OBSERVATION_JSON_FIELDS = (
    "event_id",
    "location_id",
    "city",
    "latitude",
    "longitude",
    "event_time",
    *WEATHER_COLUMNS,
    "weather_code",
    "source",
)


def _worker_contract():
    global _WORKER_CONTRACT
    try:
        return _WORKER_CONTRACT
    except NameError:
        _WORKER_CONTRACT = load_feature_contract()
        return _WORKER_CONTRACT


def _to_utc_datetime(value: Any) -> datetime:
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _spark_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _score_location(pdf):
    """Pandas grouped map: one bounded per-location state slice, batched Booster inference."""
    import os
    import pandas as pd

    if pdf.empty:
        return pd.DataFrame()
    pdf = pdf.sort_values("event_time", kind="mergesort")
    records = pdf.to_dict(orient="records")
    observations = []
    candidate_times = set()
    event_rows: dict[datetime, dict[str, Any]] = {}
    location_id = str(records[0]["location_id"])
    for row in records:
        instant = _to_utc_datetime(row["event_time"])
        observation = {
            "event_id": None if pd.isna(row.get("event_id")) else str(row.get("event_id")),
            "location_id": location_id,
            "event_time": instant,
            "latitude": None if pd.isna(row.get("latitude")) else float(row["latitude"]),
            "longitude": None if pd.isna(row.get("longitude")) else float(row["longitude"]),
            **{
                name: None if pd.isna(row.get(name)) else float(row[name])
                for name in WEATHER_COLUMNS
            },
        }
        observations.append(observation)
        event_rows[instant] = row
        if bool(row.get("is_candidate", False)):
            candidate_times.add(instant)

    feature_started = time.perf_counter()
    results = build_online_feature_series(observations, contract=_worker_contract())
    feature_seconds = time.perf_counter() - feature_started
    result_by_time = {result.feature_time: result for result in results}
    ready_results = [
        result_by_time[instant]
        for instant in sorted(candidate_times)
        if instant in result_by_time and result_by_time[instant].ready
    ]
    prediction_seconds = 0.0
    predictions = []
    worker_loads = 0
    if ready_results:
        from ml.streaming_inference.model_loader import model_load_count, predict_feature_matrix

        prediction_started = time.perf_counter()
        predictions = predict_feature_matrix(
            [result.values for result in ready_results],
            ready_results[0].feature_names,
        ).tolist()
        prediction_seconds = time.perf_counter() - prediction_started
        worker_loads = model_load_count()

    from ml.streaming_inference.model_loader import model_load_count

    worker_loads = max(worker_loads, model_load_count())
    inference_time = datetime.now(timezone.utc)
    forecast_by_time = {}
    for result, prediction in zip(ready_results, predictions, strict=True):
        source_row = event_rows[result.feature_time]
        forecast_by_time[result.feature_time] = make_forecast_record(
            location_id=location_id,
            feature_time=result.feature_time,
            prediction_temperature_c=float(prediction),
            source_event_id=None if pd.isna(source_row.get("event_id")) else str(source_row.get("event_id")),
            inference_time=inference_time,
        )

    output = []
    for instant in sorted(candidate_times):
        result = result_by_time.get(instant)
        if result is None:
            status = "CURRENT_OBSERVATION_MISSING"
            forecast = None
        else:
            status = result.status
            forecast = forecast_by_time.get(instant)
        output.append(
            {
                "location_id": location_id,
                "feature_time": _spark_utc(instant),
                "status": status,
                "forecast_id": None if forecast is None else forecast["forecast_id"],
                "target_time": None if forecast is None else _spark_utc(forecast["target_time"]),
                "prediction_temperature_c": None if forecast is None else forecast["prediction_temperature_c"],
                "model_id": None if forecast is None else MODEL_ID,
                "model_sha256": None if forecast is None else MODEL_SHA256,
                "feature_set_id": None if forecast is None else FEATURE_SET_ID,
                "feature_list_sha256": None if forecast is None else FEATURE_LIST_SHA256,
                "source_event_id": None if forecast is None else forecast["source_event_id"],
                "inference_time": None if forecast is None else _spark_utc(forecast["inference_time"]),
                "worker_pid": int(os.getpid()),
                "worker_model_loads": int(worker_loads),
                "feature_build_seconds": float(feature_seconds),
                "prediction_seconds": float(prediction_seconds),
            }
        )
    return pd.DataFrame(output)


def _jsonl_append(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n")


def _canonical_locations() -> list[dict[str, Any]]:
    catalog = load_catalog(CATALOG_PATH)
    locations = select_dataset_locations(catalog, "NATIONWIDE_63")
    if len(locations) != 63:
        raise ValueError(f"canonical inference catalog must contain 63 locations, found {len(locations)}")
    return locations


def _spark_session(app_name: str):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("WEATHER_INFERENCE_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )


def publish_nationwide_replay(spark, topic: str = INPUT_TOPIC, replay_hours: int = 72) -> dict[str, Any]:
    """Publish a deterministic, contiguous NATIONWIDE_63 window to its own Kafka topic."""
    from pyspark.sql import functions as F
    from datetime import timedelta

    from historical.location_catalog import expected_records

    if replay_hours < 25:
        raise ValueError("replay window must be at least 25 hours to exercise the first forecast")
    source = spark.read.format("delta").load(SOURCE_DELTA_PATH)
    expected = expected_records(63)
    stats = source.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("event_id").alias("event_ids"),
        F.countDistinct(F.struct("location_id", "event_time")).alias("location_hours"),
        F.min("event_time").alias("min_time"),
        F.max("event_time").alias("max_time"),
    ).first()
    actual_ids = sorted(row[0] for row in source.select("location_id").distinct().collect())
    expected_ids = sorted(item["location_id"] for item in _canonical_locations())
    per_location = {row["location_id"]: row["count"] for row in source.groupBy("location_id").count().collect()}
    if (
        stats["rows"] != expected
        or stats["event_ids"] != expected
        or stats["location_hours"] != expected
        or actual_ids != expected_ids
        or len(per_location) != 63
        or set(per_location.values()) != {52_608}
    ):
        raise ValueError(
            "NATIONWIDE_63 Delta source contract failed: "
            f"rows={stats['rows']}, event_ids={stats['event_ids']}, location_hours={stats['location_hours']}, "
            f"locations={len(actual_ids)}, counts={sorted(set(per_location.values()))}"
        )

    replay_start = stats["min_time"]
    replay_end = replay_start + timedelta(hours=replay_hours)
    replay_source = source.filter(
        (F.col("event_time") >= F.lit(replay_start))
        & (F.col("event_time") < F.lit(replay_end))
    )
    replay_stats = replay_source.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(F.struct("location_id", "event_time")).alias("location_hours"),
        F.countDistinct("location_id").alias("locations"),
        F.countDistinct("event_time").alias("hours"),
        F.min("event_time").alias("min_time"),
        F.max("event_time").alias("max_time"),
    ).first()
    expected_replay_rows = replay_hours * 63
    replay_per_location = {
        row["location_id"]: row["count"]
        for row in replay_source.groupBy("location_id").count().collect()
    }
    coordinate_versions = replay_source.groupBy("location_id").agg(
        F.countDistinct(F.struct("latitude", "longitude")).alias("coordinate_versions")
    )
    inconsistent_coordinates = coordinate_versions.filter(F.col("coordinate_versions") != 1).count()
    if (
        replay_stats["rows"] != expected_replay_rows
        or replay_stats["location_hours"] != expected_replay_rows
        or replay_stats["locations"] != 63
        or replay_stats["hours"] != replay_hours
        or set(replay_per_location) != set(expected_ids)
        or set(replay_per_location.values()) != {replay_hours}
        or inconsistent_coordinates != 0
    ):
        raise ValueError(
            "deterministic NATIONWIDE_63 replay window is incomplete or non-hourly: "
            f"rows={replay_stats['rows']}, location_hours={replay_stats['location_hours']}, "
            f"locations={replay_stats['locations']}, hours={replay_stats['hours']}, "
            f"per_location_counts={sorted(set(replay_per_location.values()))}, "
            f"locations_with_coordinate_changes={inconsistent_coordinates}"
        )

    hourly = replay_source.select(
        "event_id",
        "location_id",
        "city",
        "latitude",
        "longitude",
        F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
        *WEATHER_COLUMNS,
        "weather_code",
        F.lit("NATIONWIDE_63_DELTA_REPLAY").alias("source"),
    )
    payload = F.to_json(
        F.struct(*[F.col(name) for name in OBSERVATION_JSON_FIELDS]),
        options={"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss'Z'", "timeZone": "UTC"},
    )
    kafka = (
        hourly.repartition(8, "location_id")
        .sortWithinPartitions("location_id", "event_time")
        .select(
            F.col("location_id").cast("binary").alias("key"),
            payload.cast("binary").alias("value"),
        )
    )
    kafka.write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS).option("topic", topic).save()
    report = {
        "status": "PASS",
        "topic": topic,
        "source_delta_path": SOURCE_DELTA_PATH,
        "dataset_id": "NATIONWIDE_63",
        "source_dataset_rows": int(stats["rows"]),
        "source_dataset_location_hours": int(stats["location_hours"]),
        "published_rows": int(replay_stats["rows"]),
        "unique_location_hours": int(replay_stats["location_hours"]),
        "location_count": int(replay_stats["locations"]),
        "replay_hours": replay_hours,
        "rows_per_location": replay_hours,
        "event_time_min_utc": replay_stats["min_time"].isoformat() + "Z",
        "event_time_max_utc": replay_stats["max_time"].isoformat() + "Z",
        "publish_order": "sorted by location_id and event_time within each Kafka-key partition",
    }
    _jsonl_append(RUN_RESULTS / "replay_publish.jsonl", report)
    print(json.dumps(report, indent=2))
    return report


def _empty_delta(spark, path: Path, schema) -> None:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, str(path)):
        path.parent.mkdir(parents=True, exist_ok=True)
        spark.createDataFrame([], schema).write.format("delta").mode("errorifexists").save(str(path))


def _merge_delta(spark, path: Path, source_df, condition: str) -> None:
    from delta.tables import DeltaTable

    DeltaTable.forPath(spark, str(path)).alias("target").merge(source_df.alias("source"), condition).whenNotMatchedInsertAll().execute()


def _observation_schemas():
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    state_schema = StructType(
        [
            StructField("location_id", StringType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("event_id", StringType(), True),
            StructField("city", StringType(), True),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            *[StructField(name, DoubleType(), False) for name in WEATHER_COLUMNS],
            StructField("weather_code", IntegerType(), True),
            StructField("source", StringType(), True),
            StructField("payload_hash", StringType(), False),
        ]
    )
    forecast_schema = StructType(
        [
            StructField("forecast_id", StringType(), False),
            StructField("location_id", StringType(), False),
            StructField("feature_time", TimestampType(), False),
            StructField("target_time", TimestampType(), False),
            StructField("prediction_temperature_c", DoubleType(), False),
            StructField("model_id", StringType(), False),
            StructField("model_sha256", StringType(), False),
            StructField("feature_set_id", StringType(), False),
            StructField("feature_list_sha256", StringType(), False),
            StructField("source_event_id", StringType(), True),
            StructField("inference_time", TimestampType(), False),
        ]
    )
    score_schema = StructType(
        [
            StructField("location_id", StringType(), False),
            StructField("feature_time", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("forecast_id", StringType(), True),
            StructField("target_time", TimestampType(), True),
            StructField("prediction_temperature_c", DoubleType(), True),
            StructField("model_id", StringType(), True),
            StructField("model_sha256", StringType(), True),
            StructField("feature_set_id", StringType(), True),
            StructField("feature_list_sha256", StringType(), True),
            StructField("source_event_id", StringType(), True),
            StructField("inference_time", TimestampType(), True),
            StructField("worker_pid", LongType(), False),
            StructField("worker_model_loads", IntegerType(), False),
            StructField("feature_build_seconds", DoubleType(), False),
            StructField("prediction_seconds", DoubleType(), False),
        ]
    )
    rejection_schema = StructType(
        [
            StructField("reject_id", StringType(), False),
            StructField("topic", StringType(), True),
            StructField("partition", IntegerType(), True),
            StructField("offset", LongType(), True),
            StructField("location_id", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("reason", StringType(), False),
            StructField("raw_json", StringType(), True),
            StructField("rejected_at", TimestampType(), False),
        ]
    )
    return state_schema, forecast_schema, score_schema, rejection_schema


def _persist_rejections(spark, rejections, schema) -> int:
    from pyspark.sql import functions as F

    if rejections is None:
        return 0
    ready = rejections
    count = ready.count()
    if not count:
        return 0
    _empty_delta(spark, REJECTION_PATH, schema)
    prepared = (
        ready.withColumn(
            "reject_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.coalesce(F.col("topic"), F.lit("")),
                    F.coalesce(F.col("partition").cast("string"), F.lit("")),
                    F.coalesce(F.col("offset").cast("string"), F.lit("")),
                    F.coalesce(F.col("location_id"), F.lit("")),
                    F.coalesce(F.col("event_time").cast("string"), F.lit("")),
                    F.col("reason"),
                ),
                256,
            ),
        )
        .withColumn("rejected_at", F.current_timestamp())
        .select(*schema.fieldNames())
        .dropDuplicates(["reject_id"])
    )
    _merge_delta(spark, REJECTION_PATH, prepared, "target.reject_id = source.reject_id")
    return count


def _records_to_rejections(frame, reason: str):
    from pyspark.sql import functions as F

    return frame.select(
        "topic",
        "partition",
        "offset",
        "location_id",
        F.col("parsed_event_time").alias("event_time"),
        F.lit(reason).alias("reason"),
        "raw_json",
    )


def _write_metrics(metrics: dict[str, Any]) -> None:
    _jsonl_append(RUN_RESULTS / "inference_batches.jsonl", metrics)
    print(json.dumps(metrics, sort_keys=True))


def process_microbatch(batch_df, batch_id: int, spark) -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    started = time.perf_counter()
    source_rows = batch_df.count()
    state_schema, forecast_schema, score_schema, rejection_schema = _observation_schemas()
    _empty_delta(spark, STATE_PATH, state_schema)
    _empty_delta(spark, FORECAST_PATH, forecast_schema)
    previous_state = spark.read.format("delta").load(str(STATE_PATH)).select(*STATE_COLUMNS).cache()

    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    input_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("location_id", StringType(), True),
            StructField("city", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("event_time", StringType(), True),
            *[StructField(name, DoubleType(), True) for name in WEATHER_COLUMNS],
            StructField("weather_code", IntegerType(), True),
            StructField("source", StringType(), True),
        ]
    )
    parsed = (
        batch_df.select(
            "topic",
            "partition",
            "offset",
            F.col("value").cast("string").alias("raw_json"),
        )
        .withColumn("data", F.from_json("raw_json", input_schema))
        .select("topic", "partition", "offset", "raw_json", "data.*")
        .withColumn(
            "parsed_event_time",
            F.try_to_timestamp("event_time", F.lit("yyyy-MM-dd'T'HH:mm:ssX")),
        )
    )
    catalog_rows = [(item["location_id"], True) for item in _canonical_locations()]
    catalog_df = spark.createDataFrame(catalog_rows, ["location_id", "is_canonical_location"])
    joined = parsed.join(catalog_df, "location_id", "left")
    required_numeric = ["latitude", "longitude", *WEATHER_COLUMNS]
    finite = F.lit(True)
    for name in required_numeric:
        finite = finite & F.col(name).isNotNull() & ~F.isnan(F.col(name)) & (F.abs(F.col(name)) < F.lit(float("inf")))
    classified = (
        joined.withColumn(
            "reject_reason",
            F.when(F.col("is_canonical_location").isNull(), F.lit("UNKNOWN_LOCATION"))
            .when(F.col("parsed_event_time").isNull(), F.lit("INVALID_EVENT_TIME"))
            .when(F.col("parsed_event_time").cast("long") % F.lit(3600) != F.lit(0), F.lit("NON_HOURLY_EVENT_TIME"))
            .when(~F.col("latitude").between(-90.0, 90.0) | ~F.col("longitude").between(-180.0, 180.0), F.lit("INVALID_COORDINATE"))
            .when(~finite, F.lit("INVALID_WEATHER_VALUE")),
        )
        .withColumn("event_time", F.col("parsed_event_time"))
        .withColumn(
            "payload_hash",
            F.sha2(
                F.to_json(F.struct("latitude", "longitude", *WEATHER_COLUMNS)),
                256,
            ),
        )
        .cache()
    )
    invalid = classified.filter(F.col("reject_reason").isNotNull()).cache()
    invalid_counts = {row["reject_reason"]: int(row["count"]) for row in invalid.groupBy("reject_reason").count().collect()}
    rejection_parts = [
        _records_to_rejections(invalid.filter(F.col("reject_reason") == reason), reason)
        for reason in sorted(invalid_counts)
    ]

    valid = classified.filter(F.col("reject_reason").isNull()).cache()
    group_stats = valid.groupBy("location_id", "event_time").agg(
        F.count(F.lit(1)).alias("key_rows"),
        F.countDistinct("payload_hash").alias("payload_versions"),
    ).cache()
    duplicate_stats = group_stats.agg(
        F.coalesce(F.sum(F.when(F.col("key_rows") > 1, F.col("key_rows") - 1).otherwise(0)), F.lit(0)).alias("duplicate_rows"),
        F.coalesce(F.sum(F.when(F.col("payload_versions") > 1, F.lit(1)).otherwise(0)), F.lit(0)).alias("conflict_keys"),
    ).first()
    duplicate_rows = int(duplicate_stats["duplicate_rows"])
    conflict_key_count = int(duplicate_stats["conflict_keys"])
    conflict_keys = group_stats.filter(F.col("payload_versions") > 1).select("location_id", "event_time")
    if conflict_key_count:
        conflict_records = valid.join(conflict_keys, ["location_id", "event_time"], "inner")
        rejection_parts.append(_records_to_rejections(conflict_records, "DUPLICATE_CONFLICT"))
    unique_keys = group_stats.filter(F.col("payload_versions") == 1).select("location_id", "event_time")
    candidate_input = valid.join(unique_keys, ["location_id", "event_time"], "inner")
    dedupe_window = Window.partitionBy("location_id", "event_time").orderBy(F.col("event_id").asc_nulls_last(), F.col("offset").asc())
    incoming = (
        candidate_input.withColumn("_rank", F.row_number().over(dedupe_window))
        .filter(F.col("_rank") == 1)
        .select(*STATE_COLUMNS)
        .cache()
    )

    existing_conflicts = (
        incoming.alias("new")
        .join(previous_state.alias("old"), ["location_id", "event_time"], "inner")
        .filter(F.col("new.payload_hash") != F.col("old.payload_hash"))
        .select(*[F.col(f"new.{name}").alias(name) for name in STATE_COLUMNS])
        .cache()
    )
    existing_conflict_keys = existing_conflicts.select("location_id", "event_time").dropDuplicates()
    if existing_conflicts.limit(1).count():
        rejection_parts.append(
            existing_conflicts.withColumn("topic", F.lit(INPUT_TOPIC))
            .withColumn("partition", F.lit(None).cast("int"))
            .withColumn("offset", F.lit(None).cast("long"))
            .withColumn("parsed_event_time", F.col("event_time"))
            .withColumn("raw_json", F.lit(None).cast("string"))
            .transform(lambda frame: _records_to_rejections(frame, "DUPLICATE_CONFLICT"))
        )
    exact_existing_keys = (
        incoming.alias("new")
        .join(previous_state.alias("old"), ["location_id", "event_time"], "inner")
        .filter(F.col("new.payload_hash") == F.col("old.payload_hash"))
        .select("location_id", "event_time")
        .dropDuplicates()
    )
    exact_existing_count = exact_existing_keys.count()
    non_conflicting = (
        incoming.join(existing_conflict_keys, ["location_id", "event_time"], "left_anti")
        .join(exact_existing_keys, ["location_id", "event_time"], "left_anti")
    )

    latest_state = previous_state.groupBy("location_id").agg(F.max("event_time").alias("latest_event_time"))
    with_latest = non_conflicting.join(latest_state, "location_id", "left")
    late = with_latest.filter(
        F.col("latest_event_time").isNotNull()
        & (F.col("event_time") < F.col("latest_event_time") - F.expr(f"INTERVAL {HISTORY_RETAIN_HOURS} HOURS"))
    )
    late_keys = late.select("location_id", "event_time").dropDuplicates()
    if late.limit(1).count():
        late_rejections = (
            late.withColumn("topic", F.lit(INPUT_TOPIC))
            .withColumn("partition", F.lit(None).cast("int"))
            .withColumn("offset", F.lit(None).cast("long"))
            .withColumn("parsed_event_time", F.col("event_time"))
            .withColumn("raw_json", F.lit(None).cast("string"))
            .transform(lambda frame: _records_to_rejections(frame, "LATE_BEYOND_STATE_RETENTION"))
        )
        rejection_parts.append(late_rejections)
    accepted = non_conflicting.join(late_keys, ["location_id", "event_time"], "left_anti").select(*STATE_COLUMNS).cache()
    accepted_count = accepted.count()
    late_count = late.count()
    existing_conflict_count = existing_conflicts.count()

    reject_count = 0
    for rejection_frame in rejection_parts:
        reject_count += _persist_rejections(spark, rejection_frame, rejection_schema)

    if accepted_count:
        bounds = accepted.groupBy("location_id").agg(
            F.min("event_time").alias("dirty_start"),
            F.max("event_time").alias("dirty_end"),
        )
        history = (
            previous_state.withColumn("_source_priority", F.lit(0))
            .unionByName(accepted.withColumn("_source_priority", F.lit(1)))
            .withColumn(
                "_rank",
                F.row_number().over(
                    Window.partitionBy("location_id", "event_time").orderBy(F.col("_source_priority").desc())
                ),
            )
            .filter(F.col("_rank") == 1)
            .select(*STATE_COLUMNS)
        )
        candidate_keys = (
            history.join(bounds, "location_id", "inner")
            .filter(F.col("event_time").between(F.col("dirty_start"), F.col("dirty_end") + F.expr("INTERVAL 24 HOURS")))
            .select("location_id", "event_time")
            .unionByName(accepted.select("location_id", "event_time"))
            .dropDuplicates()
        )
        candidates = candidate_keys.withColumn("is_candidate", F.lit(True))
        scoring_input = (
            history.join(bounds.select("location_id"), "location_id", "inner")
            .join(candidates, ["location_id", "event_time"], "left")
            .withColumn("is_candidate", F.coalesce(F.col("is_candidate"), F.lit(False)))
            .repartition(8, "location_id")
            .select(*STATE_COLUMNS, "is_candidate")
        )
        scored = scoring_input.groupBy("location_id").applyInPandas(_score_location, schema=score_schema).cache()
        status_counts = {row["status"]: int(row["count"]) for row in scored.groupBy("status").count().collect()}
        per_location_metrics = (
            scored.groupBy("location_id")
            .agg(
                F.max("feature_build_seconds").alias("feature_build_seconds"),
                F.max("prediction_seconds").alias("prediction_seconds"),
                F.max("worker_model_loads").alias("worker_model_loads"),
                F.max("worker_pid").alias("worker_pid"),
            )
            .collect()
        )
        if not per_location_metrics and status_counts:
            raise RuntimeError("inference status rows were produced but worker metrics were not returned")
        predictions = (
            scored.filter(F.col("status") == "READY")
            .select(
                "forecast_id",
                "location_id",
                F.col("feature_time"),
                "target_time",
                "prediction_temperature_c",
                "model_id",
                "model_sha256",
                "feature_set_id",
                "feature_list_sha256",
                "source_event_id",
                "inference_time",
            )
            .cache()
        )
        predicted_count = predictions.count()
        existing_ids = spark.read.format("delta").load(str(FORECAST_PATH)).select("forecast_id")
        duplicate_output_rows = predictions.join(existing_ids, "forecast_id", "inner").count()
        sink_started = time.perf_counter()
        _merge_delta(spark, FORECAST_PATH, predictions, "target.forecast_id = source.forecast_id")
        sink_seconds = time.perf_counter() - sink_started

        _merge_delta(
            spark,
            STATE_PATH,
            accepted,
            "target.location_id = source.location_id AND target.event_time = source.event_time",
        )
        state_table = spark.read.format("delta").load(str(STATE_PATH))
        max_by_location = state_table.groupBy("location_id").agg(F.max("event_time").alias("latest_event_time"))
        expired = state_table.alias("s").join(max_by_location.alias("m"), "location_id").filter(
            F.col("s.event_time") < F.col("m.latest_event_time") - F.expr(f"INTERVAL {HISTORY_RETAIN_HOURS} HOURS")
        ).select("s.location_id", "s.event_time")
        from delta.tables import DeltaTable

        expired_keys = expired.cache()
        expired_count = expired_keys.count()
        if expired_count:
            DeltaTable.forPath(spark, str(STATE_PATH)).alias("target").merge(
                expired_keys.alias("source"),
                "target.location_id = source.location_id AND target.event_time = source.event_time",
            ).whenMatchedDelete().execute()

        prediction_times = sorted(float(row["prediction_seconds"]) for row in per_location_metrics)
        feature_times = sorted(float(row["feature_build_seconds"]) for row in per_location_metrics)
        loads_by_worker: dict[int, int] = {}
        for row in per_location_metrics:
            worker_pid = int(row["worker_pid"])
            loads_by_worker[worker_pid] = max(
                loads_by_worker.get(worker_pid, 0),
                max(0, int(row["worker_model_loads"])),
            )
        workers = sorted(loads_by_worker)
        model_load_count = sum(loads_by_worker.values())
        metrics = {
            "batch_id": int(batch_id),
            "source_rows": int(source_rows),
            "hourly_canonical_rows": int(accepted_count),
            "rejected_rows": int(sum(invalid_counts.values()) + conflict_key_count + existing_conflict_count + late_count),
            "reject_reasons": invalid_counts,
            "duplicate_input_rows": duplicate_rows + int(exact_existing_count),
            "duplicate_conflict_keys": int(conflict_key_count + existing_conflict_count),
            "history_too_late_rows": int(late_count),
            "feature_ready_rows": int(status_counts.get("READY", 0)),
            "insufficient_history_rows": int(status_counts.get("INSUFFICIENT_HISTORY", 0)),
            "history_gap_rows": int(status_counts.get("HISTORY_GAP", 0)),
            "invalid_feature_rows": int(status_counts.get("INVALID_FEATURES", 0)),
            "predicted_rows": int(predicted_count),
            "duplicate_output_rows": int(duplicate_output_rows),
            "new_forecast_rows": int(predicted_count - duplicate_output_rows),
            "model_load_count_across_observed_workers": int(model_load_count),
            "model_loads_by_worker_pid": {str(pid): loads_by_worker[pid] for pid in workers},
            "python_worker_count": len(workers),
            "python_worker_pids": workers,
            "feature_build_seconds_median_per_location": _percentile(feature_times, 0.5),
            "feature_build_seconds_p95_per_location": _percentile(feature_times, 0.95),
            "feature_build_seconds_max_per_location": max(feature_times, default=0.0),
            "prediction_seconds_median_per_location": _percentile(prediction_times, 0.5),
            "prediction_seconds_p95_per_location": _percentile(prediction_times, 0.95),
            "prediction_seconds_max_per_location": max(prediction_times, default=0.0),
            "sink_seconds": sink_seconds,
            "rejection_sink_rows": int(reject_count),
            "expired_state_rows": int(expired_count),
            "state_rows_retained": int(spark.read.format("delta").load(str(STATE_PATH)).count()),
            "total_batch_seconds": time.perf_counter() - started,
        }
        _write_metrics(metrics)
        scored.unpersist()
        predictions.unpersist()
    else:
        metrics = {
            "batch_id": int(batch_id),
            "source_rows": int(source_rows),
            "hourly_canonical_rows": 0,
            "rejected_rows": int(sum(invalid_counts.values()) + conflict_key_count),
            "reject_reasons": invalid_counts,
            "duplicate_input_rows": duplicate_rows + int(exact_existing_count),
            "duplicate_conflict_keys": int(conflict_key_count),
            "history_too_late_rows": 0,
            "feature_ready_rows": 0,
            "insufficient_history_rows": 0,
            "history_gap_rows": 0,
            "invalid_feature_rows": 0,
            "predicted_rows": 0,
            "duplicate_output_rows": 0,
            "new_forecast_rows": 0,
            "model_load_count_across_observed_workers": 0,
            "python_worker_pids": [],
            "feature_build_seconds_median_per_location": 0.0,
            "feature_build_seconds_p95_per_location": 0.0,
            "feature_build_seconds_max_per_location": 0.0,
            "prediction_seconds_median_per_location": 0.0,
            "prediction_seconds_p95_per_location": 0.0,
            "prediction_seconds_max_per_location": 0.0,
            "sink_seconds": 0.0,
            "rejection_sink_rows": int(reject_count),
            "expired_state_rows": 0,
            "state_rows_retained": int(previous_state.count()),
            "total_batch_seconds": time.perf_counter() - started,
        }
        _write_metrics(metrics)

    for frame in (previous_state, parsed, classified, invalid, valid, group_stats, incoming, existing_conflicts, accepted):
        try:
            frame.unpersist()
        except Exception:
            pass


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    index = int((len(values) - 1) * quantile + 0.5)
    return values[index]


def start_stream(spark, *, available_now: bool, topic: str = INPUT_TOPIC) -> None:
    if not CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _worker_contract()
    model_path = default_model_path()
    configured_sha = os.getenv("WEATHER_FORECAST_MODEL_SHA256", MODEL_SHA256)
    if configured_sha != MODEL_SHA256:
        raise ValueError(
            f"configured model SHA must match frozen V1 contract: expected {MODEL_SHA256}, found {configured_sha}"
        )
    actual_sha = sha256_file(model_path)
    if actual_sha != MODEL_SHA256:
        raise ValueError(f"model SHA mismatch before stream start: expected {MODEL_SHA256}, found {actual_sha}")
    if model_path.stat().st_size != 51_662_443:
        raise ValueError(f"model byte count mismatch before stream start: {model_path.stat().st_size}")

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", os.getenv("WEATHER_INFERENCE_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "true")
    )
    if MAX_OFFSETS_PER_TRIGGER > 0:
        stream = stream.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    source = stream.load()
    writer = (
        source.writeStream.foreachBatch(lambda frame, batch_id: process_microbatch(frame, batch_id, spark))
        .option("checkpointLocation", str(CHECKPOINT_PATH))
        .queryName("weatherForecastXgboostV1")
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=os.getenv("WEATHER_INFERENCE_TRIGGER_INTERVAL", "1 minute"))
    query = writer.start()
    query.awaitTermination()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Frozen XGBoost V1 hourly streaming inference")
    parser.add_argument("--mode", choices=("stream", "publish-replay"), default="stream")
    parser.add_argument("--topic", default=INPUT_TOPIC)
    parser.add_argument("--hours", type=int, default=72, help="contiguous hours to publish for NATIONWIDE_63 replay")
    parser.add_argument("--available-now", action="store_true", default=os.getenv("WEATHER_INFERENCE_AVAILABLE_NOW", "false").lower() in {"1", "true", "yes"})
    args = parser.parse_args(argv)
    contract = load_feature_contract()
    if len(contract.feature_names) != 73 or contract.feature_list_sha256 != FEATURE_LIST_SHA256:
        raise ValueError("inference feature contract failed startup verification")
    spark = _spark_session("weather-streaming-inference-v1")
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.mode == "publish-replay":
            publish_nationwide_replay(spark, args.topic, args.hours)
        else:
            start_stream(spark, available_now=args.available_now, topic=args.topic)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
