from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import sys
import time
from typing import Any


FEATURE_SET_ID = "WEATHER_FORECAST_FE_V1"
SOURCE_DATASET_ID = "NATIONWIDE_63"
SOURCE_DELTA_PATH = "/opt/project/data/historical/weather_hourly_vn63"
SOURCE_LINEAGE_DIR = "/opt/project/results/data-expansion/20260928T145824Z-vn63"
FULL_OUTPUT_PATH = "/opt/project/history-data/ml/weather_forecast_fe_v1"
SMOKE_OUTPUT_ROOT = "/opt/project/history-data/ml/weather_forecast_fe_v1_smoke"
RESULTS_ROOT = "/opt/project/results/feature-engineering"
TIMEZONE = "Asia/Ho_Chi_Minh"
EXPECTED_SOURCE_ROWS = 3_314_304
EXPECTED_ROWS_PER_LOCATION = 52_608
EXPECTED_OUTPUT_ROWS = 3_312_729
EXPECTED_OUTPUT_ROWS_PER_LOCATION = 52_583
EXPECTED_LOCATION_COUNT = 63
EXPECTED_SPLIT_COUNTS = {
    "TRAIN": 2_207_457,
    "VALIDATION": 553_392,
    "TEST": 551_880,
}
EXPECTED_SPLIT_ROWS_PER_LOCATION = {
    "TRAIN": 35_039,
    "VALIDATION": 8_784,
    "TEST": 8_760,
}

# The Delta contract uses the normalized names written by historical_to_delta.py.
# These mappings preserve those names in the feature schema.
CURRENT_WEATHER_COLUMNS = [
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "pressure_hpa",
    "wind_speed_kmh",
    "wind_gust_kmh",
]
SPATIAL_COLUMNS = ["latitude", "longitude"]
LAG_SPECS = {
    "temperature_c": ("temp", (1, 3, 6, 12, 24)),
    "humidity_pct": ("humidity", (1, 3, 6, 12, 24)),
    "pressure_hpa": ("pressure", (1, 3, 6, 12, 24)),
    "precipitation_mm": ("precipitation", (1, 3, 6, 24)),
    "wind_speed_kmh": ("wind_speed", (1, 3, 6, 24)),
}
ROLLING_SPECS = {
    "temperature_c": ("temp", ("mean", "std")),
    "humidity_pct": ("humidity", ("mean", "std")),
    "pressure_hpa": ("pressure", ("mean", "std")),
    "precipitation_mm": ("precipitation", ("sum",)),
    "wind_speed_kmh": ("wind_speed", ("mean",)),
}
ROLLING_HOURS = (3, 6, 24)
TIME_COLUMNS = [
    "local_hour",
    "local_day_of_week",
    "local_month",
    "local_day_of_year",
    "hour_sin",
    "hour_cos",
    "day_of_week_sin",
    "day_of_week_cos",
    "day_of_year_sin",
    "day_of_year_cos",
]
DELTA_SPECS = (
    ("temperature_c", "temp", (1, 3)),
    ("humidity_pct", "humidity", (1, 3)),
    ("pressure_hpa", "pressure", (1, 3, 6)),
    ("wind_speed_kmh", "wind_speed", (1,)),
)


def lag_feature_names() -> list[str]:
    return [
        f"{prefix}_lag_{hours}h"
        for _, (prefix, hours_list) in LAG_SPECS.items()
        for hours in hours_list
    ]


def _rolling_name(prefix: str, operation: str, hours: int) -> str:
    suffix = "std" if operation == "std" else operation
    if prefix == "precipitation" and operation == "sum":
        return f"precipitation_sum_{hours}h"
    return f"{prefix}_roll_{suffix}_{hours}h"


def rolling_feature_names() -> list[str]:
    names: list[str] = []
    for _, (prefix, operations) in ROLLING_SPECS.items():
        for hours in ROLLING_HOURS:
            for operation in operations:
                names.append(_rolling_name(prefix, operation, hours))
    return names


def delta_feature_names() -> list[str]:
    return [
        f"{prefix}_delta_{hours}h"
        for _, prefix, hours_list in DELTA_SPECS
        for hours in hours_list
    ]


MODEL_FEATURE_COLUMNS = (
    CURRENT_WEATHER_COLUMNS
    + SPATIAL_COLUMNS
    + TIME_COLUMNS
    + lag_feature_names()
    + rolling_feature_names()
    + delta_feature_names()
)
LABEL_COLUMN = "target_temperature_1h"
METADATA_COLUMNS = [
    "location_id",
    "event_time",
    "target_time",
    "weather_code",
]
OUTPUT_COLUMNS = METADATA_COLUMNS + MODEL_FEATURE_COLUMNS + [LABEL_COLUMN, "split"]


def expected_counts() -> dict[str, Any]:
    return {
        "source_rows": EXPECTED_SOURCE_ROWS,
        "source_rows_per_location": EXPECTED_ROWS_PER_LOCATION,
        "history_rows_dropped": 24 * EXPECTED_LOCATION_COUNT,
        "target_rows_dropped": EXPECTED_LOCATION_COUNT,
        "output_rows": EXPECTED_OUTPUT_ROWS,
        "output_rows_per_location": EXPECTED_OUTPUT_ROWS_PER_LOCATION,
        "split_rows": EXPECTED_SPLIT_COUNTS.copy(),
        "split_rows_per_location": EXPECTED_SPLIT_ROWS_PER_LOCATION.copy(),
    }


def cyclic_pair(value: float, period: float) -> tuple[float, float]:
    angle = 2.0 * math.pi * value / period
    return math.sin(angle), math.cos(angle)


def classify_split(target_time: datetime) -> str:
    if target_time.tzinfo is None:
        raise ValueError("target_time must be timezone-aware UTC")
    instant = target_time.astimezone(timezone.utc)
    if instant < datetime(2024, 1, 1, tzinfo=timezone.utc):
        return "TRAIN"
    if instant < datetime(2025, 1, 1, tzinfo=timezone.utc):
        return "VALIDATION"
    if instant < datetime(2026, 1, 1, tzinfo=timezone.utc):
        return "TEST"
    raise ValueError(f"target_time outside V1 split range: {instant.isoformat()}")


def _feature_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    source_definitions = {
        "temperature_c": "Observed temperature at feature event_time (Delta field temperature_c).",
        "humidity_pct": "Observed relative humidity at feature event_time (Delta field humidity_pct).",
        "precipitation_mm": "Observed precipitation at feature event_time (Delta field precipitation_mm).",
        "pressure_hpa": "Observed pressure at feature event_time (Delta field pressure_hpa).",
        "wind_speed_kmh": "Observed wind speed at feature event_time (Delta field wind_speed_kmh).",
        "wind_gust_kmh": "Observed wind gust at feature event_time (Delta field wind_gust_kmh).",
        "latitude": "Canonical location latitude from the Delta source.",
        "longitude": "Canonical location longitude from the Delta source.",
    }
    for name in CURRENT_WEATHER_COLUMNS + SPATIAL_COLUMNS:
        specs.append(
            {
                "name": name,
                "role": "feature",
                "category": "current" if name in CURRENT_WEATHER_COLUMNS else "location",
                "source": name,
                "definition": source_definitions[name],
                "uses_future_data": False,
            }
        )
    specs.extend(
        [
            {"name": "local_hour", "role": "feature", "category": "time", "source": "event_time", "definition": "Hour (0-23) after converting UTC event_time to Asia/Ho_Chi_Minh.", "uses_future_data": False},
            {"name": "local_day_of_week", "role": "feature", "category": "time", "source": "event_time", "definition": "Monday=0 through Sunday=6 in Asia/Ho_Chi_Minh.", "uses_future_data": False},
            {"name": "local_month", "role": "feature", "category": "time", "source": "event_time", "definition": "Local calendar month, 1-12.", "uses_future_data": False},
            {"name": "local_day_of_year", "role": "feature", "category": "time", "source": "event_time", "definition": "Local calendar day-of-year, 1-366.", "uses_future_data": False},
        ]
    )
    for name, value, period, description in (
        ("hour_sin", "local_hour", 24, "Sine of 2*pi*local_hour/24."),
        ("hour_cos", "local_hour", 24, "Cosine of 2*pi*local_hour/24."),
        ("day_of_week_sin", "local_day_of_week", 7, "Sine of 2*pi*local_day_of_week/7; Monday=0."),
        ("day_of_week_cos", "local_day_of_week", 7, "Cosine of 2*pi*local_day_of_week/7; Monday=0."),
        ("day_of_year_sin", "local_day_of_year-1", 365.25, "Sine of 2*pi*(local_day_of_year-1)/365.25."),
        ("day_of_year_cos", "local_day_of_year-1", 365.25, "Cosine of 2*pi*(local_day_of_year-1)/365.25."),
    ):
        specs.append(
            {
                "name": name,
                "role": "feature",
                "category": "time",
                "source": "event_time",
                "definition": description,
                "uses_future_data": False,
                "input_expression": value,
                "period": period,
            }
        )
    for source, (prefix, hours_list) in LAG_SPECS.items():
        for hours in hours_list:
            specs.append(
                {
                    "name": f"{prefix}_lag_{hours}h",
                    "role": "feature",
                    "category": "lag",
                    "source": source,
                    "definition": f"Same-location source value at event_time minus {hours} hours.",
                    "uses_future_data": False,
                }
            )
    for source, (prefix, operations) in ROLLING_SPECS.items():
        for hours in ROLLING_HOURS:
            for operation in operations:
                if operation == "std":
                    definition = f"Population standard deviation (stddev_pop) over [{hours - 1} hours before event_time, event_time], inclusive."
                elif operation == "sum":
                    definition = f"Trailing sum over [{hours - 1} hours before event_time, event_time], inclusive."
                else:
                    definition = f"Trailing mean over [{hours - 1} hours before event_time, event_time], inclusive."
                specs.append(
                    {
                        "name": _rolling_name(prefix, operation, hours),
                        "role": "feature",
                        "category": "rolling",
                        "source": source,
                        "definition": definition,
                        "uses_future_data": False,
                    }
                )
    for source, prefix, hours_list in DELTA_SPECS:
        for hours in hours_list:
            specs.append(
                {
                    "name": f"{prefix}_delta_{hours}h",
                    "role": "feature",
                    "category": "change",
                    "source": source,
                    "definition": f"Current {source} value minus its same-location value {hours} hours earlier.",
                    "uses_future_data": False,
                }
            )
    return specs


def feature_specification() -> dict[str, Any]:
    features = _feature_specs()
    return {
        "feature_set_id": FEATURE_SET_ID,
        "version": 1,
        "source_dataset_id": SOURCE_DATASET_ID,
        "feature_time_column": "event_time",
        "timezone": TIMEZONE,
        "model_features": features,
        "label": {
            "name": LABEL_COLUMN,
            "role": "target",
            "source": "temperature_c at lead(event_time, 1)",
            "definition": "Temperature at exactly target_time=event_time+1 hour in the same location.",
            "uses_future_data": True,
        },
        "metadata": [
            {"name": "location_id", "role": "identifier", "definition": "Same-location partition key; not numerically encoded as a model feature."},
            {"name": "event_time", "role": "feature_time", "definition": "UTC feature observation timestamp."},
            {"name": "target_time", "role": "label_time", "definition": "UTC timestamp exactly one hour after event_time."},
            {"name": "weather_code", "role": "context", "definition": "Preserved from Delta; not numerically modeled or ordinally encoded."},
            {"name": "split", "role": "partition", "definition": "TRAIN/VALIDATION/TEST assigned from target_time."},
        ],
        "source_mapping": {
            "temperature_2m": "temperature_c",
            "relative_humidity_2m": "humidity_pct",
            "precipitation": "precipitation_mm",
            "pressure_msl": "pressure_hpa",
            "wind_speed_10m": "wind_speed_kmh",
            "wind_gusts_10m": "wind_gust_kmh",
            "weather_code": "weather_code",
        },
        "rolling_semantics": "Trailing row windows include current time t; eligibility proves all required history is hourly-contiguous. No centered windows.",
        "feature_count": len(features),
        "output_column_count": len(OUTPUT_COLUMNS),
        "output_columns": OUTPUT_COLUMNS,
    }


def _timestamp_boundary(value: str):
    from pyspark.sql import functions as F

    return F.to_timestamp(F.lit(value), "yyyy-MM-dd HH:mm:ss")


def build_feature_frame(source_df):
    """Return (eligible output rows, pre-write diagnostics) with lazy PySpark imports."""
    from functools import reduce
    from operator import and_

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    required = {
        "location_id",
        "event_time",
        "latitude",
        "longitude",
        "weather_code",
        *CURRENT_WEATHER_COLUMNS,
    }
    missing = sorted(required - set(source_df.columns))
    if missing:
        raise ValueError(f"Delta source is missing required feature columns: {missing}")

    window = Window.partitionBy("location_id").orderBy(F.col("event_time"))
    enriched = source_df
    for source, (prefix, hours_list) in LAG_SPECS.items():
        for hours in hours_list:
            enriched = enriched.withColumn(
                f"{prefix}_lag_{hours}h", F.lag(F.col(source), hours).over(window)
            )
    for source, (prefix, operations) in ROLLING_SPECS.items():
        for hours in ROLLING_HOURS:
            trailing = window.rowsBetween(-(hours - 1), 0)
            enriched = enriched.withColumn(
                f"_rolling_count_{source}_{hours}h",
                F.count(F.col(source)).over(trailing),
            )
            for operation in operations:
                output_name = _rolling_name(prefix, operation, hours)
                if operation == "mean":
                    expression = F.avg(F.col(source)).over(trailing)
                elif operation == "std":
                    expression = F.stddev_pop(F.col(source)).over(trailing)
                else:
                    expression = F.sum(F.col(source)).over(trailing)
                enriched = enriched.withColumn(
                    output_name, expression
                )
    for source, prefix, hours_list in DELTA_SPECS:
        for hours in hours_list:
            enriched = enriched.withColumn(
                f"{prefix}_delta_{hours}h",
                F.col(source) - F.lag(F.col(source), hours).over(window),
            )

    local_time = F.from_utc_timestamp(F.col("event_time"), TIMEZONE)
    enriched = (
        enriched.withColumn("local_hour", F.hour(local_time))
        .withColumn("local_day_of_week", (F.dayofweek(local_time) + F.lit(5)) % F.lit(7))
        .withColumn("local_month", F.month(local_time))
        .withColumn("local_day_of_year", F.dayofyear(local_time))
        .withColumn(
            "hour_sin",
            F.sin(F.lit(2.0 * math.pi) * F.col("local_hour") / F.lit(24.0)),
        )
        .withColumn(
            "hour_cos",
            F.cos(F.lit(2.0 * math.pi) * F.col("local_hour") / F.lit(24.0)),
        )
        .withColumn(
            "day_of_week_sin",
            F.sin(F.lit(2.0 * math.pi) * F.col("local_day_of_week") / F.lit(7.0)),
        )
        .withColumn(
            "day_of_week_cos",
            F.cos(F.lit(2.0 * math.pi) * F.col("local_day_of_week") / F.lit(7.0)),
        )
        .withColumn(
            "day_of_year_sin",
            F.sin(
                F.lit(2.0 * math.pi)
                * (F.col("local_day_of_year") - F.lit(1))
                / F.lit(365.25)
            ),
        )
        .withColumn(
            "day_of_year_cos",
            F.cos(
                F.lit(2.0 * math.pi)
                * (F.col("local_day_of_year") - F.lit(1))
                / F.lit(365.25)
            ),
        )
        .withColumn("_previous_event_time", F.lag(F.col("event_time"), 1).over(window))
        .withColumn("_history_event_time_24h", F.lag(F.col("event_time"), 24).over(window))
        # The only forward-looking operations: the label value and its timestamp.
        .withColumn(LABEL_COLUMN, F.lead(F.col("temperature_c"), 1).over(window))
        .withColumn("target_time", F.lead(F.col("event_time"), 1).over(window))
    )

    hour_interval = F.expr("INTERVAL 1 HOUR")
    history_ok = F.coalesce(
        F.col("_history_event_time_24h")
        == F.col("event_time") - F.expr("INTERVAL 24 HOURS"),
        F.lit(False),
    )
    target_ok = F.coalesce(
        F.col("target_time") == F.col("event_time") + hour_interval,
        F.lit(False),
    )
    finite_features = [
        F.col(name).isNotNull()
        & ~F.isnan(F.col(name).cast("double"))
        & (F.abs(F.col(name).cast("double")) < F.lit(float("inf")))
        for name in MODEL_FEATURE_COLUMNS + [LABEL_COLUMN]
    ]
    numeric_ok = reduce(and_, finite_features)
    rolling_complete = reduce(
        and_,
        [
            F.col(f"_rolling_count_{source}_{hours}h") == hours
            for source in ROLLING_SPECS
            for hours in ROLLING_HOURS
        ],
    )
    all_inputs_ok = numeric_ok & rolling_complete
    previous_exists = F.col("_previous_event_time").isNotNull()
    next_exists = F.col("target_time").isNotNull()
    history_event_exists = F.col("_history_event_time_24h").isNotNull()
    diagnostics = enriched.agg(
        F.count(F.lit(1)).alias("source_rows"),
        F.sum(
            F.when(previous_exists & (F.col("event_time") != F.col("_previous_event_time") + hour_interval), 1).otherwise(0)
        ).alias("source_hourly_gap_edges"),
        F.sum(
            F.when(history_event_exists & ~history_ok, 1).otherwise(0)
        ).alias("lag24_history_continuity_violations"),
        F.sum(F.when(~history_ok, 1).otherwise(0)).alias("history_ineligible_rows"),
        F.sum(F.when(~target_ok, 1).otherwise(0)).alias("target_ineligible_rows"),
        F.sum(F.when(history_ok & target_ok & ~all_inputs_ok, 1).otherwise(0)).alias("invalid_numeric_feature_rows"),
        F.sum(F.when(history_ok & target_ok & all_inputs_ok, 1).otherwise(0)).alias("eligible_rows"),
        F.sum(
            F.when(next_exists & ~target_ok, 1).otherwise(0)
        ).alias("target_time_gap_edges"),
    ).first().asDict()

    split_expression = (
        F.when(F.col("target_time") < _timestamp_boundary("2024-01-01 00:00:00"), "TRAIN")
        .when(F.col("target_time") < _timestamp_boundary("2025-01-01 00:00:00"), "VALIDATION")
        .when(F.col("target_time") < _timestamp_boundary("2026-01-01 00:00:00"), "TEST")
    )
    output = (
        enriched.where(history_ok & target_ok & all_inputs_ok)
        .withColumn("split", split_expression)
        .select(*OUTPUT_COLUMNS)
    )
    return output, diagnostics


def _json_default(value: Any):
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if hasattr(value, "asDict"):
        return value.asDict(recursive=True)
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalized_text_sha256(path: Path) -> str:
    content = path.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(content).hexdigest()


def _load_lineage(lineage_dir: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    manifest_path = lineage_dir / "dataset_manifest.json"
    delta_validation_path = lineage_dir / "delta_validation.json"
    checksums_path = lineage_dir / "checksums.json"
    if not manifest_path.is_file() or not delta_validation_path.is_file() or not checksums_path.is_file():
        raise FileNotFoundError(f"Required nationwide lineage evidence missing from {lineage_dir}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    delta_validation = json.loads(delta_validation_path.read_text(encoding="utf-8"))
    checksums = json.loads(checksums_path.read_text(encoding="utf-8"))
    if manifest.get("dataset_id") != SOURCE_DATASET_ID:
        raise ValueError(f"Expected source dataset {SOURCE_DATASET_ID}, found {manifest.get('dataset_id')}")
    if delta_validation.get("status") != "PASS" or delta_validation.get("dataset_id") != SOURCE_DATASET_ID:
        raise ValueError("Persisted Delta validation evidence is not PASS for NATIONWIDE_63")
    if manifest.get("delta_path") != SOURCE_DELTA_PATH or delta_validation.get("delta_path") != SOURCE_DELTA_PATH:
        raise ValueError("Persisted lineage does not point to the expected immutable nationwide Delta path")
    if manifest.get("delta_records") != EXPECTED_SOURCE_ROWS or delta_validation.get("delta_records") != EXPECTED_SOURCE_ROWS:
        raise ValueError("Persisted nationwide lineage source row count is unexpected")
    ids = delta_validation.get("delta_location_ids", [])
    if len(ids) != EXPECTED_LOCATION_COUNT or len(set(ids)) != EXPECTED_LOCATION_COUNT:
        raise ValueError("Persisted nationwide lineage does not contain 63 unique Delta location IDs")
    result_files = checksums.get("result_files", {})
    lineage_hashes = {
        "dataset_manifest_sha256": _sha256(manifest_path),
        "delta_validation_sha256": _normalized_text_sha256(delta_validation_path),
        "delta_validation_sha256_raw": _sha256(delta_validation_path),
        "checksums_file_sha256": _sha256(checksums_path),
        "catalog_sha256": str(manifest.get("catalog_sha256")),
        "source_git_commit": str(manifest.get("git_commit")),
        "expected_dataset_manifest_sha256": str(
            result_files.get("dataset_manifest.json", "")
        ),
        "expected_delta_validation_sha256": str(
            result_files.get("delta_validation.json", "")
        ),
    }
    if (
        lineage_hashes["expected_dataset_manifest_sha256"]
        and lineage_hashes["dataset_manifest_sha256"]
        != lineage_hashes["expected_dataset_manifest_sha256"]
    ):
        raise ValueError("Nationwide dataset manifest checksum does not match its persisted checksum record")
    if (
        lineage_hashes["expected_delta_validation_sha256"]
        and lineage_hashes["delta_validation_sha256"]
        != lineage_hashes["expected_delta_validation_sha256"]
    ):
        raise ValueError("Nationwide Delta validation checksum does not match its persisted checksum record")
    return manifest, delta_validation, lineage_hashes


def _delta_version(spark, source_path: str) -> int:
    from delta.tables import DeltaTable

    row = DeltaTable.forPath(spark, source_path).history(1).select("version").first()
    if row is None:
        raise RuntimeError(f"No Delta history found at {source_path}")
    return int(row["version"])


def _schema_payload(dataframe) -> list[dict[str, Any]]:
    return [
        {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": bool(field.nullable),
        }
        for field in dataframe.schema.fields
    ]


def _validate_source(
    spark,
    source_path: str,
    delta_validation: dict[str, Any],
    smoke: bool,
    smoke_hours: int = 72,
    smoke_start: datetime | None = None,
):
    source_df = spark.read.format("delta").load(source_path)
    expected_fields = {
        "event_id",
        "event_type",
        "location_id",
        "city",
        "latitude",
        "longitude",
        "event_time",
        "ingestion_time",
        "temperature_c",
        "humidity_pct",
        "precipitation_mm",
        "pressure_hpa",
        "wind_speed_kmh",
        "wind_gust_kmh",
        "weather_code",
        "source",
    }
    missing = sorted(expected_fields - set(source_df.columns))
    if missing:
        raise ValueError(f"Delta source schema missing expected fields: {missing}")
    event_type = source_df.schema["event_time"].dataType.typeName()
    if event_type not in {"timestamp", "timestamp_ntz"}:
        raise TypeError(f"event_time must be a Spark timestamp, got {event_type}")
    from pyspark.sql import functions as F

    null_key_count = source_df.where(
        F.col("location_id").isNull() | F.col("event_time").isNull()
    ).count()
    if null_key_count:
        raise ValueError(f"Source Delta contains {null_key_count} null location/time keys")

    actual_rows = source_df.count()
    actual_ids = sorted(row["location_id"] for row in source_df.select("location_id").distinct().collect())
    expected_ids = sorted(delta_validation["delta_location_ids"])
    if actual_rows != EXPECTED_SOURCE_ROWS:
        raise ValueError(f"Source Delta row count {actual_rows} != expected {EXPECTED_SOURCE_ROWS}")
    if actual_ids != expected_ids or len(actual_ids) != EXPECTED_LOCATION_COUNT:
        raise ValueError("Live Delta location IDs do not exactly match the persisted 63-location validation")
    delta_total_rows = actual_rows
    if smoke:
        chosen_ids = expected_ids[:2]
        feature_start = smoke_start or datetime(2020, 1, 2, tzinfo=timezone.utc)
        feature_end = feature_start + timedelta(hours=smoke_hours)
        source_start = (feature_start - timedelta(hours=24)).replace(tzinfo=None)
        source_end = feature_end.replace(tzinfo=None)
        scoped = source_df.where(
            (source_df.location_id.isin(chosen_ids))
            & (source_df.event_time >= source_start)
            & (source_df.event_time <= source_end)
        )
        source_df = scoped
        actual_rows = source_df.count()
        actual_ids = chosen_ids
        expected_rows_by_id = {
            location_id: smoke_hours + 25 for location_id in chosen_ids
        }
    else:
        expected_rows_by_id = {location_id: EXPECTED_ROWS_PER_LOCATION for location_id in expected_ids}

    per_location_rows = (
        source_df.groupBy("location_id")
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.min("event_time").alias("min_event_time"),
            F.max("event_time").alias("max_event_time"),
        )
        .collect()
    )
    per_location_counts = {row["location_id"]: int(row["count"]) for row in per_location_rows}
    if per_location_counts != expected_rows_by_id:
        raise ValueError(f"Unexpected source per-location counts: {per_location_counts}")
    if not smoke and any(
        row["min_event_time"].strftime("%Y-%m-%dT%H:%M:%S") != "2020-01-01T00:00:00"
        or row["max_event_time"].strftime("%Y-%m-%dT%H:%M:%S") != "2025-12-31T23:00:00"
        for row in per_location_rows
    ):
        raise ValueError("Source Delta per-location time range differs from the validated 2020-2025 range")
    duplicate_count = (
        source_df.groupBy("location_id", "event_time")
        .count()
        .where("count > 1")
        .count()
    )
    if duplicate_count:
        raise ValueError(f"Found {duplicate_count} duplicate (location_id, event_time) source keys")
    weather_code_null_count = source_df.where(F.col("weather_code").isNull()).count()

    return source_df, {
        "rows": actual_rows,
        "delta_total_rows": delta_total_rows,
        "delta_location_ids": expected_ids,
        "delta_location_count": len(expected_ids),
        "location_ids": actual_ids,
        "location_count": len(actual_ids),
        "per_location_counts": per_location_counts,
        "duplicate_observation_keys": duplicate_count,
        "weather_code_null_count": weather_code_null_count,
        "schema": _schema_payload(source_df),
        "feature_start_utc": smoke_start.isoformat() if smoke and smoke_start else (
            "2020-01-02T00:00:00Z" if smoke else None
        ),
        "feature_hours": smoke_hours if smoke else None,
    }


def _numeric_quality(dataframe, column_names: list[str]) -> dict[str, dict[str, int]]:
    from pyspark.sql import functions as F

    expressions = []
    for index, name in enumerate(column_names):
        value = F.col(name).cast("double")
        expressions.extend(
            [
                F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(f"null_{index}"),
                F.sum(F.when(F.isnan(value), 1).otherwise(0)).alias(f"nan_{index}"),
                F.sum(F.when(F.abs(value) >= F.lit(float("inf")), 1).otherwise(0)).alias(f"infinite_{index}"),
            ]
        )
    row = dataframe.agg(*expressions).first()
    return {
        name: {
            "null": int(row[f"null_{index}"] or 0),
            "nan": int(row[f"nan_{index}"] or 0),
            "infinite": int(row[f"infinite_{index}"] or 0),
        }
        for index, name in enumerate(column_names)
    }


def _feature_quality_by_split(dataframe) -> dict[str, dict[str, dict[str, int]]]:
    from pyspark.sql import functions as F

    expressions = []
    for index, name in enumerate(MODEL_FEATURE_COLUMNS + [LABEL_COLUMN]):
        value = F.col(name).cast("double")
        expressions.extend(
            [
                F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(f"null_{index}"),
                F.sum(F.when(F.isnan(value), 1).otherwise(0)).alias(f"nan_{index}"),
                F.sum(F.when(F.abs(value) >= F.lit(float("inf")), 1).otherwise(0)).alias(f"infinite_{index}"),
            ]
        )
    result: dict[str, dict[str, dict[str, int]]] = {}
    for row in dataframe.groupBy("split").agg(*expressions).collect():
        result[row["split"]] = {
            name: {
                "null": int(row[f"null_{index}"] or 0),
                "nan": int(row[f"nan_{index}"] or 0),
                "infinite": int(row[f"infinite_{index}"] or 0),
            }
            for index, name in enumerate(MODEL_FEATURE_COLUMNS + [LABEL_COLUMN])
        }
    return result


def _statistics_by_split(dataframe) -> dict[str, Any]:
    from pyspark.sql import functions as F

    expressions = [
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct("location_id").alias("location_count"),
        F.min("event_time").alias("min_event_time"),
        F.max("event_time").alias("max_event_time"),
        F.min("target_time").alias("min_target_time"),
        F.max("target_time").alias("max_target_time"),
    ]
    for index, name in enumerate(MODEL_FEATURE_COLUMNS):
        value = F.col(name).cast("double")
        expressions.extend(
            [
                F.min(value).alias(f"min_{index}"),
                F.max(value).alias(f"max_{index}"),
                F.avg(value).alias(f"mean_{index}"),
                F.stddev_pop(value).alias(f"stddev_pop_{index}"),
            ]
        )
    result: dict[str, Any] = {}
    for row in dataframe.groupBy("split").agg(*expressions).collect():
        split = row["split"]
        metrics = {
            "row_count": int(row["row_count"]),
            "location_count": int(row["location_count"]),
            "min_event_time": row["min_event_time"],
            "max_event_time": row["max_event_time"],
            "min_target_time": row["min_target_time"],
            "max_target_time": row["max_target_time"],
            "features": {},
        }
        for index, name in enumerate(MODEL_FEATURE_COLUMNS):
            metrics["features"][name] = {
                "min": row[f"min_{index}"],
                "max": row[f"max_{index}"],
                "mean": row[f"mean_{index}"],
                "stddev_pop": row[f"stddev_pop_{index}"],
            }
        result[split] = metrics
    return result


def _per_location_split_counts(dataframe) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    rows = (
        dataframe.groupBy("location_id", "split")
        .agg(
            F.count("*").alias("row_count"),
            F.min("target_time").alias("min_target_time"),
            F.max("target_time").alias("max_target_time"),
        )
        .orderBy("location_id", "split")
        .collect()
    )
    return [
        {
            "location_id": row["location_id"],
            "split": row["split"],
            "row_count": int(row["row_count"]),
            "min_target_time_utc": row["min_target_time"].isoformat() if row["min_target_time"] else "",
            "max_target_time_utc": row["max_target_time"].isoformat() if row["max_target_time"] else "",
        }
        for row in rows
    ]


def _write_per_location_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "location_id",
                "split",
                "row_count",
                "min_target_time_utc",
                "max_target_time_utc",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def _inventory_parquet(output_path: Path) -> dict[str, Any]:
    files = sorted(output_path.rglob("*.parquet"))
    entries = []
    for path in files:
        stat = path.stat()
        entries.append(
            {
                "path": path.relative_to(output_path).as_posix(),
                "size_bytes": stat.st_size,
                "modified_at_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            }
        )
    return {
        "format": "Parquet",
        "compression": "Snappy",
        "partition_columns": ["split"],
        "parquet_file_count": len(entries),
        "total_bytes": sum(entry["size_bytes"] for entry in entries),
        "files": entries,
    }


def _delta_target_alignment_violations(readback, source_df) -> int:
    from pyspark.sql import functions as F

    target_source = source_df.select(
        F.col("location_id").alias("_source_location_id"),
        F.col("event_time").alias("_source_target_time"),
        F.col("temperature_c").alias("_expected_target_temperature"),
    )
    output = readback.alias("output")
    source = target_source.alias("source")
    joined = output.join(
        source,
        (F.col("output.location_id") == F.col("source._source_location_id"))
        & (F.col("output.target_time") == F.col("source._source_target_time")),
        "left",
    )
    bad = joined.where(
        F.col("source._expected_target_temperature").isNull()
        | (F.col(f"output.{LABEL_COLUMN}") != F.col("source._expected_target_temperature"))
    )
    return bad.count()


def _split_validation(
    readback,
    location_ids: list[str],
    smoke: bool,
    smoke_feature_hours: int = 72,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    counts = {
        row["split"]: int(row["count"])
        for row in readback.groupBy("split").count().collect()
    }
    locations_by_split = {
        row["split"]: int(row["location_count"])
        for row in readback.groupBy("split").agg(
            F.countDistinct("location_id").alias("location_count")
        ).collect()
    }
    per_location = _per_location_split_counts(readback)
    if smoke:
        expected_each = smoke_feature_hours
        expected_total = smoke_feature_hours * len(location_ids)
        expected = {"TRAIN": expected_total}
        if counts != expected:
            raise ValueError(f"Smoke split counts {counts} != {expected}")
        if any(row["row_count"] != expected_each for row in per_location):
            raise ValueError(f"Smoke per-location count is unexpected: {per_location}")
    else:
        if counts != EXPECTED_SPLIT_COUNTS:
            raise ValueError(f"Split counts {counts} != expected {EXPECTED_SPLIT_COUNTS}")
        expected_pairs = {
            (location_id, split): expected_count
            for location_id in location_ids
            for split, expected_count in EXPECTED_SPLIT_ROWS_PER_LOCATION.items()
        }
        actual_pairs = {
            (row["location_id"], row["split"]): row["row_count"]
            for row in per_location
        }
        if actual_pairs != expected_pairs:
            raise ValueError("Per-location split counts do not match the 63-location expected contract")
    expected_location_count = len(location_ids)
    if any(count != expected_location_count for count in locations_by_split.values()):
        raise ValueError(f"A split lost a location: {locations_by_split}")
    if set(counts) != ({"TRAIN"} if smoke else set(EXPECTED_SPLIT_COUNTS)):
        raise ValueError(f"Unexpected split categories: {counts}")
    if smoke:
        boundary_ranges = None
    else:
        boundary_ranges = {
            row["split"]: {
                "min_event_time": row["min_event_time"],
                "max_event_time": row["max_event_time"],
                "min_target_time": row["min_target_time"],
                "max_target_time": row["max_target_time"],
            }
            for row in readback.groupBy("split").agg(
                F.min("event_time").alias("min_event_time"),
                F.max("event_time").alias("max_event_time"),
                F.min("target_time").alias("min_target_time"),
                F.max("target_time").alias("max_target_time"),
            ).collect()
        }
        expected_bounds = {
            "TRAIN": ("2020-01-02T00:00:00", "2023-12-31T22:00:00", "2020-01-02T01:00:00", "2023-12-31T23:00:00"),
            "VALIDATION": ("2023-12-31T23:00:00", "2024-12-31T22:00:00", "2024-01-01T00:00:00", "2024-12-31T23:00:00"),
            "TEST": ("2024-12-31T23:00:00", "2025-12-31T22:00:00", "2025-01-01T00:00:00", "2025-12-31T23:00:00"),
        }
        for split, bounds in expected_bounds.items():
            actual = boundary_ranges[split]
            actual_values = tuple(
                actual[key].strftime("%Y-%m-%dT%H:%M:%S")
                for key in ("min_event_time", "max_event_time", "min_target_time", "max_target_time")
            )
            if actual_values != bounds:
                raise ValueError(f"{split} boundary mismatch: {actual_values} != {bounds}")
    return {
        "split_counts": counts,
        "locations_by_split": locations_by_split,
        "per_location_split_counts": per_location,
        "boundary_ranges_utc": boundary_ranges,
    }


def _readback_validation(
    spark,
    staging_path: Path,
    source_df,
    expected_columns: list[str],
    ids: list[str],
    smoke: bool,
    smoke_feature_hours: int = 72,
):
    from pyspark.sql import functions as F

    readback = spark.read.parquet(str(staging_path))
    actual_schema = _schema_payload(readback)
    actual_names = [field["name"] for field in actual_schema]
    if actual_names != expected_columns:
        raise ValueError(f"Parquet read-back schema columns differ: {actual_names}")
    output_rows = readback.count()
    expected_rows = smoke_feature_hours * len(ids) if smoke else EXPECTED_OUTPUT_ROWS
    if output_rows != expected_rows:
        raise ValueError(f"Output count {output_rows} != expected {expected_rows}")
    output_ids = sorted(row["location_id"] for row in readback.select("location_id").distinct().collect())
    if output_ids != ids:
        raise ValueError(f"Read-back location coverage differs: {output_ids} != {ids}")
    duplicate_keys = (
        readback.groupBy("location_id", "event_time")
        .count()
        .where("count > 1")
        .count()
    )
    if duplicate_keys:
        raise ValueError(f"Read-back has {duplicate_keys} duplicate observation keys")
    target_time_violations = readback.where(
        F.col("target_time") != F.col("event_time") + F.expr("INTERVAL 1 HOUR")
    ).count()
    if target_time_violations:
        raise ValueError(f"Read-back has {target_time_violations} target_time alignment violations")
    target_value_violations = _delta_target_alignment_violations(readback, source_df)
    if target_value_violations:
        raise ValueError(f"Read-back has {target_value_violations} target value mismatches against Delta")
    split_report = _split_validation(
        readback, ids, smoke, smoke_feature_hours=smoke_feature_hours
    )
    quality_by_split = _feature_quality_by_split(readback)
    non_finite_counts = {
        "null_required_feature_values": 0,
        "nan_feature_values": 0,
        "infinite_feature_values": 0,
        "per_split": quality_by_split,
    }
    for split_values in quality_by_split.values():
        for counts in split_values.values():
            non_finite_counts["null_required_feature_values"] += counts["null"]
            non_finite_counts["nan_feature_values"] += counts["nan"]
            non_finite_counts["infinite_feature_values"] += counts["infinite"]
    if any(
        non_finite_counts[key]
        for key in (
            "null_required_feature_values",
            "nan_feature_values",
            "infinite_feature_values",
        )
    ):
        raise ValueError(f"Read-back contains invalid required numeric values: {non_finite_counts}")
    null_metadata = {
        name: int(count or 0)
        for name, count in readback.select(
            *[
                F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(name)
                for name in ("location_id", "event_time", "target_time", "split", "weather_code")
            ]
        ).first().asDict().items()
    }
    required_null_metadata = {
        name: count for name, count in null_metadata.items() if name != "weather_code"
    }
    if any(required_null_metadata.values()):
        raise ValueError(
            f"Read-back contains null identity/time/partition values: {required_null_metadata}"
        )
    stats = _statistics_by_split(readback)
    return readback, {
        "status": "PASS",
        "output_rows": output_rows,
        "location_count": len(output_ids),
        "location_ids": output_ids,
        "schema": actual_schema,
        "unique_observation_keys": output_rows - duplicate_keys,
        "duplicate_observation_keys": duplicate_keys,
        "target_time_alignment_violations": target_time_violations,
        "target_temperature_alignment_violations": target_value_violations,
        "numeric_quality": non_finite_counts,
        "null_identity_time_partition_values": required_null_metadata,
        "null_weather_code_context_values": null_metadata["weather_code"],
        **split_report,
        "statistics_by_split": stats,
    }


def inspect_source(spark, source_path: str, lineage_dir: Path) -> dict[str, Any]:
    _, delta_validation, lineage_hashes = _load_lineage(lineage_dir)
    before_version = _delta_version(spark, source_path)
    source = spark.read.format("delta").load(source_path)
    count = source.count()
    ids = sorted(row["location_id"] for row in source.select("location_id").distinct().collect())
    schema = _schema_payload(source)
    result = {
        "source_delta_path": source_path,
        "source_dataset_id": SOURCE_DATASET_ID,
        "source_delta_version": before_version,
        "source_rows": count,
        "location_count": len(ids),
        "location_ids_match_manifest": ids == sorted(delta_validation["delta_location_ids"]),
        "source_schema": schema,
        "lineage_sha256": lineage_hashes,
        "spark_version": spark.version,
    }
    return result


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include UTC offset or Z")
    return parsed.astimezone(timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build leakage-safe WEATHER_FORECAST_FE_V1 Parquet from NATIONWIDE_63 Delta."
    )
    parser.add_argument("--source", default=SOURCE_DELTA_PATH)
    parser.add_argument("--lineage-dir", type=Path, default=Path(SOURCE_LINEAGE_DIR))
    parser.add_argument("--catalog", type=Path, default=Path("/opt/project/historical/locations.json"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--results-root", type=Path, default=Path(RESULTS_ROOT))
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-fe-v1"))
    parser.add_argument("--feature-git-commit", default=os.environ.get("FEATURE_GIT_COMMIT", ""))
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--smoke-hours", type=int, default=72)
    parser.add_argument("--smoke-start", default="2020-01-02T00:00:00Z")
    parser.add_argument("--inspect-source", action="store_true")
    args = parser.parse_args(argv)
    if args.shuffle_partitions <= 0:
        parser.error("--shuffle-partitions must be positive")
    if args.smoke and not 48 <= args.smoke_hours <= 72:
        parser.error("--smoke-hours must be between 48 and 72")
    return args


def _create_spark(args):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("weather-forecast-feature-engineering-v1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def run_feature_engineering(args: argparse.Namespace, spark=None) -> dict[str, Any]:
    started = time.perf_counter()
    started_at_utc = datetime.now(timezone.utc)
    if spark is None:
        spark = _create_spark(args)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark.sparkContext.setLogLevel("WARN")

    source_path = str(Path(args.source))
    _, delta_validation, lineage_hashes = _load_lineage(args.lineage_dir)
    if args.inspect_source:
        return inspect_source(spark, source_path, args.lineage_dir)

    if args.output is None:
        if args.smoke:
            output_path = Path(SMOKE_OUTPUT_ROOT) / args.run_id
        else:
            output_path = Path(FULL_OUTPUT_PATH)
    else:
        output_path = args.output
    output_path = output_path.resolve()
    source_resolved = Path(source_path).resolve()
    if (
        output_path == source_resolved
        or source_resolved in output_path.parents
        or output_path in source_resolved.parents
    ):
        raise ValueError("Output path must not be the source Delta path or a child of it")
    run_dir = args.results_root.resolve() / args.run_id
    if not args.run_id or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for character in args.run_id):
        raise ValueError("run_id may contain only letters, digits, dot, underscore and hyphen")
    staging_path = output_path.with_name(f"{output_path.name}.staging-{args.run_id}")
    if output_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing versioned output: {output_path}")
    if staging_path.exists():
        raise FileExistsError(f"Refusing to reuse existing staging output: {staging_path}")
    if run_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing run artifacts: {run_dir}")
    if args.shuffle_partitions != 16 and not args.smoke:
        print(f"WARNING: full run uses non-default shuffle partitions={args.shuffle_partitions}", file=sys.stderr)

    version_before = _delta_version(spark, source_path)
    smoke_start = _parse_utc(args.smoke_start) if args.smoke else None
    source_df, source_report = _validate_source(
        spark,
        source_path,
        delta_validation,
        smoke=args.smoke,
        smoke_hours=args.smoke_hours,
        smoke_start=smoke_start,
    )
    catalog_path = Path(args.catalog)
    if not catalog_path.is_file():
        raise FileNotFoundError(f"Canonical location catalog is missing: {catalog_path}")
    catalog_sha256 = _sha256(catalog_path)
    if catalog_sha256 != lineage_hashes["catalog_sha256"]:
        raise ValueError(
            f"Current catalog checksum {catalog_sha256} differs from nationwide lineage "
            f"{lineage_hashes['catalog_sha256']}"
        )
    source_quality = _numeric_quality(source_df, CURRENT_WEATHER_COLUMNS + SPATIAL_COLUMNS)
    source_invalid_values = sum(
        counts["null"] + counts["nan"] + counts["infinite"]
        for counts in source_quality.values()
    )

    feature_frame, prewrite = build_feature_frame(source_df)
    if int(prewrite["source_rows"]) != int(source_report["rows"]):
        raise ValueError("Feature plan source row count differs from source verification")
    if int(prewrite["source_hourly_gap_edges"] or 0) != 0:
        raise ValueError(f"Found source hourly gaps: {prewrite['source_hourly_gap_edges']}")
    if int(prewrite["target_time_gap_edges"] or 0) != 0:
        raise ValueError(f"Found target-time gaps: {prewrite['target_time_gap_edges']}")
    if int(prewrite["invalid_numeric_feature_rows"] or 0) != 0:
        raise ValueError(
            f"Found ineligible rows with invalid numeric features or incomplete rolling inputs: "
            f"{prewrite['invalid_numeric_feature_rows']}; source quality={source_quality}"
        )
    expected_eligible = (
        args.smoke_hours * len(source_report["location_ids"])
        if args.smoke
        else EXPECTED_OUTPUT_ROWS
    )
    if int(prewrite["eligible_rows"] or 0) != expected_eligible:
        raise ValueError(
            f"Eligible feature rows {prewrite['eligible_rows']} != expected {expected_eligible}; "
            f"history ineligible={prewrite['history_ineligible_rows']}, "
            f"target ineligible={prewrite['target_ineligible_rows']}"
        )
    if not args.smoke:
        expected_history_drops = 24 * EXPECTED_LOCATION_COUNT
        expected_target_drops = EXPECTED_LOCATION_COUNT
        if int(prewrite["history_ineligible_rows"] or 0) != expected_history_drops:
            raise ValueError(f"History exclusions differ from expected {expected_history_drops}")
        if int(prewrite["target_ineligible_rows"] or 0) != expected_target_drops:
            raise ValueError(f"Target exclusions differ from expected {expected_target_drops}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    (
        feature_frame.write.mode("errorifexists")
        .option("compression", "snappy")
        .partitionBy("split")
        .parquet(str(staging_path))
    )
    _, readback_report = _readback_validation(
        spark,
        staging_path,
        source_df,
        OUTPUT_COLUMNS,
        source_report["location_ids"],
        smoke=args.smoke,
        smoke_feature_hours=args.smoke_hours,
    )
    version_after = _delta_version(spark, source_path)
    if version_after != version_before:
        raise ValueError(f"Immutable source Delta changed during run: {version_before} -> {version_after}")

    # Rename the validated run-scoped staging directory only after Spark read-back passes.
    if output_path.exists():
        raise FileExistsError(f"Output path appeared during run; refusing to replace {output_path}")
    os.rename(staging_path, output_path)
    inventory = _inventory_parquet(output_path)
    if inventory["parquet_file_count"] <= 0 or inventory["total_bytes"] <= 0:
        raise ValueError("Promoted Parquet inventory is empty")

    elapsed = time.perf_counter() - started
    try:
        delta_version = importlib.metadata.version("delta-spark")
    except importlib.metadata.PackageNotFoundError:
        delta_version = "provided by Spark package coordinates"
    runtime = {
        "started_at_utc": started_at_utc.isoformat(),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(elapsed, 3),
        "spark_version": spark.version,
        "delta_python_package_version": delta_version,
        "spark_master": spark.sparkContext.master,
        "spark_application_id": spark.sparkContext.applicationId,
        "spark_default_parallelism": spark.sparkContext.defaultParallelism,
        "spark_sql_shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
        "spark_adaptive_execution": spark.conf.get("spark.sql.adaptive.enabled"),
        "spark_executor_cores": spark.conf.get("spark.executor.cores", "not reported"),
        "spark_executor_memory": spark.conf.get("spark.executor.memory", "not reported"),
        "spark_executor_instances": spark.conf.get("spark.executor.instances", "not reported"),
        "catalog_path": str(catalog_path),
        "catalog_sha256": catalog_sha256,
    }
    final_validation = {
        **readback_report,
        "source_rows": source_report["rows"],
        "source_delta_total_rows_verified": source_report["delta_total_rows"],
        "source_delta_location_count_verified": source_report["delta_location_count"],
        "source_location_count": source_report["location_count"],
        "source_schema": source_report["schema"],
        "source_duplicate_observation_keys": source_report["duplicate_observation_keys"],
        "source_weather_code_null_count": source_report["weather_code_null_count"],
        "source_model_input_quality": source_quality,
        "source_invalid_model_input_values": source_invalid_values,
        "source_hourly_gap_edges": int(prewrite["source_hourly_gap_edges"] or 0),
        "lag24_history_continuity_violations": int(
            prewrite["lag24_history_continuity_violations"] or 0
        ),
        "target_time_gap_edges": int(prewrite["target_time_gap_edges"] or 0),
        "history_ineligible_rows": int(prewrite["history_ineligible_rows"] or 0),
        "target_ineligible_rows": int(prewrite["target_ineligible_rows"] or 0),
        "invalid_numeric_feature_rows": int(prewrite["invalid_numeric_feature_rows"] or 0),
        "eligible_rows_before_write": int(prewrite["eligible_rows"] or 0),
        "rows_dropped_for_history": int(prewrite["history_ineligible_rows"] or 0),
        "rows_dropped_for_target": int(prewrite["target_ineligible_rows"] or 0),
        "total_rows_dropped": source_report["rows"] - readback_report["output_rows"],
        "model_feature_count": len(MODEL_FEATURE_COLUMNS),
        "output_column_count": len(OUTPUT_COLUMNS),
        "source_delta_version_before": version_before,
        "source_delta_version_after": version_after,
        "readback_status": "PASS",
        "spark_shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
    }
    manifest = {
        "feature_set_id": FEATURE_SET_ID,
        "run_id": args.run_id,
        "status": "PASS",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_dataset_id": SOURCE_DATASET_ID,
        "source_delta_path": source_path,
        "source_rows": source_report["rows"],
        "source_delta_total_rows_verified": source_report["delta_total_rows"],
        "source_delta_location_count_verified": source_report["delta_location_count"],
        "source_location_count": source_report["location_count"],
        "source_schema": source_report["schema"],
        "source_weather_code_null_count": source_report["weather_code_null_count"],
        "source_git_commit": lineage_hashes["source_git_commit"],
        "feature_git_commit": args.feature_git_commit or "not-provided",
        "catalog_sha256": lineage_hashes["catalog_sha256"],
        "source_dataset_manifest": str(args.lineage_dir / "dataset_manifest.json"),
        "source_dataset_manifest_sha256": lineage_hashes["dataset_manifest_sha256"],
        "source_delta_validation_sha256": lineage_hashes["delta_validation_sha256"],
        "source_delta_validation_sha256_raw": lineage_hashes["delta_validation_sha256_raw"],
        "source_checksums_file_sha256": lineage_hashes["checksums_file_sha256"],
        "expected_source_manifest_sha256": lineage_hashes["expected_dataset_manifest_sha256"],
        "expected_source_delta_validation_sha256": lineage_hashes["expected_delta_validation_sha256"],
        "source_delta_version_before": version_before,
        "source_delta_version_after": version_after,
        "output_path": str(output_path),
        "output_rows": readback_report["output_rows"],
        "location_ids": source_report["location_ids"],
        "model_feature_count": len(MODEL_FEATURE_COLUMNS),
        "output_column_count": len(OUTPUT_COLUMNS),
        "model_features": MODEL_FEATURE_COLUMNS,
        "label": LABEL_COLUMN,
        "split_policy": "By target_time: TRAIN < 2024-01-01 UTC; VALIDATION in 2024; TEST in 2025.",
        "time_zone_policy": f"UTC timestamps; calendar features use {TIMEZONE}.",
        "smoke_run": bool(args.smoke),
        "spark_shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
    }
    run_dir.mkdir(parents=True, exist_ok=False)
    specification = feature_specification()
    schema = readback_report["schema"]
    _write_json(run_dir / "feature_manifest.json", manifest)
    _write_json(run_dir / "feature_spec.json", specification)
    _write_json(run_dir / "feature_validation.json", final_validation)
    _write_json(
        run_dir / "ml_schema.json",
        {
            "feature_set_id": FEATURE_SET_ID,
            "feature_count": len(MODEL_FEATURE_COLUMNS),
            "output_column_count": len(OUTPUT_COLUMNS),
            "columns": schema,
        },
    )
    _write_json(run_dir / "parquet_inventory.json", inventory)
    _write_json(run_dir / "statistics_by_split.json", readback_report["statistics_by_split"])
    _write_json(run_dir / "runtime_summary.json", runtime)
    _write_per_location_csv(run_dir / "per_location_split_counts.csv", readback_report["per_location_split_counts"])
    artifact_checksums = {}
    for path in sorted(run_dir.iterdir()):
        if path.is_file():
            artifact_checksums[path.name] = _sha256(path)
    _write_json(
        run_dir / "checksums.json",
        {
            "algorithm": "SHA-256",
            "scope": "All small report artifacts. Parquet files are inventoried by path, size and modification time.",
            "files": artifact_checksums,
        },
    )
    return {
        "status": "PASS",
        "run_id": args.run_id,
        "output_path": str(output_path),
        "results_path": str(run_dir),
        "source_rows": source_report["rows"],
        "output_rows": readback_report["output_rows"],
        "locations": source_report["location_count"],
        "feature_count": len(MODEL_FEATURE_COLUMNS),
        "output_column_count": len(OUTPUT_COLUMNS),
        "split_counts": readback_report["split_counts"],
        "parquet_file_count": inventory["parquet_file_count"],
        "parquet_total_bytes": inventory["total_bytes"],
        "runtime_seconds": elapsed,
        "source_delta_version_before": version_before,
        "source_delta_version_after": version_after,
        "target_alignment_violations": readback_report["target_time_alignment_violations"],
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spark = None
    try:
        spark = _create_spark(args)
        result = run_feature_engineering(args, spark=spark)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))
        return 0
    except Exception as exc:
        print(f"FEATURE_ENGINEERING_FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
