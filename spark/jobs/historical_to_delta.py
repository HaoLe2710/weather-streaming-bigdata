from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys


HISTORY_DATA_ROOT = Path("/opt/project/history-data/historical")
DELTA_ROOT = Path("/opt/project/data/historical")
CATALOG_PATH = Path("/opt/project/historical/locations.json")
RESULTS_ROOT = Path("/opt/project/results/data-expansion")


def dataset_paths(dataset: str, source_override: str | None = None, target_override: str | None = None) -> tuple[Path, Path]:
    normalized = dataset.strip().upper().replace("-", "_")
    if normalized == "BENCHMARK_20":
        default_source = HISTORY_DATA_ROOT / "raw"
        default_target = DELTA_ROOT / "weather_hourly"
    elif normalized == "NATIONWIDE_63":
        default_source = HISTORY_DATA_ROOT / "nationwide_63" / "raw"
        default_target = DELTA_ROOT / "weather_hourly_vn63"
    else:
        raise ValueError("dataset must be benchmark-20 or nationwide-63")
    return Path(source_override) if source_override else default_source, Path(target_override) if target_override else default_target


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert a named historical dataset to Delta.")
    parser.add_argument("--dataset", choices=("benchmark-20", "nationwide-63"), default="benchmark-20")
    parser.add_argument("--source", help="Optional explicit JSONL.GZ file or directory override.")
    parser.add_argument("--target", help="Optional explicit Delta target override.")
    parser.add_argument("--catalog", type=Path, default=CATALOG_PATH)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--run-id", default="nationwide-63")
    return parser.parse_args(argv)


def source_glob(source: Path) -> str:
    if source.is_file() or source.name.endswith(".jsonl.gz") or "*" in str(source):
        return str(source)
    return str(source / "weather_*.jsonl.gz")


def _format_utc(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value.replace("+00:00", "Z")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_conversion(args: argparse.Namespace, spark=None) -> dict:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    owned_spark = spark is None
    if spark is None:
        spark = (
            SparkSession.builder
            .appName("HistoricalWeatherToDelta")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")

    source_path, target_path = dataset_paths(args.dataset, args.source, args.target)
    dataset_id = args.dataset.strip().upper().replace("-", "_")
    sys.path.insert(0, str(args.catalog.parent))
    try:
        from location_catalog import expected_records, load_catalog, select_dataset_locations
        catalog = load_catalog(args.catalog)
        locations = select_dataset_locations(catalog, dataset_id)
    except (ImportError, OSError, ValueError) as exc:
        raise RuntimeError(f"cannot validate catalog for {dataset_id}: {exc}") from exc
    expected_ids = {item["location_id"] for item in locations}
    expected_count = expected_records(len(locations))

    source = source_glob(source_path)
    print(f"Dataset: {dataset_id}")
    print(f"Reading historical JSONL: {source}")
    print(f"Writing Delta: {target_path}")

    raw = spark.read.json(source)
    raw_count = raw.count()
    print("Raw records:", raw_count)

    weather = (
        raw
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("year", F.year("event_time"))
    )
    deduplicated = weather.dropDuplicates(["event_id"])
    deduplicated_count = deduplicated.count()
    raw_duplicate_ids = raw_count - deduplicated_count

    valid = deduplicated.filter(
        F.col("event_id").isNotNull()
        & F.col("location_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("temperature_c").between(-90.0, 60.0)
        & F.col("humidity_pct").between(0.0, 100.0)
        & (F.col("precipitation_mm") >= 0)
    )
    valid_count = valid.count()
    invalid_or_duplicate_count = raw_count - valid_count
    print("Valid records:", valid_count)

    (
        valid.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year")
        .save(str(target_path))
    )

    delta = spark.read.format("delta").load(str(target_path))
    delta_count = delta.count()
    delta_unique_event_ids = delta.select("event_id").distinct().count()
    delta_location_ids = sorted(
        row["location_id"]
        for row in delta.select("location_id").distinct().collect()
        if row["location_id"] is not None
    )
    delta_per_location = {
        row["location_id"]: row["count"]
        for row in delta.groupBy("location_id").count().collect()
        if row["location_id"] is not None
    }
    bounds = delta.agg(
        F.min("event_time").alias("min_event_time"),
        F.max("event_time").alias("max_event_time"),
    ).first()
    unknown_location_ids = sorted(set(delta_location_ids) - expected_ids)
    target_row_match = delta_count == valid_count
    duplicate_delta_ids = delta_count - delta_unique_event_ids
    time_range = (
        _format_utc(bounds["min_event_time"]),
        _format_utc(bounds["max_event_time"]),
    )
    expected_time_range = (
        "2020-01-01T00:00:00Z",
        "2025-12-31T23:00:00Z",
    )
    status = "PASS" if (
        target_row_match
        and duplicate_delta_ids == 0
        and not unknown_location_ids
        and len(delta_location_ids) == len(expected_ids)
        and time_range == expected_time_range
    ) else "INCOMPLETE"
    report = {
        "status": status,
        "dataset_id": dataset_id,
        "run_id": args.run_id,
        "source_path": str(source_path),
        "source_glob": source,
        "delta_path": str(target_path),
        "catalog_location_count": len(locations),
        "catalog_location_ids": sorted(expected_ids),
        "delta_location_ids": delta_location_ids,
        "unique_location_ids": len(delta_location_ids),
        "expected_records": expected_count,
        "raw_records": raw_count,
        "raw_duplicate_event_ids_removed": raw_duplicate_ids,
        "invalid_or_duplicate_raw_records_excluded": invalid_or_duplicate_count,
        "raw_valid_records": valid_count,
        "delta_records": delta_count,
        "delta_unique_event_ids": delta_unique_event_ids,
        "delta_duplicate_event_ids": duplicate_delta_ids,
        "delta_matches_raw_valid_records": target_row_match,
        "expected_time_range": list(expected_time_range),
        "actual_time_range": list(time_range),
        "records_per_location": delta_per_location,
        "partition_column": "year",
        "validated_at": datetime.now(timezone.utc).isoformat(),
    }
    report_path = args.report
    if report_path is None:
        report_path = RESULTS_ROOT / args.run_id / "delta_validation.json"
    report_path = Path(report_path)
    _write_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Delta validation report: {report_path}")
    if owned_spark:
        spark.stop()
    return report


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = run_conversion(args)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
