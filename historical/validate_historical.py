"""Validate hourly nationwide raw files without imputing provider gaps."""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import uuid
from typing import Any

try:
    from .location_catalog import (
        CATALOG_PATH,
        HISTORICAL_END_YEAR,
        HISTORICAL_START_YEAR,
        expected_hours,
        expected_records,
        load_catalog,
        select_dataset_locations,
    )
except ImportError:
    from location_catalog import (
        CATALOG_PATH,
        HISTORICAL_END_YEAR,
        HISTORICAL_START_YEAR,
        expected_hours,
        expected_records,
        load_catalog,
        select_dataset_locations,
    )


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_DIR = REPO_ROOT / "data" / "historical" / "nationwide_63" / "raw"
DEFAULT_RESULTS_DIR = REPO_ROOT / "results" / "data-expansion"
TIMESTAMP_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _day_indexes() -> dict[int, dict[str, int]]:
    indexes = {}
    for year in range(HISTORICAL_START_YEAR, HISTORICAL_END_YEAR + 1):
        days = 366 if expected_hours(year) == 8784 else 365
        first = date(year, 1, 1)
        indexes[year] = {
            (first + timedelta(days=index)).isoformat(): index
            for index in range(days)
        }
    return indexes


def _hour_position(event_time: Any, days: dict[int, dict[str, int]]) -> tuple[int, int] | None:
    if not isinstance(event_time, str) or not TIMESTAMP_PATTERN.fullmatch(event_time):
        return None
    try:
        year = int(event_time[:4])
        hour = int(event_time[11:13])
        minute = int(event_time[14:16])
        second = int(event_time[17:19])
        day_index = days[year][event_time[:10]]
    except (KeyError, ValueError):
        return None
    if not 0 <= hour <= 23 or minute != 0 or second != 0:
        return None
    return year, day_index * 24 + hour


def _make_presence(locations: list[dict[str, Any]]) -> dict[str, dict[int, bytearray]]:
    return {
        location["location_id"]: {
            year: bytearray((expected_hours(year) + 7) // 8)
            for year in range(HISTORICAL_START_YEAR, HISTORICAL_END_YEAR + 1)
        }
        for location in locations
    }


def _has_hour(mask: bytearray, index: int) -> bool:
    return bool(mask[index >> 3] & (1 << (index & 7)))


def _set_hour(mask: bytearray, index: int) -> bool:
    byte_index, bit = index >> 3, 1 << (index & 7)
    already_present = bool(mask[byte_index] & bit)
    mask[byte_index] |= bit
    return already_present


def _quality_fields(record: dict[str, Any]) -> list[str]:
    invalid = []
    temperature = record.get("temperature_c")
    humidity = record.get("humidity_pct")
    precipitation = record.get("precipitation_mm")
    if (
        not isinstance(temperature, (int, float))
        or isinstance(temperature, bool)
        or not math.isfinite(temperature)
        or not -90 <= temperature <= 60
    ):
        invalid.append("temperature_c")
    if (
        not isinstance(humidity, (int, float))
        or isinstance(humidity, bool)
        or not math.isfinite(humidity)
        or not 0 <= humidity <= 100
    ):
        invalid.append("humidity_pct")
    if (
        not isinstance(precipitation, (int, float))
        or isinstance(precipitation, bool)
        or not math.isfinite(precipitation)
        or precipitation < 0
    ):
        invalid.append("precipitation_mm")
    for field, low, high in (("latitude", -90, 90), ("longitude", -180, 180)):
        value = record.get(field)
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or not low <= value <= high
        ):
            invalid.append(field)
    return invalid


def _missing_timestamps(mask: bytearray, year: int) -> list[str]:
    first = datetime(year, 1, 1, tzinfo=timezone.utc)
    return [
        (first + timedelta(hours=index)).isoformat(timespec="seconds").replace("+00:00", "Z")
        for index in range(expected_hours(year))
        if not _has_hour(mask, index)
    ]


def validate_raw_dataset(
    raw_dir: str | Path,
    locations: list[dict[str, Any]],
) -> dict[str, Any]:
    raw_dir = Path(raw_dir)
    expected_ids = {location["location_id"] for location in locations}
    days = _day_indexes()
    presence = _make_presence(locations)
    per_location = {
        location["location_id"]: {
            "location_id": location["location_id"],
            "province_name": location["province_name"],
            "representative_place": location["representative_place"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "actual_records": 0,
            "expected_records": expected_records(1),
            "missing_records": 0,
            "min_event_time": None,
            "max_event_time": None,
            "duplicate_count": 0,
            "duplicate_observation_count": 0,
            "invalid_coordinate_records": 0,
            "quality_violation_count": 0,
            "status": "PASS",
            "missing_timestamps": [],
        }
        for location in locations
    }
    year_actual = {year: 0 for year in range(HISTORICAL_START_YEAR, HISTORICAL_END_YEAR + 1)}
    year_expected = {
        year: expected_records(len(locations), [year])
        for year in range(HISTORICAL_START_YEAR, HISTORICAL_END_YEAR + 1)
    }
    year_unique_locations = {year: set() for year in year_actual}
    event_ids: set[str] = set()
    duplicate_event_ids = 0
    duplicate_observation_keys = 0
    null_counts = {field: 0 for field in ("event_id", "location_id", "event_time")}
    event_id_integrity_errors = 0
    sorting_violations = 0
    quality_violation_count = 0
    quality_samples: list[dict[str, str]] = []
    unknown_ids: set[str] = set()
    unknown_location_records = 0
    malformed_times: list[dict[str, Any]] = []
    minimum_time = None
    maximum_time = None
    previous_key = None
    total_records = 0
    files = sorted(raw_dir.glob("weather_*.jsonl.gz"))

    for path in files:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue
                total_records += 1
                record = json.loads(line)
                event_id = record.get("event_id")
                location_id = record.get("location_id")
                event_time = record.get("event_time")
                for field, value in (
                    ("event_id", event_id),
                    ("location_id", location_id),
                    ("event_time", event_time),
                ):
                    if value is None:
                        null_counts[field] += 1

                if isinstance(event_id, str):
                    if event_id in event_ids:
                        duplicate_event_ids += 1
                        if isinstance(location_id, str) and location_id in per_location:
                            per_location[location_id]["duplicate_count"] += 1
                    else:
                        event_ids.add(event_id)

                if event_id != f"{location_id}_{event_time}":
                    event_id_integrity_errors += 1

                if isinstance(event_time, str):
                    key = (event_time, str(location_id))
                    if previous_key is not None and key < previous_key:
                        sorting_violations += 1
                    previous_key = key
                    minimum_time = event_time if minimum_time is None or event_time < minimum_time else minimum_time
                    maximum_time = event_time if maximum_time is None or event_time > maximum_time else maximum_time

                if not isinstance(location_id, str) or location_id not in expected_ids:
                    unknown_ids.add(str(location_id))
                    unknown_location_records += 1
                    continue

                location_stats = per_location[location_id]
                location_stats["actual_records"] += 1
                if (
                    location_stats["min_event_time"] is None
                    or (isinstance(event_time, str) and event_time < location_stats["min_event_time"])
                ):
                    location_stats["min_event_time"] = event_time
                if (
                    location_stats["max_event_time"] is None
                    or (isinstance(event_time, str) and event_time > location_stats["max_event_time"])
                ):
                    location_stats["max_event_time"] = event_time

                position = _hour_position(event_time, days)
                if position is None:
                    if len(malformed_times) < 100:
                        malformed_times.append({
                            "file": path.name,
                            "line": line_number,
                            "location_id": location_id,
                            "event_time": event_time,
                        })
                    sorting_violations += 1
                else:
                    year, hour_index = position
                    year_actual[year] += 1
                    year_unique_locations[year].add(location_id)
                    if _set_hour(presence[location_id][year], hour_index):
                        duplicate_observation_keys += 1
                        location_stats["duplicate_observation_count"] += 1

                invalid_fields = _quality_fields(record)
                if invalid_fields:
                    quality_violation_count += len(invalid_fields)
                    location_stats["quality_violation_count"] += len(invalid_fields)
                    if len(quality_samples) < 100:
                        quality_samples.append({
                            "location_id": location_id,
                            "event_time": str(event_time),
                            "fields": ",".join(invalid_fields),
                        })
                if any(field in invalid_fields for field in ("latitude", "longitude")):
                    location_stats["invalid_coordinate_records"] += 1

    missing_by_year = {year: 0 for year in year_actual}
    affected_locations_by_year = {year: [] for year in year_actual}
    for location_id, stats in per_location.items():
        actual = stats["actual_records"]
        for year, mask in presence[location_id].items():
            missing_times = _missing_timestamps(mask, year)
            missing_by_year[year] += len(missing_times)
            if missing_times:
                affected_locations_by_year[year].append(location_id)
                stats["missing_timestamps"].extend(missing_times)
        stats["missing_records"] = len(stats["missing_timestamps"])
        if stats["missing_records"] or actual != stats["expected_records"]:
            stats["status"] = "MISSING_HOURS"
        if actual > stats["expected_records"]:
            stats["status"] = "DUPLICATE"
        if stats["invalid_coordinate_records"]:
            stats["status"] = "INVALID_COORDINATE"
        if stats["duplicate_count"] or stats["duplicate_observation_count"]:
            stats["status"] = "DUPLICATE"

    year_report = []
    for year in year_actual:
        year_report.append({
            "year": year,
            "expected": year_expected[year],
            "actual": year_actual[year],
            "difference": year_actual[year] - year_expected[year],
            "missing_records": missing_by_year[year],
            "affected_locations": sorted(affected_locations_by_year[year]),
            "unique_location_ids": len(year_unique_locations[year]),
        })

    expected_total = expected_records(len(locations))
    actual_total = total_records
    missing_total = sum(row["missing_records"] for row in per_location.values())
    unique_location_ids = len({
        row["location_id"] for row in per_location.values() if row["actual_records"] > 0
    })
    status = "PASS"
    if (
        actual_total != expected_total
        or duplicate_event_ids
        or duplicate_observation_keys
        or unknown_ids
        or any(null_counts.values())
        or event_id_integrity_errors
        or sorting_violations
        or quality_violation_count
        or unique_location_ids != len(locations)
    ):
        status = "INCOMPLETE"
    return {
        "status": status,
        "raw_directory": str(raw_dir),
        "raw_files": [str(path) for path in files],
        "total_records": actual_total,
        "expected_records": expected_total,
        "missing_records": missing_total,
        "unique_event_ids": len(event_ids),
        "duplicate_event_ids": duplicate_event_ids,
        "duplicate_observation_keys": duplicate_observation_keys,
        "unknown_location_records": unknown_location_records,
        "unique_location_ids": unique_location_ids,
        "location_count": len(locations),
        "records_per_year": year_report,
        "min_event_time": minimum_time,
        "max_event_time": maximum_time,
        "null_counts": null_counts,
        "unknown_location_ids": sorted(unknown_ids),
        "sorting_violations": sorting_violations,
        "malformed_timestamp_samples": malformed_times,
        "event_id_integrity_errors": event_id_integrity_errors,
        "weather_quality_violation_count": quality_violation_count,
        "weather_quality_samples": quality_samples,
        "per_location": list(per_location.values()),
    }


def _write_reports(report: dict[str, Any], output_json: Path) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_json.with_name(output_json.name + ".partial")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(output_json)
    locations_csv = output_json.with_name("per_location_validation.csv")
    fields = [
        "location_id", "province_name", "representative_place", "latitude", "longitude",
        "actual_records", "expected_records", "missing_records", "min_event_time",
        "max_event_time", "duplicate_count", "status", "missing_timestamps",
    ]
    with locations_csv.open("w", newline="", encoding="utf-8-sig") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for row in report["per_location"]:
            writer.writerow({
                **row,
                "missing_timestamps": ",".join(row["missing_timestamps"]),
            })
    years_csv = output_json.with_name("year_validation.csv")
    with years_csv.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=[
            "year", "expected", "actual", "difference", "unique_location_ids",
        ])
        writer.writeheader()
        writer.writerows(report["records_per_year"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--catalog", type=Path, default=CATALOG_PATH)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--run-id")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    locations = select_dataset_locations(load_catalog(args.catalog), "nationwide-63")
    report = validate_raw_dataset(args.raw_dir, locations)
    report.update({
        "run_id": run_id,
        "admin_snapshot": "VN_63_PRE_2025_MERGER",
        "catalog_sha256": hashlib.sha256(args.catalog.read_bytes()).hexdigest(),
        "validated_at": datetime.now(timezone.utc).isoformat(),
    })
    output = args.output or DEFAULT_RESULTS_DIR / run_id / "historical_validation.json"
    _write_reports(report, output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={output}")
    print(f"per_location_report={output.with_name('per_location_validation.csv')}")
    print(f"year_report={output.with_name('year_validation.csv')}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
