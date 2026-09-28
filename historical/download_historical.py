from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import heapq
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
from typing import Any, Iterable, Iterator

import httpx

try:
    from .location_catalog import (
        CATALOG_PATH, DATASET_BENCHMARK_20, DATASET_NATIONWIDE_63,
        HISTORICAL_END_YEAR, HISTORICAL_START_YEAR, ORIGINAL_20_IDS,
        expected_hours, expected_records, load_catalog, select_dataset_locations,
    )
except ImportError:
    from location_catalog import (
        CATALOG_PATH, DATASET_BENCHMARK_20, DATASET_NATIONWIDE_63,
        HISTORICAL_END_YEAR, HISTORICAL_START_YEAR, ORIGINAL_20_IDS,
        expected_hours, expected_records, load_catalog, select_dataset_locations,
    )


BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
LOCATIONS_FILE = CATALOG_PATH
DATA_ROOT = Path("/opt/project/history-data/historical")
RESULTS_ROOT = Path("/opt/project/results/data-expansion")
BATCH_SIZE = 10
REQUEST_DELAY_SECONDS = 5
MAX_RETRIES = 3
REQUEST_TIMEOUT_SECONDS = 120
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "pressure_msl",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
]


def load_locations(catalog_path: str | Path = LOCATIONS_FILE, dataset: str = "benchmark-20") -> list[dict[str, Any]]:
    return select_dataset_locations(load_catalog(catalog_path), dataset)


def chunks(items: list[Any], size: int) -> Iterator[list[Any]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    for index in range(0, len(items), size):
        yield items[index:index + size]


def normalize_time(value: str) -> str:
    if len(value) == 16:
        value += ":00"
    return value if value.endswith("Z") else value + "Z"


def _retry_delay(response: httpx.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(max(float(retry_after), 0), 120)
            except ValueError:
                pass
    return float(min(2 ** (attempt - 1), 30))


def fetch_batch(
    client: httpx.Client,
    locations: list[dict[str, Any]],
    year: int,
    max_retries: int = MAX_RETRIES,
    sleep=time.sleep,
    retry_counts: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    if not locations:
        raise ValueError("cannot request an empty location batch")
    params = {
        "latitude": ",".join(str(location["latitude"]) for location in locations),
        "longitude": ",".join(str(location["longitude"]) for location in locations),
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "UTC",
    }
    batch_key = f"{year}:" + ",".join(item["location_id"] for item in locations)
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        response = None
        try:
            response = client.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 429 or response.status_code >= 500:
                last_error = httpx.HTTPStatusError(
                    f"retryable Open-Meteo status {response.status_code}",
                    request=response.request,
                    response=response,
                )
                if attempt < max_retries:
                    if retry_counts is not None:
                        retry_counts[batch_key] = retry_counts.get(batch_key, 0) + 1
                    sleep(_retry_delay(response, attempt))
                    continue
                break
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return [payload]
            if not isinstance(payload, list):
                raise ValueError("Open-Meteo batch response must be an object or list")
            return payload
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_error = exc
            retryable = not isinstance(exc, httpx.HTTPStatusError) or (
                exc.response.status_code == 429 or exc.response.status_code >= 500
            )
            if not retryable or attempt >= max_retries:
                break
            if retry_counts is not None:
                retry_counts[batch_key] = retry_counts.get(batch_key, 0) + 1
            sleep(_retry_delay(response, attempt))
        except ValueError as exc:
            last_error = exc
            break
    raise RuntimeError(
        f"historical request failed year={year} "
        f"locations={[item['location_id'] for item in locations]} "
        f"attempts={max_retries}: {last_error}"
    ) from last_error


def map_batch_responses(locations: list[dict[str, Any]], payload: Any) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Pair responses with request coordinates by their deterministic request order."""
    responses = [payload] if isinstance(payload, dict) else payload
    if not isinstance(responses, list):
        raise ValueError("Open-Meteo response must be an object or list")
    if len(responses) != len(locations):
        raise ValueError(
            f"API returned {len(responses)} locations for {len(locations)} requested coordinates"
        )
    if any(not isinstance(response, dict) for response in responses):
        raise ValueError("each Open-Meteo location response must be an object")
    return list(zip(locations, responses, strict=True))


def convert_location(location: dict[str, Any], response: dict[str, Any]) -> list[dict[str, Any]]:
    if response.get("utc_offset_seconds", 0) != 0:
        raise ValueError(f"provider response for {location['location_id']} is not UTC")
    hourly = response.get("hourly")
    if not isinstance(hourly, dict) or not isinstance(hourly.get("time"), list):
        raise ValueError(f"provider response has no hourly timeline for {location['location_id']}")
    times = hourly["time"]
    for variable in HOURLY_VARIABLES:
        values = hourly.get(variable)
        if not isinstance(values, list) or len(values) != len(times):
            raise ValueError(f"hourly variable {variable} does not align for {location['location_id']}")
    records = []
    for index, raw_time in enumerate(times):
        event_time = normalize_time(raw_time)
        records.append({
            "event_id": f"{location['location_id']}_{event_time}",
            "event_type": "WEATHER_OBSERVATION",
            "location_id": location["location_id"],
            "city": location["name"],
            "latitude": response.get("latitude", location["latitude"]),
            "longitude": response.get("longitude", location["longitude"]),
            "event_time": event_time,
            "ingestion_time": None,
            "temperature_c": hourly["temperature_2m"][index],
            "humidity_pct": hourly["relative_humidity_2m"][index],
            "precipitation_mm": hourly["precipitation"][index],
            "pressure_hpa": hourly["pressure_msl"][index],
            "wind_speed_kmh": hourly["wind_speed_10m"][index],
            "wind_gust_kmh": hourly["wind_gusts_10m"][index],
            "weather_code": hourly["weather_code"][index],
            "source": "OPEN_METEO_HISTORICAL",
        })
    return records


def iter_records(path: str | Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as source:
        for line in source:
            if line.strip():
                yield json.loads(line)


def count_records(path: str | Path) -> int:
    return sum(1 for _ in iter_records(path))


def _unit_validation(path: str | Path, location: dict[str, Any], year: int) -> dict[str, Any]:
    count = duplicate_count = 0
    previous_time = previous_key = minimum = maximum = None
    for record in iter_records(path):
        event_time = record.get("event_time")
        location_id = record.get("location_id")
        key = (event_time, location_id)
        count += 1
        if location_id != location["location_id"]:
            return {"valid": False, "reason": "wrong_location_id", "actual_records": count}
        if not isinstance(event_time, str) or not event_time.startswith(f"{year}-"):
            return {"valid": False, "reason": "wrong_year_or_missing_time", "actual_records": count}
        if record.get("event_id") != f"{location_id}_{event_time}":
            return {"valid": False, "reason": "event_id_mismatch", "actual_records": count}
        if previous_key is not None and key < previous_key:
            return {"valid": False, "reason": "sorting_violation", "actual_records": count}
        if event_time == previous_time:
            duplicate_count += 1
        previous_time, previous_key = event_time, key
        minimum = event_time if minimum is None or event_time < minimum else minimum
        maximum = event_time if maximum is None or event_time > maximum else maximum
    expected = expected_hours(year)
    valid = count > 0 and duplicate_count == 0 and count <= expected
    return {
        "valid": valid,
        "actual_records": count,
        "expected_records": expected,
        "missing_records": max(0, expected - count),
        "duplicate_count": duplicate_count,
        "min_event_time": minimum,
        "max_event_time": maximum,
        "reason": None if valid else "empty_or_duplicate_or_excess_records",
    }


def unit_file_valid(path: str | Path, location: dict[str, Any], year: int) -> bool:
    try:
        return bool(_unit_validation(path, location, year)["valid"])
    except (OSError, EOFError, json.JSONDecodeError, KeyError, TypeError):
        return False


def write_unit(path: str | Path, location: dict[str, Any], year: int, records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".partial")
    temporary.unlink(missing_ok=True)
    ordered = sorted(records, key=lambda row: row["event_time"])
    if not ordered:
        raise ValueError(f"refusing to promote empty unit {location['location_id']} {year}")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as output:
        for record in ordered:
            output.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
    validation = _unit_validation(temporary, location, year)
    if not validation["valid"]:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"invalid unit {location['location_id']} {year}: {validation}")
    temporary.replace(destination)
    return validation


def _write_record(output, record: dict[str, Any]) -> None:
    output.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def seed_benchmark_units(
    legacy_raw_dir: str | Path,
    unit_dir: str | Path,
    locations: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """Split the old 20-location yearly files into resumable location/year units."""
    legacy_raw_dir, unit_dir = Path(legacy_raw_dir), Path(unit_dir)
    original_ids = set(ORIGINAL_20_IDS)
    originals = [item for item in locations if item["location_id"] in original_ids]
    seeded: list[str] = []
    errors: list[str] = []
    for year in range(HISTORICAL_START_YEAR, HISTORICAL_END_YEAR + 1):
        source_path = legacy_raw_dir / f"weather_{year}.jsonl.gz"
        if not source_path.exists():
            continue
        missing = [
            item for item in originals
            if not unit_file_valid(unit_dir / item["location_id"] / f"{year}.jsonl.gz", item, year)
        ]
        if not missing:
            continue
        writers: dict[str, Any] = {}
        temporary_paths: dict[str, Path] = {}
        needed_ids = {item["location_id"] for item in missing}
        try:
            for location in missing:
                location_id = location["location_id"]
                target = unit_dir / location_id / f"{year}.jsonl.gz"
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_name(target.name + ".partial")
                temporary.unlink(missing_ok=True)
                temporary_paths[location_id] = temporary
                writers[location_id] = gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6)
            source_ids: set[str] = set()
            for record in iter_records(source_path):
                location_id = record.get("location_id")
                if location_id not in original_ids:
                    raise ValueError(f"legacy source contains unexpected ID {location_id!r}")
                source_ids.add(location_id)
                if location_id in needed_ids:
                    _write_record(writers[location_id], record)
            for writer in writers.values():
                writer.close()
            for location in missing:
                location_id = location["location_id"]
                temporary = temporary_paths[location_id]
                validation = _unit_validation(temporary, location, year)
                if not validation["valid"]:
                    raise ValueError(f"legacy unit {location_id} {year} is invalid: {validation}")
                temporary.replace(unit_dir / location_id / f"{year}.jsonl.gz")
                seeded.append(f"{location_id}:{year}")
            if not source_ids.issuperset(needed_ids):
                errors.append(f"{year}: legacy source lacked IDs {sorted(needed_ids - source_ids)}")
        except Exception as exc:
            errors.append(f"{year}: {exc}")
            for writer in writers.values():
                if not writer.closed:
                    writer.close()
            for temporary in temporary_paths.values():
                temporary.unlink(missing_ok=True)
    return seeded, errors


def validate_raw_year(path: str | Path, locations: list[dict[str, Any]], year: int) -> dict[str, Any]:
    expected_ids = {item["location_id"] for item in locations}
    counts = {location_id: 0 for location_id in expected_ids}
    previous_key = previous_event_id = minimum = maximum = None
    sorting_violations = duplicate_count = total = 0
    unknown_ids: set[str] = set()
    try:
        for record in iter_records(path):
            total += 1
            event_time = record.get("event_time")
            location_id = record.get("location_id")
            event_id = record.get("event_id")
            if location_id not in expected_ids:
                unknown_ids.add(str(location_id))
                continue
            if not isinstance(event_time, str) or not event_time.startswith(f"{year}-"):
                sorting_violations += 1
                continue
            if event_id != f"{location_id}_{event_time}":
                sorting_violations += 1
            key = (event_time, location_id)
            if previous_key is not None and key < previous_key:
                sorting_violations += 1
            if event_id == previous_event_id:
                duplicate_count += 1
            previous_key, previous_event_id = key, event_id
            counts[location_id] += 1
            minimum = event_time if minimum is None or event_time < minimum else minimum
            maximum = event_time if maximum is None or event_time > maximum else maximum
    except (OSError, EOFError, json.JSONDecodeError, TypeError):
        return {"valid": False, "reason": "unreadable_raw_file"}
    per_location_expected = expected_hours(year)
    missing_by_location = {
        location_id: max(0, per_location_expected - count)
        for location_id, count in counts.items()
        if count < per_location_expected
    }
    valid = (
        total > 0 and not unknown_ids and not sorting_violations
        and not duplicate_count and all(count > 0 for count in counts.values())
    )
    expected_total = expected_records(len(locations), [year])
    return {
        "valid": valid,
        "year": year,
        "actual_records": total,
        "expected_records": expected_total,
        "missing_records": max(0, expected_total - total),
        "unique_location_ids": sum(count > 0 for count in counts.values()),
        "records_per_location": counts,
        "missing_by_location": missing_by_location,
        "unique_event_ids": total - duplicate_count,
        "duplicate_event_ids": duplicate_count,
        "unknown_location_ids": sorted(unknown_ids),
        "sorting_violations": sorting_violations,
        "min_event_time": minimum,
        "max_event_time": maximum,
    }


def merge_year(locations: list[dict[str, Any]], unit_dir: str | Path, output_path: str | Path, year: int) -> dict[str, Any]:
    unit_dir = Path(unit_dir)
    unit_paths = [
        unit_dir / location["location_id"] / f"{year}.jsonl.gz"
        for location in locations
        if unit_file_valid(unit_dir / location["location_id"] / f"{year}.jsonl.gz", location, year)
    ]
    if not unit_paths:
        return {"valid": False, "year": year, "actual_records": 0, "missing_units": len(locations)}
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".partial")
    temporary.unlink(missing_ok=True)
    merged = heapq.merge(
        *(iter_records(path) for path in unit_paths),
        key=lambda row: (row["event_time"], row["location_id"]),
    )
    try:
        with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as output:
            for record in merged:
                _write_record(output, record)
        validation = validate_raw_year(temporary, locations, year)
        if validation["actual_records"] == 0:
            temporary.unlink(missing_ok=True)
            return validation
        temporary.replace(destination)
        validation["missing_units"] = sum(
            not unit_file_valid(unit_dir / loc["location_id"] / f"{year}.jsonl.gz", loc, year)
            for loc in locations
        )
        return validation
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def download_year(
    client: httpx.Client,
    locations: list[dict[str, Any]],
    year: int,
    unit_dir: str | Path,
    raw_dir: str | Path,
    *,
    batch_size: int = BATCH_SIZE,
    request_delay_seconds: float = REQUEST_DELAY_SECONDS,
    max_retries: int = MAX_RETRIES,
    retry_counts: dict[str, int] | None = None,
    request_counter: dict[str, int] | None = None,
    sleep=time.sleep,
) -> dict[str, Any]:
    unit_dir, raw_dir = Path(unit_dir), Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_path = raw_dir / f"weather_{year}.jsonl.gz"
    if output_path.exists():
        existing = validate_raw_year(output_path, locations, year)
        if existing.get("valid") and existing.get("missing_records") == 0:
            print(f"[SKIP] year={year} already validated: {output_path}")
            return existing

    missing = [
        location for location in locations
        if not unit_file_valid(unit_dir / location["location_id"] / f"{year}.jsonl.gz", location, year)
    ]
    failed: list[str] = []
    for batch_number, batch in enumerate(chunks(missing, batch_size), start=1):
        print(f"[DOWNLOAD] year={year} batch={batch_number} locations={len(batch)}")
        if request_counter is not None:
            request_counter["batch_requests"] = request_counter.get("batch_requests", 0) + 1
        try:
            payload = fetch_batch(
                client, batch, year, max_retries=max_retries,
                sleep=sleep, retry_counts=retry_counts,
            )
            mapped = map_batch_responses(batch, payload)
        except Exception as exc:
            failed.extend(location["location_id"] for location in batch)
            print(f"[FAILED] year={year} ids={[x['location_id'] for x in batch]} error={exc}")
        else:
            for location, response in mapped:
                try:
                    records = convert_location(location, response)
                    result = write_unit(
                        unit_dir / location["location_id"] / f"{year}.jsonl.gz",
                        location, year, records,
                    )
                    print(
                        f"[UNIT] {location['location_id']} {year} "
                        f"records={result['actual_records']:,} missing={result['missing_records']:,}"
                    )
                except Exception as exc:
                    failed.append(location["location_id"])
                    print(f"[FAILED] {location['location_id']} {year}: {exc}")
        if request_delay_seconds:
            sleep(request_delay_seconds)

    result = merge_year(locations, unit_dir, output_path, year)
    result["failed_location_ids"] = sorted(set(failed))
    result["output_path"] = str(output_path)
    print(
        f"[YEAR] {year} actual={result.get('actual_records', 0):,} "
        f"expected={result.get('expected_records', 0):,} "
        f"missing={result.get('missing_records', 0):,} "
        f"unique_locations={result.get('unique_location_ids', 0)}"
    )
    return result


def _git_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True, timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return os.getenv("GIT_COMMIT")


def run_download(args: argparse.Namespace, client: httpx.Client | None = None) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    run_id = args.run_id or (started.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
    catalog_path = Path(args.catalog)
    catalog = load_catalog(catalog_path)
    locations = select_dataset_locations(catalog, args.dataset)
    dataset_id = (
        DATASET_BENCHMARK_20
        if args.dataset.strip().upper().replace("-", "_") == DATASET_BENCHMARK_20
        else DATASET_NATIONWIDE_63
    )
    data_root = Path(args.data_root)
    if dataset_id == DATASET_BENCHMARK_20:
        unit_dir = data_root / "staging" / "benchmark_20" / "units"
        raw_dir = data_root / "raw"
    else:
        unit_dir = data_root / "nationwide_63" / "staging" / "units"
        raw_dir = data_root / "nationwide_63" / "raw"
    reused, reuse_errors = [], []
    if dataset_id == DATASET_NATIONWIDE_63:
        reused, reuse_errors = seed_benchmark_units(data_root / "raw", unit_dir, locations)
        if reuse_errors:
            print(f"[REUSE WARNING] {reuse_errors}")

    owned_client = client is None
    active_client = client or httpx.Client()
    retry_counts: dict[str, int] = {}
    request_counter: dict[str, int] = {}
    year_results = []
    try:
        for year in range(HISTORICAL_START_YEAR, HISTORICAL_END_YEAR + 1):
            year_results.append(download_year(
                active_client, locations, year, unit_dir, raw_dir,
                batch_size=args.batch_size,
                request_delay_seconds=args.request_delay,
                max_retries=args.max_retries,
                retry_counts=retry_counts,
                request_counter=request_counter,
            ))
    finally:
        if owned_client:
            active_client.close()

    successful, failed = [], []
    for location in locations:
        for year in range(HISTORICAL_START_YEAR, HISTORICAL_END_YEAR + 1):
            unit_path = unit_dir / location["location_id"] / f"{year}.jsonl.gz"
            key = f"{location['location_id']}:{year}"
            annual_count = next(
                (
                    result.get("records_per_location", {}).get(location["location_id"], 0)
                    for result in year_results
                    if result.get("year") == year
                ),
                0,
            )
            unit_ok = unit_file_valid(unit_path, location, year)
            annual_ok = (
                dataset_id == DATASET_BENCHMARK_20
                and annual_count == expected_hours(year)
            )
            (successful if unit_ok or annual_ok else failed).append(key)

    ended = datetime.now(timezone.utc)
    report_dir = Path(args.results_dir) if args.results_dir else RESULTS_ROOT / run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    checksum = hashlib.sha256(catalog_path.read_bytes()).hexdigest()
    summary_path = report_dir / "historical_download_summary.json"
    manifest = {
        "run_id": run_id,
        "git_commit": _git_commit(),
        "dataset_id": dataset_id,
        "admin_snapshot": "VN_63_PRE_2025_MERGER",
        "catalog_sha256": checksum,
        "location_count": len(locations),
        "date_range": {
            "start": f"{HISTORICAL_START_YEAR}-01-01T00:00:00Z",
            "end": f"{HISTORICAL_END_YEAR}-12-31T23:00:00Z",
        },
        "years": list(range(HISTORICAL_START_YEAR, HISTORICAL_END_YEAR + 1)),
        "weather_variables": HOURLY_VARIABLES,
        "timezone": "UTC",
        "provider": "Open-Meteo Historical Weather API",
        "endpoint": BASE_URL,
        "batch_size": args.batch_size,
        "requested_location_years": len(locations) * (HISTORICAL_END_YEAR - HISTORICAL_START_YEAR + 1),
        "successful_location_years": successful,
        "failed_location_years": failed,
        "reused_benchmark_location_years": reused,
        "reuse_errors": reuse_errors,
        "retry_counts": retry_counts,
        "request_count": request_counter.get("batch_requests", 0),
        "total_http_attempts": request_counter.get("batch_requests", 0) + sum(retry_counts.values()),
        "start_time": started.isoformat(),
        "end_time": ended.isoformat(),
        "output_paths": [
            str(raw_dir / f"weather_{year}.jsonl.gz")
            for year in range(HISTORICAL_START_YEAR, HISTORICAL_END_YEAR + 1)
        ],
        "summary_path": str(summary_path),
        "year_results": year_results,
        "actual_records": sum(result.get("actual_records", 0) for result in year_results),
        "expected_records": expected_records(len(locations)),
        "missing_records": sum(result.get("missing_records", 0) for result in year_results),
        "failed_location_year_count": len(failed),
        "status": (
            "PASS" if not failed and all(
                result.get("valid") and result.get("missing_records") == 0
                for result in year_results
            ) else "INCOMPLETE"
        ),
    }
    temporary = summary_path.with_name(summary_path.name + ".partial")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(summary_path)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resumable Open-Meteo historical weather downloader.")
    parser.add_argument(
        "--dataset", choices=("benchmark-20", "nationwide-63"), default="benchmark-20",
        help="Select the immutable BENCHMARK_20 source or separate NATIONWIDE_63 expansion.",
    )
    parser.add_argument("--catalog", type=Path, default=LOCATIONS_FILE)
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT)
    parser.add_argument("--results-dir", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--request-delay", type=float, default=REQUEST_DELAY_SECONDS)
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")
    if args.request_delay < 0:
        raise ValueError("--request-delay cannot be negative")
    if args.max_retries <= 0:
        raise ValueError("--max-retries must be positive")
    result = run_download(args)
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
