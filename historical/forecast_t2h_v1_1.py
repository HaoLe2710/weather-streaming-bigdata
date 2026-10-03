"""Resumable acquisition and validation for the V1.1 T2H source contract.

This module is deliberately separate from ``download_historical.py``. It never
changes the existing Archive/Delta inputs used by the T1H pipeline.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import random
import time as time_module
from typing import Any, Callable, Iterable, Mapping, Sequence

import httpx

try:
    from .location_catalog import CATALOG_PATH, DATASET_NATIONWIDE_63, load_catalog, select_dataset_locations
except ImportError:  # pragma: no cover - direct script execution
    from location_catalog import CATALOG_PATH, DATASET_NATIONWIDE_63, load_catalog, select_dataset_locations


SOURCE_CONTRACT_ID = "WEATHER_FORECAST_SOURCE_T2H_V1_1"
MODEL_ID = "WEATHER_XGBOOST_GLOBAL_T2H_V1_1"
DEFAULT_START_DATE = date(2020, 1, 1)
DEFAULT_END_DATE = date(2025, 12, 31)
DEFAULT_DATA_ROOT = Path("data/historical_forecast")
DEFAULT_RUN_ID = "20261003T070520Z-xgb-t2h-v1-1"
DEFAULT_ARTIFACT_ROOT = Path("results/modeling-t2h") / DEFAULT_RUN_ID
DEFAULT_BATCH_SIZE = 10
DEFAULT_MAX_RETRIES = 3
DEFAULT_REQUEST_DELAY_SECONDS = 30.0
REQUEST_TIMEOUT_SECONDS = 120.0
DEFAULT_MAX_MINUTELY_API_CALL_UNITS = 600.0
DEFAULT_MAX_HOURLY_API_CALL_UNITS = 5_000.0
DEFAULT_MAX_DAILY_API_CALL_UNITS = 10_000.0
LIVE_FORECAST_ENDPOINT = "https://api.open-meteo.com/v1/forecast"

PREDICTOR_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "pressure_msl",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
)
TARGET_VARIABLES = ("temperature_2m",)
PREDICTOR_UNITS = {
    "temperature_2m": "°C",
    "relative_humidity_2m": "%",
    "precipitation": "mm",
    "pressure_msl": "hPa",
    "wind_speed_10m": "km/h",
    "wind_gusts_10m": "km/h",
    "weather_code": "wmo code",
}
TARGET_UNITS = {"temperature_2m": "°C"}
PREDICTOR_COLUMNS = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "precipitation": "precipitation_mm",
    "pressure_msl": "pressure_hpa",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_gusts_10m": "wind_gust_kmh",
    "weather_code": "weather_code",
}
SOURCE_CONFIG = {
    "predictors": {
        "endpoint": "https://historical-forecast-api.open-meteo.com/v1/forecast",
        "model_id": "ecmwf_ifs",
        "variables": PREDICTOR_VARIABLES,
        "units": PREDICTOR_UNITS,
    },
    "targets": {
        "endpoint": "https://archive-api.open-meteo.com/v1/archive",
        "model_id": "era5",
        "variables": TARGET_VARIABLES,
        "units": TARGET_UNITS,
    },
}


class DailyQuotaReached(RuntimeError):
    """Raised before an HTTP request would exceed the public daily quota."""


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def atomic_write_json(path: str | Path, value: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, target)


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def chunks(items: Sequence[Any], size: int) -> list[list[Any]]:
    if size < 1:
        raise ValueError("batch size must be positive")
    return [list(items[index:index + size]) for index in range(0, len(items), size)]


def year_windows(start: date, end: date) -> list[tuple[date, date]]:
    if end < start:
        raise ValueError("end_date must not precede start_date")
    result = []
    for year in range(start.year, end.year + 1):
        result.append((max(start, date(year, 1, 1)), min(end, date(year, 12, 31))))
    return result


def load_nationwide_locations(catalog_path: str | Path = CATALOG_PATH) -> list[dict[str, Any]]:
    """Select the canonical catalog without maintaining a second location list."""
    locations = select_dataset_locations(load_catalog(catalog_path), DATASET_NATIONWIDE_63)
    ids = [str(location["location_id"]) for location in locations]
    coordinates = [(float(location["latitude"]), float(location["longitude"])) for location in locations]
    if len(locations) != 63 or len(set(ids)) != 63 or len(set(coordinates)) != 63:
        raise ValueError("NATIONWIDE_63 must contain 63 unique IDs and valid unique coordinates")
    for location in locations:
        latitude = float(location["latitude"])
        longitude = float(location["longitude"])
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            raise ValueError(f"invalid catalog coordinates for {location['location_id']}")
    return locations


def _iso_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    if parsed.minute or parsed.second or parsed.microsecond:
        raise ValueError(f"provider time is not aligned to a UTC hour: {value!r}")
    return parsed


def _distance_km(latitude_a: float, longitude_a: float, latitude_b: float, longitude_b: float) -> float:
    radius = 6371.0088
    phi_a = math.radians(latitude_a)
    phi_b = math.radians(latitude_b)
    delta_phi = math.radians(latitude_b - latitude_a)
    delta_lambda = math.radians(longitude_b - longitude_a)
    haversine = math.sin(delta_phi / 2) ** 2 + math.cos(phi_a) * math.cos(phi_b) * math.sin(delta_lambda / 2) ** 2
    return 2 * radius * math.asin(min(1.0, math.sqrt(haversine)))


def build_request_parameters(
    locations: Sequence[Mapping[str, Any]],
    source_name: str,
    start: date,
    end: date,
) -> dict[str, str]:
    try:
        config = SOURCE_CONFIG[source_name]
    except KeyError as exc:
        raise ValueError(f"unknown source role: {source_name}") from exc
    return {
        "latitude": ",".join(format(float(item["latitude"]), ".8f").rstrip("0").rstrip(".") for item in locations),
        "longitude": ",".join(format(float(item["longitude"]), ".8f").rstrip("0").rstrip(".") for item in locations),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(config["variables"]),
        "timezone": "GMT",
        "timeformat": "iso8601",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "models": str(config["model_id"]),
    }


def map_batch_responses(
    locations: Sequence[Mapping[str, Any]],
    payload: Any,
    *,
    source_name: str,
) -> list[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    """Validate count, shape, and positional coordinates before pairing a batch.

    Open-Meteo returns one response object per requested coordinate. The audit
    evidence separately cross-checks first/middle/last positions against
    singleton calls; this function also rejects positional responses outside a
    source-specific geographic envelope.
    """
    responses = [payload] if isinstance(payload, Mapping) else payload
    if not isinstance(responses, list) or len(responses) != len(locations):
        count = len(responses) if isinstance(responses, list) else "non-list"
        raise ValueError(f"batch response count {count} does not match {len(locations)} requested coordinates")
    max_distance_km = 45.0 if source_name == "predictors" else 60.0
    pairs = []
    for index, (location, response) in enumerate(zip(locations, responses, strict=True)):
        if not isinstance(response, Mapping):
            raise ValueError(f"response {index} is not an object")
        returned_latitude = response.get("latitude")
        returned_longitude = response.get("longitude")
        if not isinstance(returned_latitude, (float, int)) or not isinstance(returned_longitude, (float, int)):
            raise ValueError(f"response {index} lacks returned grid coordinates")
        distance = _distance_km(
            float(location["latitude"]),
            float(location["longitude"]),
            float(returned_latitude),
            float(returned_longitude),
        )
        if distance > max_distance_km:
            raise ValueError(
                f"response {index} grid cell is {distance:.1f} km from the positional catalog coordinate "
                f"for {location['location_id']}; response order cannot be trusted"
            )
        # A response can be within a broad grid-cell envelope for two nearby
        # catalog points. Reject an apparent permutation instead of silently
        # attaching one location's values to another location ID.
        distances = [
            _distance_km(
                float(candidate["latitude"]),
                float(candidate["longitude"]),
                float(returned_latitude),
                float(returned_longitude),
            )
            for candidate in locations
        ]
        if distance > min(distances) + 0.1:
            nearest_index = distances.index(min(distances))
            raise ValueError(
                f"response {index} grid coordinate is nearer to batch index {nearest_index}; "
                "response order cannot be trusted"
            )
        pairs.append((location, response))
    return pairs


def validate_hourly_response(
    response: Mapping[str, Any],
    *,
    source_name: str,
) -> tuple[list[datetime], dict[str, list[Any]], dict[str, str]]:
    config = SOURCE_CONFIG[source_name]
    hourly = response.get("hourly")
    units = response.get("hourly_units")
    if not isinstance(hourly, Mapping) or not isinstance(hourly.get("time"), list):
        raise ValueError("provider response is missing an hourly time array")
    if not isinstance(units, Mapping):
        raise ValueError("provider response is missing hourly_units")
    times = [_iso_utc(value) for value in hourly["time"]]
    if not times:
        raise ValueError("provider response contains no hourly timestamps")
    if times != sorted(times) or len(times) != len(set(times)):
        raise ValueError("provider timestamps must be sorted and unique")
    arrays: dict[str, list[Any]] = {}
    for variable in config["variables"]:
        values = hourly.get(variable)
        if not isinstance(values, list) or len(values) != len(times):
            raise ValueError(f"hourly variable {variable} is missing or not aligned with timestamps")
        expected_unit = config["units"][variable]
        if units.get(variable) != expected_unit:
            raise ValueError(f"unexpected unit for {variable}: {units.get(variable)!r}; expected {expected_unit!r}")
        arrays[variable] = values
    if response.get("timezone") not in {"GMT", "UTC"} or response.get("utc_offset_seconds", 0) != 0:
        raise ValueError("provider response must be timezone GMT with zero UTC offset")
    return times, arrays, {name: str(units[name]) for name in config["variables"]}


def _valid_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def normalize_response(
    pairs: Sequence[tuple[Mapping[str, Any], Mapping[str, Any]]],
    *,
    source_name: str,
    chunk_id: str,
    retrieved_at_utc: str,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    config = SOURCE_CONFIG[source_name]
    by_location: dict[str, list[dict[str, Any]]] = {}
    response_summaries = []
    null_counts = {name: 0 for name in config["variables"]}
    nonfinite_counts = {name: 0 for name in config["variables"]}
    all_times: list[datetime] = []

    for location, response in pairs:
        times, arrays, units = validate_hourly_response(response, source_name=source_name)
        location_id = str(location["location_id"])
        records = []
        for row_index, valid_time in enumerate(times):
            record: dict[str, Any] = {
                "provider": "open-meteo",
                "source_role": source_name,
                "endpoint": config["endpoint"],
                "model_id": config["model_id"],
                "source_contract_id": SOURCE_CONTRACT_ID,
                "chunk_id": chunk_id,
                "retrieved_at_utc": retrieved_at_utc,
                "location_id": location_id,
                "city": str(location.get("name") or location.get("province_name") or location_id),
                "latitude": float(location["latitude"]),
                "longitude": float(location["longitude"]),
                "provider_grid_latitude": float(response["latitude"]),
                "provider_grid_longitude": float(response["longitude"]),
                "valid_time": valid_time,
            }
            for variable in config["variables"]:
                value = arrays[variable][row_index]
                if value is None:
                    null_counts[variable] += 1
                elif not _valid_numeric(value):
                    nonfinite_counts[variable] += 1
                if source_name == "predictors":
                    column = PREDICTOR_COLUMNS[variable]
                else:
                    column = "temperature_target_c"
                record[column] = value
            records.append(record)
        by_location[location_id] = records
        all_times.extend(times)
        response_summaries.append({
            "location_id": location_id,
            "row_count": len(records),
            "first_valid_time": times[0].isoformat() if times else None,
            "last_valid_time": times[-1].isoformat() if times else None,
            "provider_grid_latitude": float(response["latitude"]),
            "provider_grid_longitude": float(response["longitude"]),
            "hourly_units": units,
        })

    return by_location, {
        "response_locations": len(pairs),
        "rows": sum(len(rows) for rows in by_location.values()),
        "null_counts": null_counts,
        "nonfinite_counts": nonfinite_counts,
        "response_summaries": response_summaries,
        "distinct_hour_counts": {
            str(location["location_id"]): len(by_location[str(location["location_id"])])
            for location, _ in pairs
        },
    }


def _retry_delay(response: httpx.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(max(float(retry_after), 0.0), 120.0)
            except ValueError:
                pass
    if response is not None and response.status_code == 429:
        # The free API's current 429 body explicitly says to retry in one
        # minute. Its response has not always included Retry-After.
        return 60.0
    base = min(2 ** (attempt - 1), 30)
    return float(base) + random.uniform(0.0, min(base / 4, 1.0))


def estimated_api_call_units(
    source_name: str,
    start: date,
    end: date,
    location_count: int,
) -> float:
    """Estimate free-tier units using the provider's 14-day/10-variable basis.

    The estimate is conservative about coordinate count and is stored in the
    download ledger. It is a pacing/quota guard, not a provider invoice.
    """
    if location_count < 1 or end < start:
        raise ValueError("call-unit estimate requires locations and an ordered date range")
    variable_count = len(SOURCE_CONFIG[source_name]["variables"])
    inclusive_days = (end - start).days + 1
    return location_count * (inclusive_days / 14.0) * (variable_count / 10.0)


def quota_wait_seconds(
    ledger: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    requested_units: float,
    limit: float,
    window_seconds: float,
) -> float:
    """Return the wait needed to fit a request in a rolling call-unit window.

    Every recorded request attempt counts toward the rolling provider limits,
    including rejected 429 responses. The daily safety guard uses the same
    conservative attempt accounting; ``charged`` is a separate local estimate.
    """
    if requested_units < 0 or limit <= 0 or window_seconds <= 0:
        raise ValueError("quota units, limit, and window must be positive")
    if requested_units > limit:
        raise ValueError(
            f"single request estimate {requested_units:.2f} exceeds rolling limit {limit:.2f}"
        )
    now_utc = now.replace(tzinfo=timezone.utc) if now.tzinfo is None else now.astimezone(timezone.utc)
    cutoff = now_utc - timedelta(seconds=window_seconds)
    active: list[tuple[datetime, float]] = []
    for record in ledger:
        try:
            at = datetime.fromisoformat(str(record["at_utc"]).replace("Z", "+00:00"))
            at = at.replace(tzinfo=timezone.utc) if at.tzinfo is None else at.astimezone(timezone.utc)
            units = float(record["estimated_api_call_units"])
        except (KeyError, TypeError, ValueError):
            continue
        if cutoff < at <= now_utc and units > 0:
            active.append((at, units))

    used = sum(units for _, units in active)
    if used + requested_units <= limit:
        return 0.0

    # Drop the oldest attempts until the remaining active window plus this
    # request fits. The extra second avoids retrying at the exact boundary.
    wait_seconds = 0.0
    for at, units in sorted(active):
        used -= units
        wait_seconds = max(
            wait_seconds,
            (at + timedelta(seconds=window_seconds) - now_utc).total_seconds() + 1.0,
        )
        if used + requested_units <= limit:
            return max(0.0, wait_seconds)
    return max(0.0, wait_seconds)


def fetch_json(
    client: httpx.Client,
    endpoint: str,
    parameters: Mapping[str, str],
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    sleep=time_module.sleep,
    on_attempt: Callable[[int], None] | None = None,
    on_result: Callable[[int, int | None], None] | None = None,
) -> Any:
    if max_retries < 1:
        raise ValueError("max_retries must be positive")
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        if on_attempt is not None:
            on_attempt(attempt)
        response: httpx.Response | None = None
        result_recorded = False
        try:
            response = client.get(endpoint, params=parameters, timeout=REQUEST_TIMEOUT_SECONDS)
            if on_result is not None:
                on_result(attempt, response.status_code)
                result_recorded = True
            if response.status_code == 429 or response.status_code >= 500:
                last_error = httpx.HTTPStatusError(
                    f"retryable Open-Meteo HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
                if attempt < max_retries:
                    sleep(_retry_delay(response, attempt))
                    continue
                break
            response.raise_for_status()
            return response.json()
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            if on_result is not None and not result_recorded:
                on_result(attempt, response.status_code if response is not None else None)
            last_error = exc
            retryable = not isinstance(exc, httpx.HTTPStatusError) or (
                exc.response.status_code == 429 or exc.response.status_code >= 500
            )
            if not retryable or attempt >= max_retries:
                break
            sleep(_retry_delay(response, attempt))
        except (ValueError, json.JSONDecodeError) as exc:
            last_error = exc
            break
    raise RuntimeError(f"Open-Meteo request failed after {max_retries} bounded attempts: {last_error}") from last_error


def probe_live_forecast_alignment(
    *,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    catalog_path: str | Path = CATALOG_PATH,
    client: httpx.Client | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    sleep=time_module.sleep,
) -> dict[str, Any]:
    """Persist a current Best Match versus explicitly pinned IFS sample.

    This is a compatibility snapshot only; Forecast API responses do not expose
    the model selected by Best Match, so it cannot establish future stability.
    """
    locations = load_nationwide_locations(catalog_path)
    base_parameters = {
        "latitude": ",".join(format(float(item["latitude"]), ".8f").rstrip("0").rstrip(".") for item in locations),
        "longitude": ",".join(format(float(item["longitude"]), ".8f").rstrip("0").rstrip(".") for item in locations),
        "hourly": ",".join(PREDICTOR_VARIABLES),
        "timezone": "GMT",
        "timeformat": "iso8601",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "forecast_days": "1",
    }
    explicit_parameters = {**base_parameters, "models": str(SOURCE_CONFIG["predictors"]["model_id"])}
    own_client = client is None
    session = client or httpx.Client(headers={"User-Agent": "weather-streaming-bigdata/t2h-v1.1"})
    try:
        generic_payload = fetch_json(
            session,
            LIVE_FORECAST_ENDPOINT,
            base_parameters,
            max_retries=max_retries,
            sleep=sleep,
        )
        explicit_payload = fetch_json(
            session,
            LIVE_FORECAST_ENDPOINT,
            explicit_parameters,
            max_retries=max_retries,
            sleep=sleep,
        )
    finally:
        if own_client:
            session.close()

    generic_pairs = map_batch_responses(locations, generic_payload, source_name="predictors")
    explicit_pairs = map_batch_responses(locations, explicit_payload, source_name="predictors")
    value_mismatches = {name: 0 for name in PREDICTOR_VARIABLES}
    null_counts = {
        "best_match": {name: 0 for name in PREDICTOR_VARIABLES},
        "ecmwf_ifs": {name: 0 for name in PREDICTOR_VARIABLES},
    }
    timestamp_mismatches = 0
    unit_mismatches = 0
    comparable_hour_count = 0
    coordinate_mismatches = 0
    hours_per_location: list[int] = []
    for (location_a, response_a), (location_b, response_b) in zip(generic_pairs, explicit_pairs, strict=True):
        if str(location_a["location_id"]) != str(location_b["location_id"]):
            raise AssertionError("live forecast source pairs lost canonical location order")
        if (
            float(response_a["latitude"]) != float(response_b["latitude"])
            or float(response_a["longitude"]) != float(response_b["longitude"])
        ):
            coordinate_mismatches += 1
        times_a, arrays_a, units_a = validate_hourly_response(response_a, source_name="predictors")
        times_b, arrays_b, units_b = validate_hourly_response(response_b, source_name="predictors")
        if times_a != times_b:
            timestamp_mismatches += max(len(times_a), len(times_b))
            continue
        comparable_hour_count += len(times_a)
        hours_per_location.append(len(times_a))
        if units_a != units_b:
            unit_mismatches += sum(units_a.get(name) != units_b.get(name) for name in PREDICTOR_VARIABLES)
        for variable in PREDICTOR_VARIABLES:
            left = arrays_a[variable]
            right = arrays_b[variable]
            if len(left) != len(right):
                value_mismatches[variable] += max(len(left), len(right))
            else:
                value_mismatches[variable] += sum(a != b for a, b in zip(left, right, strict=True))
            null_counts["best_match"][variable] += sum(value is None for value in left)
            null_counts["ecmwf_ifs"][variable] += sum(value is None for value in right)

    report = {
        "provider": "Open-Meteo",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "endpoint": LIVE_FORECAST_ENDPOINT,
        "dataset_id": DATASET_NATIONWIDE_63,
        "locations": len(locations),
        "requested_horizon": "forecast_days=1",
        "hours_per_location": sorted(set(hours_per_location)),
        "comparable_timestamp_pairs": comparable_hour_count,
        "variables": list(PREDICTOR_VARIABLES),
        "units": PREDICTOR_UNITS,
        "best_match_parameters": base_parameters,
        "explicit_parameters": explicit_parameters,
        "model_identity_exposed_by_response": False,
        "estimated_api_call_units": 2 * estimated_api_call_units(
            "predictors", datetime.now(timezone.utc).date(), datetime.now(timezone.utc).date(), len(locations)
        ),
        "comparison": {
            "selection": "automatic Best Match versus explicit ecmwf_ifs",
            "coordinate_mismatches": coordinate_mismatches,
            "timestamp_mismatches": timestamp_mismatches,
            "unit_mismatches": unit_mismatches,
            "value_mismatches_by_variable": value_mismatches,
            "null_counts": null_counts,
            "exact_match": coordinate_mismatches == 0 and timestamp_mismatches == 0 and unit_mismatches == 0 and not any(value_mismatches.values()),
            "interpretation": "Current sample only; Best Match model identity is not returned and can change in future responses.",
        },
        "raw_responses": {
            "best_match": generic_payload,
            "ecmwf_ifs": explicit_payload,
        },
    }
    atomic_write_json(Path(artifact_root) / "live_forecast_probe.json", report)
    return report


def _normalized_paths(root: Path, source_name: str, year: int, locations: Sequence[Mapping[str, Any]], chunk_id: str) -> dict[str, Path]:
    model_id = SOURCE_CONFIG[source_name]["model_id"]
    return {
        str(location["location_id"]): (
            root / "normalized" / source_name / f"model_id={model_id}" / f"year={year}"
            / f"location_id={location['location_id']}" / f"{chunk_id}.parquet"
        )
        for location in locations
    }


def _chunk_id(source_name: str, start: date, end: date, batch_index: int, locations: Sequence[Mapping[str, Any]]) -> str:
    ids = ",".join(str(location["location_id"]) for location in locations)
    suffix = hashlib.sha256(ids.encode("utf-8")).hexdigest()[:10]
    return f"{source_name}-{start:%Y%m%d}-{end:%Y%m%d}-b{batch_index:02d}-{suffix}"


def _completed_chunk_is_valid(entry: Mapping[str, Any], root: Path) -> bool:
    if entry.get("status") != "SUCCESS":
        return False
    files = entry.get("files")
    if not isinstance(files, Mapping):
        return False
    for key in ("raw", "normalized"):
        items = files.get(key)
        items = [items] if isinstance(items, Mapping) else items
        if not isinstance(items, list) or not items:
            return False
        for item in items:
            path = root / str(item.get("path", ""))
            if not path.is_file() or sha256_file(path) != item.get("sha256"):
                return False
    raw = files.get("raw")
    if isinstance(raw, Mapping):
        metadata_path = root / str(raw.get("metadata_path", ""))
        if not metadata_path.is_file() or sha256_file(metadata_path) != raw.get("metadata_sha256"):
            return False
    return True


def download_range(
    *,
    data_root: str | Path = DEFAULT_DATA_ROOT,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    catalog_path: str | Path = CATALOG_PATH,
    start_date: date = DEFAULT_START_DATE,
    end_date: date = DEFAULT_END_DATE,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_retries: int = DEFAULT_MAX_RETRIES,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    max_daily_api_call_units: float = DEFAULT_MAX_DAILY_API_CALL_UNITS,
    client: httpx.Client | None = None,
    sleep=time_module.sleep,
) -> dict[str, Any]:
    """Download both contracted sources with chunk-level resume and checksums."""
    root = Path(data_root).resolve()
    artifact_dir = Path(artifact_root)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    locations = load_nationwide_locations(catalog_path)
    batches = chunks(locations, batch_size)
    manifest_path = artifact_dir / "download_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest = {
            "source_contract_id": SOURCE_CONTRACT_ID,
            "provider": "Open-Meteo",
            "date_start_inclusive": start_date.isoformat(),
            "date_end_inclusive": end_date.isoformat(),
            "dataset_id": DATASET_NATIONWIDE_63,
            "location_count": len(locations),
            "location_ids": [str(item["location_id"]) for item in locations],
            "batch_size": batch_size,
            "source_models": {name: value["model_id"] for name, value in SOURCE_CONFIG.items()},
            "rolling_api_call_unit_limits": {
                "minutely": DEFAULT_MAX_MINUTELY_API_CALL_UNITS,
                "hourly": DEFAULT_MAX_HOURLY_API_CALL_UNITS,
            },
            "chunks": [],
        }
    if manifest.get("source_contract_id") != SOURCE_CONTRACT_ID:
        raise ValueError("existing download manifest belongs to another source contract")
    if manifest.get("date_start_inclusive") != start_date.isoformat() or manifest.get("date_end_inclusive") != end_date.isoformat():
        raise ValueError("resume manifest range differs; use a distinct data/artifact root for another range")
    expected_location_ids = [str(item["location_id"]) for item in locations]
    expected_models = {name: value["model_id"] for name, value in SOURCE_CONFIG.items()}
    if manifest.get("location_ids", expected_location_ids) != expected_location_ids:
        raise ValueError("resume manifest location catalog differs; use a distinct data/artifact root")
    if int(manifest.get("batch_size", batch_size)) != batch_size:
        raise ValueError("resume manifest batch size differs; use a distinct data/artifact root")
    if manifest.get("source_models", expected_models) != expected_models:
        raise ValueError("resume manifest model selection differs; use a distinct data/artifact root")
    if max_daily_api_call_units <= 0:
        raise ValueError("max_daily_api_call_units must be positive")
    manifest.setdefault("rolling_api_call_unit_limits", {
        "minutely": DEFAULT_MAX_MINUTELY_API_CALL_UNITS,
        "hourly": DEFAULT_MAX_HOURLY_API_CALL_UNITS,
    })

    ledger = manifest.setdefault("daily_quota_ledger", [])
    if not ledger and manifest.get("chunks"):
        # Backfill this run's earlier attempts when upgrading a partial manifest
        # created before quota accounting was added. Reconstruct all attempts,
        # including rejected 429s, using the chunk's retrieval timestamp.
        for old_entry in manifest["chunks"]:
            retrieved = old_entry.get("retrieved_at_utc")
            if not retrieved:
                continue
            try:
                estimated = estimated_api_call_units(
                    str(old_entry["source_role"]),
                    date.fromisoformat(str(old_entry["date_start_inclusive"])),
                    date.fromisoformat(str(old_entry["date_end_inclusive"])),
                    len(old_entry["location_ids"]),
                )
                status = old_entry.get("status")
                reason = str(old_entry.get("failure_reason") or "")
                attempts = max(0, int(old_entry.get("attempts", 0)))
                if status in {"SUCCESS", "RUNNING"}:
                    attempts = max(1, attempts)
                rejected_429 = "HTTP 429" in reason
            except (KeyError, TypeError, ValueError):
                continue
            for attempt_index in range(attempts):
                ledger.append({
                    "at_utc": retrieved,
                    "chunk_id": old_entry.get("chunk_id"),
                    "attempt": attempt_index + 1,
                    "estimated_api_call_units": estimated,
                    "charged": not rejected_429,
                    "response_status": 429 if rejected_429 else "backfilled_attempt",
                    "ledger_source": "backfilled_chunk_attempt",
                })
        manifest["quota_ledger_backfilled"] = True

    def daily_units_used(day: date, *, charged_only: bool = False) -> float:
        total = 0.0
        for record in ledger:
            try:
                record_day = datetime.fromisoformat(str(record["at_utc"]).replace("Z", "+00:00")).date()
                if record_day == day and (not charged_only or record.get("charged", True)):
                    total += float(record["estimated_api_call_units"])
            except (KeyError, TypeError, ValueError):
                continue
        return total

    known = {str(entry.get("chunk_id")): entry for entry in manifest.get("chunks", [])}
    total_elapsed_started = time_module.perf_counter()
    own_client = client is None
    session = client or httpx.Client(headers={"User-Agent": "weather-streaming-bigdata/t2h-v1.1"})
    try:
        for source_name, config in SOURCE_CONFIG.items():
            for start, end in year_windows(start_date, end_date):
                for batch_index, location_batch in enumerate(batches):
                    chunk_id = _chunk_id(source_name, start, end, batch_index, location_batch)
                    existing = known.get(chunk_id)
                    if existing and _completed_chunk_is_valid(existing, root):
                        continue

                    parameters = build_request_parameters(location_batch, source_name, start, end)
                    raw_path = (
                        root / "raw" / source_name / f"model_id={config['model_id']}" / f"year={start.year}"
                        / f"{chunk_id}.json.gz"
                    )
                    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
                    entry: dict[str, Any] = {
                        "chunk_id": chunk_id,
                        "source_role": source_name,
                        "endpoint": config["endpoint"],
                        "model_id": config["model_id"],
                        "date_start_inclusive": start.isoformat(),
                        "date_end_inclusive": end.isoformat(),
                        "location_ids": [str(item["location_id"]) for item in location_batch],
                        "request_parameters": parameters,
                        "retrieved_at_utc": retrieved_at,
                        "status": "RUNNING",
                        "attempts": 0,
                        "estimated_api_call_units": estimated_api_call_units(
                            source_name, start, end, len(location_batch)
                        ),
                    }
                    known[chunk_id] = entry
                    manifest["chunks"] = list(known.values())
                    atomic_write_json(manifest_path, manifest)
                    try:
                        def record_attempt(attempt: int) -> None:
                            attempt_units = float(entry["estimated_api_call_units"])
                            quota_wait_total = 0.0
                            while True:
                                now_utc = datetime.now(timezone.utc)
                                minute_wait = quota_wait_seconds(
                                    ledger,
                                    now=now_utc,
                                    requested_units=attempt_units,
                                    limit=DEFAULT_MAX_MINUTELY_API_CALL_UNITS,
                                    window_seconds=60.0,
                                )
                                hour_wait = quota_wait_seconds(
                                    ledger,
                                    now=now_utc,
                                    requested_units=attempt_units,
                                    limit=DEFAULT_MAX_HOURLY_API_CALL_UNITS,
                                    window_seconds=3600.0,
                                )
                                quota_wait = max(minute_wait, hour_wait)
                                if quota_wait <= 0:
                                    break
                                quota_wait_total += quota_wait
                                entry.update({
                                    "status": "WAITING_FOR_QUOTA",
                                    "quota_wait_seconds_total": round(quota_wait_total, 3),
                                    "quota_wait_until_utc": (
                                        now_utc + timedelta(seconds=quota_wait)
                                    ).isoformat(timespec="seconds").replace("+00:00", "Z"),
                                })
                                manifest["chunks"] = list(known.values())
                                atomic_write_json(manifest_path, manifest)
                                sleep(quota_wait)

                            attempt_time = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
                            attempt_day = datetime.now(timezone.utc).date()
                            used = daily_units_used(attempt_day)
                            if used + attempt_units > max_daily_api_call_units:
                                raise DailyQuotaReached(
                                    f"daily API call-unit budget would exceed {max_daily_api_call_units:.0f} "
                                    f"(already estimated {used:.2f}, next chunk {attempt_units:.2f})"
                                )
                            ledger.append({
                                "at_utc": attempt_time,
                                "chunk_id": chunk_id,
                                "attempt": attempt,
                                "estimated_api_call_units": attempt_units,
                                "charged": True,
                                "response_status": "pending",
                                "ledger_source": "request_attempt",
                            })
                            entry["attempts"] = attempt
                            entry["last_attempt_at_utc"] = attempt_time
                            entry["quota_wait_seconds_total"] = round(quota_wait_total, 3)
                            entry["status"] = "RUNNING"
                            manifest["chunks"] = list(known.values())
                            atomic_write_json(manifest_path, manifest)

                        def record_result(attempt: int, status_code: int | None) -> None:
                            for record in reversed(ledger):
                                if record.get("chunk_id") == chunk_id and record.get("attempt") == attempt:
                                    record["response_status"] = status_code if status_code is not None else "transport_error"
                                    # A throttled request is rejected before provider data processing.
                                    # Count all other responses and transport-ambiguous failures.
                                    record["charged"] = status_code != 429
                                    break
                            manifest["chunks"] = list(known.values())
                            atomic_write_json(manifest_path, manifest)

                        payload = fetch_json(
                            session,
                            config["endpoint"],
                            parameters,
                            max_retries=max_retries,
                            sleep=sleep,
                            on_attempt=record_attempt,
                            on_result=record_result,
                        )
                        pairs = map_batch_responses(location_batch, payload, source_name=source_name)
                        normalized, summary = normalize_response(
                            pairs,
                            source_name=source_name,
                            chunk_id=chunk_id,
                            retrieved_at_utc=retrieved_at,
                        )

                        raw_path.parent.mkdir(parents=True, exist_ok=True)
                        temporary_raw = raw_path.with_suffix(raw_path.suffix + ".tmp")
                        with gzip.open(temporary_raw, "wt", encoding="utf-8", newline="") as output:
                            json.dump(payload, output, ensure_ascii=False, separators=(",", ":"))
                        os.replace(temporary_raw, raw_path)
                        raw_sha = sha256_file(raw_path)
                        metadata_path = raw_path.with_suffix(".meta.json")
                        atomic_write_json(metadata_path, {
                            "chunk_id": chunk_id,
                            "provider": "Open-Meteo",
                            "endpoint": config["endpoint"],
                            "model_id": config["model_id"],
                            "source_contract_id": SOURCE_CONTRACT_ID,
                            "request_parameters": parameters,
                            "request_location_ids_in_order": [str(item["location_id"]) for item in location_batch],
                            "retrieved_at_utc": retrieved_at,
                            "response_count": summary["response_locations"],
                            "raw_response_sha256": raw_sha,
                        })

                        import pyarrow as pa
                        import pyarrow.parquet as pq

                        normalized_files = []
                        for location_id, records in normalized.items():
                            destination = _normalized_paths(root, source_name, start.year, location_batch, chunk_id)[location_id]
                            destination.parent.mkdir(parents=True, exist_ok=True)
                            temp_path = destination.with_suffix(".parquet.tmp")
                            table = pa.Table.from_pylist(records)
                            pq.write_table(table, temp_path, compression="snappy", use_dictionary=True)
                            os.replace(temp_path, destination)
                            normalized_files.append({
                                "location_id": location_id,
                                "path": destination.relative_to(root).as_posix(),
                                "row_count": len(records),
                                "sha256": sha256_file(destination),
                            })

                        entry.update({
                            "status": "SUCCESS",
                            "response_count": summary["response_locations"],
                            "row_count": summary["rows"],
                            "null_counts": summary["null_counts"],
                            "nonfinite_counts": summary["nonfinite_counts"],
                            "response_summaries": summary["response_summaries"],
                            "files": {
                                "raw": {
                                    "path": raw_path.relative_to(root).as_posix(),
                                    "sha256": raw_sha,
                                    "metadata_path": metadata_path.relative_to(root).as_posix(),
                                    "metadata_sha256": sha256_file(metadata_path),
                                },
                                "normalized": normalized_files,
                            },
                            "failure_reason": None,
                        })
                    except DailyQuotaReached as exc:
                        entry.update({
                            "status": "DEFERRED_DAILY_LIMIT",
                            "failure_reason": None,
                            "deferred_reason": str(exc),
                        })
                    except Exception as exc:
                        entry.update({
                            "status": "FAILED",
                            "failure_reason": f"{type(exc).__name__}: {str(exc)[:1000]}",
                            "failed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        })
                    manifest["chunks"] = list(known.values())
                    atomic_write_json(manifest_path, manifest)
                    if request_delay_seconds > 0:
                        sleep(request_delay_seconds)
    finally:
        if own_client:
            session.close()

    failed = [entry for entry in known.values() if entry.get("status") != "SUCCESS"]
    failed = [entry for entry in failed if entry.get("status") != "DEFERRED_DAILY_LIMIT"]
    deferred = [entry for entry in known.values() if entry.get("status") == "DEFERRED_DAILY_LIMIT"]
    manifest["completed_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    manifest["elapsed_seconds"] = time_module.perf_counter() - total_elapsed_started
    manifest["completed_chunk_count"] = sum(entry.get("status") == "SUCCESS" for entry in known.values())
    manifest["failed_chunk_count"] = len(failed)
    manifest["deferred_chunk_count"] = len(deferred)
    manifest["estimated_api_call_units_for_contract"] = sum(
        estimated_api_call_units(source_name, window_start, window_end, len(batch))
        for source_name in SOURCE_CONFIG
        for window_start, window_end in year_windows(start_date, end_date)
        for batch in batches
    )
    today = datetime.now(timezone.utc).date()
    manifest["estimated_api_call_units_used_today"] = daily_units_used(today)
    manifest["estimated_api_call_units_charged_today"] = daily_units_used(today, charged_only=True)
    manifest["daily_api_call_unit_limit"] = max_daily_api_call_units
    manifest["daily_attempt_estimate_exceeds_limit"] = (
        manifest["estimated_api_call_units_used_today"] > max_daily_api_call_units
    )
    manifest["status"] = "DEFERRED_DAILY_LIMIT" if deferred else "COMPLETE" if not failed else "FAILED"
    atomic_write_json(manifest_path, manifest)
    if failed:
        raise RuntimeError(f"{len(failed)} source chunks failed; details are persisted in {manifest_path}")
    return manifest


def validate_normalized_sources(
    *,
    data_root: str | Path = DEFAULT_DATA_ROOT,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    catalog_path: str | Path = CATALOG_PATH,
    start_date: date = DEFAULT_START_DATE,
    end_date: date = DEFAULT_END_DATE,
) -> dict[str, Any]:
    """Report timestamp gaps, duplicates, nulls, non-finite values, and row counts."""
    import pyarrow.parquet as pq

    root = Path(data_root).resolve()
    artifact_dir = Path(artifact_root)
    locations = load_nationwide_locations(catalog_path)
    sources: dict[str, Any] = {}
    for source_name, config in SOURCE_CONFIG.items():
        variable_columns = (
            [PREDICTOR_COLUMNS[name] for name in config["variables"]]
            if source_name == "predictors"
            else ["temperature_target_c"]
        )
        expected_by_year = {
            year: int((datetime.combine(min(end_date, date(year, 12, 31)), time.min, tzinfo=timezone.utc)
                       - datetime.combine(max(start_date, date(year, 1, 1)), time.min, tzinfo=timezone.utc)).total_seconds() / 3600) + 24
            for year in range(start_date.year, end_date.year + 1)
        }
        per_location = []
        total_rows = 0
        total_duplicates = 0
        total_gaps = 0
        total_nulls = {name: 0 for name in variable_columns}
        total_nonfinite = {name: 0 for name in variable_columns}
        for location in locations:
            location_id = str(location["location_id"])
            all_times: list[datetime] = []
            nulls = {name: 0 for name in variable_columns}
            nonfinite = {name: 0 for name in variable_columns}
            for year in range(start_date.year, end_date.year + 1):
                location_dir = (
                    root / "normalized" / source_name / f"model_id={config['model_id']}"
                    / f"year={year}" / f"location_id={location_id}"
                )
                files = sorted(location_dir.glob("*.parquet"))
                for path in files:
                    table = pq.read_table(path)
                    all_times.extend(table["valid_time"].to_pylist())
                    for column in variable_columns:
                        values = table[column].to_pylist()
                        nulls[column] += sum(value is None for value in values)
                        nonfinite[column] += sum(
                            value is not None and not _valid_numeric(value) for value in values
                        )
            all_times.sort()
            duplicate_count = len(all_times) - len(set(all_times))
            expected_times = []
            for year, hours in expected_by_year.items():
                year_start = datetime.combine(max(start_date, date(year, 1, 1)), time.min, tzinfo=timezone.utc)
                expected_times.extend(year_start + timedelta(hours=offset) for offset in range(hours))
            observed_set = set(all_times)
            expected_set = set(expected_times)
            missing = sorted(expected_set - observed_set)
            unexpected = sorted(observed_set - expected_set)
            gap_intervals = []
            if missing:
                interval_start = previous = missing[0]
                for instant in missing[1:]:
                    if instant != previous + timedelta(hours=1):
                        gap_intervals.append({"start": interval_start.isoformat(), "end": previous.isoformat()})
                        interval_start = instant
                    previous = instant
                gap_intervals.append({"start": interval_start.isoformat(), "end": previous.isoformat()})
            item = {
                "location_id": location_id,
                "rows": len(all_times),
                "expected_rows": len(expected_times),
                "duplicate_rows": duplicate_count,
                "missing_hour_count": len(missing),
                "unexpected_hour_count": len(unexpected),
                "gap_intervals": gap_intervals,
                "null_counts": nulls,
                "nonfinite_counts": nonfinite,
            }
            per_location.append(item)
            total_rows += len(all_times)
            total_duplicates += duplicate_count
            total_gaps += len(missing) + len(unexpected)
            for column in variable_columns:
                total_nulls[column] += nulls[column]
                total_nonfinite[column] += nonfinite[column]

        sources[source_name] = {
            "endpoint": config["endpoint"],
            "model_id": config["model_id"],
            "variables": list(config["variables"]),
            "normalized_rows": total_rows,
            "expected_rows": sum(item["expected_rows"] for item in per_location),
            "location_count": len(per_location),
            "duplicate_rows": total_duplicates,
            "gap_or_unexpected_hour_count": total_gaps,
            "null_counts": total_nulls,
            "nonfinite_counts": total_nonfinite,
            "per_location": per_location,
        }

    errors = []
    warnings = []
    for source_name, source_report in sources.items():
        if source_report["location_count"] != 63:
            errors.append(f"{source_name}: expected 63 locations")
        if source_report["duplicate_rows"]:
            errors.append(f"{source_name}: duplicate location-hour keys")
        missing_hours = sum(item["missing_hour_count"] for item in source_report["per_location"])
        unexpected_hours = sum(item["unexpected_hour_count"] for item in source_report["per_location"])
        if missing_hours:
            warnings.append(
                f"{source_name}: {missing_hours} expected hours are missing; affected feature/target rows will be excluded without imputation"
            )
        if unexpected_hours:
            errors.append(f"{source_name}: found {unexpected_hours} timestamps outside the requested interval")
        if any(source_report["null_counts"].values()):
            warnings.append(
                f"{source_name}: null source values will make affected feature/target rows ineligible; no imputation will be applied"
            )
        if any(source_report["nonfinite_counts"].values()):
            errors.append(f"{source_name}: non-numeric or non-finite source values were found")
    has_gaps = bool(warnings)
    report = {
        "source_contract_id": SOURCE_CONTRACT_ID,
        "date_start_inclusive": start_date.isoformat(),
        "date_end_inclusive": end_date.isoformat(),
        "dataset_id": DATASET_NATIONWIDE_63,
        "status": "FAIL" if errors else "PASS_WITH_GAPS" if has_gaps else "PASS",
        "errors": errors,
        "warnings": warnings,
        "gap_policy": "No forward fill, backward fill, interpolation, or copied-hour fabrication; feature rows requiring unavailable history are excluded.",
        "sources": sources,
    }
    atomic_write_json(Path(artifact_root) / "raw_validation.json", report)
    return report


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--catalog", type=Path, default=CATALOG_PATH)
    parser.add_argument("--start-date", type=date.fromisoformat, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=date.fromisoformat, default=DEFAULT_END_DATE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--request-delay-seconds", type=float, default=DEFAULT_REQUEST_DELAY_SECONDS)
    parser.add_argument("--max-daily-api-call-units", type=float, default=DEFAULT_MAX_DAILY_API_CALL_UNITS)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--probe-live-forecast", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.probe_live_forecast:
        report = probe_live_forecast_alignment(
            artifact_root=args.artifact_root,
            catalog_path=args.catalog,
            max_retries=args.max_retries,
        )
        print(json.dumps({
            "status": "PASS" if report["comparison"]["exact_match"] else "FAIL",
            "locations": report["locations"],
            "timestamp_mismatches": report["comparison"]["timestamp_mismatches"],
            "value_mismatches_by_variable": report["comparison"]["value_mismatches_by_variable"],
            "artifact": str(args.artifact_root / "live_forecast_probe.json"),
        }, indent=2))
        return 0 if report["comparison"]["exact_match"] else 2
    if args.validate_only:
        report = validate_normalized_sources(
            data_root=args.data_root,
            artifact_root=args.artifact_root,
            catalog_path=args.catalog,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    else:
        manifest = download_range(
            data_root=args.data_root,
            artifact_root=args.artifact_root,
            catalog_path=args.catalog,
            start_date=args.start_date,
            end_date=args.end_date,
            batch_size=args.batch_size,
            max_retries=args.max_retries,
            request_delay_seconds=args.request_delay_seconds,
            max_daily_api_call_units=args.max_daily_api_call_units,
        )
        if manifest.get("deferred_chunk_count", 0):
            print(json.dumps({
                "status": manifest["status"],
                "completed_chunk_count": manifest["completed_chunk_count"],
                "deferred_chunk_count": manifest["deferred_chunk_count"],
                "estimated_api_call_units_used_today": manifest["estimated_api_call_units_used_today"],
                "daily_api_call_unit_limit": manifest["daily_api_call_unit_limit"],
                "resume_command": "Re-run this command after the provider's daily quota resets.",
            }, indent=2))
            return 0
        report = validate_normalized_sources(
            data_root=args.data_root,
            artifact_root=args.artifact_root,
            catalog_path=args.catalog,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        print(json.dumps({"download_manifest": str(args.artifact_root / "download_manifest.json"), "completed_chunks": manifest["completed_chunk_count"], "validation_status": report["status"]}, indent=2))
    return 0 if report["status"] in {"PASS", "PASS_WITH_GAPS"} else 2


if __name__ == "__main__":  # pragma: no cover - exercised by CLI/notebook
    raise SystemExit(main())
