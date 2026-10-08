from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import json
import math
import os
from pathlib import Path
import random
import re
import sys
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
import zlib

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from historical.location_catalog import DATASET_NATIONWIDE_63, select_dataset_locations  # noqa: E402
from ml.streaming_inference.contract import load_feature_contract  # noqa: E402
from producer.weather_producer import (  # noqa: E402
    OPEN_METEO_URL,
    chunks,
    load_cities,
)


DEFAULT_TOPIC = "weather.hourly.observations.v1"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_POLL_INTERVAL_SECONDS = 300.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_BATCH_SIZE = 63
DEFAULT_KAFKA_STARTUP_TIMEOUT_SECONDS = 60.0
LIVE_SOURCE = "OPEN_METEO_LIVE_HOURLY"
EVENT_TYPE = "WEATHER_HOURLY"
HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "pressure_msl",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
)
EXPECTED_UNITS = {
    "temperature_2m": "°C",
    "relative_humidity_2m": "%",
    "precipitation": "mm",
    "pressure_msl": "hPa",
    "wind_speed_10m": "km/h",
    "wind_gusts_10m": "km/h",
    "weather_code": "wmo code",
}
EVENT_FIELDS = (
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
    "weather_code",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "source",
)
VALUE_FIELD_MAP = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "precipitation": "precipitation_mm",
    "pressure_msl": "pressure_hpa",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_gusts_10m": "wind_gust_kmh",
    "weather_code": "weather_code",
}
MAX_COORDINATE_DISTANCE_KM = 15.0


def load_live_locations(catalog_path: str | Path) -> list[dict[str, Any]]:
    """Load only the frozen NATIONWIDE_63 catalog used by the inference job."""
    catalog = load_cities(catalog_path)
    locations = select_dataset_locations(catalog, DATASET_NATIONWIDE_63)
    ids = [item.get("location_id") for item in locations]
    names = [item.get("name") or item.get("province_name") for item in locations]
    coordinates = [(item.get("latitude"), item.get("longitude")) for item in locations]
    if len(locations) != 63 or len(set(ids)) != 63 or len(set(names)) != 63:
        raise ValueError("live hourly publisher requires 63 unique canonical locations and names")
    if any(not _valid_coordinate_pair(latitude, longitude) for latitude, longitude in coordinates):
        raise ValueError("canonical catalog contains an invalid coordinate")
    if len(set(coordinates)) != 63:
        raise ValueError("canonical catalog contains duplicate coordinates")
    return locations


def required_history_hours(feature_names: Sequence[str]) -> int:
    """Derive the greatest lookback needed by the frozen feature list."""
    if not feature_names:
        raise ValueError("feature contract must contain at least one feature")
    lookbacks = [
        int(match.group(1))
        for name in feature_names
        if (match := re.search(r"(?:_lag_|_delta_)(\d+)h$", name)) is not None
    ]
    windows = [
        int(match.group(1))
        for name in feature_names
        if (match := re.search(r"(?:_roll_(?:mean|std)_|_sum_)(\d+)h$", name)) is not None
    ]
    # Rolling/sum windows include the feature-time row; a window of N rows
    # therefore needs N-1 hours before that row.
    return max([*lookbacks, *(window - 1 for window in windows), 0])


def parse_utc_hour(value: Any) -> datetime:
    """Parse a provider timestamp and reject any non-hourly or invalid instant."""
    if not isinstance(value, str) or not value:
        raise ValueError("provider hourly timestamp must be a non-empty string")
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid provider hourly timestamp {value!r}") from exc
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    else:
        instant = instant.astimezone(timezone.utc)
    if instant.minute != 0 or instant.second != 0 or instant.microsecond != 0:
        raise ValueError(f"provider timestamp is not aligned to a UTC hour: {value!r}")
    return instant


def format_utc_hour(instant: datetime) -> str:
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("UTC hourly timestamp must be timezone-aware")
    value = instant.astimezone(timezone.utc)
    if value.minute != 0 or value.second != 0 or value.microsecond != 0:
        raise ValueError("UTC hourly timestamp is not aligned to an hour")
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def safe_hour_cutoff(now: datetime) -> datetime:
    """Only consider the last completed UTC hour, never the in-progress hour."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("current time must be timezone-aware")
    current_hour = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return current_hour - timedelta(hours=1)


def select_latest_safe_hour(
    provider_hour_sets: Iterable[Iterable[datetime]],
    now: datetime,
) -> datetime:
    """Choose the latest common provider hour at or before the closed-hour cutoff."""
    sets = [set(hours) for hours in provider_hour_sets]
    if not sets or any(not hours for hours in sets):
        raise ValueError("cannot select a safe hour without provider timestamps")
    common = set.intersection(*sets)
    cutoff = safe_hour_cutoff(now)
    candidates = [hour for hour in common if hour <= cutoff]
    if not candidates:
        raise ValueError(f"no common provider hour is available at or before {format_utc_hour(cutoff)}")
    return max(candidates)


def _valid_coordinate_pair(latitude: Any, longitude: Any) -> bool:
    return (
        _finite_number(latitude)
        and _finite_number(longitude)
        and -90.0 <= float(latitude) <= 90.0
        and -180.0 <= float(longitude) <= 180.0
    )


def _finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _coordinate_distance_km(left: tuple[float, float], right: tuple[float, float]) -> float:
    from math import asin, cos, radians, sin, sqrt

    lat1, lon1 = map(radians, left)
    lat2, lon2 = map(radians, right)
    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1
    haversine = sin(delta_lat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(delta_lon / 2) ** 2
    return 6371.0088 * 2 * asin(min(1.0, sqrt(haversine)))


def _normalize_responses(locations: Sequence[dict[str, Any]], payload: Any) -> list[dict[str, Any]]:
    responses = [payload] if isinstance(payload, dict) and len(locations) == 1 else payload
    if not isinstance(responses, list) or len(responses) != len(locations):
        actual = len(responses) if isinstance(responses, list) else "non-list"
        raise ValueError(f"provider returned {actual} location results for {len(locations)} requested")
    if any(not isinstance(response, dict) for response in responses):
        raise ValueError("each provider location result must be a JSON object")
    return responses


def _live_retry_delay(response: httpx.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(retry_after)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
                except (TypeError, ValueError, OverflowError):
                    pass
        if response.status_code == 429:
            base_delay = (60.0, 120.0, 300.0)[min(max(attempt - 1, 0), 2)]
            return random.uniform(base_delay * 0.8, base_delay)
    return float(min(2 ** (attempt - 1), 30))


def _request_live_json_with_retry(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, Any],
    max_retries: int,
    timeout: float,
    sleep: Callable[[float], None],
    on_retry: Callable[[float], None] | None = None,
    retry_deadline_monotonic: float | None = None,
) -> Any:
    """Retry provider failures without crossing into a different safe hour."""
    if max_retries <= 0:
        raise ValueError("max_retries must be positive")
    last_error: Exception | None = None

    def wait_before_retry(response: httpx.Response | None, attempt: int) -> bool:
        delay = _live_retry_delay(response, attempt)
        remaining = None
        if retry_deadline_monotonic is not None:
            remaining = retry_deadline_monotonic - time.monotonic()
            if remaining <= 0:
                return False
            delay = min(delay, remaining)
        if on_retry is not None:
            on_retry(delay)
        sleep(delay)
        # If the rate limit expires after this safe hour, let the daemon's next
        # poll select the then-current hour instead of retrying stale work.
        if remaining is not None and delay >= remaining:
            return False
        return True

    for attempt in range(1, max_retries + 1):
        response = None
        try:
            response = client.get(url, params=params, timeout=timeout)
            if response.status_code == 429 or response.status_code >= 500:
                last_error = httpx.HTTPStatusError(
                    f"retryable Open-Meteo status {response.status_code}",
                    request=response.request,
                    response=response,
                )
                if attempt >= max_retries:
                    break
                if not wait_before_retry(response, attempt):
                    break
                continue
            response.raise_for_status()
            return response.json()
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_error = exc
            retryable = not isinstance(exc, httpx.HTTPStatusError) or (
                exc.response.status_code == 429 or exc.response.status_code >= 500
            )
            if not retryable or attempt >= max_retries:
                break
            if not wait_before_retry(response, attempt):
                break
        except (ValueError, KeyError) as exc:
            last_error = exc
            break

    if last_error is not None:
        raise last_error
    raise RuntimeError("Open-Meteo request failed without an error response")


def hourly_request_params(
    locations: Sequence[dict[str, Any]],
    history_hours: int,
    *,
    provider_model: str | None = None,
) -> dict[str, Any]:
    if not locations:
        raise ValueError("cannot request an empty location batch")
    if history_hours < 0:
        raise ValueError("history_hours cannot be negative")
    params = {
        "latitude": ",".join(str(location["latitude"]) for location in locations),
        "longitude": ",".join(str(location["longitude"]) for location in locations),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "UTC",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        # past_hours counts timestamps before the current UTC hour. The safe
        # endpoint is the previous hour, so H prior hours need H+1 returned rows.
        "past_hours": history_hours + 1,
        # Bound the response to the current provider hour; that hour is then
        # deliberately filtered by safe_hour_cutoff until it is complete.
        "forecast_hours": 1,
    }
    if provider_model:
        params["models"] = provider_model
    return params


def fetch_hourly_responses(
    client: httpx.Client,
    locations: Sequence[dict[str, Any]],
    *,
    history_hours: int,
    endpoint: str = OPEN_METEO_URL,
    batch_size: int = DEFAULT_BATCH_SIZE,
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    provider_model: str | None = None,
    sleep=time.sleep,
    retry_deadline_monotonic: float | None = None,
) -> dict[str, Any]:
    """Fetch ordered hourly responses; failed batches are reported by exact ID."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if request_timeout_seconds <= 0 or max_retries <= 0:
        raise ValueError("request timeout and retry count must be positive")
    started = time.perf_counter()
    responses_by_id: dict[str, dict[str, Any]] = {}
    failed: dict[str, str] = {}
    retry_counts: dict[str, int] = {}
    retry_delays: dict[str, list[float]] = {}
    api_latencies: list[float] = []
    api_requests = 0

    for batch in chunks(list(locations), batch_size):
        ids = [str(location["location_id"]) for location in batch]
        batch_key = ",".join(ids)
        params = hourly_request_params(batch, history_hours, provider_model=provider_model)

        def record_retry(delay: float) -> None:
            retry_counts[batch_key] = retry_counts.get(batch_key, 0) + 1
            retry_delays.setdefault(batch_key, []).append(round(delay, 3))

        request_started = time.perf_counter()
        api_requests += 1
        try:
            payload = _request_live_json_with_retry(
                client,
                endpoint,
                params=params,
                max_retries=max_retries,
                timeout=request_timeout_seconds,
                sleep=sleep,
                on_retry=record_retry,
                retry_deadline_monotonic=retry_deadline_monotonic,
            )
            api_latencies.append(time.perf_counter() - request_started)
            normalized = _normalize_responses(batch, payload)
            for location, response in zip(batch, normalized, strict=True):
                location_id = str(location["location_id"])
                if response.get("error"):
                    failed[location_id] = str(response.get("reason", "provider location error"))
                else:
                    responses_by_id[location_id] = response
        except Exception as exc:
            api_latencies.append(time.perf_counter() - request_started)
            for location_id in ids:
                failed[location_id] = f"{type(exc).__name__}: {exc}"

    return {
        "responses_by_id": responses_by_id,
        "failed_locations": failed,
        "retry_counts": retry_counts,
        "retry_delays_seconds": retry_delays,
        "api_request_count": api_requests,
        "api_latencies_seconds": api_latencies,
        "api_cycle_seconds": time.perf_counter() - started,
    }


def _parse_location_timeline(
    location: Mapping[str, Any],
    response: Mapping[str, Any],
) -> tuple[dict[datetime, dict[str, Any]], dict[str, Any]]:
    location_id = str(location.get("location_id", ""))
    if response.get("error"):
        raise ValueError(str(response.get("reason", "provider location error")))
    if response.get("utc_offset_seconds") != 0:
        raise ValueError(f"provider response for {location_id} is not UTC")
    if response.get("timezone") not in {"UTC", "GMT"}:
        raise ValueError(f"provider response for {location_id} has unexpected timezone {response.get('timezone')!r}")

    latitude = response.get("latitude")
    longitude = response.get("longitude")
    if not _valid_coordinate_pair(latitude, longitude):
        raise ValueError(f"provider returned invalid coordinates for {location_id}")
    distance = _coordinate_distance_km(
        (float(location["latitude"]), float(location["longitude"])),
        (float(latitude), float(longitude)),
    )
    if distance > MAX_COORDINATE_DISTANCE_KM:
        raise ValueError(f"provider coordinates for {location_id} are {distance:.2f} km from its canonical request")

    hourly = response.get("hourly")
    units = response.get("hourly_units")
    if not isinstance(hourly, Mapping) or not isinstance(units, Mapping):
        raise ValueError(f"provider response has no hourly values or units for {location_id}")
    for variable, unit in EXPECTED_UNITS.items():
        if units.get(variable) != unit:
            raise ValueError(f"provider unit mismatch for {location_id}.{variable}: {units.get(variable)!r} != {unit!r}")

    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        raise ValueError(f"provider response has no hourly timeline for {location_id}")
    arrays: dict[str, list[Any]] = {}
    for variable in HOURLY_VARIABLES:
        values = hourly.get(variable)
        if not isinstance(values, list) or len(values) != len(times):
            raise ValueError(f"hourly variable {variable} does not align for {location_id}")
        arrays[variable] = values

    indexed: dict[datetime, dict[str, Any]] = {}
    for index, raw_time in enumerate(times):
        instant = parse_utc_hour(raw_time)
        if instant in indexed:
            raise ValueError(f"provider returned duplicate hourly timestamp {format_utc_hour(instant)} for {location_id}")
        indexed[instant] = {variable: arrays[variable][index] for variable in HOURLY_VARIABLES}
    if len(indexed) != len(times):
        raise ValueError(f"provider timeline is not unique for {location_id}")

    metadata = {
        "provider_latitude": float(latitude),
        "provider_longitude": float(longitude),
        "canonical_coordinate_distance_km": distance,
        "timezone": response.get("timezone"),
        "utc_offset_seconds": response.get("utc_offset_seconds"),
        "hourly_units": dict(units),
    }
    return indexed, metadata


def _normalize_weather_values(location_id: str, values: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for provider_name, event_name in VALUE_FIELD_MAP.items():
        value = values.get(provider_name)
        if not _finite_number(value):
            raise ValueError(f"{location_id}.{provider_name} is missing or non-finite")
        number = float(value)
        if provider_name == "temperature_2m" and not -90.0 <= number <= 60.0:
            raise ValueError(f"{location_id}.temperature_2m is outside the accepted physical range")
        if provider_name == "relative_humidity_2m" and not 0.0 <= number <= 100.0:
            raise ValueError(f"{location_id}.relative_humidity_2m is outside 0..100")
        if provider_name == "precipitation" and number < 0.0:
            raise ValueError(f"{location_id}.precipitation is negative")
        if provider_name == "pressure_msl" and not 500.0 <= number <= 1200.0:
            raise ValueError(f"{location_id}.pressure_msl is outside the accepted range")
        if provider_name in {"wind_speed_10m", "wind_gusts_10m"} and number < 0.0:
            raise ValueError(f"{location_id}.{provider_name} is negative")
        if provider_name == "weather_code":
            if not 0 <= number <= 99 or not number.is_integer():
                raise ValueError(f"{location_id}.weather_code must be a WMO integer code from 0 to 99")
            normalized[event_name] = int(number)
        else:
            normalized[event_name] = number
    return normalized


def _event_for_hour(
    location: Mapping[str, Any],
    metadata: Mapping[str, Any],
    event_time: datetime,
    values: Mapping[str, Any],
    ingestion_time: datetime,
) -> dict[str, Any]:
    location_id = str(location["location_id"])
    timestamp = format_utc_hour(event_time)
    if ingestion_time.tzinfo is None or ingestion_time.utcoffset() is None:
        raise ValueError("ingestion_time must be timezone-aware")
    normalized = _normalize_weather_values(location_id, values)
    event = {
        "event_id": f"{LIVE_SOURCE}|{location_id}|{timestamp}",
        "event_type": EVENT_TYPE,
        "location_id": location_id,
        "city": str(location.get("name") or location.get("province_name") or ""),
        "latitude": float(metadata["provider_latitude"]),
        "longitude": float(metadata["provider_longitude"]),
        "event_time": timestamp,
        "ingestion_time": ingestion_time.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        **normalized,
        "source": LIVE_SOURCE,
    }
    missing = set(EVENT_FIELDS) - set(event)
    if missing:
        raise ValueError(f"live event is missing canonical schema fields: {sorted(missing)}")
    return event


def build_hourly_events(
    locations: Sequence[dict[str, Any]],
    responses_by_id: Mapping[str, Mapping[str, Any]],
    *,
    now: datetime,
    history_hours: int,
    bootstrap: bool,
    ingestion_time: datetime | None = None,
    prior_failures: Mapping[str, str] | None = None,
    provider_model: str | None = None,
    provider_endpoint: str = OPEN_METEO_URL,
) -> dict[str, Any]:
    """Select common safe UTC data, enforce continuity, and build canonical events."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if history_hours < 0:
        raise ValueError("history_hours cannot be negative")
    ingestion_time = ingestion_time or datetime.now(timezone.utc)
    failures = dict(prior_failures or {})
    timelines: dict[str, dict[datetime, dict[str, Any]]] = {}
    metadata_by_id: dict[str, dict[str, Any]] = {}
    location_by_id = {str(item["location_id"]): item for item in locations}

    for location_id, response in responses_by_id.items():
        location = location_by_id.get(str(location_id))
        if location is None:
            failures[str(location_id)] = "response location ID is not in the canonical catalog"
            continue
        try:
            timeline, metadata = _parse_location_timeline(location, response)
            timelines[str(location_id)] = timeline
            metadata_by_id[str(location_id)] = metadata
        except (TypeError, ValueError, KeyError) as exc:
            failures[str(location_id)] = f"{type(exc).__name__}: {exc}"

    if not timelines:
        return {
            "safe_hour": None,
            "events": [],
            "failed_locations": failures,
            "history_gap_locations": [],
            "future_provider_rows_filtered": 0,
            "provider_timestamps_per_location": {},
            "coordinate_distances_km": {},
            "provider_coordinates_by_location": {},
        }

    try:
        safe_hour = select_latest_safe_hour((timeline.keys() for timeline in timelines.values()), now)
    except ValueError as exc:
        for location_id in timelines:
            failures.setdefault(location_id, f"NO_COMMON_SAFE_HOUR: {exc}")
        return {
            "safe_hour": None,
            "events": [],
            "failed_locations": failures,
            "history_gap_locations": [],
            "future_provider_rows_filtered": 0,
            "provider_timestamps_per_location": {key: len(value) for key, value in timelines.items()},
            "coordinate_distances_km": {key: value["canonical_coordinate_distance_km"] for key, value in metadata_by_id.items()},
            "provider_coordinates_by_location": {
                key: [value["provider_latitude"], value["provider_longitude"]]
                for key, value in metadata_by_id.items()
            },
        }

    cutoff = safe_hour_cutoff(now)
    future_provider_rows_filtered = sum(
        1
        for timeline in timelines.values()
        for event_time in timeline
        if event_time > cutoff
    )
    required_rows = history_hours + 1 if bootstrap else 1
    expected_times = [safe_hour - timedelta(hours=offset) for offset in range(required_rows - 1, -1, -1)]
    gap_locations: list[str] = []
    events: list[dict[str, Any]] = []

    for location_id, timeline in timelines.items():
        selected_times = expected_times if bootstrap else [safe_hour]
        missing_times = [instant for instant in selected_times if instant not in timeline]
        if missing_times:
            failures[location_id] = "BOOTSTRAP_HISTORY_GAP: missing " + ",".join(
                format_utc_hour(instant) for instant in missing_times
            )
            gap_locations.append(location_id)
            continue
        location_events: list[dict[str, Any]] = []
        try:
            for event_time in selected_times:
                if event_time > safe_hour or event_time > cutoff:
                    raise ValueError("future or incomplete provider hour crossed the safe-hour cutoff")
                event = _event_for_hour(
                        location_by_id[location_id],
                        metadata_by_id[location_id],
                        event_time,
                        timeline[event_time],
                        ingestion_time,
                    )
                if provider_model:
                    # Keep the shared V1 event builder unchanged. T2H carries
                    # canonical catalog coordinates for feature parity while
                    # retaining provider grid coordinates as provenance.
                    event.update(
                        {
                            "latitude": float(location_by_id[location_id]["latitude"]),
                            "longitude": float(location_by_id[location_id]["longitude"]),
                            "provider": "Open-Meteo",
                            "provider_endpoint": provider_endpoint,
                            "provider_model": provider_model,
                            "source_retrieved_at": ingestion_time.astimezone(timezone.utc)
                            .isoformat(timespec="seconds")
                            .replace("+00:00", "Z"),
                            "provider_grid_latitude": float(metadata_by_id[location_id]["provider_latitude"]),
                            "provider_grid_longitude": float(metadata_by_id[location_id]["provider_longitude"]),
                        }
                    )
                location_events.append(event)
        except (TypeError, ValueError, KeyError) as exc:
            failures[location_id] = f"INVALID_LIVE_OBSERVATION: {exc}"
            if bootstrap and location_id not in gap_locations:
                gap_locations.append(location_id)
            continue
        events.extend(location_events)

    if bootstrap and failures:
        # A partial bootstrap is not a warm canonical state. Do not publish any
        # rows that might make only a subset of locations appear ready.
        events = []

    events.sort(key=lambda event: (event["event_time"], event["location_id"]))
    return {
        "safe_hour": format_utc_hour(safe_hour),
        "events": events,
        "failed_locations": failures,
        "history_gap_locations": sorted(set(gap_locations)),
        "future_provider_rows_filtered": future_provider_rows_filtered,
        "provider_timestamps_per_location": {key: len(value) for key, value in timelines.items()},
        "coordinate_distances_km": {
            key: value["canonical_coordinate_distance_km"] for key, value in metadata_by_id.items()
        },
        "provider_coordinates_by_location": {
            key: [value["provider_latitude"], value["provider_longitude"]]
            for key, value in metadata_by_id.items()
        },
    }


class PublishedHourCache:
    """Track published location-hours, optionally persisting them across restarts."""

    def __init__(self, state_path: Path | None = None) -> None:
        self._last_published: dict[str, datetime] = {}
        self._state_path = state_path
        if state_path is not None and state_path.exists():
            self._load()

    def _load(self) -> None:
        assert self._state_path is not None
        try:
            state = json.loads(self._state_path.read_text(encoding="utf-8"))
            if not isinstance(state, dict) or state.get("version") != 1 or not isinstance(state.get("last_published"), dict):
                raise ValueError("unsupported published-hour cache format")
            self._last_published = {
                str(location_id): parse_utc_hour(hour)
                for location_id, hour in state["last_published"].items()
            }
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"could not load published-hour cache {self._state_path}: {exc}") from exc

    def _save(self) -> None:
        if self._state_path is None:
            return
        state = {
            "version": 1,
            "last_published": {
                location_id: format_utc_hour(hour)
                for location_id, hour in sorted(self._last_published.items())
            },
        }
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._state_path.with_name(self._state_path.name + ".partial")
        temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(self._state_path)

    def has_completed(self, safe_hour: datetime, location_ids: Sequence[str]) -> bool:
        if not location_ids:
            return False
        target = parse_utc_hour(format_utc_hour(safe_hour))
        earliest = datetime.min.replace(tzinfo=timezone.utc)
        return all(
            self._last_published.get(str(location_id), earliest) >= target
            for location_id in location_ids
        )

    def filter_new(self, events: Sequence[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
        selected: list[dict[str, Any]] = []
        duplicates = 0
        for event in events:
            location_id = str(event["location_id"])
            event_time = parse_utc_hour(event["event_time"])
            if event_time <= self._last_published.get(location_id, datetime.min.replace(tzinfo=timezone.utc)):
                duplicates += 1
                continue
            selected.append(event)
        return selected, duplicates

    def mark_published(self, events: Sequence[dict[str, Any]]) -> None:
        for event in events:
            self._last_published[str(event["location_id"])] = parse_utc_hour(event["event_time"])
        if events:
            self._save()


def kafka_key(event: Mapping[str, Any]) -> str:
    """Unique deterministic location-hour key; partition is pinned by location."""
    return f"{event['location_id']}|{event['event_time']}"


def kafka_partition(location_id: str, partition_count: int) -> int:
    """Keep every hour for one location ordered on a single Kafka partition."""
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    return zlib.crc32(location_id.encode("utf-8")) % partition_count


def topic_partition_count(producer: Any, topic: str, timeout: float) -> int:
    metadata = producer.list_topics(topic=topic, timeout=timeout)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None:
        raise RuntimeError(f"Kafka topic metadata missing for {topic}")
    if topic_metadata.error is not None:
        raise RuntimeError(f"Kafka topic metadata error for {topic}: {topic_metadata.error}")
    count = len(topic_metadata.partitions)
    if count <= 0:
        raise RuntimeError(f"Kafka topic {topic} has no partitions")
    return count


def publish_events(
    producer: Any,
    topic: str,
    events: Sequence[dict[str, Any]],
    *,
    partition_count: int,
    flush_timeout_seconds: float = 120.0,
) -> dict[str, Any]:
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    failures: list[str] = []
    delivery_count = 0

    def delivered(error: Any, message: Any) -> None:
        nonlocal delivery_count
        if error is not None:
            failures.append(str(error))
        else:
            delivery_count += 1

    started = time.perf_counter()
    enqueued = 0
    for event in events:
        producer.produce(
            topic=topic,
            partition=kafka_partition(str(event["location_id"]), partition_count),
            key=kafka_key(event),
            value=json.dumps(event, ensure_ascii=False, allow_nan=False, separators=(",", ":")),
            callback=delivered,
        )
        enqueued += 1
        producer.poll(0)
    remaining = producer.flush(timeout=flush_timeout_seconds)
    publish_seconds = time.perf_counter() - started
    return {
        "events_enqueued": enqueued,
        "events_delivered": delivery_count,
        "delivery_failures": failures,
        "producer_flush_remaining": int(remaining),
        "kafka_publish_seconds": publish_seconds,
    }


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int((len(ordered) - 1) * quantile + 0.5)
    return ordered[index]


def collect_resource_usage(process_started: float, process_cpu_started: float) -> dict[str, Any]:
    result: dict[str, Any] = {
        "process_cpu_seconds": max(0.0, time.process_time() - process_cpu_started),
        "process_elapsed_seconds": max(0.0, time.perf_counter() - process_started),
        "peak_rss_bytes": None,
    }
    try:
        import resource

        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports KiB, macOS reports bytes; the producer image is Linux.
        result["peak_rss_bytes"] = int(peak_rss * 1024)
    except (ImportError, OSError, ValueError):
        pass
    return result


def _write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    temporary.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_cycle(
    client: httpx.Client,
    producer: Any,
    *,
    locations: Sequence[dict[str, Any]],
    topic: str,
    endpoint: str,
    history_hours: int,
    bootstrap: bool,
    partition_count: int,
    batch_size: int = DEFAULT_BATCH_SIZE,
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    provider_model: str | None = None,
    cache: PublishedHourCache | None = None,
    now: datetime | None = None,
    sleep=time.sleep,
) -> dict[str, Any]:
    cycle_started = time.perf_counter()
    current_time = now or datetime.now(timezone.utc)
    desired_safe_hour = safe_hour_cutoff(current_time)
    already_published = (
        cache is not None
        and not bootstrap
        and cache.has_completed(desired_safe_hour, [str(location["location_id"]) for location in locations])
    )
    if already_published:
        fetch = {
            "responses_by_id": {},
            "failed_locations": {},
            "retry_counts": {},
            "retry_delays_seconds": {},
            "api_request_count": 0,
            "api_latencies_seconds": [],
            "api_cycle_seconds": 0.0,
        }
        built = {
            "safe_hour": format_utc_hour(desired_safe_hour),
            "events": [],
            "failed_locations": {},
            "history_gap_locations": [],
            "future_provider_rows_filtered": 0,
            "provider_timestamps_per_location": {},
            "coordinate_distances_km": {},
            "provider_coordinates_by_location": {},
        }
        event_build_seconds = 0.0
        skipped_same_hour = len(locations)
    else:
        next_safe_hour_at = desired_safe_hour + timedelta(hours=2)
        retry_window_seconds = max(
            0.0,
            (next_safe_hour_at - current_time.astimezone(timezone.utc)).total_seconds(),
        )
        fetch = fetch_hourly_responses(
            client,
            locations,
            history_hours=history_hours,
            endpoint=endpoint,
            batch_size=batch_size,
            request_timeout_seconds=request_timeout_seconds,
            max_retries=max_retries,
            provider_model=provider_model,
            sleep=sleep,
            retry_deadline_monotonic=time.monotonic() + retry_window_seconds,
        )
        retrieved_at = datetime.now(timezone.utc) if provider_model else None
        build_started = time.perf_counter()
        built = build_hourly_events(
            locations,
            fetch["responses_by_id"],
            now=current_time,
            history_hours=history_hours,
            bootstrap=bootstrap,
            ingestion_time=retrieved_at,
            prior_failures=fetch["failed_locations"],
            provider_model=provider_model,
            provider_endpoint=endpoint,
        )
        event_build_seconds = time.perf_counter() - build_started
        skipped_same_hour = 0
    events = built["events"]
    if cache is not None and not bootstrap and not already_published:
        events, skipped_same_hour = cache.filter_new(events)
    publishing = publish_events(
        producer,
        topic,
        events,
        partition_count=partition_count,
    ) if events else {
        "events_enqueued": 0,
        "events_delivered": 0,
        "delivery_failures": [],
        "producer_flush_remaining": 0,
        "kafka_publish_seconds": 0.0,
    }
    if (
        cache is not None
        and events
        and publishing["events_delivered"] == len(events)
        and not publishing["delivery_failures"]
        and publishing["producer_flush_remaining"] == 0
    ):
        cache.mark_published(events)
    api_latencies = fetch["api_latencies_seconds"]
    failed = built["failed_locations"]
    return {
        "mode": "bootstrap" if bootstrap else "once",
        "topic": topic,
        "provider_endpoint": endpoint,
        "provider_source": LIVE_SOURCE,
        "provider_model": provider_model,
        "provider_values_are_modelled": True,
        "requested_locations": len(locations),
        "successful_locations": len(locations) - len(failed),
        "failed_locations": failed,
        "failed_location_ids": sorted(failed),
        "safe_hour": built["safe_hour"],
        "history_hours_requested": history_hours,
        "bootstrap_observation_hours_requested": history_hours + 1 if bootstrap else 1,
        "bootstrap_observation_hours_accepted": (len(events) // len(locations)) if bootstrap and locations else 0,
        "events_built": len(built["events"]),
        "events_enqueued": publishing["events_enqueued"],
        "events_delivered": publishing["events_delivered"],
        "delivery_failures": publishing["delivery_failures"],
        "producer_flush_remaining": publishing["producer_flush_remaining"],
        "unique_location_hour_keys": len({(event["location_id"], event["event_time"]) for event in events}),
        "duplicate_physical_messages": 0,
        "duplicate_canonical_keys": 0,
        "same_hour_cache_skips": skipped_same_hour,
        "future_provider_rows_filtered": built["future_provider_rows_filtered"],
        "history_gap_location_ids": built["history_gap_locations"],
        "provider_timestamps_per_location": built["provider_timestamps_per_location"],
        "coordinate_distance_km_by_location": built["coordinate_distances_km"],
        "provider_coordinates_by_location": built["provider_coordinates_by_location"],
        "api_request_count": fetch["api_request_count"],
        "api_latency_median_seconds": _percentile(api_latencies, 0.5),
        "api_latency_p95_seconds": _percentile(api_latencies, 0.95),
        "api_cycle_seconds": fetch["api_cycle_seconds"],
        "event_build_seconds": event_build_seconds,
        "kafka_publish_seconds": publishing["kafka_publish_seconds"],
        "full_cycle_seconds": time.perf_counter() - cycle_started,
        "retry_counts": fetch["retry_counts"],
        "retry_delays_seconds": fetch["retry_delays_seconds"],
    }


def _wait_for_kafka(producer: Any, topic: str, timeout_seconds: float) -> int:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return topic_partition_count(producer, topic, timeout=min(10.0, timeout_seconds))
        except Exception as exc:
            last_error = exc
            time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))
    raise RuntimeError(f"Kafka did not become ready for topic {topic}: {last_error}") from last_error


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish canonical UTC-hour Open-Meteo inputs for weather inference.")
    parser.add_argument("--mode", choices=("once", "daemon", "bootstrap"), default="once")
    parser.add_argument("--bootstrap-servers", default=os.getenv("WEATHER_LIVE_HOURLY_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS))
    parser.add_argument("--topic", default=os.getenv("WEATHER_LIVE_HOURLY_TOPIC", DEFAULT_TOPIC))
    parser.add_argument("--endpoint", default=os.getenv("WEATHER_LIVE_HOURLY_ENDPOINT", OPEN_METEO_URL))
    parser.add_argument("--provider-model", default=os.getenv("WEATHER_LIVE_HOURLY_PROVIDER_MODEL"))
    parser.add_argument("--catalog", type=Path, default=Path(os.getenv("WEATHER_LIVE_HOURLY_CATALOG", str(PROJECT_ROOT / "historical" / "locations.json"))))
    parser.add_argument("--poll-interval-seconds", type=float, default=float(os.getenv("WEATHER_LIVE_HOURLY_POLL_INTERVAL_SECONDS", str(DEFAULT_POLL_INTERVAL_SECONDS))))
    parser.add_argument("--request-timeout-seconds", type=float, default=float(os.getenv("WEATHER_LIVE_HOURLY_REQUEST_TIMEOUT_SECONDS", str(DEFAULT_REQUEST_TIMEOUT_SECONDS))))
    parser.add_argument("--max-retries", type=int, default=int(os.getenv("WEATHER_LIVE_HOURLY_MAX_RETRIES", str(DEFAULT_MAX_RETRIES))))
    parser.add_argument("--history-hours", type=int, default=int(os.getenv("WEATHER_LIVE_HOURLY_HISTORY_HOURS", "0")))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("WEATHER_LIVE_HOURLY_BATCH_SIZE", str(DEFAULT_BATCH_SIZE))))
    parser.add_argument("--kafka-startup-timeout-seconds", type=float, default=float(os.getenv("WEATHER_LIVE_HOURLY_KAFKA_STARTUP_TIMEOUT_SECONDS", str(DEFAULT_KAFKA_STARTUP_TIMEOUT_SECONDS))))
    parser.add_argument("--max-polls", type=int, default=0, help="Daemon poll limit for supervised runs; 0 runs until interrupted.")
    parser.add_argument("--summary-json", type=Path, default=None)
    args = parser.parse_args(argv)
    if args.poll_interval_seconds < 0 or args.request_timeout_seconds <= 0:
        parser.error("poll interval must be non-negative and request timeout positive")
    if args.mode == "daemon" and args.poll_interval_seconds < 30:
        parser.error("daemon poll interval must be at least 30 seconds")
    if args.max_retries <= 0 or args.batch_size <= 0 or args.max_polls < 0:
        parser.error("retry count and batch size must be positive; max polls cannot be negative")
    if args.kafka_startup_timeout_seconds <= 0:
        parser.error("Kafka startup timeout must be positive")
    if args.provider_model and not args.provider_model.strip():
        parser.error("provider model cannot be blank")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    contract = load_feature_contract(PROJECT_ROOT)
    minimum_history = required_history_hours(contract.feature_names)
    history_hours = args.history_hours or minimum_history
    if history_hours < minimum_history:
        raise ValueError(f"history hours {history_hours} is below the frozen feature contract requirement {minimum_history}")
    locations = load_live_locations(args.catalog)
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError("confluent-kafka is required to publish live hourly observations") from exc

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "weather-openmeteo-live-hourly-v1",
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": max(60_000, int(args.request_timeout_seconds * args.max_retries * 1000)),
    })
    partition_count = _wait_for_kafka(producer, args.topic, args.kafka_startup_timeout_seconds)
    process_started = time.perf_counter()
    process_cpu_started = time.process_time()
    history_hours_requested = history_hours
    cache = (
        PublishedHourCache(PROJECT_ROOT / "results" / "live_hourly_weather_producer" / "published_hours.json")
        if args.mode == "daemon"
        else None
    )
    summaries: list[dict[str, Any]] = []

    try:
        with httpx.Client() as client:
            poll_index = 0
            while True:
                summary = run_cycle(
                    client,
                    producer,
                    locations=locations,
                    topic=args.topic,
                    endpoint=args.endpoint,
                    history_hours=history_hours_requested,
                    bootstrap=args.mode == "bootstrap",
                    partition_count=partition_count,
                    batch_size=args.batch_size,
                    request_timeout_seconds=args.request_timeout_seconds,
                    max_retries=args.max_retries,
                    provider_model=args.provider_model,
                    cache=cache,
                )
                summary["poll_index"] = poll_index
                summary["mode"] = args.mode
                summaries.append(summary)
                if args.summary_json:
                    _write_summary(args.summary_json, {
                        "status": "PASS" if not summary["failed_locations"] and not summary["delivery_failures"] and summary["producer_flush_remaining"] == 0 else "PARTIAL_OR_FAILED",
                        "mode": args.mode,
                        "topic": args.topic,
                        "provider_endpoint": args.endpoint,
                        "catalog_location_count": len(locations),
                        "frozen_history_hours": minimum_history,
                        "history_hours_requested": history_hours_requested,
                        "polls_completed": len(summaries),
                        "poll_results": summaries,
                        "runtime_resources": collect_resource_usage(process_started, process_cpu_started),
                    })
                print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)

                if args.mode != "daemon":
                    return 0 if not summary["failed_locations"] and not summary["delivery_failures"] and summary["producer_flush_remaining"] == 0 else 1
                poll_index += 1
                if args.max_polls and poll_index >= args.max_polls:
                    return 0 if all(not item["failed_locations"] and not item["delivery_failures"] for item in summaries) else 1
                time.sleep(args.poll_interval_seconds)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
