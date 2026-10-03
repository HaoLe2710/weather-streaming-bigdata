from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
from typing import Any, Iterable, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .evaluation_contract import LIVE_REFERENCE_SOURCE, parse_utc_hour


FORECAST_ENDPOINT = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "pressure_msl",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
)
EVENT_FIELDS = (
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "pressure_hpa",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "weather_code",
)


def safe_hour(now: datetime) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("current time must be timezone-aware UTC")
    hour = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return hour - timedelta(hours=1)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    value = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(value))


def _time_index(response: Mapping[str, Any], target_time: datetime) -> int:
    timezone_name = str(response.get("timezone") or "")
    offset = int(response.get("utc_offset_seconds") or 0)
    if timezone_name not in {"UTC", "GMT"} or offset != 0:
        raise ValueError(f"provider did not return a UTC timeline: timezone={timezone_name!r}, offset={offset}")
    hourly = response.get("hourly")
    if not isinstance(hourly, Mapping) or not isinstance(hourly.get("time"), list):
        raise ValueError("provider response has no hourly time array")
    target = target_time.strftime("%Y-%m-%dT%H:%M")
    try:
        return hourly["time"].index(target)
    except ValueError as exc:
        raise ValueError(f"requested target hour {target} is outside provider response") from exc


def _location_response(response: Any) -> list[Mapping[str, Any]]:
    if isinstance(response, list):
        locations = response
    elif isinstance(response, Mapping):
        locations = [response]
    else:
        raise ValueError("provider response must be a location object or list")
    if not all(isinstance(item, Mapping) for item in locations):
        raise ValueError("provider response contains a non-object location")
    return locations


def fetch_live_reference_backfill(
    target_time: datetime | str,
    locations: Iterable[Mapping[str, Any]],
    *,
    endpoint: str = FORECAST_ENDPOINT,
    now: datetime | None = None,
    timeout_seconds: float = 60.0,
) -> list[dict[str, Any]]:
    """Fetch one already-safe hourly Forecast API target and label it backfill.

    This is an explicitly requested historical retrieval path. It never uses a
    future target: the requested hour must be no newer than the producer's
    closed-hour safe cutoff at retrieval time.
    """

    target = parse_utc_hour(target_time)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("backfill clock must be timezone-aware UTC")
    current = current.astimezone(timezone.utc)
    if target > safe_hour(current):
        raise ValueError(
            f"target {target.isoformat()} is newer than the safe completed hour {safe_hour(current).isoformat()}"
        )
    location_list = list(locations)
    if len(location_list) != 63 or len({str(item.get("location_id")) for item in location_list}) != 63:
        raise ValueError("live reference backfill requires the 63 unique NATIONWIDE_63 locations")

    query = urlencode(
        {
            "latitude": ",".join(str(item["latitude"]) for item in location_list),
            "longitude": ",".join(str(item["longitude"]) for item in location_list),
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": "UTC",
            "temperature_unit": "celsius",
            "wind_speed_unit": "kmh",
            "precipitation_unit": "mm",
            "past_hours": 25,
            "forecast_hours": 1,
        }
    )
    request = Request(f"{endpoint}?{query}", headers={"User-Agent": "weather-forecast-monitoring-v1"})
    with urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    provider_locations = _location_response(payload)
    if len(provider_locations) != len(location_list):
        raise ValueError(f"provider returned {len(provider_locations)} locations; expected {len(location_list)}")

    timestamp = target.strftime("%Y-%m-%dT%H:00:00Z")
    ingestion_time = current.strftime("%Y-%m-%dT%H:%M:%SZ")
    events: list[dict[str, Any]] = []
    for location, provider in zip(location_list, provider_locations, strict=True):
        provider_lat = float(provider.get("latitude"))
        provider_lon = float(provider.get("longitude"))
        distance = _haversine_km(float(location["latitude"]), float(location["longitude"]), provider_lat, provider_lon)
        if distance > 15.0:
            raise ValueError(f"provider grid for {location['location_id']} is {distance:.2f} km from canonical location")
        index = _time_index(provider, target)
        hourly = provider["hourly"]
        series = {name: hourly.get(name) for name in HOURLY_VARIABLES}
        if any(not isinstance(column, list) or index >= len(column) for column in series.values()):
            raise ValueError(f"provider hourly arrays are incomplete for {location['location_id']}")
        values = {name: column[index] for name, column in series.items()}
        if values["temperature_2m"] is None or not math.isfinite(float(values["temperature_2m"])):
            raise ValueError(f"provider temperature is missing or non-finite for {location['location_id']} at {timestamp}")
        normalized = {
            "temperature_c": float(values["temperature_2m"]),
            "humidity_pct": None if values["relative_humidity_2m"] is None else float(values["relative_humidity_2m"]),
            "precipitation_mm": None if values["precipitation"] is None else float(values["precipitation"]),
            "pressure_hpa": None if values["pressure_msl"] is None else float(values["pressure_msl"]),
            "wind_speed_kmh": None if values["wind_speed_10m"] is None else float(values["wind_speed_10m"]),
            "wind_gust_kmh": None if values["wind_gusts_10m"] is None else float(values["wind_gusts_10m"]),
            "weather_code": None if values["weather_code"] is None else int(values["weather_code"]),
        }
        for field in EVENT_FIELDS:
            value = normalized[field]
            if value is not None and not math.isfinite(float(value)):
                raise ValueError(f"provider returned non-finite {field} for {location['location_id']}")
        location_id = str(location["location_id"])
        events.append(
            {
                "event_id": f"{LIVE_REFERENCE_SOURCE}|{location_id}|{timestamp}",
                "event_type": "WEATHER_HOURLY",
                "location_id": location_id,
                "city": str(location.get("name") or location.get("province_name") or ""),
                "latitude": provider_lat,
                "longitude": provider_lon,
                "event_time": timestamp,
                "ingestion_time": ingestion_time,
                **normalized,
                "source": LIVE_REFERENCE_SOURCE,
                "reference_retrieval_mode": "LIVE_SOURCE_BACKFILL",
            }
        )
    if len(events) != 63 or len({event["location_id"] for event in events}) != 63:
        raise ValueError("live source backfill did not produce a complete 63-location hour")
    return events
