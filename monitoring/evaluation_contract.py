from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from typing import Any, Mapping

from ml.streaming_inference.contract import (
    FEATURE_LIST_SHA256,
    FEATURE_SET_ID,
    MODEL_ID,
    MODEL_SHA256,
)
from ml.streaming_inference.online_features import deterministic_forecast_id


EVALUATION_VERSION = "FORECAST_EVALUATION_V1"
LIVE_REFERENCE_SOURCE = "OPEN_METEO_LIVE_HOURLY"
SUPPORTED_MODEL_ID = MODEL_ID
SUPPORTED_MODEL_SHA256 = MODEL_SHA256
SUPPORTED_FEATURE_SET_ID = FEATURE_SET_ID
SUPPORTED_FEATURE_LIST_SHA256 = FEATURE_LIST_SHA256
FORECAST_HORIZON = timedelta(hours=1)

WEATHER_PAYLOAD_FIELDS = (
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "pressure_hpa",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "weather_code",
    "latitude",
    "longitude",
)

EVALUATION_STATUSES = frozenset(
    {
        "PENDING_TARGET_TIME",
        "PENDING_REFERENCE",
        "PENDING_BASELINE",
        "READY",
        "EVALUATED",
        "REFERENCE_CONFLICT",
        "INVALID_PROVENANCE",
    }
)


def parse_utc_timestamp(value: Any, *, assume_naive_utc: bool = False) -> datetime:
    """Parse an aware timestamp and normalize it to UTC.

    Spark returns naive Python datetimes for TimestampType. Callers may accept
    those only when the Spark session timezone is explicitly set to UTC.
    """

    if isinstance(value, datetime):
        instant = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            instant = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"invalid UTC timestamp {value!r}") from exc
    else:
        raise ValueError(f"timestamp must be an ISO string or datetime, got {type(value).__name__}")
    if instant.tzinfo is None or instant.utcoffset() is None:
        if not assume_naive_utc:
            raise ValueError("timestamp must be timezone-aware UTC")
        instant = instant.replace(tzinfo=timezone.utc)
    return instant.astimezone(timezone.utc)


def parse_utc_hour(value: Any, *, assume_naive_utc: bool = False) -> datetime:
    instant = parse_utc_timestamp(value, assume_naive_utc=assume_naive_utc)
    if instant.minute or instant.second or instant.microsecond:
        raise ValueError(f"timestamp must be exactly on a UTC hour: {instant.isoformat()}")
    return instant


def format_utc_hour(value: Any, *, assume_naive_utc: bool = False) -> str:
    return parse_utc_hour(value, assume_naive_utc=assume_naive_utc).strftime("%Y-%m-%dT%H:00:00Z")


def finite_number(value: Any, *, field: str, required: bool = True) -> float | None:
    if value is None:
        if required:
            raise ValueError(f"{field} is required")
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    return number


def canonical_payload(record: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for field in WEATHER_PAYLOAD_FIELDS:
        value = record.get(field)
        if field == "weather_code":
            if value is None:
                payload[field] = None
            else:
                parsed = finite_number(value, field=field)
                if parsed is None or not parsed.is_integer():
                    raise ValueError("weather_code must be an integer")
                payload[field] = int(parsed)
        else:
            payload[field] = finite_number(
                value,
                field=field,
                required=field == "temperature_c",
            )
    return payload


def payload_sha256(record: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        canonical_payload(record),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def stable_id(*parts: Any) -> str:
    canonical = "\0".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def observation_key(source: str, location_id: str, event_time: Any, *, assume_naive_utc: bool = False) -> tuple[str, str, str]:
    return (
        str(source),
        str(location_id),
        format_utc_hour(event_time, assume_naive_utc=assume_naive_utc),
    )


def evaluation_id(forecast_id: str, reference_source: str | None) -> str:
    return stable_id(EVALUATION_VERSION, forecast_id, reference_source or "UNRESOLVED_SOURCE")


def reference_revision_id(
    source: str,
    location_id: str,
    event_time: Any,
    old_hash: str,
    new_hash: str,
    *,
    assume_naive_utc: bool = False,
) -> str:
    return stable_id(
        "REFERENCE_REVISION_DETECTED",
        *observation_key(source, location_id, event_time, assume_naive_utc=assume_naive_utc),
        old_hash,
        new_hash,
    )


def forecast_validation_errors(
    forecast: Mapping[str, Any],
    known_location_ids: set[str] | frozenset[str],
    *,
    assume_naive_utc: bool = False,
) -> list[str]:
    errors: list[str] = []
    location_id = str(forecast.get("location_id") or "")
    if not location_id or location_id not in known_location_ids:
        errors.append("UNKNOWN_LOCATION")
    if forecast.get("model_id") != SUPPORTED_MODEL_ID:
        errors.append("UNSUPPORTED_MODEL_ID")
    if forecast.get("model_sha256") != SUPPORTED_MODEL_SHA256:
        errors.append("UNSUPPORTED_MODEL_SHA256")
    if forecast.get("feature_set_id") != SUPPORTED_FEATURE_SET_ID:
        errors.append("UNSUPPORTED_FEATURE_SET_ID")
    if forecast.get("feature_list_sha256") != SUPPORTED_FEATURE_LIST_SHA256:
        errors.append("UNSUPPORTED_FEATURE_LIST_SHA256")
    if not forecast.get("forecast_id"):
        errors.append("MISSING_FORECAST_ID")
    try:
        feature_time = parse_utc_hour(forecast.get("feature_time"), assume_naive_utc=assume_naive_utc)
        target_time = parse_utc_hour(forecast.get("target_time"), assume_naive_utc=assume_naive_utc)
        if target_time != feature_time + FORECAST_HORIZON:
            errors.append("INVALID_FORECAST_HORIZON")
        if location_id and forecast.get("forecast_id"):
            expected_id = deterministic_forecast_id(location_id, feature_time)
            if forecast.get("forecast_id") != expected_id:
                errors.append("INVALID_FORECAST_ID")
    except (TypeError, ValueError):
        errors.append("INVALID_FORECAST_TIME")
    try:
        finite_number(forecast.get("prediction_temperature_c"), field="prediction_temperature_c")
    except ValueError:
        errors.append("INVALID_PREDICTION")
    try:
        parse_utc_timestamp(forecast.get("inference_time"), assume_naive_utc=assume_naive_utc)
    except (TypeError, ValueError):
        errors.append("INVALID_INFERENCE_TIME")
    return errors


def observation_validation_errors(
    observation: Mapping[str, Any],
    known_location_ids: set[str] | frozenset[str],
    *,
    assume_naive_utc: bool = False,
) -> list[str]:
    errors: list[str] = []
    source = str(observation.get("source") or "")
    location_id = str(observation.get("location_id") or "")
    if not source:
        errors.append("MISSING_SOURCE")
    if not location_id or location_id not in known_location_ids:
        errors.append("UNKNOWN_LOCATION")
    if not observation.get("event_id"):
        errors.append("MISSING_EVENT_ID")
    try:
        parse_utc_hour(observation.get("event_time"), assume_naive_utc=assume_naive_utc)
    except (TypeError, ValueError):
        errors.append("INVALID_EVENT_TIME")
    try:
        canonical_payload(observation)
    except ValueError as exc:
        errors.append(f"INVALID_WEATHER_PAYLOAD:{exc}")
    return errors
