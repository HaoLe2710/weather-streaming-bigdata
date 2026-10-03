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
from ml.streaming_inference.t2h_contract import (
    FEATURE_COUNT as T2H_FEATURE_COUNT,
    FEATURE_LIST_SHA256 as T2H_FEATURE_LIST_SHA256,
    FEATURE_SET_ID as T2H_FEATURE_SET_ID,
    FORECAST_HORIZON_HOURS as T2H_FORECAST_HORIZON_HOURS,
    HISTORICAL_FORECAST_ENDPOINT as T2H_HISTORICAL_ENDPOINT,
    LIVE_ENDPOINT as T2H_LIVE_ENDPOINT,
    LIVE_SOURCE as T2H_LIVE_SOURCE,
    MODEL_ID as T2H_MODEL_ID,
    MODEL_SHA256 as T2H_MODEL_SHA256,
    PROVIDER_MODEL as T2H_PROVIDER_MODEL,
    PROVIDER_NAME as T2H_PROVIDER_NAME,
    REPLAY_SOURCE as T2H_REPLAY_SOURCE,
)
from ml.streaming_inference.t2h_runtime import deterministic_t2h_forecast_id


EVALUATION_VERSION = "FORECAST_EVALUATION_V1"
LIVE_REFERENCE_SOURCE = "OPEN_METEO_LIVE_HOURLY"
SUPPORTED_MODEL_ID = MODEL_ID
SUPPORTED_MODEL_SHA256 = MODEL_SHA256
SUPPORTED_FEATURE_SET_ID = FEATURE_SET_ID
SUPPORTED_FEATURE_LIST_SHA256 = FEATURE_LIST_SHA256
FORECAST_HORIZON = timedelta(hours=1)
T2H_FORECAST_HORIZON = timedelta(hours=T2H_FORECAST_HORIZON_HOURS)
SUPPORTED_FORECAST_CONTRACTS = {
    SUPPORTED_MODEL_ID: {
        "model_sha256": SUPPORTED_MODEL_SHA256,
        "feature_set_id": SUPPORTED_FEATURE_SET_ID,
        "feature_list_sha256": SUPPORTED_FEATURE_LIST_SHA256,
        "horizon": FORECAST_HORIZON,
    },
    T2H_MODEL_ID: {
        "model_sha256": T2H_MODEL_SHA256,
        "feature_set_id": T2H_FEATURE_SET_ID,
        "feature_list_sha256": T2H_FEATURE_LIST_SHA256,
        "horizon": T2H_FORECAST_HORIZON,
    },
}

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
    model_id = forecast.get("model_id")
    model_contract = SUPPORTED_FORECAST_CONTRACTS.get(model_id)
    if model_contract is None:
        errors.append("UNSUPPORTED_MODEL_ID")
    elif forecast.get("model_sha256") != model_contract["model_sha256"]:
        errors.append("UNSUPPORTED_MODEL_SHA256")
    if model_contract is not None and forecast.get("feature_set_id") != model_contract["feature_set_id"]:
        errors.append("UNSUPPORTED_FEATURE_SET_ID")
    if model_contract is not None and forecast.get("feature_list_sha256") != model_contract["feature_list_sha256"]:
        errors.append("UNSUPPORTED_FEATURE_LIST_SHA256")
    if not forecast.get("forecast_id"):
        errors.append("MISSING_FORECAST_ID")
    try:
        feature_time = parse_utc_hour(forecast.get("feature_time"), assume_naive_utc=assume_naive_utc)
        target_time = parse_utc_hour(forecast.get("target_time"), assume_naive_utc=assume_naive_utc)
        expected_horizon = model_contract["horizon"] if model_contract is not None else FORECAST_HORIZON
        if target_time != feature_time + expected_horizon:
            errors.append("INVALID_FORECAST_HORIZON")
        if location_id and forecast.get("forecast_id"):
            if model_id == T2H_MODEL_ID:
                expected_id = deterministic_t2h_forecast_id(location_id, feature_time, target_time)
            else:
                expected_id = deterministic_forecast_id(location_id, feature_time)
            if forecast.get("forecast_id") != expected_id:
                errors.append("INVALID_FORECAST_ID")
        if model_id == T2H_MODEL_ID:
            if forecast.get("feature_count") != T2H_FEATURE_COUNT:
                errors.append("INVALID_FEATURE_COUNT")
            if forecast.get("forecast_horizon_hours") != T2H_FORECAST_HORIZON_HOURS:
                errors.append("INVALID_FORECAST_HORIZON_HOURS")
            origin = forecast.get("execution_origin")
            if origin not in {"LIVE_PROSPECTIVE", "REPLAY_VALIDATION", "BACKFILL"}:
                errors.append("INVALID_EXECUTION_ORIGIN")
            if forecast.get("provider") != T2H_PROVIDER_NAME or forecast.get("provider_model") != T2H_PROVIDER_MODEL:
                errors.append("INVALID_T2H_PROVIDER")
            if not forecast.get("provider_endpoint") or not forecast.get("source_event_id"):
                errors.append("INVALID_T2H_PROVENANCE")
            try:
                inference_time = parse_utc_timestamp(forecast.get("inference_time"), assume_naive_utc=assume_naive_utc)
                lead_seconds = finite_number(forecast.get("forecast_lead_seconds"), field="forecast_lead_seconds")
                if lead_seconds != (target_time - inference_time).total_seconds():
                    errors.append("INVALID_FORECAST_LEAD")
                retrieval_time = parse_utc_timestamp(
                    forecast.get("source_retrieved_at"), assume_naive_utc=assume_naive_utc
                )
                if retrieval_time > inference_time:
                    errors.append("INVALID_SOURCE_RETRIEVAL_TIME")
                if origin == "LIVE_PROSPECTIVE":
                    if forecast.get("source") != T2H_LIVE_SOURCE or forecast.get("provider_endpoint") != T2H_LIVE_ENDPOINT:
                        errors.append("INVALID_LIVE_SOURCE")
                    if target_time <= inference_time or lead_seconds <= 0:
                        errors.append("NON_POSITIVE_LIVE_FORECAST_LEAD")
                    safe_hour = inference_time.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
                    if feature_time > safe_hour:
                        errors.append("INVALID_COMPLETED_SAFE_HOUR")
                elif origin == "REPLAY_VALIDATION":
                    if (
                        forecast.get("source") != T2H_REPLAY_SOURCE
                        or forecast.get("provider_endpoint") != T2H_HISTORICAL_ENDPOINT
                    ):
                        errors.append("INVALID_REPLAY_SOURCE")
            except (TypeError, ValueError):
                errors.append("INVALID_FORECAST_LEAD")
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
