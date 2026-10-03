from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import math
import os
from typing import Any, Mapping

from .online_features import parse_utc_hour
from .t2h_contract import (
    FEATURE_COUNT,
    FEATURE_LIST_SHA256,
    FEATURE_SET_ID,
    FORECAST_HORIZON_HOURS,
    HISTORICAL_FORECAST_ENDPOINT,
    LIVE_SOURCE,
    LIVE_ENDPOINT,
    MODEL_ID,
    MODEL_SHA256,
    PROVIDER_MODEL,
    PROVIDER_NAME,
    REPLAY_SOURCE,
)


EXECUTION_ORIGINS = frozenset({"LIVE_PROSPECTIVE", "REPLAY_VALIDATION", "BACKFILL"})


@dataclass(frozen=True)
class T2HForecastBuild:
    status: str
    forecast: dict[str, Any] | None
    forecast_lead_seconds: float
    reason: str | None = None


def _utc_timestamp(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        instant = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            instant = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO timestamp") from exc
    else:
        raise ValueError(f"{field_name} must be a timezone-aware timestamp")
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return instant.astimezone(timezone.utc)


def deterministic_t2h_forecast_id(
    location_id: str,
    feature_time: datetime | str,
    target_time: datetime | str | None = None,
) -> str:
    feature_instant = parse_utc_hour(feature_time)
    target_instant = target_time or (feature_instant + timedelta(hours=FORECAST_HORIZON_HOURS))
    target = parse_utc_hour(target_instant)
    if (target - feature_instant).total_seconds() != 7200:
        raise ValueError("T2H forecast ID requires target_time exactly 7200 seconds after feature_time")
    parts = (
        MODEL_ID,
        str(location_id),
        feature_instant.strftime("%Y-%m-%dT%H:%M:%SZ"),
        target.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _minimum_useful_lead() -> int | None:
    raw = os.getenv("WEATHER_T2H_MINIMUM_USEFUL_LEAD_SECONDS")
    if raw is None or raw.strip() == "":
        return None
    value = int(raw)
    if value < 0:
        raise ValueError("WEATHER_T2H_MINIMUM_USEFUL_LEAD_SECONDS cannot be negative")
    return value


def build_t2h_forecast_record(
    *,
    location_id: str,
    feature_time: datetime | str,
    prediction_temperature_c: float,
    source_event_id: str | None,
    inference_time: datetime,
    provider: str | None,
    provider_endpoint: str | None,
    provider_model: str | None,
    source_retrieved_at: datetime | str | None,
    source: str | None,
    execution_origin: str,
) -> T2HForecastBuild:
    feature_instant = parse_utc_hour(feature_time)
    inferred = _utc_timestamp(inference_time, field_name="inference_time")
    target_time = feature_instant + timedelta(hours=FORECAST_HORIZON_HOURS)
    if (target_time - feature_instant).total_seconds() != 7200:
        raise AssertionError("T2H target timestamp construction drifted from exactly +2h")
    prediction = float(prediction_temperature_c)
    if not math.isfinite(prediction):
        raise ValueError("T2H forecast prediction must be finite")
    if execution_origin not in EXECUTION_ORIGINS:
        raise ValueError(f"unsupported T2H execution_origin: {execution_origin!r}")
    if provider != PROVIDER_NAME or provider_model != PROVIDER_MODEL:
        raise ValueError("T2H forecasts require Open-Meteo provider_model=ecmwf_ifs")
    if not provider_endpoint:
        raise ValueError("T2H forecast provider_endpoint is required")
    if not source_event_id:
        raise ValueError("T2H forecast source_event_id is required")
    retrieved = _utc_timestamp(source_retrieved_at, field_name="source_retrieved_at")
    if retrieved > inferred:
        raise ValueError("T2H source retrieval timestamp cannot be after inference_time")

    lead_seconds = (target_time - inferred).total_seconds()
    if execution_origin == "LIVE_PROSPECTIVE":
        if source != LIVE_SOURCE:
            raise ValueError("LIVE_PROSPECTIVE requires the canonical live hourly source")
        if provider_endpoint != LIVE_ENDPOINT:
            raise ValueError("LIVE_PROSPECTIVE requires the Open-Meteo Forecast API endpoint")
        safe_hour = inferred.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        if feature_instant > safe_hour:
            raise ValueError("LIVE_PROSPECTIVE feature_time is newer than the latest completed safe hour")
        if target_time <= inferred:
            return T2HForecastBuild(
                status="NON_PROSPECTIVE_SKIPPED",
                forecast=None,
                forecast_lead_seconds=lead_seconds,
                reason="target_time is not after inference_time",
            )
    elif execution_origin == "REPLAY_VALIDATION":
        if source != REPLAY_SOURCE or provider_endpoint != HISTORICAL_FORECAST_ENDPOINT:
            raise ValueError("REPLAY_VALIDATION requires the canonical Open-Meteo Historical Forecast source")

    useful_lead = _minimum_useful_lead()
    record = {
        "forecast_id": deterministic_t2h_forecast_id(location_id, feature_instant, target_time),
        "location_id": str(location_id),
        "feature_time": feature_instant,
        "source_timestamp": feature_instant,
        "target_time": target_time,
        "inference_time": inferred,
        "forecast_lead_seconds": lead_seconds,
        "prediction_temperature_c": prediction,
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "feature_count": FEATURE_COUNT,
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
        "provider": PROVIDER_NAME,
        "provider_endpoint": str(provider_endpoint),
        "provider_model": PROVIDER_MODEL,
        "source_retrieved_at": retrieved,
        "source": str(source),
        "source_event_id": str(source_event_id),
        "execution_origin": execution_origin,
        "minimum_useful_lead_seconds": useful_lead,
        "minimum_useful_lead_met": None if useful_lead is None else lead_seconds >= useful_lead,
    }
    return T2HForecastBuild("READY", record, lead_seconds)
