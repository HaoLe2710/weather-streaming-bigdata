"""Small UTC helpers for checking live forecast timing contracts."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _require_hour_aligned(value: datetime, field_name: str) -> datetime:
    instant = _as_utc(value, field_name)
    if instant.minute or instant.second or instant.microsecond:
        raise ValueError(f"{field_name} must be aligned to a UTC hour")
    return instant


def current_utc_hour(now: datetime) -> datetime:
    """Return the UTC hour containing ``now``."""
    instant = _as_utc(now, "now")
    return instant.replace(minute=0, second=0, microsecond=0)


def last_completed_utc_hour(now: datetime) -> datetime:
    """Return the most recent UTC hour boundary strictly before the current hour."""
    return current_utc_hour(now) - timedelta(hours=1)


def forecast_target_time(feature_time: datetime, horizon_hours: int) -> datetime:
    """Calculate the truthful target timestamp for an hourly model horizon."""
    feature = _require_hour_aligned(feature_time, "feature_time")
    if isinstance(horizon_hours, bool) or not isinstance(horizon_hours, int) or horizon_hours <= 0:
        raise ValueError("horizon_hours must be a positive integer")
    return feature + timedelta(hours=horizon_hours)


def forecast_lead_seconds(target_time: datetime, forecast_created_at: datetime) -> float:
    """Return target minus creation time; positive means the target is still future."""
    target = _require_hour_aligned(target_time, "target_time")
    created = _as_utc(forecast_created_at, "forecast_created_at")
    return (target - created).total_seconds()


def is_prospective_forecast(target_time: datetime, forecast_created_at: datetime) -> bool:
    """A forecast is prospective only when its target is strictly in the future."""
    return forecast_lead_seconds(target_time, forecast_created_at) > 0
