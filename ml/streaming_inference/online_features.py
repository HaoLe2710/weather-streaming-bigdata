from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .contract import FeatureContract, load_feature_contract


TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
HISTORY_HOURS = 24
WINDOW_SIZE = HISTORY_HOURS + 1
WEATHER_COLUMNS = (
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "pressure_hpa",
    "wind_speed_kmh",
    "wind_gust_kmh",
)
LAG_SPECS = {
    "temperature_c": ("temp", (1, 3, 6, 12, 24)),
    "humidity_pct": ("humidity", (1, 3, 6, 12, 24)),
    "pressure_hpa": ("pressure", (1, 3, 6, 12, 24)),
    "precipitation_mm": ("precipitation", (1, 3, 6, 24)),
    "wind_speed_kmh": ("wind_speed", (1, 3, 6, 24)),
}
ROLLING_SPECS = {
    "temperature_c": ("temp", ("mean", "std")),
    "humidity_pct": ("humidity", ("mean", "std")),
    "pressure_hpa": ("pressure", ("mean", "std")),
    "precipitation_mm": ("precipitation", ("sum",)),
    "wind_speed_kmh": ("wind_speed", ("mean",)),
}
ROLLING_HOURS = (3, 6, 24)
DELTA_SPECS = (
    ("temperature_c", "temp", (1, 3)),
    ("humidity_pct", "humidity", (1, 3)),
    ("pressure_hpa", "pressure", (1, 3, 6)),
    ("wind_speed_kmh", "wind_speed", (1,)),
)


@dataclass(frozen=True)
class FeatureBuild:
    location_id: str
    feature_time: datetime
    status: str
    feature_names: tuple[str, ...] = ()
    values: tuple[float, ...] = ()
    reason: str | None = None

    @property
    def ready(self) -> bool:
        return self.status == "READY"


def parse_utc_hour(value: Any) -> datetime:
    if isinstance(value, datetime):
        instant = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            instant = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"invalid event_time {value!r}") from exc
    else:
        raise ValueError(f"event_time must be an ISO timestamp, got {type(value).__name__}")
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("event_time must be timezone-aware UTC")
    instant = instant.astimezone(timezone.utc)
    if instant.minute or instant.second or instant.microsecond:
        raise ValueError(f"event_time must be exactly on a UTC hour: {instant.isoformat()}")
    return instant


def _rolling_name(prefix: str, operation: str, hours: int) -> str:
    if prefix == "precipitation" and operation == "sum":
        return f"precipitation_sum_{hours}h"
    suffix = "std" if operation == "std" else operation
    return f"{prefix}_roll_{suffix}_{hours}h"


def _same_observation(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    columns = ("location_id", "latitude", "longitude", *WEATHER_COLUMNS)
    return all(left.get(name) == right.get(name) for name in columns)


def _normalize_observations(
    observations: Iterable[Mapping[str, Any]],
) -> tuple[list[tuple[datetime, Mapping[str, Any]]], set[datetime]]:
    by_time: dict[datetime, Mapping[str, Any]] = {}
    conflicts: set[datetime] = set()
    for row in observations:
        if not isinstance(row, Mapping):
            continue
        try:
            event_time = parse_utc_hour(row.get("event_time"))
        except ValueError:
            continue
        previous = by_time.get(event_time)
        if previous is None:
            by_time[event_time] = row
        elif not _same_observation(previous, row):
            conflicts.add(event_time)
    return sorted(by_time.items()), conflicts


def _finite_number(row: Mapping[str, Any], name: str) -> float | None:
    value = row.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _feature_values(history: Sequence[Mapping[str, Any]], event_time: datetime) -> dict[str, float] | None:
    current = history[-1]
    values: dict[str, float] = {}
    for name in WEATHER_COLUMNS + ("latitude", "longitude"):
        number = _finite_number(current, name)
        if number is None:
            return None
        values[name] = number

    local = event_time.astimezone(TIMEZONE)
    local_day_of_week = local.weekday()  # Monday=0, matching the offline Spark expression.
    local_day_of_year = local.timetuple().tm_yday
    values.update(
        {
            "local_hour": float(local.hour),
            "local_day_of_week": float(local_day_of_week),
            "local_month": float(local.month),
            "local_day_of_year": float(local_day_of_year),
            "hour_sin": math.sin(2.0 * math.pi * local.hour / 24.0),
            "hour_cos": math.cos(2.0 * math.pi * local.hour / 24.0),
            "day_of_week_sin": math.sin(2.0 * math.pi * local_day_of_week / 7.0),
            "day_of_week_cos": math.cos(2.0 * math.pi * local_day_of_week / 7.0),
            "day_of_year_sin": math.sin(2.0 * math.pi * (local_day_of_year - 1) / 365.25),
            "day_of_year_cos": math.cos(2.0 * math.pi * (local_day_of_year - 1) / 365.25),
        }
    )

    for source, (prefix, hours_list) in LAG_SPECS.items():
        for hours in hours_list:
            number = _finite_number(history[-hours - 1], source)
            if number is None:
                return None
            values[f"{prefix}_lag_{hours}h"] = number

    for source, (prefix, operations) in ROLLING_SPECS.items():
        for hours in ROLLING_HOURS:
            sample = [_finite_number(row, source) for row in history[-hours:]]
            if any(number is None for number in sample):
                return None
            numeric = [float(number) for number in sample if number is not None]
            for operation in operations:
                if operation == "mean":
                    result = math.fsum(numeric) / hours
                elif operation == "std":
                    mean = math.fsum(numeric) / hours
                    result = math.sqrt(math.fsum((number - mean) ** 2 for number in numeric) / hours)
                else:
                    result = math.fsum(numeric)
                values[_rolling_name(prefix, operation, hours)] = result

    for source, prefix, hours_list in DELTA_SPECS:
        current_value = _finite_number(current, source)
        if current_value is None:
            return None
        for hours in hours_list:
            lagged = _finite_number(history[-hours - 1], source)
            if lagged is None:
                return None
            values[f"{prefix}_delta_{hours}h"] = current_value - lagged

    return values


def build_online_feature_series(
    observations: Iterable[Mapping[str, Any]],
    *,
    feature_names: Sequence[str] | None = None,
    contract: FeatureContract | None = None,
) -> list[FeatureBuild]:
    """Build one online result per UTC event hour using only observations at or before it."""
    selected_contract = contract or load_feature_contract()
    ordered_names = tuple(feature_names) if feature_names is not None else selected_contract.feature_names
    if ordered_names != selected_contract.feature_names:
        raise ValueError("online feature order must exactly match canonical model_feature_list.json")

    ordered, conflicts = _normalize_observations(observations)
    if not ordered:
        return []
    first_location = str(ordered[0][1].get("location_id", ""))
    if any(str(row.get("location_id", "")) != first_location for _, row in ordered):
        raise ValueError("online feature history must contain a single location_id")

    results: list[FeatureBuild] = []
    rolling_rows: list[tuple[datetime, Mapping[str, Any]]] = []
    for index, (event_time, row) in enumerate(ordered):
        rolling_rows.append((event_time, row))
        if event_time in conflicts:
            results.append(FeatureBuild(first_location, event_time, "DUPLICATE_CONFLICT", reason="different payloads share a location-hour key"))
            continue
        if index < HISTORY_HOURS:
            results.append(FeatureBuild(first_location, event_time, "INSUFFICIENT_HISTORY", reason="need 24 prior hourly observations plus current"))
            continue

        history_rows = rolling_rows[index - HISTORY_HOURS:index + 1]
        expected_times = [event_time - timedelta(hours=offset) for offset in range(HISTORY_HOURS, -1, -1)]
        if [item[0] for item in history_rows] != expected_times:
            results.append(FeatureBuild(first_location, event_time, "HISTORY_GAP", reason="required 25 hourly timestamps are not contiguous"))
            continue
        if any(item[0] in conflicts for item in history_rows):
            results.append(FeatureBuild(first_location, event_time, "DUPLICATE_CONFLICT", reason="conflicting duplicate falls inside required history"))
            continue

        history = [item[1] for item in history_rows]
        computed = _feature_values(history, event_time)
        if computed is None:
            results.append(FeatureBuild(first_location, event_time, "INVALID_FEATURES", reason="null, non-numeric, or non-finite model input"))
            continue
        if set(computed) != set(ordered_names):
            missing = sorted(set(ordered_names) - set(computed))
            extra = sorted(set(computed) - set(ordered_names))
            raise ValueError(f"online feature definitions differ from frozen contract: missing={missing}, extra={extra}")
        ordered_values = tuple(computed[name] for name in ordered_names)
        if not all(math.isfinite(value) for value in ordered_values):
            results.append(FeatureBuild(first_location, event_time, "INVALID_FEATURES", reason="non-finite computed model input"))
            continue
        results.append(FeatureBuild(first_location, event_time, "READY", ordered_names, ordered_values))
    return results


def build_online_feature_vector(
    observations: Iterable[Mapping[str, Any]],
    feature_time: datetime | str,
    *,
    feature_names: Sequence[str] | None = None,
    contract: FeatureContract | None = None,
) -> FeatureBuild:
    requested = parse_utc_hour(feature_time)
    rows = list(observations)
    series = build_online_feature_series(rows, feature_names=feature_names, contract=contract)
    for result in series:
        if result.feature_time == requested:
            return result
    location_id = next((str(row.get("location_id", "")) for row in rows if isinstance(row, Mapping)), "")
    return FeatureBuild(location_id, requested, "CURRENT_OBSERVATION_MISSING", reason="no observation exists at feature_time")


def deterministic_forecast_id(location_id: str, feature_time: datetime | str) -> str:
    instant = parse_utc_hour(feature_time)
    timestamp = instant.strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = f"WEATHER_XGBOOST_GLOBAL_T1H_V1|{location_id}|{timestamp}|PT1H".encode("utf-8")
    import hashlib

    return hashlib.sha256(payload).hexdigest()


def make_forecast_record(
    *,
    location_id: str,
    feature_time: datetime | str,
    prediction_temperature_c: float,
    source_event_id: str | None,
    inference_time: datetime | None = None,
) -> dict[str, Any]:
    feature_instant = parse_utc_hour(feature_time)
    target_time = feature_instant + timedelta(hours=1)
    prediction = float(prediction_temperature_c)
    if not math.isfinite(prediction):
        raise ValueError("forecast prediction must be finite")
    inferred = inference_time or datetime.now(timezone.utc)
    if inferred.tzinfo is None or inferred.utcoffset() is None:
        raise ValueError("inference_time must be timezone-aware")
    from .contract import FEATURE_LIST_SHA256, FEATURE_SET_ID, MODEL_ID, MODEL_SHA256

    return {
        "forecast_id": deterministic_forecast_id(location_id, feature_instant),
        "location_id": location_id,
        "feature_time": feature_instant,
        "target_time": target_time,
        "prediction_temperature_c": prediction,
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "source_event_id": source_event_id,
        "inference_time": inferred.astimezone(timezone.utc),
    }
