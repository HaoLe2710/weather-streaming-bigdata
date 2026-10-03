from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Iterable, Mapping

from .evaluation_contract import (
    EVALUATION_VERSION,
    LIVE_REFERENCE_SOURCE,
    evaluation_id,
    finite_number,
    format_utc_hour,
    forecast_validation_errors,
    observation_key,
    parse_utc_hour,
    parse_utc_timestamp,
    payload_sha256,
    stable_id,
)


def _spark_time(value: Any) -> str:
    return format_utc_hour(value, assume_naive_utc=True)


def _forecast_signature(row: Mapping[str, Any]) -> str:
    return json.dumps(dict(row), sort_keys=True, default=str, separators=(",", ":"))


def _base_evaluation(
    forecast: Mapping[str, Any],
    *,
    evaluation_time: datetime,
    status: str,
    reason: str | None = None,
    source: str | None = None,
    cohort_id: str | None = None,
) -> dict[str, Any]:
    forecast_id = str(forecast.get("forecast_id") or "")

    def safe_hour(value: Any) -> datetime | None:
        try:
            return parse_utc_hour(value, assume_naive_utc=True)
        except (TypeError, ValueError):
            return None

    def safe_timestamp(value: Any) -> datetime | None:
        try:
            return parse_utc_timestamp(value, assume_naive_utc=True)
        except (TypeError, ValueError):
            return None

    try:
        model_prediction = finite_number(forecast.get("prediction_temperature_c"), field="prediction_temperature_c", required=False)
    except ValueError:
        model_prediction = None
    return {
        "evaluation_id": evaluation_id(forecast_id or "MISSING_FORECAST_ID", source),
        "evaluation_version": EVALUATION_VERSION,
        "forecast_id": forecast_id,
        "cohort_id": cohort_id,
        "location_id": forecast.get("location_id"),
        "feature_time": safe_hour(forecast.get("feature_time")),
        "target_time": safe_hour(forecast.get("target_time")),
        "model_id": forecast.get("model_id"),
        "model_sha256": forecast.get("model_sha256"),
        "feature_set_id": forecast.get("feature_set_id"),
        "feature_list_sha256": forecast.get("feature_list_sha256"),
        "model_prediction_temperature_c": model_prediction,
        "persistence_prediction_temperature_c": None,
        "reference_temperature_c": None,
        "humidity_pct": None,
        "precipitation_mm": None,
        "pressure_hpa": None,
        "wind_speed_kmh": None,
        "wind_gust_kmh": None,
        "weather_code": None,
        "model_error_c": None,
        "model_absolute_error_c": None,
        "model_squared_error_c2": None,
        "persistence_error_c": None,
        "persistence_absolute_error_c": None,
        "persistence_squared_error_c2": None,
        "feature_reference_event_id": None,
        "feature_reference_payload_sha256": None,
        "target_reference_event_id": None,
        "reference_source": source,
        "reference_payload_sha256": None,
        "target_reference_received": False,
        "target_time_passed": False,
        "forecast_inference_time": safe_timestamp(forecast.get("inference_time")),
        "reference_ingestion_time": None,
        "evaluation_time": evaluation_time,
        "evaluation_lag_seconds": None,
        "reference_arrival_lag_seconds": None,
        "evaluation_mode": "UNCLASSIFIED",
        "status": status,
        "invalid_reason": reason,
    }


def _select_feature_reference(
    candidates: list[Mapping[str, Any]],
    source_event_id: str | None,
) -> Mapping[str, Any] | None:
    # A timestamp/location-only fallback can bind a forecast to a different
    # source that happens to have the same key. Require the event identity that
    # the frozen inference record already carries.
    if not source_event_id:
        return None
    matching = [row for row in candidates if str(row.get("event_id")) == str(source_event_id)]
    if len(matching) == 1:
        return matching[0]
    if matching and len({(row.get("source"), row.get("location_id"), _spark_time(row.get("event_time"))) for row in matching}) == 1:
        return matching[0]
    return None


def _deduplicate_forecasts(forecasts: Iterable[Mapping[str, Any]]):
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    missing_ids: list[Mapping[str, Any]] = []
    conflicting: set[str] = set()
    duplicate_count = 0
    for forecast in forecasts:
        forecast_id = str(forecast.get("forecast_id") or "")
        if not forecast_id:
            missing_ids.append(forecast)
            continue
        grouped.setdefault(forecast_id, []).append(forecast)
    unique: dict[str, Mapping[str, Any]] = {}
    for forecast_id, candidates in sorted(grouped.items()):
        ordered = sorted(candidates, key=_forecast_signature)
        unique[forecast_id] = ordered[0]
        duplicate_count += len(candidates) - 1
        if len({_forecast_signature(row) for row in candidates}) > 1:
            conflicting.add(forecast_id)
    for forecast in sorted(missing_ids, key=_forecast_signature):
        identity = f"INVALID_FORECAST_{stable_id(_forecast_signature(forecast))}"
        unique[identity] = forecast
    return unique, conflicting, duplicate_count


def _evaluation_mode(source: str | None, retrieval_mode: Any) -> str:
    if source == LIVE_REFERENCE_SOURCE:
        return "LIVE_SOURCE_BACKFILL" if retrieval_mode == "LIVE_SOURCE_BACKFILL" else "LIVE_PROSPECTIVE"
    return "REPLAY" if source else "UNCLASSIFIED"


def evaluate_forecasts(
    forecasts: Iterable[Mapping[str, Any]],
    observations: Iterable[Mapping[str, Any]],
    *,
    evaluation_time: datetime,
    known_location_ids: set[str] | frozenset[str],
    conflicted_reference_keys: set[tuple[str, str, str]] | frozenset[tuple[str, str, str]] = frozenset(),
    existing_evaluations: Mapping[str, Mapping[str, Any]] | None = None,
    cohort_id: str | None = None,
    assume_naive_utc: bool = False,
) -> dict[str, Any]:
    """Create a current state row per forecast without rewriting completed rows."""

    if evaluation_time.tzinfo is None or evaluation_time.utcoffset() is None:
        if not assume_naive_utc:
            raise ValueError("evaluation_time must be timezone-aware UTC")
        evaluation_time = evaluation_time.replace(tzinfo=timezone.utc)
    evaluation_time = evaluation_time.astimezone(timezone.utc)

    by_key: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    by_location_time: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    invalid_observations: list[Mapping[str, Any]] = []
    duplicate_observation_count = 0
    dynamic_conflicts = set(conflicted_reference_keys)
    observation_groups: dict[tuple[str, str, str], list[Mapping[str, Any]]] = {}
    for row in observations:
        try:
            key = observation_key(
                str(row.get("source") or ""),
                str(row.get("location_id") or ""),
                row.get("event_time"),
                assume_naive_utc=assume_naive_utc,
            )
        except (TypeError, ValueError):
            invalid_observations.append(row)
            continue
        canonical_row = dict(row)
        try:
            canonical_row["reference_payload_sha256"] = str(
                row.get("reference_payload_sha256") or payload_sha256(row)
            )
        except ValueError:
            invalid_observations.append(row)
            continue
        observation_groups.setdefault(key, []).append(canonical_row)
    for key, candidates in observation_groups.items():
        hashes = {str(row.get("reference_payload_sha256") or "") for row in candidates}
        if len(hashes) > 1:
            # A corrupted archive with conflicting duplicates is not a safe
            # reference, even if the separate revision table missed the key.
            dynamic_conflicts.add(key)
            invalid_observations.extend(candidates)
            by_location_time.setdefault((key[1], key[2]), []).extend(candidates)
            continue
        duplicate_observation_count += len(candidates) - 1
        by_key[key] = candidates[0]
        by_location_time.setdefault((key[1], key[2]), []).append(candidates[0])

    unique_forecasts, conflicting_forecasts, duplicate_forecast_count = _deduplicate_forecasts(forecasts)
    prior = existing_evaluations or {}
    rows: list[dict[str, Any]] = []
    for forecast_id, forecast in unique_forecasts.items():
        existing = prior.get(forecast_id)
        if existing and existing.get("status") == "EVALUATED":
            rows.append(dict(existing))
            continue

        persisted_forecast = dict(forecast)
        if not persisted_forecast.get("forecast_id"):
            persisted_forecast["forecast_id"] = forecast_id
        target_time_passed = False
        try:
            parsed_target_time = parse_utc_hour(forecast.get("target_time"), assume_naive_utc=assume_naive_utc)
            target_time_passed = evaluation_time >= parsed_target_time
        except (TypeError, ValueError):
            pass
        errors = forecast_validation_errors(forecast, known_location_ids, assume_naive_utc=assume_naive_utc)
        if forecast_id in conflicting_forecasts:
            errors.append("CONFLICTING_FORECAST_ID")
        if errors:
            rows.append(
                _base_evaluation(
                    persisted_forecast,
                    evaluation_time=evaluation_time,
                    status="INVALID_PROVENANCE",
                    reason=";".join(sorted(set(errors))),
                    cohort_id=cohort_id,
                )
            )
            rows[-1]["target_time_passed"] = target_time_passed
            continue

        location_id = str(forecast["location_id"])
        feature_time = parse_utc_hour(forecast["feature_time"], assume_naive_utc=assume_naive_utc)
        target_time = parse_utc_hour(forecast["target_time"], assume_naive_utc=assume_naive_utc)
        persisted_forecast["target_time_passed"] = evaluation_time >= target_time
        feature_key_time = format_utc_hour(feature_time)
        target_key_time = format_utc_hour(target_time)
        candidates = by_location_time.get((location_id, feature_key_time), [])
        feature_reference = _select_feature_reference(candidates, forecast.get("source_event_id"))
        source = str(feature_reference["source"]) if feature_reference else None
        feature_retrieval_mode = feature_reference.get("reference_retrieval_mode") if feature_reference else None
        base = _base_evaluation(
            persisted_forecast,
            evaluation_time=evaluation_time,
            status="PENDING_BASELINE",
            source=source,
            cohort_id=cohort_id,
        )
        base["target_time_passed"] = evaluation_time >= target_time
        base["evaluation_mode"] = _evaluation_mode(source, feature_retrieval_mode)

        if not feature_reference:
            base["status"] = "PENDING_BASELINE"
            base["invalid_reason"] = (
                "SOURCE_EVENT_ID_MISSING"
                if not forecast.get("source_event_id")
                else "FEATURE_TIME_REFERENCE_MISSING_OR_AMBIGUOUS"
            )
            rows.append(base)
            continue

        feature_reference_key = observation_key(source, location_id, feature_time)
        if feature_reference_key in dynamic_conflicts:
            base["status"] = "REFERENCE_CONFLICT"
            base["invalid_reason"] = "FEATURE_TIME_REFERENCE_REVISION_DETECTED"
            base["feature_reference_event_id"] = feature_reference.get("event_id")
            rows.append(base)
            continue

        base["feature_reference_event_id"] = feature_reference.get("event_id")
        base["feature_reference_payload_sha256"] = feature_reference.get("reference_payload_sha256")
        base["persistence_prediction_temperature_c"] = finite_number(
            feature_reference.get("temperature_c"), field="feature_time temperature"
        )

        if evaluation_time < target_time:
            base["status"] = "PENDING_TARGET_TIME"
            base["invalid_reason"] = None
            rows.append(base)
            continue

        target_reference_key = observation_key(source, location_id, target_time)
        if target_reference_key in dynamic_conflicts:
            base["status"] = "REFERENCE_CONFLICT"
            base["target_reference_received"] = True
            base["invalid_reason"] = "TARGET_TIME_REFERENCE_REVISION_DETECTED"
            rows.append(base)
            continue
        target_reference = by_key.get(target_reference_key)
        if target_reference is None:
            base["status"] = "PENDING_REFERENCE"
            base["invalid_reason"] = None
            rows.append(base)
            continue

        base["target_reference_received"] = True
        reference_temperature = finite_number(target_reference.get("temperature_c"), field="target reference temperature")
        model_prediction = finite_number(forecast.get("prediction_temperature_c"), field="model prediction")
        persistence_prediction = finite_number(feature_reference.get("temperature_c"), field="feature-time temperature")
        model_error = model_prediction - reference_temperature
        persistence_error = persistence_prediction - reference_temperature
        reference_ingestion_time = parse_utc_timestamp(
            target_reference.get("ingestion_time"),
            assume_naive_utc=assume_naive_utc,
        )
        arrival_lag = (reference_ingestion_time - target_time).total_seconds()
        retrieval_mode = str(target_reference.get("reference_retrieval_mode") or "")
        evaluation_mode = _evaluation_mode(source, retrieval_mode)
        target_weather = {
            name: finite_number(target_reference.get(name), field=name, required=False)
            for name in (
                "humidity_pct",
                "precipitation_mm",
                "pressure_hpa",
                "wind_speed_kmh",
                "wind_gust_kmh",
            )
        }
        weather_code = target_reference.get("weather_code")
        if weather_code is not None:
            weather_code = int(finite_number(weather_code, field="weather_code"))
        base.update(
            {
                "evaluation_id": evaluation_id(str(forecast["forecast_id"]), source),
                "cohort_id": cohort_id,
                "reference_temperature_c": reference_temperature,
                **target_weather,
                "weather_code": weather_code,
                "model_error_c": model_error,
                "model_absolute_error_c": abs(model_error),
                "model_squared_error_c2": model_error * model_error,
                "persistence_error_c": persistence_error,
                "persistence_absolute_error_c": abs(persistence_error),
                "persistence_squared_error_c2": persistence_error * persistence_error,
                "target_reference_event_id": target_reference.get("event_id"),
                "reference_source": source,
                "reference_payload_sha256": target_reference.get("reference_payload_sha256"),
                "reference_ingestion_time": reference_ingestion_time,
                "evaluation_lag_seconds": (evaluation_time - target_time).total_seconds(),
                "reference_arrival_lag_seconds": arrival_lag,
                "evaluation_mode": evaluation_mode,
                "status": "EVALUATED",
                "invalid_reason": None,
            }
        )
        rows.append(base)

    return {
        "evaluations": rows,
        "duplicate_forecast_count": duplicate_forecast_count,
        "duplicate_observation_count": duplicate_observation_count,
        "invalid_observation_count": len(invalid_observations),
        "conflicted_reference_keys": dynamic_conflicts,
    }


def evaluation_schema():
    from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("evaluation_id", StringType(), False),
            StructField("evaluation_version", StringType(), False),
            StructField("forecast_id", StringType(), False),
            StructField("cohort_id", StringType(), True),
            StructField("location_id", StringType(), True),
            StructField("feature_time", TimestampType(), True),
            StructField("target_time", TimestampType(), True),
            StructField("model_id", StringType(), True),
            StructField("model_sha256", StringType(), True),
            StructField("feature_set_id", StringType(), True),
            StructField("feature_list_sha256", StringType(), True),
            StructField("model_prediction_temperature_c", DoubleType(), True),
            StructField("persistence_prediction_temperature_c", DoubleType(), True),
            StructField("reference_temperature_c", DoubleType(), True),
            StructField("humidity_pct", DoubleType(), True),
            StructField("precipitation_mm", DoubleType(), True),
            StructField("pressure_hpa", DoubleType(), True),
            StructField("wind_speed_kmh", DoubleType(), True),
            StructField("wind_gust_kmh", DoubleType(), True),
            StructField("weather_code", IntegerType(), True),
            StructField("model_error_c", DoubleType(), True),
            StructField("model_absolute_error_c", DoubleType(), True),
            StructField("model_squared_error_c2", DoubleType(), True),
            StructField("persistence_error_c", DoubleType(), True),
            StructField("persistence_absolute_error_c", DoubleType(), True),
            StructField("persistence_squared_error_c2", DoubleType(), True),
            StructField("feature_reference_event_id", StringType(), True),
            StructField("feature_reference_payload_sha256", StringType(), True),
            StructField("target_reference_event_id", StringType(), True),
            StructField("reference_source", StringType(), True),
            StructField("reference_payload_sha256", StringType(), True),
            StructField("target_reference_received", BooleanType(), False),
            StructField("target_time_passed", BooleanType(), False),
            StructField("forecast_inference_time", TimestampType(), True),
            StructField("reference_ingestion_time", TimestampType(), True),
            StructField("evaluation_time", TimestampType(), False),
            StructField("evaluation_lag_seconds", DoubleType(), True),
            StructField("reference_arrival_lag_seconds", DoubleType(), True),
            StructField("evaluation_mode", StringType(), False),
            StructField("status", StringType(), False),
            StructField("invalid_reason", StringType(), True),
        ]
    )
