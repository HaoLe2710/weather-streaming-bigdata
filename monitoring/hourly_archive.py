from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .evaluation_contract import (
    LIVE_REFERENCE_SOURCE,
    canonical_payload,
    observation_key,
    observation_validation_errors,
    payload_sha256,
    reference_revision_id,
    stable_id,
)


def normalize_observation(
    observation: Mapping[str, Any],
    known_location_ids: set[str] | frozenset[str],
    *,
    archived_at: datetime | None = None,
    assume_naive_utc: bool = False,
) -> dict[str, Any]:
    errors = observation_validation_errors(
        observation,
        known_location_ids,
        assume_naive_utc=assume_naive_utc,
    )
    if errors:
        raise ValueError(";".join(errors))
    from .evaluation_contract import parse_utc_hour, parse_utc_timestamp

    now = archived_at or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        if not assume_naive_utc:
            raise ValueError("archived_at must be timezone-aware UTC")
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    event_time = parse_utc_hour(observation["event_time"], assume_naive_utc=assume_naive_utc)
    ingestion_value = observation.get("ingestion_time")
    inferred_ingestion_time = ingestion_value is None
    ingestion_time = (
        now
        if inferred_ingestion_time
        else parse_utc_timestamp(ingestion_value, assume_naive_utc=assume_naive_utc)
    )
    payload = canonical_payload(observation)
    source = str(observation["source"])
    retrieval_mode = str(
        observation.get("reference_retrieval_mode")
        or ("LIVE_PROSPECTIVE" if source == LIVE_REFERENCE_SOURCE else "REPLAY")
    )
    return {
        "archive_id": stable_id(*observation_key(source, str(observation["location_id"]), event_time)),
        "event_id": str(observation["event_id"]),
        "event_type": observation.get("event_type"),
        "source": source,
        "location_id": str(observation["location_id"]),
        "city": observation.get("city"),
        "latitude": payload["latitude"],
        "longitude": payload["longitude"],
        "event_time": event_time,
        "ingestion_time": ingestion_time,
        "first_archived_at": now,
        "reference_retrieval_mode": retrieval_mode,
        **{name: payload[name] for name in payload if name not in {"latitude", "longitude"}},
        "reference_payload_sha256": payload_sha256(observation),
        "ingestion_time_inferred": inferred_ingestion_time,
    }


def revision_record(
    canonical: Mapping[str, Any],
    incoming: Mapping[str, Any],
    *,
    detected_at: datetime | None = None,
    assume_naive_utc: bool = False,
) -> dict[str, Any]:
    source = str(canonical["source"])
    location_id = str(canonical["location_id"])
    event_time = canonical["event_time"]
    old_hash = str(canonical["reference_payload_sha256"])
    new_hash = str(incoming["reference_payload_sha256"])
    observed_at = detected_at or datetime.now(timezone.utc)
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        if not assume_naive_utc:
            raise ValueError("detected_at must be timezone-aware UTC")
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return {
        "revision_id": reference_revision_id(
            source,
            location_id,
            event_time,
            old_hash,
            new_hash,
            assume_naive_utc=assume_naive_utc,
        ),
        "source": source,
        "location_id": location_id,
        "event_time": event_time,
        "old_payload_sha256": old_hash,
        "new_payload_sha256": new_hash,
        "first_event_id": str(canonical["event_id"]),
        "later_event_id": str(incoming["event_id"]),
        "first_ingestion_time": canonical.get("ingestion_time"),
        "later_ingestion_time": incoming.get("ingestion_time"),
        "first_archived_at": canonical.get("first_archived_at"),
        "detected_at": observed_at.astimezone(timezone.utc),
        "status": "REFERENCE_REVISION_DETECTED",
    }


def canonicalize_batch(
    observations: Iterable[Mapping[str, Any]],
    known_location_ids: set[str] | frozenset[str],
    *,
    archived_at: datetime | None = None,
) -> dict[str, Any]:
    """Deduplicate a batch in arrival order and retain revision evidence."""

    accepted: dict[tuple[str, str, str], dict[str, Any]] = {}
    conflicts: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    duplicate_count = 0
    for raw in observations:
        try:
            row = normalize_observation(raw, known_location_ids, archived_at=archived_at)
        except (TypeError, ValueError) as exc:
            rejected.append({"event_id": raw.get("event_id"), "reason": str(exc), "raw": dict(raw)})
            continue
        key = observation_key(row["source"], row["location_id"], row["event_time"])
        prior = accepted.get(key)
        if prior is None:
            accepted[key] = row
        elif prior["reference_payload_sha256"] == row["reference_payload_sha256"]:
            duplicate_count += 1
        else:
            conflicts.append(revision_record(prior, row, detected_at=archived_at))
    return {
        "observations": list(accepted.values()),
        "conflicts": conflicts,
        "rejected": rejected,
        "duplicate_count": duplicate_count,
    }


def archive_schema():
    from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("archive_id", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), True),
            StructField("source", StringType(), False),
            StructField("location_id", StringType(), False),
            StructField("city", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("event_time", TimestampType(), False),
            StructField("ingestion_time", TimestampType(), False),
            StructField("first_archived_at", TimestampType(), False),
            StructField("reference_retrieval_mode", StringType(), False),
            StructField("temperature_c", DoubleType(), False),
            StructField("humidity_pct", DoubleType(), True),
            StructField("precipitation_mm", DoubleType(), True),
            StructField("pressure_hpa", DoubleType(), True),
            StructField("wind_speed_kmh", DoubleType(), True),
            StructField("wind_gust_kmh", DoubleType(), True),
            StructField("weather_code", IntegerType(), True),
            StructField("reference_payload_sha256", StringType(), False),
            StructField("ingestion_time_inferred", BooleanType(), False),
        ]
    )


def revision_schema():
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("revision_id", StringType(), False),
            StructField("source", StringType(), False),
            StructField("location_id", StringType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("old_payload_sha256", StringType(), False),
            StructField("new_payload_sha256", StringType(), False),
            StructField("first_event_id", StringType(), True),
            StructField("later_event_id", StringType(), True),
            StructField("first_ingestion_time", TimestampType(), True),
            StructField("later_ingestion_time", TimestampType(), True),
            StructField("first_archived_at", TimestampType(), True),
            StructField("detected_at", TimestampType(), False),
            StructField("status", StringType(), False),
        ]
    )
