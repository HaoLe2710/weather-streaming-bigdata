from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Iterable, Mapping, Sequence

from ml.streaming_inference.t2h_contract import (
    FEATURE_COUNT,
    FEATURE_LIST_SHA256,
    FEATURE_SET_ID,
    FORECAST_HORIZON_HOURS,
    LIVE_ENDPOINT,
    LIVE_SOURCE,
    MODEL_BYTES,
    MODEL_ID,
    MODEL_SHA256,
    PROVIDER_MODEL,
    PROVIDER_NAME,
    default_feature_list_path,
    default_model_path,
    load_t2h_feature_contract,
)
from monitoring.evaluation_contract import (
    LIVE_REFERENCE_SOURCE,
    evaluation_id,
    forecast_validation_errors,
    observation_key,
    parse_utc_hour,
    parse_utc_timestamp,
    payload_sha256,
    reference_revision_id,
)
from monitoring.forecast_evaluator import evaluate_forecasts
from monitoring.hourly_archive import revision_record
from monitoring.metrics import calculate_metrics, distribution_stats
from validation.runtime_stabilization_t2h import classify_hourly_cycle_gap


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE_ROOT = REPOSITORY_ROOT / "data" / "runtime" / "prospective-live-t2h"
DEFAULT_RESULTS_ROOT = REPOSITORY_ROOT / "results" / "prospective-live-t2h"
FORECAST_GRACE_PERIOD_SECONDS = 15 * 60
REFERENCE_GRACE_PERIOD_SECONDS = 2 * 60 * 60
READINESS_MAX_AGE_SECONDS = 6 * 60 * 60
READINESS_POLL_SECONDS = 10
MAX_EXPECTED_LOCATIONS = 63
EXPECTED_TARGET_HOURS = 168  # Seven consecutive UTC target days.
EXPECTED_SLOTS = MAX_EXPECTED_LOCATIONS * EXPECTED_TARGET_HOURS
COHORT_PROTOCOL_ID = "T2H_LIVE_PROSPECTIVE_168H_V1"
COMPLETE_COHORT_LABEL = f"COMPLETE_{EXPECTED_SLOTS}"
TERMINAL_SLOT_STATES = frozenset(
    {"EVALUATED", "FORECAST_MISSING", "MISSING_REFERENCE", "REFERENCE_CONFLICT", "INVALID"}
)
REFERENCE_PENDING_STATES = frozenset({"PENDING_TARGET_TIME", "PENDING_BASELINE", "PENDING_REFERENCE"})
PROCESS_STARTED_AT = datetime.now(timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | Any) -> str:
    instant = parse_utc_timestamp(value, assume_naive_utc=True)
    return instant.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _hour_text(value: Any) -> str:
    return parse_utc_hour(value, assume_naive_utc=True).strftime("%Y-%m-%dT%H:00:00Z")


def _cohort_protocol_errors(manifest: Mapping[str, Any]) -> list[str]:
    """Reject incompatible or noncontiguous cohort manifests before state mutation."""
    errors: list[str] = []
    expected_fields = {
        "cohort_protocol_id": COHORT_PROTOCOL_ID,
        "expected_target_hours": EXPECTED_TARGET_HOURS,
        "expected_locations": MAX_EXPECTED_LOCATIONS,
        "expected_forecasts": EXPECTED_SLOTS,
        "expected_slots": EXPECTED_SLOTS,
    }
    for field, expected in expected_fields.items():
        if manifest.get(field) != expected:
            errors.append(f"INVALID_{field.upper()}")

    locations = manifest.get("canonical_locations")
    if not isinstance(locations, list) or len(locations) != MAX_EXPECTED_LOCATIONS or len(set(locations)) != MAX_EXPECTED_LOCATIONS:
        errors.append("INVALID_CANONICAL_LOCATIONS")

    try:
        first = parse_utc_hour(manifest["cohort_start_target_time"], assume_naive_utc=True)
        expected_times = [_hour_text(first + timedelta(hours=offset)) for offset in range(EXPECTED_TARGET_HOURS)]
        if manifest.get("target_times") != expected_times:
            errors.append("NONCONTIGUOUS_TARGET_WINDOW")
        if _hour_text(manifest["cohort_end_target_time"]) != expected_times[-1]:
            errors.append("INVALID_END_TARGET_TIME")
    except (KeyError, TypeError, ValueError):
        errors.append("INVALID_TARGET_WINDOW")
    return errors


def _start_request_protocol_errors(
    request: Mapping[str, Any],
    *,
    expected_run_id: str | None = None,
) -> list[str]:
    """Reject legacy or inconsistent start requests before freezing a cohort."""
    if not isinstance(request, Mapping):
        return ["INVALID_START_REQUEST"]
    errors: list[str] = []
    expected_fields = {
        "cohort_protocol_id": COHORT_PROTOCOL_ID,
        "expected_target_hours": EXPECTED_TARGET_HOURS,
        "expected_locations": MAX_EXPECTED_LOCATIONS,
        "expected_slots": EXPECTED_SLOTS,
    }
    for field, expected in expected_fields.items():
        if request.get(field) != expected:
            errors.append(f"INVALID_START_REQUEST_{field.upper()}")

    request_run_id = request.get("run_id")
    if not isinstance(request_run_id, str) or not request_run_id.strip():
        errors.append("INVALID_START_REQUEST_RUN_ID")
    elif expected_run_id is not None and request_run_id != expected_run_id:
        errors.append("START_REQUEST_RUN_ID_MISMATCH")
    return errors


def _start_request_contract_errors(request: Mapping[str, Any]) -> list[str]:
    """Check the immutable model/provider contract recorded at cohort start."""
    if not isinstance(request, Mapping):
        return ["INVALID_START_REQUEST"]
    expected_fields = {
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_count": FEATURE_COUNT,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "provider_model": PROVIDER_MODEL,
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
    }
    errors = [
        f"INVALID_START_REQUEST_{field.upper()}"
        for field, expected in expected_fields.items()
        if request.get(field) != expected
    ]
    if not isinstance(request.get("contract_fingerprint"), str) or not request.get("contract_fingerprint"):
        errors.append("INVALID_START_REQUEST_CONTRACT_FINGERPRINT")
    return errors


def _manifest_request_protocol_errors(
    request: Mapping[str, Any],
    manifest: Mapping[str, Any],
    *,
    expected_run_id: str | None = None,
) -> list[str]:
    errors = _cohort_protocol_errors(manifest)
    request_run_id = request.get("run_id")
    manifest_run_id = manifest.get("run_id")
    if manifest_run_id != request_run_id:
        errors.append("START_REQUEST_MANIFEST_RUN_ID_MISMATCH")
    if expected_run_id is not None and (request_run_id != expected_run_id or manifest_run_id != expected_run_id):
        errors.append("ACTIVE_RUN_ID_MISMATCH")
    for field in ("cohort_protocol_id", "expected_target_hours", "expected_locations", "expected_slots"):
        if manifest.get(field) != request.get(field):
            errors.append(f"START_REQUEST_MANIFEST_{field.upper()}_MISMATCH")
    try:
        start = parse_utc_hour(manifest["cohort_start_target_time"], assume_naive_utc=True)
        expected_cohort_id = f"prospective-t2h-{start.strftime('%Y%m%dT%H%M%SZ')}"
        if manifest.get("cohort_id") != expected_cohort_id:
            errors.append("INVALID_COHORT_ID_FOR_TARGET_WINDOW")
    except (KeyError, TypeError, ValueError):
        pass  # _cohort_protocol_errors already reports an invalid target window.
    return errors


def _manifest_request_contract_errors(
    request: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> list[str]:
    errors = _start_request_contract_errors(request)
    for field in (
        "model_id",
        "model_sha256",
        "feature_set_id",
        "feature_count",
        "feature_list_sha256",
        "provider_model",
        "forecast_horizon_hours",
        "contract_fingerprint",
    ):
        if manifest.get(field) != request.get(field):
            errors.append(f"START_REQUEST_MANIFEST_{field.upper()}_MISMATCH")
    expected_fields = {
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_count": FEATURE_COUNT,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "provider_model": PROVIDER_MODEL,
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
    }
    for field, expected in expected_fields.items():
        if manifest.get(field) != expected:
            errors.append(f"INVALID_MANIFEST_{field.upper()}")
    if not isinstance(manifest.get("contract_fingerprint"), str) or not manifest.get("contract_fingerprint"):
        errors.append("INVALID_MANIFEST_CONTRACT_FINGERPRINT")
    return errors


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return iso_utc(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _jsonable(value.item())
        except (ValueError, TypeError):
            pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + f".{os.getpid()}.tmp")
    data = json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n"
    with temp.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    raw_lines = path.read_bytes().splitlines(keepends=True)
    for line_number, raw_line in enumerate(raw_lines, 1):
        try:
            line = raw_line.decode("utf-8")
        except UnicodeDecodeError as exc:
            if line_number == len(raw_lines) and not raw_line.endswith((b"\n", b"\r")):
                break
            raise ValueError(f"invalid UTF-8 JSONL at {path}:{line_number}: {exc}") from exc
        if not raw_line.endswith((b"\n", b"\r")) and line_number == len(raw_lines):
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                # A process or host can stop during the final append. Ignore only
                # that uncommitted tail; a later idempotent write can repair it.
                break
        else:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_number}: {exc}") from exc
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _append_jsonl_once(path: Path, row: Mapping[str, Any], *, id_field: str) -> bool:
    identifier = str(row.get(id_field) or "")
    if not identifier:
        raise ValueError(f"{id_field} is required for an idempotent JSONL append")
    if any(str(existing.get(id_field) or "") == identifier for existing in _read_jsonl(path)):
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    _repair_jsonl_tail(path)
    encoded = json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(encoded + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    return True


def _repair_jsonl_tail(path: Path) -> None:
    if not path.is_file():
        return
    with path.open("r+b") as stream:
        payload = stream.read()
        if not payload or payload.endswith((b"\n", b"\r")):
            return
        last_newline = payload.rfind(b"\n")
        tail = payload[last_newline + 1 :]
        try:
            json.loads(tail.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            stream.truncate(last_newline + 1)
        else:
            stream.seek(0, os.SEEK_END)
            stream.write(b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def validate_runtime_contract(
    *,
    model_path: str | Path | None = None,
    feature_list_path: str | Path | None = None,
    model_id: str = MODEL_ID,
    model_sha256: str = MODEL_SHA256,
    feature_set_id: str = FEATURE_SET_ID,
    feature_list_sha256: str = FEATURE_LIST_SHA256,
    forecast_horizon_hours: int = FORECAST_HORIZON_HOURS,
    provider_model: str = PROVIDER_MODEL,
) -> dict[str, Any]:
    model_file = Path(model_path) if model_path else default_model_path()
    feature_file = Path(feature_list_path) if feature_list_path else default_feature_list_path()
    problems: list[str] = []
    actual_model_sha = None
    actual_model_bytes = None
    try:
        actual_model_bytes = model_file.stat().st_size
        actual_model_sha = hashlib.sha256(model_file.read_bytes()).hexdigest()
    except OSError as exc:
        problems.append(f"CANONICAL_MODEL_UNAVAILABLE:{exc}")
    if model_id != MODEL_ID:
        problems.append("MODEL_ID_DRIFT")
    if model_sha256 != MODEL_SHA256 or actual_model_sha != MODEL_SHA256:
        problems.append("MODEL_SHA_MISMATCH")
    if actual_model_bytes is not None and actual_model_bytes != MODEL_BYTES:
        problems.append("MODEL_SIZE_MISMATCH")
    if feature_set_id != FEATURE_SET_ID or feature_list_sha256 != FEATURE_LIST_SHA256:
        problems.append("FEATURE_CONTRACT_IDENTITY_DRIFT")
    if forecast_horizon_hours != FORECAST_HORIZON_HOURS:
        problems.append("FORECAST_HORIZON_DRIFT")
    if provider_model != PROVIDER_MODEL:
        problems.append("PROVIDER_MODEL_DRIFT")
    feature_names: Sequence[str] = ()
    actual_feature_sha = None
    try:
        contract = load_t2h_feature_contract(feature_file)
        feature_names = contract.feature_names
        actual_feature_sha = contract.feature_list_sha256
    except (OSError, ValueError) as exc:
        problems.append(f"FEATURE_CONTRACT_INVALID:{exc}")
    if len(feature_names) != FEATURE_COUNT or actual_feature_sha != FEATURE_LIST_SHA256:
        problems.append("FEATURE_LIST_MISMATCH")
    body = {
        "model_id": model_id,
        "model_sha256": actual_model_sha,
        "model_size_bytes": actual_model_bytes,
        "feature_set_id": feature_set_id,
        "feature_count": len(feature_names),
        "feature_list_sha256": actual_feature_sha,
        "forecast_horizon_hours": forecast_horizon_hours,
        "provider": PROVIDER_NAME,
        "provider_model": provider_model,
    }
    fingerprint = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    ).hexdigest()
    return {
        "status": "PASS" if not problems else "COHORT_CONTRACT_DRIFT",
        "validated_at": iso_utc(utc_now()),
        "model_path": str(model_file),
        "feature_list_path": str(feature_file),
        "training_performed": False,
        **body,
        "contract_fingerprint": fingerprint,
        "problems": problems,
    }


def _forecast_errors(forecast: Mapping[str, Any], location_ids: set[str] | frozenset[str]) -> list[str]:
    errors = forecast_validation_errors(forecast, location_ids, assume_naive_utc=True)
    if forecast.get("execution_origin") != "LIVE_PROSPECTIVE":
        errors.append("INVALID_EXECUTION_ORIGIN")
    if forecast.get("provider") != PROVIDER_NAME:
        errors.append("INVALID_PROVIDER")
    if forecast.get("provider_model") != PROVIDER_MODEL:
        errors.append("INVALID_PROVIDER_MODEL")
    if forecast.get("provider_endpoint") != LIVE_ENDPOINT or forecast.get("source") != LIVE_SOURCE:
        errors.append("INVALID_LIVE_SOURCE")
    return sorted(set(errors))


def _receipt_for_forecast(
    forecast: Mapping[str, Any],
    receipts_by_id: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[Mapping[str, Any] | None, list[str]]:
    forecast_id = str(forecast.get("forecast_id") or "")
    receipts = list(receipts_by_id.get(forecast_id, ()))
    if not receipts:
        return None, ["MISSING_PERSISTENCE_RECEIPT"]
    signatures = {
        (
            str(row.get("forecast_persisted_at") or ""),
            str(row.get("location_id") or ""),
            str(row.get("target_time") or ""),
            str(row.get("record_sha256") or ""),
        )
        for row in receipts
    }
    if len(signatures) != 1:
        return None, ["CONFLICTING_PERSISTENCE_RECEIPTS"]
    receipt = receipts[0]
    errors: list[str] = []
    try:
        persisted_at = parse_utc_timestamp(receipt.get("forecast_persisted_at"), assume_naive_utc=True)
        inference_at = parse_utc_timestamp(forecast.get("inference_time"), assume_naive_utc=True)
        if persisted_at < inference_at:
            errors.append("PERSISTENCE_PRECEDES_INFERENCE")
        if str(receipt.get("location_id")) != str(forecast.get("location_id")):
            errors.append("PERSISTENCE_LOCATION_MISMATCH")
        if _hour_text(receipt.get("target_time")) != _hour_text(forecast.get("target_time")):
            errors.append("PERSISTENCE_TARGET_MISMATCH")
        if str(receipt.get("record_sha256") or "") != _forecast_record_sha256(forecast):
            errors.append("PERSISTENCE_RECORD_HASH_MISMATCH")
    except (TypeError, ValueError):
        errors.append("INVALID_PERSISTENCE_TIMESTAMP")
    return receipt, errors


def _forecast_record_sha256(forecast: Mapping[str, Any]) -> str:
    fields = (
        "forecast_id",
        "location_id",
        "feature_time",
        "target_time",
        "inference_time",
        "prediction_temperature_c",
        "model_id",
        "model_sha256",
        "feature_set_id",
        "feature_list_sha256",
        "feature_count",
        "forecast_horizon_hours",
        "execution_origin",
        "provider",
        "provider_endpoint",
        "provider_model",
        "source",
        "source_event_id",
        "source_timestamp",
        "source_retrieved_at",
        "forecast_lead_seconds",
    )
    body = {field: _jsonable(forecast.get(field)) for field in fields}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _receipt_map(receipts: Iterable[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    result: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in receipts:
        forecast_id = str(row.get("forecast_id") or "")
        if forecast_id:
            result[forecast_id].append(row)
    return dict(result)


def _request_time(request: Mapping[str, Any]) -> datetime:
    return parse_utc_timestamp(request["start_requested_at"], assume_naive_utc=True)


def _cycle_groups(
    forecasts: Iterable[Mapping[str, Any]],
    receipts: Iterable[Mapping[str, Any]],
    *,
    location_ids: set[str] | frozenset[str],
    requested_at: datetime,
) -> dict[str, list[Mapping[str, Any]]]:
    receipts_by_id = _receipt_map(receipts)
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for forecast in forecasts:
        if forecast.get("execution_origin") != "LIVE_PROSPECTIVE":
            continue
        try:
            target = parse_utc_hour(forecast.get("target_time"), assume_naive_utc=True)
            inferred = parse_utc_timestamp(forecast.get("inference_time"), assume_naive_utc=True)
        except (TypeError, ValueError):
            continue
        receipt, receipt_errors = _receipt_for_forecast(forecast, receipts_by_id)
        if receipt is None or receipt_errors:
            continue
        try:
            persisted = parse_utc_timestamp(receipt.get("forecast_persisted_at"), assume_naive_utc=True)
        except (TypeError, ValueError):
            continue
        if inferred < requested_at or persisted < requested_at:
            continue
        if _forecast_errors(forecast, location_ids):
            continue
        if _forecast_record_sha256(forecast) != str(receipt.get("record_sha256") or ""):
            continue
        groups[_hour_text(target)].append(forecast)
    return dict(groups)


def freeze_cohort_manifest(
    forecasts: Iterable[Mapping[str, Any]],
    receipts: Iterable[Mapping[str, Any]],
    *,
    request: Mapping[str, Any],
    known_location_ids: set[str] | frozenset[str],
    frozen_at: datetime | None = None,
) -> dict[str, Any] | None:
    request_protocol_errors = _start_request_protocol_errors(request)
    if request_protocol_errors:
        raise ValueError(f"COHORT_PROTOCOL_DRIFT: {', '.join(request_protocol_errors)}")
    request_contract_errors = _start_request_contract_errors(request)
    if request_contract_errors:
        raise ValueError(f"COHORT_CONTRACT_DRIFT: {', '.join(request_contract_errors)}")
    if len(known_location_ids) != MAX_EXPECTED_LOCATIONS:
        raise ValueError(f"canonical location set must contain exactly {MAX_EXPECTED_LOCATIONS} locations")
    requested_at = _request_time(request)
    groups = _cycle_groups(
        forecasts,
        receipts,
        location_ids=known_location_ids,
        requested_at=requested_at,
    )
    eligible: list[datetime] = []
    for target_text, rows in groups.items():
        unique_by_location: dict[str, Mapping[str, Any]] = {}
        duplicate_locations: set[str] = set()
        for row in rows:
            location = str(row.get("location_id") or "")
            if location in unique_by_location:
                duplicate_locations.add(location)
            unique_by_location[location] = row
        if duplicate_locations or set(unique_by_location) != set(known_location_ids):
            continue
        eligible.append(parse_utc_hour(target_text))
    if not eligible:
        return None
    t0 = min(eligible)
    last_target = t0 + timedelta(hours=EXPECTED_TARGET_HOURS - 1)
    freeze_time = (frozen_at or utc_now()).astimezone(timezone.utc)
    cohort_id = f"prospective-t2h-{t0.strftime('%Y%m%dT%H%M%SZ')}"
    return {
        "status": "COLLECTING",
        "cohort_id": cohort_id,
        "run_id": str(request["run_id"]),
        "created_at": iso_utc(freeze_time),
        "cohort_frozen_at": iso_utc(freeze_time),
        "start_requested_at": iso_utc(requested_at),
        "cohort_start_target_time": _hour_text(t0),
        "cohort_end_target_time": _hour_text(last_target),
        "target_times": [_hour_text(t0 + timedelta(hours=index)) for index in range(EXPECTED_TARGET_HOURS)],
        "cohort_protocol_id": COHORT_PROTOCOL_ID,
        "expected_target_hours": EXPECTED_TARGET_HOURS,
        "expected_locations": MAX_EXPECTED_LOCATIONS,
        "expected_forecasts": EXPECTED_SLOTS,
        "expected_slots": EXPECTED_SLOTS,
        "canonical_locations": sorted(known_location_ids),
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_count": FEATURE_COUNT,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "provider": PROVIDER_NAME,
        "provider_model": PROVIDER_MODEL,
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
        "execution_origin": "LIVE_PROSPECTIVE",
        "offline_target": "ERA5 reanalysis",
        "prospective_reference": "Open-Meteo live Forecast API ecmwf_ifs operational model-product reference; not station ground truth",
        "forecast_grace_period_seconds": int(request["forecast_grace_period_seconds"]),
        "reference_grace_period_seconds": int(request["reference_grace_period_seconds"]),
        "runtime_configuration": dict(request.get("runtime_configuration", {})),
        "contract_fingerprint": request["contract_fingerprint"],
        "training_performed": False,
    }


def _new_slot(target_time: str, location_id: str) -> dict[str, Any]:
    return {
        "target_time": target_time,
        "location_id": location_id,
        "status": "FORECAST_PENDING",
        "forecast": None,
        "forecast_persisted_at": None,
        "issuance_boundary": None,
        "issuance_latency_seconds": None,
        "evaluation": None,
        "invalid_reason": None,
    }


def _slot_key(target_time: Any, location_id: Any) -> str:
    return f"{_hour_text(target_time)}|{str(location_id)}"


def _state_slots(
    manifest: Mapping[str, Any],
    old_state: Mapping[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    previous = (old_state or {}).get("slots") or {}
    slots: dict[str, dict[str, Any]] = {}
    for target in manifest["target_times"]:
        for location in manifest["canonical_locations"]:
            key = _slot_key(target, location)
            slots[key] = dict(previous.get(key) or _new_slot(target, location))
    return slots


def _reference_observation(row: Mapping[str, Any]) -> dict[str, Any] | None:
    source = str(row.get("source") or "")
    provider = row.get("provider")
    provider_model = row.get("provider_model")
    if (
        source != LIVE_REFERENCE_SOURCE
        or provider != PROVIDER_NAME
        or provider_model != PROVIDER_MODEL
        or row.get("provider_endpoint") != LIVE_ENDPOINT
    ):
        return None
    try:
        event_time = parse_utc_hour(row.get("event_time"), assume_naive_utc=True)
        retrieved_at = parse_utc_timestamp(row.get("source_retrieved_at"), assume_naive_utc=True)
        # The live producer only publishes completed hours. Keep the same boundary explicit here.
        if retrieved_at < event_time + timedelta(hours=1):
            return None
        observation = {
            "event_id": row.get("event_id"),
            "event_type": row.get("event_type") or "WEATHER_HOURLY",
            "source": source,
            "location_id": str(row.get("location_id") or ""),
            "event_time": event_time,
            "ingestion_time": retrieved_at,
            "first_archived_at": retrieved_at,
            "reference_retrieval_mode": "LIVE_PROSPECTIVE",
            "temperature_c": _row_value(row, "temperature_c", "temperature_2m"),
            "humidity_pct": _row_value(row, "humidity_pct", "relative_humidity_2m"),
            "precipitation_mm": _row_value(row, "precipitation_mm", "precipitation"),
            "pressure_hpa": _row_value(row, "pressure_hpa", "pressure_msl"),
            "wind_speed_kmh": _row_value(row, "wind_speed_kmh", "wind_speed_10m"),
            "wind_gust_kmh": _row_value(row, "wind_gust_kmh", "wind_gusts_10m"),
            "weather_code": row.get("weather_code"),
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "provider": provider,
            "provider_model": provider_model,
            "provider_endpoint": row.get("provider_endpoint"),
            "source_retrieved_at": retrieved_at,
        }
        observation["reference_payload_sha256"] = payload_sha256(observation)
        return observation
    except (TypeError, ValueError):
        return None


def _row_value(row: Mapping[str, Any], canonical_name: str, provider_name: str) -> Any:
    value = row.get(canonical_name)
    return value if value is not None else row.get(provider_name)


def _revision_key(row: Mapping[str, Any]) -> tuple[str, str, str] | None:
    try:
        return observation_key(
            str(row.get("source") or LIVE_REFERENCE_SOURCE),
            str(row.get("location_id") or ""),
            row.get("event_time"),
            assume_naive_utc=True,
        )
    except (TypeError, ValueError):
        return None


def _revision_index(revisions: Iterable[Mapping[str, Any]]) -> tuple[set[tuple[str, str, str]], dict[str, dict[str, Any]]]:
    keys: set[tuple[str, str, str]] = set()
    unique: dict[str, dict[str, Any]] = {}
    for row in revisions:
        revision_id = str(row.get("revision_id") or "")
        key = _revision_key(row)
        if key:
            keys.add(key)
        if revision_id:
            unique[revision_id] = dict(row)
    return keys, unique


def _stats(values: Iterable[Any]) -> dict[str, Any]:
    stats = distribution_stats(values)
    return {
        "count": stats["count"],
        "min": stats["min"],
        "p05": stats["p05"],
        "median": stats["p50"],
        "mean": stats["mean"],
        "p95": stats["p95"],
        "max": stats["max"],
    }


def _within_window(target: Any, manifest: Mapping[str, Any]) -> bool:
    try:
        instant = parse_utc_hour(target, assume_naive_utc=True)
        first = parse_utc_hour(manifest["cohort_start_target_time"])
        last = parse_utc_hour(manifest["cohort_end_target_time"])
        return first <= instant <= last
    except (TypeError, ValueError):
        return False


def _canonical_slot_metrics(slots: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    evaluated = [dict(slot["evaluation"]) for slot in slots if slot.get("status") == "EVALUATED" and slot.get("evaluation")]
    overall = calculate_metrics(evaluated, minimum_sample=1)
    location_rows: list[dict[str, Any]] = []
    for location in sorted({str(slot["location_id"]) for slot in slots}):
        group = [row for row in evaluated if str(row.get("location_id")) == location]
        metrics = calculate_metrics(group, minimum_sample=1)
        slot_group = [slot for slot in slots if str(slot["location_id"]) == location]
        location_rows.append(
            {
                "location_id": location,
                "expected_evaluations": EXPECTED_TARGET_HOURS,
                "valid_evaluation_count": len(group),
                "missing_count": EXPECTED_TARGET_HOURS - len(group),
                "forecast_missing_count": sum(slot.get("status") == "FORECAST_MISSING" for slot in slot_group),
                "reference_missing_count": sum(slot.get("status") == "MISSING_REFERENCE" for slot in slot_group),
                "reference_conflict_count": sum(slot.get("status") == "REFERENCE_CONFLICT" for slot in slot_group),
                "invalid_count": sum(slot.get("status") == "INVALID" for slot in slot_group),
                "xgboost_mae_c": metrics.get("model_mae"),
                "persistence_mae_c": metrics.get("persistence_mae"),
                "mae_skill": metrics.get("mae_skill"),
                "xgboost_bias_c": metrics.get("model_bias"),
            }
        )
    target_rows: list[dict[str, Any]] = []
    for target in sorted({str(slot["target_time"]) for slot in slots}):
        group_slots = [slot for slot in slots if str(slot["target_time"]) == target]
        group = [row for row in evaluated if _hour_text(row.get("target_time")) == target]
        metrics = calculate_metrics(group, minimum_sample=1)
        target_rows.append(
            {
                "target_time": target,
                "expected_forecasts": MAX_EXPECTED_LOCATIONS,
                "valid_forecasts": sum(
                    slot.get("forecast") is not None and slot.get("status") != "INVALID"
                    for slot in group_slots
                ),
                "valid_evaluations": len(group),
                "missing_forecasts": sum(slot.get("status") == "FORECAST_MISSING" for slot in group_slots),
                "missing_references": sum(slot.get("status") == "MISSING_REFERENCE" for slot in group_slots),
                "reference_conflicts": sum(slot.get("status") == "REFERENCE_CONFLICT" for slot in group_slots),
                "invalid": sum(slot.get("status") == "INVALID" for slot in group_slots),
                "xgboost_mae_c": metrics.get("model_mae"),
                "persistence_mae_c": metrics.get("persistence_mae"),
                "mae_skill": metrics.get("mae_skill"),
                "xgboost_bias_c": metrics.get("model_bias"),
                "xgboost_rmse_c": metrics.get("model_rmse"),
                "persistence_rmse_c": metrics.get("persistence_rmse"),
            }
        )
    return overall, location_rows, target_rows


def _better_equal_worse(rows: Iterable[Mapping[str, Any]], model_field: str, persistence_field: str) -> dict[str, int]:
    counts = {"better": 0, "equal": 0, "worse": 0, "not_evaluable": 0}
    for row in rows:
        model = row.get(model_field)
        persistence = row.get(persistence_field)
        if model is None or persistence is None:
            counts["not_evaluable"] += 1
        elif model < persistence:
            counts["better"] += 1
        elif model > persistence:
            counts["worse"] += 1
        else:
            counts["equal"] += 1
    return counts


def _slot_output_row(slot: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    forecast = slot.get("forecast") or {}
    evaluation = slot.get("evaluation") or {}
    return {
        "cohort_id": manifest["cohort_id"],
        "forecast_id": forecast.get("forecast_id"),
        "evaluation_id": evaluation.get("evaluation_id"),
        "location_id": slot.get("location_id"),
        "feature_time": forecast.get("feature_time"),
        "target_time": slot.get("target_time"),
        "inference_time": forecast.get("inference_time"),
        "forecast_persisted_at": slot.get("forecast_persisted_at"),
        "issuance_boundary": slot.get("issuance_boundary"),
        "issuance_latency_seconds": slot.get("issuance_latency_seconds"),
        "forecast_lead_seconds": forecast.get("forecast_lead_seconds"),
        "model_prediction_temperature_c": forecast.get("prediction_temperature_c"),
        "persistence_prediction_temperature_c": evaluation.get("persistence_prediction_temperature_c"),
        "reference_temperature_c": evaluation.get("reference_temperature_c"),
        "model_error_c": evaluation.get("model_error_c"),
        "persistence_error_c": evaluation.get("persistence_error_c"),
        "model_absolute_error_c": evaluation.get("model_absolute_error_c"),
        "persistence_absolute_error_c": evaluation.get("persistence_absolute_error_c"),
        "model_squared_error_c2": evaluation.get("model_squared_error_c2"),
        "persistence_squared_error_c2": evaluation.get("persistence_squared_error_c2"),
        "model_id": forecast.get("model_id"),
        "model_sha256": forecast.get("model_sha256"),
        "feature_set_id": forecast.get("feature_set_id"),
        "feature_list_sha256": forecast.get("feature_list_sha256"),
        "feature_count": forecast.get("feature_count"),
        "provider": forecast.get("provider"),
        "provider_model": forecast.get("provider_model"),
        "execution_origin": forecast.get("execution_origin"),
        "evaluation_mode": evaluation.get("evaluation_mode"),
        "reference_event_id": evaluation.get("target_reference_event_id"),
        "reference_ingestion_time": evaluation.get("reference_ingestion_time"),
        "reference_arrival_lag_seconds": evaluation.get("reference_arrival_lag_seconds"),
        "evaluation_time": evaluation.get("evaluation_time"),
        "evaluation_lag_seconds": evaluation.get("evaluation_lag_seconds"),
        "status": slot.get("status"),
        "invalid_reason": slot.get("invalid_reason") or evaluation.get("invalid_reason"),
    }


def _write_evaluations_parquet(spark: Any, path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

    timestamp_fields = {
        "feature_time",
        "target_time",
        "inference_time",
        "forecast_persisted_at",
        "issuance_boundary",
        "reference_ingestion_time",
        "evaluation_time",
    }
    double_fields = {
        "issuance_latency_seconds",
        "forecast_lead_seconds",
        "model_prediction_temperature_c",
        "persistence_prediction_temperature_c",
        "reference_temperature_c",
        "model_error_c",
        "persistence_error_c",
        "model_absolute_error_c",
        "persistence_absolute_error_c",
        "model_squared_error_c2",
        "persistence_squared_error_c2",
        "reference_arrival_lag_seconds",
        "evaluation_lag_seconds",
    }
    integer_fields = {"feature_count"}
    names = tuple(rows[0].keys()) if rows else tuple(_slot_output_row({}, {"cohort_id": ""}).keys())
    schema = StructType(
        [
            StructField(
                name,
                TimestampType() if name in timestamp_fields else DoubleType() if name in double_fields else IntegerType() if name in integer_fields else StringType(),
                True,
            )
            for name in names
        ]
    )

    def spark_value(name: str, value: Any) -> Any:
        if name in timestamp_fields and value is not None:
            return parse_utc_timestamp(value, assume_naive_utc=True).replace(tzinfo=None)
        return value

    values = [tuple(spark_value(name, row.get(name)) for name in names) for row in rows]
    path.parent.mkdir(parents=True, exist_ok=True)
    spark.createDataFrame(values, schema=schema).write.mode("overwrite").parquet(str(path))


def _checksum_tree(root: Path, *, parquet_root: Path | None = None) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "checksums.json":
            continue
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
        entries.append({"path": str(path.relative_to(root)).replace("\\", "/"), "bytes": size, "sha256": digest.hexdigest()})
    return {"algorithm": "sha256", "files": entries, "file_count": len(entries)}


def _final_artifacts(
    spark: Any,
    *,
    manifest: Mapping[str, Any],
    slots: Sequence[Mapping[str, Any]],
    status: Mapping[str, Any],
    runtime_contract: Mapping[str, Any],
    revisions: Sequence[Mapping[str, Any]],
    preflight_tests: Mapping[str, Any] | None,
    results_root: Path,
) -> dict[str, Any]:
    run_results = results_root / str(manifest["run_id"])
    run_results.mkdir(parents=True, exist_ok=True)
    now = utc_now()
    rows = [_slot_output_row(slot, manifest) for slot in slots]
    evaluated = [row for row in rows if row.get("status") == "EVALUATED"]
    overall, per_location, per_target_hour = _canonical_slot_metrics(slots)
    terminal_count = sum(slot.get("status") in TERMINAL_SLOT_STATES for slot in slots)
    slot_counts = Counter(str(slot.get("status")) for slot in slots)
    valid_forecasts = sum(
        slot.get("forecast") is not None and slot.get("status") != "INVALID"
        for slot in slots
    )
    expected_count = int(manifest["expected_slots"])
    forecast_completeness = valid_forecasts / expected_count * 100.0 if expected_count else 0.0
    evaluation_completeness = len(evaluated) / expected_count * 100.0 if expected_count else 0.0
    if valid_forecasts == expected_count and len(evaluated) == expected_count:
        completeness = COMPLETE_COHORT_LABEL
    elif terminal_count == expected_count:
        completeness = "COMPLETE_WINDOW_WITH_MISSINGNESS"
    else:
        completeness = "VALIDATION_INCOMPLETE"

    locations = _better_equal_worse(per_location, "xgboost_mae_c", "persistence_mae_c")
    hours = _better_equal_worse(per_target_hour, "xgboost_mae_c", "persistence_mae_c")
    model_mae = overall.get("model_mae")
    persistence_mae = overall.get("persistence_mae")
    model_rmse = overall.get("model_rmse")
    persistence_rmse = overall.get("persistence_rmse")
    if model_mae is None or persistence_mae is None or model_rmse is None or persistence_rmse is None:
        skill = "INSUFFICIENT_VALID_EVALUATIONS"
    elif model_mae < persistence_mae and model_rmse < persistence_rmse:
        skill = "POSITIVE_PROSPECTIVE_SKILL"
    elif (model_mae < persistence_mae) != (model_rmse < persistence_rmse):
        skill = "MIXED_PROSPECTIVE_SKILL"
    else:
        skill = "NO_PROSPECTIVE_SKILL_VS_PERSISTENCE"

    target_times = list(manifest["target_times"])
    forecast_hourly: list[dict[str, Any]] = []
    reference_hourly: list[dict[str, Any]] = []
    issuance_hourly: list[dict[str, Any]] = []
    all_leads: list[float] = []
    for target in target_times:
        group = [slot for slot in slots if str(slot.get("target_time")) == target]
        forecast_rows = [
            slot for slot in group
            if slot.get("forecast") and slot.get("status") != "INVALID"
        ]
        issued = [slot for slot in forecast_rows if slot.get("issuance_latency_seconds") is not None]
        latencies = [float(slot["issuance_latency_seconds"]) for slot in issued]
        leads = [float(slot["forecast"]["forecast_lead_seconds"]) for slot in forecast_rows if slot["forecast"].get("forecast_lead_seconds") is not None]
        all_leads.extend(leads)
        latency_stats = _stats(latencies)
        target_dt = parse_utc_hour(target)
        feature_time = target_dt - timedelta(hours=FORECAST_HORIZON_HOURS)
        boundary = feature_time + timedelta(hours=1)
        all_63 = len(issued) == MAX_EXPECTED_LOCATIONS and len({slot["location_id"] for slot in issued}) == MAX_EXPECTED_LOCATIONS
        slo_pass = all_63 and latency_stats["max"] is not None and 0 <= latency_stats["max"] <= 60
        issuance_hourly.append(
            {
                "target_time": target,
                "feature_time": _hour_text(feature_time),
                "issuance_boundary": iso_utc(boundary),
                "first_forecast_persisted_latency_seconds": latency_stats["min"],
                "median_forecast_persisted_latency_seconds": latency_stats["median"],
                "p95_forecast_persisted_latency_seconds": latency_stats["p95"],
                "last_all_63_persisted_latency_seconds": latency_stats["max"] if all_63 else None,
                "locations_persisted": len({slot["location_id"] for slot in issued}),
                "all_63_forecasts_persisted_within_60_seconds": bool(slo_pass),
                "status": "PASS" if slo_pass else "FAIL",
            }
        )
        forecast_hourly.append(
            {
                "target_time": target,
                "expected_locations": MAX_EXPECTED_LOCATIONS,
                "valid_prospective_forecasts": len(forecast_rows),
                "missing_forecasts": sum(slot.get("status") == "FORECAST_MISSING" for slot in group),
                "invalid_forecasts": sum(slot.get("status") == "INVALID" for slot in group),
                "locations_missing": sorted(
                    slot["location_id"] for slot in group if slot.get("status") in {"FORECAST_MISSING", "INVALID"}
                ),
            }
        )
        reference_hourly.append(
            {
                "target_time": target,
                "expected_references": MAX_EXPECTED_LOCATIONS,
                "valid_evaluations": sum(slot.get("status") == "EVALUATED" for slot in group),
                "missing_references": sum(slot.get("status") == "MISSING_REFERENCE" for slot in group),
                "reference_conflicts": sum(slot.get("status") == "REFERENCE_CONFLICT" for slot in group),
                "pending": sum(
                    slot.get("status") in {"PENDING_TARGET_TIME", "REFERENCE_PENDING"}
                    for slot in group
                ),
                "locations_missing_reference": sorted(
                    slot["location_id"] for slot in group if slot.get("status") == "MISSING_REFERENCE"
                ),
            }
        )

    reference_lags = [float(row["reference_arrival_lag_seconds"]) for row in evaluated if row.get("reference_arrival_lag_seconds") is not None]
    evaluation_lags = [float(row["evaluation_lag_seconds"]) for row in evaluated if row.get("evaluation_lag_seconds") is not None]
    unique_revisions = {str(row.get("revision_id")): dict(row) for row in revisions if row.get("revision_id")}
    target_start = parse_utc_hour(manifest["cohort_start_target_time"]) - timedelta(hours=FORECAST_HORIZON_HOURS)
    target_end = parse_utc_hour(manifest["cohort_end_target_time"])
    cohort_revisions = []
    for revision in unique_revisions.values():
        try:
            event_time = parse_utc_hour(revision.get("event_time"), assume_naive_utc=True)
        except (TypeError, ValueError):
            continue
        if target_start <= event_time <= target_end:
            cohort_revisions.append(revision)

    duplicate_info = status.get("duplicate_validation", {})
    provenance = status.get("provenance_validation", {})
    metrics = {
        "cohort_id": manifest["cohort_id"],
        "status": "FINALIZED",
        "pipeline_data_validity": "PASS" if not provenance.get("blocking_violations") else "FAIL",
        "model_skill_classification": skill,
        "cohort_completeness_classification": completeness,
        "expected_slots": expected_count,
        "prospective_forecasts_created": valid_forecasts,
        "valid_evaluations": len(evaluated),
        "missing_forecasts": slot_counts.get("FORECAST_MISSING", 0),
        "missing_references": slot_counts.get("MISSING_REFERENCE", 0),
        "reference_conflicts": slot_counts.get("REFERENCE_CONFLICT", 0),
        "invalid_provenance": slot_counts.get("INVALID", 0),
        "evaluation_completeness_pct": evaluation_completeness,
        "forecast_completeness_pct": forecast_completeness,
        "xgboost": {
            "mae_c": overall.get("model_mae"),
            "rmse_c": overall.get("model_rmse"),
            "r2": overall.get("model_r2"),
            "bias_c": overall.get("model_bias"),
        },
        "persistence": {
            "mae_c": overall.get("persistence_mae"),
            "rmse_c": overall.get("persistence_rmse"),
            "r2": overall.get("persistence_r2"),
            "bias_c": overall.get("persistence_bias"),
        },
        "skill_vs_persistence": {
            "mae_improvement_c": overall.get("mae_improvement_c"),
            "mae_improvement_pct": overall.get("mae_improvement_pct"),
            "mae_skill": overall.get("mae_skill"),
            "rmse_reduction_c": overall.get("rmse_improvement_c"),
            "rmse_reduction_pct": overall.get("rmse_improvement_pct"),
        },
        "locations_better_equal_worse": locations,
        "target_hours_better_equal_worse": hours,
        "reference_semantics": manifest["prospective_reference"],
        "offline_target_semantics": manifest["offline_target"],
        "statistical_scope": "168 consecutive UTC target hours over seven days; not a long-term nationwide guarantee",
        "duplicate_validation": duplicate_info,
        "provenance_validation": provenance,
    }

    artifact_payloads: dict[str, Any] = {
        "cohort_manifest.json": dict(manifest),
        "cohort_status.json": dict(status),
        "model_contract_validation.json": dict(runtime_contract),
        "provider_contract_validation.json": {
            "status": "PASS" if provenance.get("provider_model_violations", 0) == 0 else "FAIL",
            "provider": PROVIDER_NAME,
            "endpoint": LIVE_ENDPOINT,
            "provider_model": PROVIDER_MODEL,
            "forecast_origin_required": "LIVE_PROSPECTIVE",
            "provider_model_violations": provenance.get("provider_model_violations", 0),
        },
        "hourly_forecast_completeness.json": {
            "expected_target_hours": EXPECTED_TARGET_HOURS,
            "expected_locations_per_target_hour": MAX_EXPECTED_LOCATIONS,
            "hours": forecast_hourly,
        },
        "hourly_reference_completeness.json": {
            "expected_target_hours": EXPECTED_TARGET_HOURS,
            "expected_references_per_target_hour": MAX_EXPECTED_LOCATIONS,
            "hours": reference_hourly,
        },
        "issuance_slo.json": {
            "definition": "forecast_persisted_at - (feature_time + 1h); measured after Delta MERGE completed",
            "target_seconds": 60,
            "status": "PASS" if issuance_hourly and all(row["status"] == "PASS" for row in issuance_hourly) else "FAIL",
            "cycles_passed": sum(row["status"] == "PASS" for row in issuance_hourly),
            "cycles_failed": sum(row["status"] == "FAIL" for row in issuance_hourly),
            "hours": issuance_hourly,
        },
        "lead_time_summary.json": {
            "execution_origin": "LIVE_PROSPECTIVE",
            "unit": "seconds",
            **_stats(all_leads),
        },
        "reference_latency.json": {
            "definition": "reference_ingestion_time - target_time",
            "unit": "seconds",
            **_stats(reference_lags),
        },
        "evaluation_latency.json": {
            "definition": "evaluation_time - target_time",
            "unit": "seconds",
            **_stats(evaluation_lags),
        },
        "prospective_metrics.json": metrics,
        "per_location_metrics.json": {
            "locations_expected": MAX_EXPECTED_LOCATIONS,
            "locations": per_location,
            "better_equal_worse": locations,
        },
        "per_target_hour_metrics.json": {
            "target_hours_expected": EXPECTED_TARGET_HOURS,
            "hours": per_target_hour,
            "better_equal_worse": hours,
        },
        "missingness.json": {
            "expected_slots": expected_count,
            "status_counts": dict(sorted(slot_counts.items())),
            "forecast_missing_slots": [
                {"target_time": slot["target_time"], "location_id": slot["location_id"]}
                for slot in slots
                if slot.get("status") == "FORECAST_MISSING"
            ],
            "reference_missing_slots": [
                {"target_time": slot["target_time"], "location_id": slot["location_id"]}
                for slot in slots
                if slot.get("status") == "MISSING_REFERENCE"
            ],
            "reference_conflicts": [
                {"target_time": slot["target_time"], "location_id": slot["location_id"]}
                for slot in slots
                if slot.get("status") == "REFERENCE_CONFLICT"
            ],
            "invalid_slots": [
                {"target_time": slot["target_time"], "location_id": slot["location_id"], "reason": slot.get("invalid_reason")}
                for slot in slots
                if slot.get("status") == "INVALID"
            ],
        },
        "duplicate_validation.json": dict(duplicate_info),
        "provenance_validation.json": dict(provenance),
        "reference_revision_audit.json": {
            "revision_count": len(cohort_revisions),
            "conflicted_reference_key_count": len({_revision_key(row) for row in cohort_revisions if _revision_key(row)}),
            "policy": "first archived payload is retained; later differing payloads are audited and never replace the original",
            "revisions": cohort_revisions,
        },
        "delayed_era5_manifest.json": {
            "cohort_id": manifest["cohort_id"],
            "model_id": MODEL_ID,
            "model_sha256": MODEL_SHA256,
            "offline_target": "ERA5 reanalysis",
            "status": "PREPARED_NOT_FETCHED",
            "future_reference_policy": "Join the frozen cohort's exact location_id and target_time to an ERA5 manifest when available; do not alter immediate Open-Meteo prospective metrics.",
            "source_evaluations_path": "prospective_evaluations.parquet",
            "required_row_identity": ["cohort_id", "location_id", "feature_time", "target_time", "model_prediction_temperature_c", "persistence_prediction_temperature_c"],
        },
        "runtime_summary.json": {
            "run_id": manifest["run_id"],
            "cohort_id": manifest["cohort_id"],
            "started_process_at": status.get("process_started_at"),
            "finalized_at": status.get("finalized_at"),
            "last_update_at": status.get("last_update_at"),
            "microbatch_update_count": status.get("microbatch_update_count"),
            "outages": status.get("outages", []),
            "collector_errors": status.get("collector_errors", []),
            "restart_count": status.get("restart_count", 0),
        },
        "unit_regression_tests.json": dict(preflight_tests or {"status": "NOT_RECORDED"}),
    }
    for name, payload in artifact_payloads.items():
        _atomic_json(run_results / name, payload)

    parquet_path = run_results / "prospective_evaluations.parquet"
    parquet_rows = [_slot_output_row(slot, manifest) for slot in slots]
    _write_evaluations_parquet(spark, parquet_path, parquet_rows)
    checksums = _checksum_tree(run_results)
    checksums["cohort_id"] = manifest["cohort_id"]
    checksums["parquet_path"] = str(parquet_path)
    _atomic_json(run_results / "checksums.json", checksums)
    return {"results_path": str(run_results), "checksums": checksums}


def update_cohort_state(
    *,
    state_dir: str | Path,
    now: datetime,
    forecasts: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    observations: Sequence[Mapping[str, Any]],
    revisions: Sequence[Mapping[str, Any]],
    rejected_observations: Sequence[Mapping[str, Any]],
    known_location_ids: set[str] | frozenset[str],
    runtime_contract: Mapping[str, Any],
    results_root: str | Path = DEFAULT_RESULTS_ROOT,
    preflight_tests: Mapping[str, Any] | None = None,
    spark: Any = None,
) -> dict[str, Any]:
    state_root = Path(state_dir)
    request = _read_json(state_root / "start_request.json")
    if not request:
        return {"status": "NO_COHORT_REQUEST", "run_id": state_root.name}

    request_protocol_errors = _start_request_protocol_errors(request, expected_run_id=state_root.name)
    if request_protocol_errors:
        return {
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": state_root.name,
            "expected_cohort_protocol_id": COHORT_PROTOCOL_ID,
            "protocol_errors": request_protocol_errors,
            "finalization_allowed": False,
        }
    request_contract_errors = _start_request_contract_errors(request)
    if request_contract_errors:
        return {
            "status": "COHORT_CONTRACT_DRIFT",
            "run_id": state_root.name,
            "contract_errors": request_contract_errors,
            "finalization_allowed": False,
        }

    now = parse_utc_timestamp(now, assume_naive_utc=True)
    old_state = _read_json(state_root / "cohort_state.json", {}) or {}
    prior_status = _read_json(state_root / "cohort_status.json", {}) or {}
    if not isinstance(old_state, Mapping) or not isinstance(prior_status, Mapping):
        return {
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": state_root.name,
            "protocol_errors": ["INVALID_COHORT_STATE_OR_STATUS"],
            "finalization_allowed": False,
        }
    manifest_path = state_root / "cohort_manifest.json"
    manifest_exists = manifest_path.exists()
    manifest = _read_json(manifest_path)
    if manifest_exists and not isinstance(manifest, Mapping):
        return {
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": state_root.name,
            "protocol_errors": ["INVALID_FROZEN_COHORT_MANIFEST"],
            "finalization_allowed": False,
        }

    if manifest_exists:
        protocol_errors = _manifest_request_protocol_errors(
            request,
            manifest,
            expected_run_id=state_root.name,
        )
        if old_state.get("manifest") is not None and old_state.get("manifest") != manifest:
            protocol_errors.append("COHORT_STATE_MANIFEST_MISMATCH")
        if prior_status.get("run_id") not in (None, state_root.name):
            protocol_errors.append("COHORT_STATUS_RUN_ID_MISMATCH")
        if prior_status.get("cohort_id") not in (None, manifest.get("cohort_id")):
            protocol_errors.append("COHORT_STATUS_COHORT_ID_MISMATCH")
        if protocol_errors:
            drift = {
                "status": "COHORT_PROTOCOL_DRIFT",
                "cohort_id": manifest.get("cohort_id"),
                "run_id": state_root.name,
                "last_update_at": iso_utc(now),
                "expected_cohort_protocol_id": COHORT_PROTOCOL_ID,
                "protocol_errors": sorted(set(protocol_errors)),
                "finalization_allowed": False,
            }
            _atomic_json(state_root / "protocol_drift.json", drift)
            _atomic_json(state_root / "cohort_status.json", drift)
            return drift
        contract_errors = _manifest_request_contract_errors(request, manifest)
        if contract_errors:
            drift = {
                "status": "COHORT_CONTRACT_DRIFT",
                "cohort_id": manifest.get("cohort_id"),
                "run_id": state_root.name,
                "last_update_at": iso_utc(now),
                "contract_errors": sorted(set(contract_errors)),
                "finalization_allowed": False,
            }
            _atomic_json(state_root / "cohort_status.json", drift)
            return drift
    elif (state_root / "cohort_state.json").exists():
        return {
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": state_root.name,
            "protocol_errors": ["COHORT_STATE_EXISTS_WITHOUT_FROZEN_MANIFEST"],
            "finalization_allowed": False,
        }
    elif prior_status.get("run_id") not in (None, state_root.name):
        return {
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": state_root.name,
            "protocol_errors": ["COHORT_STATUS_RUN_ID_MISMATCH"],
            "finalization_allowed": False,
        }
    elif prior_status.get("cohort_id") is not None or prior_status.get("target_hours_frozen") is True:
        return {
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": state_root.name,
            "protocol_errors": ["COHORT_STATUS_HAS_FROZEN_WINDOW_WITHOUT_MANIFEST"],
            "finalization_allowed": False,
        }

    if runtime_contract.get("status") != "PASS" or runtime_contract.get("contract_fingerprint") != request.get("contract_fingerprint"):
        drift = {
            "status": "COHORT_CONTRACT_DRIFT",
            "cohort_id": (manifest or {}).get("cohort_id"),
            "run_id": state_root.name,
            "last_update_at": iso_utc(now),
            "expected_fingerprint": request.get("contract_fingerprint"),
            "actual_contract": dict(runtime_contract),
            "finalization_allowed": False,
        }
        _atomic_json(state_root / "cohort_status.json", drift)
        return drift

    if not manifest_exists:
        manifest = freeze_cohort_manifest(
            forecasts,
            receipts,
            request=request,
            known_location_ids=known_location_ids,
            frozen_at=now,
        )
        if not manifest:
            pending = {
                "status": "WAITING_FOR_VALID_CYCLE",
                "run_id": request.get("run_id"),
                "start_requested_at": request.get("start_requested_at"),
                "expected_locations": len(known_location_ids),
                "target_hours_frozen": False,
                "last_update_at": iso_utc(now),
                "process_started_at": iso_utc(PROCESS_STARTED_AT),
                "ready_cycles_observed": sorted(_cycle_groups(forecasts, receipts, location_ids=known_location_ids, requested_at=_request_time(request))),
            }
            _atomic_json(state_root / "cohort_status.json", pending)
            return pending
        _atomic_json(state_root / "cohort_manifest.json", manifest)
    if old_state.get("status") == "FINALIZED":
        if spark is not None:
            results_path = Path(results_root) / str(manifest["run_id"])
            if not (results_path / "checksums.json").is_file():
                status = _read_json(state_root / "cohort_status.json", {}) or old_state
                _final_artifacts(
                    spark,
                    manifest=manifest,
                    slots=list((old_state.get("slots") or {}).values()),
                    status=status,
                    runtime_contract=runtime_contract,
                    revisions=revisions,
                    preflight_tests=preflight_tests,
                    results_root=Path(results_root),
                )
        return old_state

    slots = _state_slots(manifest, old_state)
    receipts_by_id = _receipt_map(receipts)
    window_forecasts: list[Mapping[str, Any]] = []
    excluded_origins = Counter()
    out_of_catalog: list[dict[str, Any]] = []
    invalid_time_rows = 0
    for row in forecasts:
        origin = str(row.get("execution_origin") or "UNCLASSIFIED")
        try:
            target = parse_utc_hour(row.get("target_time"), assume_naive_utc=True)
        except (TypeError, ValueError):
            if origin == "LIVE_PROSPECTIVE":
                invalid_time_rows += 1
            continue
        if not _within_window(target, manifest):
            continue
        if origin != "LIVE_PROSPECTIVE":
            excluded_origins[origin] += 1
            continue
        window_forecasts.append(row)
        if str(row.get("location_id") or "") not in known_location_ids:
            out_of_catalog.append({"forecast_id": row.get("forecast_id"), "location_id": row.get("location_id"), "target_time": row.get("target_time")})

    by_slot: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_id: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_logical: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    provider_violations = 0
    offset_violations = 0
    nonpositive_leads = 0
    invalid_record_count = 0
    for row in window_forecasts:
        forecast_id = str(row.get("forecast_id") or "")
        location = str(row.get("location_id") or "")
        try:
            target = _hour_text(row.get("target_time"))
        except (TypeError, ValueError):
            invalid_record_count += 1
            continue
        if location not in known_location_ids:
            continue
        by_slot[_slot_key(target, location)].append(row)
        if forecast_id:
            by_id[forecast_id].append(row)
        by_logical[(target, location)].append(row)
        if row.get("provider") != PROVIDER_NAME or row.get("provider_model") != PROVIDER_MODEL or row.get("provider_endpoint") != LIVE_ENDPOINT or row.get("source") != LIVE_SOURCE:
            provider_violations += 1
        try:
            feature_time = parse_utc_hour(row.get("feature_time"), assume_naive_utc=True)
            target_time = parse_utc_hour(row.get("target_time"), assume_naive_utc=True)
            if (target_time - feature_time).total_seconds() != FORECAST_HORIZON_HOURS * 3600:
                offset_violations += 1
        except (TypeError, ValueError):
            offset_violations += 1
        try:
            lead_seconds = float(row.get("forecast_lead_seconds"))
            if not math.isfinite(lead_seconds) or lead_seconds <= 0:
                nonpositive_leads += 1
        except (TypeError, ValueError):
            nonpositive_leads += 1

    duplicate_ids = sum(max(0, len(rows) - 1) for rows in by_id.values())
    duplicate_logical = sum(max(0, len({str(row.get("forecast_id") or "") for row in rows}) - 1) for rows in by_logical.values())
    duplicate_receipts = sum(max(0, len(items) - 1) for items in receipts_by_id.values())
    evaluation_ids: list[str] = []
    cohort_first = parse_utc_hour(manifest["cohort_start_target_time"])
    reference_conflict_keys, revision_by_id = _revision_index(revisions)
    rejection_conflict_keys: set[tuple[str, str, str]] = set()
    for rejected in rejected_observations:
        if str(rejected.get("reason") or "") != "DUPLICATE_CONFLICT":
            continue
        try:
            key = observation_key(
                LIVE_REFERENCE_SOURCE,
                str(rejected.get("location_id") or ""),
                rejected.get("event_time"),
                assume_naive_utc=True,
            )
        except (TypeError, ValueError):
            continue
        try:
            conflict_time = parse_utc_hour(key[2])
        except ValueError:
            continue
        if cohort_first - timedelta(hours=FORECAST_HORIZON_HOURS) <= conflict_time <= parse_utc_hour(manifest["cohort_end_target_time"]):
            rejection_conflict_keys.add(key)
    reference_conflict_keys.update(rejection_conflict_keys)

    for key, slot in slots.items():
        if slot.get("status") in TERMINAL_SLOT_STATES:
            prior_evaluation = slot.get("evaluation") or {}
            if prior_evaluation.get("evaluation_id"):
                evaluation_ids.append(str(prior_evaluation["evaluation_id"]))
            continue
        target_time = parse_utc_hour(slot["target_time"])
        issue_boundary = target_time - timedelta(hours=1)
        slot["issuance_boundary"] = iso_utc(issue_boundary)
        candidates = by_slot.get(key, [])
        if slot.get("forecast") is None and candidates:
            distinct = {str(row.get("forecast_id") or "") for row in candidates}
            if len(distinct) > 1:
                slot["status"] = "INVALID"
                slot["invalid_reason"] = "DUPLICATE_LOGICAL_FORECAST"
                continue
            forecast = dict(candidates[0])
            receipt, receipt_errors = _receipt_for_forecast(forecast, receipts_by_id)
            errors = _forecast_errors(forecast, known_location_ids) + receipt_errors
            if receipt:
                if _forecast_record_sha256(forecast) != str(receipt.get("record_sha256") or ""):
                    errors.append("PERSISTENCE_RECORD_HASH_MISMATCH")
            if errors:
                slot["status"] = "INVALID"
                slot["invalid_reason"] = ";".join(sorted(set(errors)))
                slot["forecast"] = forecast
                continue
            persisted_at = parse_utc_timestamp(receipt["forecast_persisted_at"], assume_naive_utc=True)
            inference_at = parse_utc_timestamp(forecast["inference_time"], assume_naive_utc=True)
            if persisted_at < _request_time(request) or inference_at < _request_time(request):
                slot["status"] = "INVALID"
                slot["invalid_reason"] = "FORECAST_CREATED_BEFORE_COHORT_REQUEST"
                slot["forecast"] = forecast
                continue
            slot["forecast"] = forecast
            slot["forecast_persisted_at"] = iso_utc(persisted_at)
            slot["issuance_latency_seconds"] = (persisted_at - issue_boundary).total_seconds()
            slot["status"] = "FORECAST_AVAILABLE"
            slot["invalid_reason"] = None
        elif slot.get("forecast") is None:
            if now >= issue_boundary + timedelta(seconds=int(manifest["forecast_grace_period_seconds"])):
                slot["status"] = "FORECAST_MISSING"
            else:
                slot["status"] = "FORECAST_PENDING"

    references = [item for row in observations if (item := _reference_observation(row)) is not None]
    provider_reference_violations = sum(
        1
        for row in observations
        if _within_reference_window(row.get("event_time"), manifest)
        and (
            row.get("source") != LIVE_REFERENCE_SOURCE
            or row.get("provider") != PROVIDER_NAME
            or row.get("provider_model") != PROVIDER_MODEL
            or row.get("provider_endpoint") != LIVE_ENDPOINT
        )
    )
    pending_slots = [
        slot for slot in slots.values()
        if slot.get("forecast") is not None and slot.get("status") not in TERMINAL_SLOT_STATES
    ]
    evaluation_result = evaluate_forecasts(
        [slot["forecast"] for slot in pending_slots],
        references,
        evaluation_time=now,
        known_location_ids=set(known_location_ids),
        conflicted_reference_keys=reference_conflict_keys,
        cohort_id=str(manifest["cohort_id"]),
        assume_naive_utc=True,
    ) if pending_slots else {"evaluations": []}
    evaluations_by_id = {str(row.get("forecast_id") or ""): row for row in evaluation_result["evaluations"]}
    for slot in pending_slots:
        forecast_id = str(slot["forecast"].get("forecast_id") or "")
        evaluation = evaluations_by_id.get(forecast_id)
        if not evaluation:
            slot["status"] = "INVALID"
            slot["invalid_reason"] = "EVALUATOR_DID_NOT_RETURN_FORECAST"
            continue
        if evaluation.get("evaluation_id"):
            evaluation_ids.append(str(evaluation["evaluation_id"]))
        evaluation_status = str(evaluation.get("status") or "")
        if evaluation_status == "EVALUATED":
            slot["status"] = "EVALUATED"
            slot["evaluation"] = evaluation
            continue
        if evaluation_status == "REFERENCE_CONFLICT":
            slot["status"] = "REFERENCE_CONFLICT"
            slot["evaluation"] = evaluation
            slot["invalid_reason"] = evaluation.get("invalid_reason")
            continue
        if evaluation_status == "INVALID_PROVENANCE":
            slot["status"] = "INVALID"
            slot["evaluation"] = evaluation
            slot["invalid_reason"] = evaluation.get("invalid_reason")
            continue
        reference_deadline = parse_utc_hour(slot["target_time"]) + timedelta(seconds=int(manifest["reference_grace_period_seconds"]))
        slot["evaluation"] = evaluation
        if evaluation_status == "PENDING_TARGET_TIME":
            slot["status"] = "PENDING_TARGET_TIME"
            slot["invalid_reason"] = None
        elif now >= reference_deadline:
            slot["status"] = "MISSING_REFERENCE"
            slot["invalid_reason"] = evaluation.get("invalid_reason") or "REFERENCE_GRACE_EXPIRED"
        else:
            slot["status"] = "REFERENCE_PENDING"
            slot["invalid_reason"] = None

    duplicate_eval_ids = len(evaluation_ids) - len(set(evaluation_ids))
    slot_rows = list(slots.values())
    counts = Counter(str(slot.get("status")) for slot in slot_rows)
    terminal_count = sum(counts[state] for state in TERMINAL_SLOT_STATES)
    eval_rows = [slot["evaluation"] for slot in slot_rows if slot.get("status") == "EVALUATED" and slot.get("evaluation")]
    interim_metrics = calculate_metrics(eval_rows, minimum_sample=1)
    contract_violations = 0
    for slot in slot_rows:
        if slot.get("status") == "INVALID":
            contract_violations += 1
    blocking = []
    if duplicate_ids or duplicate_logical or duplicate_receipts or duplicate_eval_ids:
        blocking.append("DUPLICATE_CANONICAL_RECORDS")
    if offset_violations:
        blocking.append("TARGET_OFFSET_VIOLATIONS")
    if nonpositive_leads:
        blocking.append("NONPOSITIVE_LIVE_LEADS")
    if provider_violations or provider_reference_violations:
        blocking.append("PROVIDER_MODEL_VIOLATIONS")
    if out_of_catalog:
        blocking.append("UNKNOWN_LOCATION_RECORDS")
    if invalid_time_rows:
        blocking.append("INVALID_TARGET_TIME_RECORDS")
    if contract_violations:
        blocking.append("INVALID_PROVENANCE_SLOTS")
    duplicate_report = {
        "duplicate_forecast_ids": duplicate_ids,
        "duplicate_logical_forecasts": duplicate_logical,
        "duplicate_persistence_receipts": duplicate_receipts,
        "duplicate_evaluation_ids": duplicate_eval_ids,
        "status": "PASS" if not (duplicate_ids or duplicate_logical or duplicate_receipts or duplicate_eval_ids) else "FAIL",
    }
    provenance_report = {
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_count": FEATURE_COUNT,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "provider": PROVIDER_NAME,
        "provider_model": PROVIDER_MODEL,
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
        "forecast_origin_required": "LIVE_PROSPECTIVE",
        "provider_model_violations": provider_violations + provider_reference_violations,
        "forecast_provider_model_violations": provider_violations,
        "reference_provider_model_violations": provider_reference_violations,
        "target_offset_violations": offset_violations,
        "nonpositive_lead_violations": nonpositive_leads,
        "invalid_forecast_records": invalid_record_count,
        "invalid_target_time_records": invalid_time_rows,
        "unknown_location_records": out_of_catalog,
        "excluded_nonprospective_forecasts": dict(excluded_origins),
        "invalid_provenance_slots": contract_violations,
        "blocking_violations": blocking,
        "status": "PASS" if not blocking else "FAIL",
    }
    now_text = iso_utc(now)
    prior_update = old_state.get("last_update_at")
    outages = list(old_state.get("outages") or [])
    cadence_warnings = list(old_state.get("cadence_warnings") or [])
    last_cycle_classification = None
    prior_process_started = old_state.get("process_started_at")
    process_restarted = bool(prior_process_started and prior_process_started != iso_utc(PROCESS_STARTED_AT))
    restart_count = int(old_state.get("restart_count", 0))
    if process_restarted:
        restart_count += 1
    if prior_update:
        try:
            prior_dt = parse_utc_timestamp(prior_update, assume_naive_utc=True)
            last_cycle_classification = classify_hourly_cycle_gap(
                prior_dt,
                now,
                process_restarted=process_restarted,
                process_started_at=PROCESS_STARTED_AT if process_restarted else None,
                cadence_seconds=int(os.getenv("WEATHER_PROSPECTIVE_EXPECTED_CADENCE_SECONDS", "3600")),
                grace_seconds=int(os.getenv("WEATHER_PROSPECTIVE_CADENCE_GRACE_SECONDS", "900")),
            )
            if last_cycle_classification["late_by_seconds"] > 0:
                warning = {
                    "warning_id": hashlib.sha256(
                        f"{manifest['run_id']}|{prior_update}|{now_text}|hourly-cadence-warning".encode()
                    ).hexdigest(),
                    "run_id": manifest["run_id"],
                    "last_successful_update_at": prior_update,
                    "observed_at": now_text,
                    **last_cycle_classification,
                    "status": "LATE_CYCLE_WITHIN_GRACE" if not last_cycle_classification["is_outage"] else "LATE_CYCLE",
                }
                if _append_jsonl_once(state_root / "cadence_warnings.jsonl", warning, id_field="warning_id"):
                    cadence_warnings.append(warning)
            if last_cycle_classification["is_outage"]:
                outage = {
                    "outage_id": hashlib.sha256(
                        f"{manifest['run_id']}|{prior_update}|{now_text}|{last_cycle_classification['classification']}".encode()
                    ).hexdigest(),
                    "run_id": manifest["run_id"],
                    "outage_start_estimate": last_cycle_classification["outage_start_estimate"],
                    "outage_detected_at": now_text,
                    "duration_seconds_estimate": last_cycle_classification["duration_seconds_estimate"],
                    "reason": last_cycle_classification["classification"],
                    "timing_quality": last_cycle_classification["timing_quality"],
                    "gap_seconds": last_cycle_classification["gap_seconds"],
                    "expected_cadence_seconds": last_cycle_classification["expected_cadence_seconds"],
                    "grace_period_seconds": last_cycle_classification["grace_period_seconds"],
                    "process_restarted": last_cycle_classification["process_restarted"],
                }
                if _append_jsonl_once(state_root / "outages.jsonl", outage, id_field="outage_id"):
                    outages.append(outage)
        except (TypeError, ValueError):
            pass
    status_text = "FINALIZED" if terminal_count == EXPECTED_SLOTS else "COLLECTING"
    status = {
        "status": status_text,
        "run_id": manifest["run_id"],
        "cohort_id": manifest["cohort_id"],
        "cohort_start_target_time": manifest["cohort_start_target_time"],
        "cohort_end_target_time": manifest["cohort_end_target_time"],
        "expected_target_hours": EXPECTED_TARGET_HOURS,
        "expected_locations": MAX_EXPECTED_LOCATIONS,
        "expected_slots": EXPECTED_SLOTS,
        "prospective_forecast_count": sum(
            slot.get("forecast") is not None and slot.get("status") != "INVALID"
            for slot in slot_rows
        ),
        "valid_evaluation_count": counts["EVALUATED"],
        "terminal_slots": terminal_count,
        "completed_target_hours": sum(
            all(slots[_slot_key(target, location)].get("status") in TERMINAL_SLOT_STATES for location in manifest["canonical_locations"])
            for target in manifest["target_times"]
        ),
        "slot_status_counts": dict(sorted(counts.items())),
        "pending_target": sum(slot.get("status") in {"FORECAST_PENDING", "PENDING_TARGET_TIME"} for slot in slot_rows),
        "pending_reference": counts["REFERENCE_PENDING"],
        "missing_forecasts": counts["FORECAST_MISSING"],
        "missing_references": counts["MISSING_REFERENCE"],
        "reference_conflicts": counts["REFERENCE_CONFLICT"],
        "invalid_provenance": counts["INVALID"],
        "duplicate_validation": duplicate_report,
        "provenance_validation": provenance_report,
        "reference_revision_count": len(revision_by_id),
        "reference_conflict_key_count": len(reference_conflict_keys),
        "interim_metrics_label": "INTERIM",
        "interim_metrics": interim_metrics,
        "last_update_at": now_text,
        "process_started_at": iso_utc(PROCESS_STARTED_AT),
        "microbatch_update_count": int(old_state.get("microbatch_update_count", 0)) + 1,
        "restart_count": restart_count,
        "outages": outages,
        "cadence_warnings": cadence_warnings,
        "last_cycle_classification": last_cycle_classification,
        "finalization_allowed": terminal_count == EXPECTED_SLOTS,
    }
    if status_text == "FINALIZED":
        status["finalized_at"] = now_text
        if all(slot.get("status") == "EVALUATED" for slot in slot_rows):
            status["cohort_completeness_classification"] = COMPLETE_COHORT_LABEL
        else:
            status["cohort_completeness_classification"] = "COMPLETE_WINDOW_WITH_MISSINGNESS"

    updated_state = {
        "status": status_text,
        "manifest": manifest,
        "slots": slots,
        "last_update_at": now_text,
        "process_started_at": iso_utc(PROCESS_STARTED_AT),
        "microbatch_update_count": status["microbatch_update_count"],
        "restart_count": restart_count,
        "outages": outages,
        "cadence_warnings": cadence_warnings,
        "last_cycle_classification": last_cycle_classification,
        "duplicate_validation": duplicate_report,
        "provenance_validation": provenance_report,
    }
    _atomic_json(state_root / "cohort_state.json", updated_state)
    _atomic_json(state_root / "cohort_status.json", status)
    if status_text == "FINALIZED" and spark is not None:
        _final_artifacts(
            spark,
            manifest=manifest,
            slots=slot_rows,
            status=status,
            runtime_contract=runtime_contract,
            revisions=list(revision_by_id.values()),
            preflight_tests=preflight_tests,
            results_root=Path(results_root),
        )
    return status


def _within_reference_window(value: Any, manifest: Mapping[str, Any]) -> bool:
    try:
        event_time = parse_utc_hour(value, assume_naive_utc=True)
        first = parse_utc_hour(manifest["cohort_start_target_time"]) - timedelta(hours=FORECAST_HORIZON_HOURS)
        last = parse_utc_hour(manifest["cohort_end_target_time"])
        return first <= event_time <= last
    except (TypeError, ValueError):
        return False


def _state_observation_for_revision(row: Mapping[str, Any]) -> dict[str, Any]:
    retrieved = row.get("source_retrieved_at") or row.get("event_time")
    observation = {
        "event_id": row.get("event_id"),
        "source": row.get("source") or LIVE_REFERENCE_SOURCE,
        "location_id": row.get("location_id"),
        "event_time": row.get("event_time"),
        "ingestion_time": retrieved,
        "first_archived_at": retrieved,
        "temperature_c": _row_value(row, "temperature_c", "temperature_2m"),
        "humidity_pct": _row_value(row, "humidity_pct", "relative_humidity_2m"),
        "precipitation_mm": _row_value(row, "precipitation_mm", "precipitation"),
        "pressure_hpa": _row_value(row, "pressure_hpa", "pressure_msl"),
        "wind_speed_kmh": _row_value(row, "wind_speed_kmh", "wind_speed_10m"),
        "wind_gust_kmh": _row_value(row, "wind_gust_kmh", "wind_gusts_10m"),
        "weather_code": row.get("weather_code"),
        "latitude": row.get("latitude"),
        "longitude": row.get("longitude"),
    }
    observation["reference_payload_sha256"] = payload_sha256(observation)
    return observation


def record_reference_revisions_from_spark(
    conflict_frame: Any,
    previous_state: Any,
    output_path: str | Path,
    *,
    state_columns: Sequence[str],
) -> int:
    """Persist first-wins revision evidence before a conflicted batch is released."""
    if conflict_frame is None or conflict_frame.limit(1).count() == 0:
        return 0
    from pyspark.sql import functions as F

    new_df = conflict_frame.alias("new")
    old_df = previous_state.alias("old")
    paired = new_df.join(
        old_df,
        (F.col("new.location_id") == F.col("old.location_id"))
        & (F.col("new.event_time") == F.col("old.event_time")),
        "inner",
    ).select(
        *[F.col(f"new.{name}").alias(f"new_{name}") for name in state_columns],
        *[F.col(f"old.{name}").alias(f"old_{name}") for name in state_columns],
    )
    output = Path(output_path)
    count = 0
    for raw in paired.collect():
        values = raw.asDict(recursive=True)
        old_row = {name: values.get(f"old_{name}") for name in state_columns}
        new_row = {name: values.get(f"new_{name}") for name in state_columns}
        try:
            first = _state_observation_for_revision(old_row)
            later = _state_observation_for_revision(new_row)
            if first["reference_payload_sha256"] == later["reference_payload_sha256"] and old_row.get("provider_model") == new_row.get("provider_model"):
                continue
            record = revision_record(first, later, detected_at=utc_now(), assume_naive_utc=True)
            record.update(
                {
                    "old_provider": old_row.get("provider"),
                    "new_provider": new_row.get("provider"),
                    "old_provider_model": old_row.get("provider_model"),
                    "new_provider_model": new_row.get("provider_model"),
                    "provider_model_violation": new_row.get("provider_model") != PROVIDER_MODEL,
                    "old_payload_hash": old_row.get("payload_hash") or first["reference_payload_sha256"],
                    "new_payload_hash": new_row.get("payload_hash") or later["reference_payload_sha256"],
                    "old_temperature_c": first.get("temperature_c"),
                    "new_temperature_c": later.get("temperature_c"),
                    "old_retrieved_at": iso_utc(old_row.get("source_retrieved_at") or old_row["event_time"]),
                    "new_retrieved_at": iso_utc(new_row.get("source_retrieved_at") or new_row["event_time"]),
                    "location_id": str(first["location_id"]),
                    "target_time": _hour_text(first["event_time"]),
                }
            )
            if _append_jsonl_once(output, record, id_field="revision_id"):
                count += 1
        except (KeyError, TypeError, ValueError):
            continue
    return count


def _delta_rows(spark: Any, path: str | Path, *, columns: Sequence[str] | None = None) -> list[dict[str, Any]]:
    frame = spark.read.format("delta").load(str(path))
    if columns:
        available = set(frame.columns)
        frame = frame.select(*[name for name in columns if name in available])
    return [row.asDict(recursive=True) for row in frame.collect()]


def _persisted_forecast_receipts(
    predictions: Any,
    existing_ids: Any,
    *,
    sink_path: Path,
    merge_forecasts: Any,
) -> tuple[int, float, list[dict[str, Any]]]:
    """Reserved for focused tests; the Spark job performs the Delta merge itself."""
    candidate_ids = {
        str(row["forecast_id"])
        for row in predictions.join(existing_ids, "forecast_id", "left_anti").select("forecast_id").collect()
    }
    started = time.perf_counter()
    merge_forecasts()
    persisted_at = utc_now()
    receipts: list[dict[str, Any]] = []
    for row in predictions.collect():
        record = row.asDict(recursive=True)
        record["forecast_persisted_at"] = iso_utc(persisted_at)
        record["persistence_receipt_kind"] = (
            "NEW_DELTA_WRITE" if str(record.get("forecast_id")) in candidate_ids else "RECOVERED_POST_MERGE_CONFIRMATION"
        )
        record["record_sha256"] = _forecast_record_sha256(record)
        _append_jsonl_once(sink_path, record, id_field="forecast_id")
        receipts.append(record)
    return len(receipts), time.perf_counter() - started, receipts


def record_forecast_persistence_receipts(
    predictions: Any,
    *,
    existing_ids: Any,
    new_forecast_ids: Iterable[str] | None = None,
    sink_path: str | Path,
    persisted_at: datetime | None = None,
) -> int:
    """Record when each forecast was confirmed available after the Delta MERGE.

    Previously persisted rows without a receipt (for example after a driver crash
    between the Delta commit and receipt append) receive a conservative recovery
    confirmation time. The immutable receipt preserves that distinction.
    """
    candidate_ids = (
        {str(forecast_id) for forecast_id in new_forecast_ids}
        if new_forecast_ids is not None
        else {
            str(row["forecast_id"])
            for row in predictions.join(existing_ids, "forecast_id", "left_anti").select("forecast_id").collect()
        }
    )
    confirmed_at = iso_utc(persisted_at or utc_now())
    output = Path(sink_path)
    written = 0
    for row in predictions.collect():
        record = row.asDict(recursive=True)
        record["forecast_persisted_at"] = confirmed_at
        record["persistence_receipt_kind"] = (
            "NEW_DELTA_WRITE" if str(record.get("forecast_id")) in candidate_ids else "RECOVERED_POST_MERGE_CONFIRMATION"
        )
        record["record_sha256"] = _forecast_record_sha256(record)
        if _append_jsonl_once(output, record, id_field="forecast_id"):
            written += 1
    return written


def _runtime_contract_from_environment() -> dict[str, Any]:
    return validate_runtime_contract(
        model_path=os.getenv("WEATHER_T2H_MODEL_PATH") or None,
        feature_list_path=os.getenv("WEATHER_T2H_FEATURE_LIST_PATH") or None,
        model_id=os.getenv("WEATHER_T2H_MODEL_ID", MODEL_ID),
        model_sha256=os.getenv("WEATHER_T2H_MODEL_SHA256", MODEL_SHA256),
        feature_set_id=os.getenv("WEATHER_T2H_FEATURE_SET_ID", FEATURE_SET_ID),
        feature_list_sha256=os.getenv("WEATHER_T2H_FEATURE_LIST_SHA256", FEATURE_LIST_SHA256),
        forecast_horizon_hours=int(os.getenv("WEATHER_T2H_FORECAST_HORIZON_HOURS", str(FORECAST_HORIZON_HOURS))),
        provider_model=os.getenv("WEATHER_T2H_PROVIDER_MODEL", PROVIDER_MODEL),
    )


def record_startup_contract_validation(state_dir: str | Path) -> dict[str, Any]:
    """Persist contract validation before the Spark stream starts processing."""
    state_root = Path(state_dir)
    state_root.mkdir(parents=True, exist_ok=True)
    contract = _runtime_contract_from_environment()
    _atomic_json(state_root / "model_contract_validation.json", contract)
    if contract.get("status") == "PASS":
        return contract
    request = _read_json(state_root / "start_request.json", {}) or {}
    manifest = _read_json(state_root / "cohort_manifest.json", {}) or {}
    if request:
        _atomic_json(
            state_root / "cohort_status.json",
            {
                "status": "COHORT_CONTRACT_DRIFT",
                "run_id": request.get("run_id"),
                "cohort_id": manifest.get("cohort_id"),
                "last_update_at": iso_utc(utc_now()),
                "expected_fingerprint": request.get("contract_fingerprint"),
                "actual_contract": contract,
                "finalization_allowed": False,
            },
        )
    readiness_request = _read_json(state_root / "readiness_request.json", {}) or {}
    if readiness_request:
        _atomic_json(
            state_root / "readiness.json",
            {
                "status": "FAIL",
                "reason": "CANONICAL_CONTRACT_INVALID",
                "run_id": readiness_request.get("run_id"),
                "contract": contract,
            },
        )
    return contract


def record_startup_model_failure(state_dir: str | Path, error: BaseException) -> None:
    """Expose model-load failures to readiness or the active cohort status."""
    state_root = Path(state_dir)
    state_root.mkdir(parents=True, exist_ok=True)
    failure = {
        "status": "MODEL_STARTUP_VALIDATION_FAILED",
        "at": iso_utc(utc_now()),
        "error_type": type(error).__name__,
        "error": str(error),
        "finalization_allowed": False,
    }
    request = _read_json(state_root / "start_request.json", {}) or {}
    if request:
        prior = _read_json(state_root / "cohort_status.json", {}) or {}
        _atomic_json(
            state_root / "cohort_status.json",
            {**prior, **failure, "run_id": request.get("run_id"), "cohort_id": prior.get("cohort_id")},
        )
    readiness_request = _read_json(state_root / "readiness_request.json", {}) or {}
    if readiness_request:
        _atomic_json(
            state_root / "readiness.json",
            {"status": "FAIL", "reason": failure["status"], **failure, "run_id": readiness_request.get("run_id")},
        )


def _readiness_update(
    *,
    state_dir: Path,
    forecasts: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    observations: Sequence[Mapping[str, Any]],
    known_location_ids: set[str] | frozenset[str],
    runtime_contract: Mapping[str, Any],
    startup_validation_path: Path,
) -> dict[str, Any] | None:
    request = _read_json(state_dir / "readiness_request.json")
    if not request:
        return None
    if runtime_contract.get("status") != "PASS":
        result = {"status": "FAIL", "reason": "CANONICAL_CONTRACT_INVALID", "contract": dict(runtime_contract)}
        _atomic_json(state_dir / "readiness.json", result)
        return result
    requested_at = parse_utc_timestamp(request["requested_at"], assume_naive_utc=True)
    groups = _cycle_groups(forecasts, receipts, location_ids=known_location_ids, requested_at=requested_at)
    full_cycle = None
    for target in sorted(groups):
        by_location = {str(row.get("location_id")): row for row in groups[target]}
        if set(by_location) == set(known_location_ids) and len(groups[target]) == MAX_EXPECTED_LOCATIONS:
            full_cycle = [by_location[key] for key in sorted(by_location)]
            break
    if full_cycle is None:
        result = {
            "status": "WAITING",
            "run_id": request.get("run_id"),
            "requested_at": request.get("requested_at"),
            "expected_locations": len(known_location_ids),
            "complete_cycles_observed": sorted(groups),
            "last_checked_at": iso_utc(utc_now()),
        }
        _atomic_json(state_dir / "readiness.json", result)
        return result
    startup_rows = _read_jsonl(startup_validation_path)
    startup = startup_rows[-1] if startup_rows else (_read_json(startup_validation_path, {}) or {})
    if startup.get("status") != "PASS" or startup.get("model_sha256") != MODEL_SHA256:
        result = {"status": "FAIL", "reason": "MODEL_STARTUP_VALIDATION_FAILED", "startup": startup}
        _atomic_json(state_dir / "readiness.json", result)
        return result
    now = utc_now()
    monitoring = evaluate_forecasts(
        full_cycle,
        [_reference_observation(row) for row in observations if _reference_observation(row) is not None],
        evaluation_time=now,
        known_location_ids=set(known_location_ids),
        assume_naive_utc=True,
    )
    monitor_rows = monitoring["evaluations"]
    monitor_status_counts = Counter(str(row.get("status") or "UNCLASSIFIED") for row in monitor_rows)
    monitor_mode_counts = Counter(str(row.get("evaluation_mode") or "UNCLASSIFIED") for row in monitor_rows)
    monitor_invalid_reasons = Counter(
        str(row.get("invalid_reason")) for row in monitor_rows if row.get("invalid_reason")
    )
    monitor_ok = len(monitor_rows) == MAX_EXPECTED_LOCATIONS and all(
        not row.get("invalid_reason") and row.get("evaluation_mode") == "LIVE_PROSPECTIVE"
        for row in monitor_rows
    )
    receipts_by_id = _receipt_map(receipts)
    values: list[float] = []
    errors: list[str] = []
    for forecast in full_cycle:
        errors.extend(_forecast_errors(forecast, known_location_ids))
        receipt, receipt_errors = _receipt_for_forecast(forecast, receipts_by_id)
        errors.extend(receipt_errors)
        if receipt:
            try:
                values.append(float(forecast.get("forecast_lead_seconds")))
            except (TypeError, ValueError):
                errors.append("INVALID_LEAD")
    target_times = {str(row.get("target_time")) for row in full_cycle}
    if len(target_times) != 1:
        errors.append("TARGET_TIME_NOT_UNIFORM")
    if len(values) != MAX_EXPECTED_LOCATIONS or any(value <= 0 for value in values):
        errors.append("LIVE_LEAD_NOT_POSITIVE_OR_INCOMPLETE")
    if not monitor_ok:
        errors.append("MONITORING_EVALUATOR_REJECTED_READINESS_CYCLE")
    result = {
        "status": "PASS" if not errors else "FAIL",
        "run_id": request.get("run_id"),
        "checked_at": iso_utc(now),
        "target_time": _hour_text(next(iter(target_times))) if len(target_times) == 1 else None,
        "forecast_count": len(full_cycle),
        "locations": sorted(str(row.get("location_id")) for row in full_cycle),
        "positive_lead_count": sum(value > 0 for value in values),
        "provider_model": PROVIDER_MODEL,
        "target_offset_violations": sum(
            (parse_utc_hour(row["target_time"], assume_naive_utc=True) - parse_utc_hour(row["feature_time"], assume_naive_utc=True)).total_seconds() != 7200
            for row in full_cycle
        ),
        "monitoring_status": "PASS" if monitor_ok else "FAIL",
        "monitoring_row_count": len(monitor_rows),
        "monitoring_status_counts": dict(sorted(monitor_status_counts.items())),
        "monitoring_mode_counts": dict(sorted(monitor_mode_counts.items())),
        "monitoring_invalid_reasons": dict(sorted(monitor_invalid_reasons.items())),
        "errors": sorted(set(errors)),
        "status_scope": "readiness cycle is isolated from the official cohort",
    }
    _atomic_json(state_dir / "readiness.json", result)
    return result


def update_from_spark(
    spark: Any,
    *,
    forecast_path: str | Path,
    state_path: str | Path,
    rejection_path: str | Path,
    receipts_path: str | Path,
    revision_path: str | Path,
    state_dir: str | Path,
    known_locations: Sequence[Mapping[str, Any]],
    results_root: str | Path,
    startup_validation_path: str | Path,
) -> dict[str, Any] | None:
    state_root = Path(state_dir)
    runtime_contract = _runtime_contract_from_environment()
    if runtime_contract.get("status") != "PASS":
        _atomic_json(state_root / "contract_drift.json", runtime_contract)
    try:
        forecasts = _delta_rows(spark, forecast_path)
        observations_raw = _delta_rows(spark, state_path)
        rejected = _delta_rows(spark, rejection_path)
    except Exception as exc:
        error = {"at": iso_utc(utc_now()), "type": type(exc).__name__, "message": str(exc)}
        _append_jsonl_once(state_root / "collector_errors.jsonl", {**error, "error_id": hashlib.sha256(json.dumps(error, sort_keys=True).encode()).hexdigest()}, id_field="error_id")
        return None
    receipts = _read_jsonl(Path(receipts_path))
    revisions = _read_jsonl(Path(revision_path))
    location_ids = {str(row["location_id"]) for row in known_locations}
    readiness = _readiness_update(
        state_dir=state_root,
        forecasts=forecasts,
        receipts=receipts,
        observations=observations_raw,
        known_location_ids=location_ids,
        runtime_contract=runtime_contract,
        startup_validation_path=Path(startup_validation_path),
    )
    if not _read_json(state_root / "start_request.json"):
        return readiness
    preflight_path = Path(
        os.getenv("WEATHER_PROSPECTIVE_PREFLIGHT_PATH", str(DEFAULT_STATE_ROOT / "preflight_test_results.json"))
    )
    preflight = _read_json(preflight_path)
    return update_cohort_state(
        state_dir=state_root,
        now=utc_now(),
        forecasts=forecasts,
        receipts=receipts,
        observations=observations_raw,
        revisions=revisions,
        rejected_observations=rejected,
        known_location_ids=location_ids,
        runtime_contract=runtime_contract,
        results_root=results_root,
        preflight_tests=preflight,
        spark=spark,
    )


def _run_compose(run_id: str, *, services: Sequence[str] | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["WEATHER_INFERENCE_RUN_ID"] = run_id
    cmd = ["docker", "compose", "--profile", "t2h-live", "up", "-d"]
    if services:
        cmd.extend(services)
    else:
        cmd.extend(
            ["broker", "spark-master", "spark-worker", "live-hourly-producer-t2h", "streaming-inference-t2h-live"]
        )
    return subprocess.run(cmd, cwd=REPOSITORY_ROOT, env=env, text=True, capture_output=True, check=False)


def _stop_readiness_services() -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "t2h-live",
            "stop",
            "live-hourly-producer-t2h",
            "streaming-inference-t2h-live",
        ],
        cwd=REPOSITORY_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _timestamp_run_id(prefix: str) -> str:
    return f"{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{prefix}"


def _write_start_request(
    *,
    state_root: Path,
    run_id: str,
    contract: Mapping[str, Any],
    forecast_grace_period_seconds: int = FORECAST_GRACE_PERIOD_SECONDS,
    reference_grace_period_seconds: int = REFERENCE_GRACE_PERIOD_SECONDS,
    requested_at: datetime | None = None,
) -> Path:
    if forecast_grace_period_seconds <= 0 or reference_grace_period_seconds <= 0:
        raise ValueError("forecast and reference grace periods must be positive")
    run_state = state_root / run_id
    if (run_state / "start_request.json").exists() or (run_state / "cohort_manifest.json").exists():
        raise FileExistsError(f"prospective run state already exists: {run_state}")
    request = {
        "run_id": run_id,
        "start_requested_at": iso_utc(requested_at or utc_now()),
        "cohort_protocol_id": COHORT_PROTOCOL_ID,
        "expected_target_hours": EXPECTED_TARGET_HOURS,
        "expected_locations": MAX_EXPECTED_LOCATIONS,
        "expected_slots": EXPECTED_SLOTS,
        "forecast_grace_period_seconds": int(forecast_grace_period_seconds),
        "forecast_grace_rationale": "15 minutes covers three current 5-minute live producer cycles before freezing a missing forecast slot.",
        "reference_grace_period_seconds": int(reference_grace_period_seconds),
        "reference_grace_rationale": "2 hours from target_time covers the next safe-hour boundary at target_time+1h plus one hour for normal pipeline delay or recovery.",
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_count": FEATURE_COUNT,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "provider_model": PROVIDER_MODEL,
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
        "contract_fingerprint": contract["contract_fingerprint"],
        "runtime_configuration": {
            "compose_profile": "t2h-live",
            "services": [
                "broker",
                "spark-master",
                "spark-worker",
                "live-hourly-producer-t2h",
                "streaming-inference-t2h-live",
            ],
            "input_topic": "weather.hourly.observations.t2h.live.v1",
            "producer_poll_interval_seconds": 300,
            "producer_history_hours": 48,
            "provider": PROVIDER_NAME,
            "provider_endpoint": LIVE_ENDPOINT,
            "provider_model": PROVIDER_MODEL,
            "safe_hour_rule": "floor(current UTC hour) - 1 hour",
            "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
            "forecast_delta_path": f"/opt/project/data/streaming/t2h_v1_1/{run_id}/forecasts",
            "state_delta_path": f"/opt/project/data/streaming/t2h_v1_1/{run_id}/state_live",
            "rejection_delta_path": f"/opt/project/data/streaming/t2h_v1_1/{run_id}/rejected_live",
            "checkpoint_path": f"/opt/project/data/checkpoints/t2h_v1_1/{run_id}/live",
            "training_performed": False,
        },
        "training_performed": False,
    }
    protocol_errors = _start_request_protocol_errors(request, expected_run_id=run_id)
    if protocol_errors:
        raise ValueError(f"COHORT_PROTOCOL_DRIFT: {', '.join(protocol_errors)}")
    contract_errors = _start_request_contract_errors(request)
    if contract_errors:
        raise ValueError(f"COHORT_CONTRACT_DRIFT: {', '.join(contract_errors)}")
    _atomic_json(run_state / "start_request.json", request)
    return run_state


def _default_model_contract() -> dict[str, Any]:
    return validate_runtime_contract(
        model_path=os.getenv("WEATHER_T2H_MODEL_PATH") or None,
        feature_list_path=os.getenv("WEATHER_T2H_FEATURE_LIST_PATH") or None,
    )


def _preflight(args: argparse.Namespace, state_root: Path) -> int:
    commands = [
        ("compileall", [sys.executable, "-m", "compileall", "."]),
        ("pytest", [sys.executable, "-m", "pytest", "-q"]),
        ("git_diff_check", ["git", "diff", "--check"]),
        ("docker_compose_config", ["docker", "compose", "--profile", "t2h-live", "config", "--quiet"]),
    ]
    records: dict[str, Any] = {}
    for name, command in commands:
        started = time.perf_counter()
        completed = subprocess.run(command, cwd=REPOSITORY_ROOT, text=True, capture_output=True, check=False)
        records[name] = {
            "status": "PASS" if completed.returncode == 0 else "FAIL",
            "returncode": completed.returncode,
            "duration_seconds": time.perf_counter() - started,
            "stdout_tail": completed.stdout[-4000:],
            "stderr_tail": completed.stderr[-4000:],
        }
    pytest_output = "\n".join(
        str(records.get("pytest", {}).get(key) or "") for key in ("stdout_tail", "stderr_tail")
    )
    pytest_counts = {
        name: sum(int(value) for value in re.findall(rf"(\d+)\s+{name}\b", pytest_output))
        for name in ("passed", "skipped", "failed", "error")
    }
    summary = {
        "status": "PASS" if records and all(row["status"] == "PASS" for row in records.values()) and len(records) == len(commands) else "FAIL",
        "run_at": iso_utc(utc_now()),
        "commands": records,
        "pytest_counts": pytest_counts,
        "model_training_performed": False,
        "cohort_protocol_id": COHORT_PROTOCOL_ID,
    }
    _atomic_json(state_root / "preflight_test_results.json", summary)
    print(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2))
    return 0 if summary["status"] == "PASS" else 1


def _readiness(args: argparse.Namespace, state_root: Path) -> int:
    contract = _default_model_contract()
    if contract.get("status") != "PASS":
        print(json.dumps(contract, indent=2))
        return 1
    run_id = args.run_id or _timestamp_run_id("prospective-readiness-t2h-v1")
    run_state = state_root / run_id
    run_state.mkdir(parents=True, exist_ok=False)
    request = {
        "run_id": run_id,
        "requested_at": iso_utc(utc_now()),
        "contract_fingerprint": contract["contract_fingerprint"],
        "cohort_protocol_id": COHORT_PROTOCOL_ID,
        "status": "WAITING",
    }
    _atomic_json(run_state / "readiness_request.json", request)
    completed = _run_compose(run_id)
    if completed.returncode:
        stopped = _stop_readiness_services()
        print(completed.stdout)
        print(completed.stderr, file=sys.stderr)
        if stopped.returncode:
            print(stopped.stderr, file=sys.stderr)
        return completed.returncode
    print(completed.stdout)
    deadline = time.monotonic() + max(0, int(args.wait_seconds))
    while time.monotonic() < deadline:
        readiness = _read_json(run_state / "readiness.json")
        if readiness and readiness.get("status") == "PASS":
            readiness["contract_fingerprint"] = contract["contract_fingerprint"]
            readiness["cohort_protocol_id"] = COHORT_PROTOCOL_ID
            readiness["run_id"] = run_id
            stopped = _stop_readiness_services()
            readiness["readiness_services_stopped"] = stopped.returncode == 0
            if stopped.returncode:
                readiness["status"] = "FAIL"
                readiness["cleanup_error"] = stopped.stderr[-4000:]
            _atomic_json(state_root / "readiness.json", readiness)
            print(json.dumps(readiness, indent=2))
            return 0 if readiness["status"] == "PASS" else 1
        if readiness and readiness.get("status") == "FAIL":
            stopped = _stop_readiness_services()
            readiness["readiness_services_stopped"] = stopped.returncode == 0
            if stopped.returncode:
                readiness["cleanup_error"] = stopped.stderr[-4000:]
            print(json.dumps(readiness, indent=2))
            return 1
        time.sleep(READINESS_POLL_SECONDS)
    result = _read_json(run_state / "readiness.json", {"status": "WAITING", "run_id": run_id})
    result["status"] = "READINESS_INCOMPLETE"
    result["waited_seconds"] = int(args.wait_seconds)
    stopped = _stop_readiness_services()
    result["readiness_services_stopped"] = stopped.returncode == 0
    if stopped.returncode:
        result["cleanup_error"] = stopped.stderr[-4000:]
    _atomic_json(state_root / "readiness.json", result)
    print(json.dumps(result, indent=2))
    return 2


def _start(args: argparse.Namespace, state_root: Path) -> int:
    preflight = _read_json(state_root / "preflight_test_results.json")
    readiness = _read_json(state_root / "readiness.json")
    contract = _default_model_contract()
    if not preflight or preflight.get("status") != "PASS" or preflight.get("cohort_protocol_id") != COHORT_PROTOCOL_ID:
        print("Run preflight and pass all blocking checks before starting the official cohort.", file=sys.stderr)
        return 2
    readiness_age: float | None = None
    if readiness and readiness.get("checked_at"):
        try:
            readiness_age = (
                utc_now() - parse_utc_timestamp(readiness["checked_at"], assume_naive_utc=True)
            ).total_seconds()
        except (TypeError, ValueError):
            readiness_age = None
    readiness_passes = bool(
        readiness
        and readiness.get("status") == "PASS"
        and readiness.get("cohort_protocol_id") == COHORT_PROTOCOL_ID
        and readiness.get("contract_fingerprint") == contract.get("contract_fingerprint")
        and readiness_age is not None
        and 0 <= readiness_age <= READINESS_MAX_AGE_SECONDS
        and readiness.get("forecast_count") == MAX_EXPECTED_LOCATIONS
        and readiness.get("positive_lead_count") == MAX_EXPECTED_LOCATIONS
        and readiness.get("target_offset_violations") == 0
        and readiness.get("monitoring_status") == "PASS"
        and readiness.get("provider_model") == PROVIDER_MODEL
    )
    if not readiness_passes:
        print("A fresh 63-location readiness cycle with the current frozen contract is required before cohort start.", file=sys.stderr)
        return 2
    if contract.get("status") != "PASS":
        print(json.dumps(contract, indent=2), file=sys.stderr)
        return 1
    active = _read_json(state_root / "active_run.json")
    if active:
        prior = _read_json(state_root / active["run_id"] / "cohort_status.json", {}) or {}
        if prior.get("status") != "FINALIZED":
            print(f"An unfinished cohort already exists: {active['run_id']}. Use status or resume.", file=sys.stderr)
            return 2
        print("This state root already contains a finalized official cohort; select a new explicit state root for another study.", file=sys.stderr)
        return 2
    run_id = args.run_id or _timestamp_run_id("prospective-live-t2h-v1")
    run_state = _write_start_request(
        state_root=state_root,
        run_id=run_id,
        contract=contract,
        forecast_grace_period_seconds=args.forecast_grace_seconds,
        reference_grace_period_seconds=args.reference_grace_seconds,
    )
    _atomic_json(state_root / "active_run.json", {"run_id": run_id, "started_at": iso_utc(utc_now())})
    completed = _run_compose(run_id)
    if completed.returncode:
        _atomic_json(run_state / "compose_start_error.json", {"returncode": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr})
        print(completed.stdout)
        print(completed.stderr, file=sys.stderr)
        print("The immutable cohort request is retained. Use resume after fixing the service failure.", file=sys.stderr)
        return completed.returncode
    print(completed.stdout)
    print(f"Official collection requested. The cohort window freezes only when a complete valid 63-location target cycle is persisted after {run_state / 'start_request.json'}.")
    return 0


def _active_run(state_root: Path) -> tuple[str, Path] | None:
    active = _read_json(state_root / "active_run.json")
    if not active or not active.get("run_id"):
        return None
    run_id = str(active["run_id"])
    return run_id, state_root / run_id


def _status(args: argparse.Namespace, state_root: Path) -> int:
    active = _active_run(state_root)
    if not active:
        print(json.dumps({"status": "NO_OFFICIAL_COHORT", "readiness": _read_json(state_root / "readiness.json")}, indent=2))
        return 0
    run_id, run_state = active
    status = _read_json(run_state / "cohort_status.json")
    if not status:
        request = _read_json(run_state / "start_request.json", {})
        status = {"status": "WAITING_FOR_VALID_CYCLE", "run_id": run_id, **request}
    print(json.dumps(status, ensure_ascii=False, indent=2))
    return 0


def _resume(args: argparse.Namespace, state_root: Path) -> int:
    active = _active_run(state_root)
    if not active:
        print("No official cohort exists to resume. Run start after readiness.", file=sys.stderr)
        return 2
    run_id, run_state = active
    request = _read_json(run_state / "start_request.json")
    if not request:
        print("The active cohort request is missing; refusing to create a new cohort.", file=sys.stderr)
        return 1

    request_protocol_errors = _start_request_protocol_errors(request, expected_run_id=run_id)
    if request_protocol_errors:
        print(json.dumps({
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": run_id,
            "protocol_errors": request_protocol_errors,
        }, indent=2), file=sys.stderr)
        return 2
    request_contract_errors = _start_request_contract_errors(request)
    if request_contract_errors:
        print(json.dumps({
            "status": "COHORT_CONTRACT_DRIFT",
            "run_id": run_id,
            "contract_errors": request_contract_errors,
        }, indent=2), file=sys.stderr)
        return 1

    manifest_path = run_state / "cohort_manifest.json"
    manifest_exists = manifest_path.exists()
    manifest = _read_json(manifest_path)
    cohort_state_path = run_state / "cohort_state.json"
    cohort_state = _read_json(cohort_state_path, {}) or {}
    prior_status = _read_json(run_state / "cohort_status.json", {}) or {}
    if not isinstance(cohort_state, Mapping) or not isinstance(prior_status, Mapping):
        print(json.dumps({
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": run_id,
            "protocol_errors": ["INVALID_COHORT_STATE_OR_STATUS"],
        }, indent=2), file=sys.stderr)
        return 2
    if manifest_exists and not isinstance(manifest, Mapping):
        print(json.dumps({
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": run_id,
            "protocol_errors": ["INVALID_FROZEN_COHORT_MANIFEST"],
        }, indent=2), file=sys.stderr)
        return 2
    if manifest_exists:
        protocol_errors = _manifest_request_protocol_errors(
            request,
            manifest,
            expected_run_id=run_id,
        )
        if cohort_state_path.exists():
            frozen_state_manifest = cohort_state.get("manifest")
            if frozen_state_manifest is None:
                protocol_errors.append("COHORT_STATE_MISSING_FROZEN_MANIFEST")
            elif frozen_state_manifest != manifest:
                protocol_errors.append("COHORT_STATE_MANIFEST_MISMATCH")
        if prior_status.get("run_id") not in (None, run_id):
            protocol_errors.append("COHORT_STATUS_RUN_ID_MISMATCH")
        if prior_status.get("cohort_id") not in (None, manifest.get("cohort_id")):
            protocol_errors.append("COHORT_STATUS_COHORT_ID_MISMATCH")
        if protocol_errors:
            print(json.dumps({
                "status": "COHORT_PROTOCOL_DRIFT",
                "run_id": run_id,
                "protocol_errors": sorted(set(protocol_errors)),
            }, indent=2), file=sys.stderr)
            return 2
        contract_errors = _manifest_request_contract_errors(request, manifest)
        if contract_errors:
            print(json.dumps({
                "status": "COHORT_CONTRACT_DRIFT",
                "run_id": run_id,
                "contract_errors": sorted(set(contract_errors)),
            }, indent=2), file=sys.stderr)
            return 1
    elif cohort_state_path.exists():
        print(json.dumps({
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": run_id,
            "protocol_errors": ["COHORT_STATE_EXISTS_WITHOUT_FROZEN_MANIFEST"],
        }, indent=2), file=sys.stderr)
        return 2
    elif prior_status.get("run_id") not in (None, run_id):
        print(json.dumps({
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": run_id,
            "protocol_errors": ["COHORT_STATUS_RUN_ID_MISMATCH"],
        }, indent=2), file=sys.stderr)
        return 2
    elif prior_status.get("cohort_id") is not None or prior_status.get("target_hours_frozen") is True:
        print(json.dumps({
            "status": "COHORT_PROTOCOL_DRIFT",
            "run_id": run_id,
            "protocol_errors": ["COHORT_STATUS_HAS_FROZEN_WINDOW_WITHOUT_MANIFEST"],
        }, indent=2), file=sys.stderr)
        return 2

    contract = _default_model_contract()
    if contract.get("status") != "PASS" or contract.get("contract_fingerprint") != request.get("contract_fingerprint"):
        drift = {"status": "COHORT_CONTRACT_DRIFT", "expected": request.get("contract_fingerprint"), "actual": contract}
        _atomic_json(run_state / "contract_drift.json", drift)
        print(json.dumps(drift, indent=2), file=sys.stderr)
        return 1
    last_update = prior_status.get("last_update_at")
    if last_update:
        try:
            gap = (utc_now() - parse_utc_timestamp(last_update, assume_naive_utc=True)).total_seconds()
            if gap > 15 * 60:
                _append_jsonl_once(
                    run_state / "resume_events.jsonl",
                    {
                        "event_id": hashlib.sha256(f"{run_id}|{last_update}|resume".encode()).hexdigest(),
                        "run_id": run_id,
                        "last_successful_update_at": last_update,
                        "resume_requested_at": iso_utc(utc_now()),
                        "outage_duration_estimate_seconds": max(0.0, gap - 300.0),
                        "reason": "resume_after_missing_cohort_heartbeat",
                    },
                    id_field="event_id",
                )
        except (TypeError, ValueError):
            pass
    completed = _run_compose(run_id)
    print(completed.stdout)
    if completed.returncode:
        print(completed.stderr, file=sys.stderr)
    return completed.returncode


def _finalize(args: argparse.Namespace, state_root: Path) -> int:
    active = _active_run(state_root)
    if not active:
        print("No official cohort exists.", file=sys.stderr)
        return 2
    run_id, run_state = active
    status = _read_json(run_state / "cohort_status.json", {}) or {}
    if status.get("status") != "FINALIZED" or not status.get("finalization_allowed"):
        print(json.dumps({"status": "VALIDATION_INCOMPLETE", "run_id": run_id, "current_status": status}, indent=2))
        return 2
    result_dir = DEFAULT_RESULTS_ROOT / run_id
    required = ("cohort_manifest.json", "prospective_metrics.json", "checksums.json", "prospective_evaluations.parquet")
    missing = [name for name in required if not (result_dir / name).exists()]
    if missing:
        print(json.dumps({"status": "FINAL_ARTIFACTS_MISSING", "paths": missing}, indent=2), file=sys.stderr)
        return 1
    print(json.dumps({"status": "FINALIZED", "run_id": run_id, "results_path": str(result_dir)}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect a frozen, strictly prospective T2H live cohort.")
    parser.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    preflight = subparsers.add_parser("preflight", help="run required compile, regression, Compose, and diff checks")
    preflight.set_defaults(handler=_preflight)
    readiness = subparsers.add_parser("readiness", help="run an isolated 63-location live readiness cycle")
    readiness.add_argument("--run-id")
    readiness.add_argument("--wait-seconds", type=int, default=1200)
    readiness.set_defaults(handler=_readiness)
    start = subparsers.add_parser("start", help="freeze T0 from the first complete valid cycle after this request")
    start.add_argument("--run-id")
    start.add_argument("--forecast-grace-seconds", type=int, default=FORECAST_GRACE_PERIOD_SECONDS)
    start.add_argument("--reference-grace-seconds", type=int, default=REFERENCE_GRACE_PERIOD_SECONDS)
    start.set_defaults(handler=_start)
    status = subparsers.add_parser("status", help="show persisted cohort progress")
    status.set_defaults(handler=_status)
    resume = subparsers.add_parser("resume", help="resume the same cohort ID, window, and model contract")
    resume.set_defaults(handler=_resume)
    finalize = subparsers.add_parser("finalize", help=f"verify that all {EXPECTED_SLOTS:,} slots are terminal")
    finalize.set_defaults(handler=_finalize)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    state_root = Path(args.state_root).resolve()
    state_root.mkdir(parents=True, exist_ok=True)
    return int(args.handler(args, state_root))


if __name__ == "__main__":
    raise SystemExit(main())
