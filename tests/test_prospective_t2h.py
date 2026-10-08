from __future__ import annotations

from datetime import datetime, timedelta, timezone
import pytest

from ml.streaming_inference.t2h_contract import (
    FEATURE_COUNT,
    FEATURE_LIST_SHA256,
    FEATURE_SET_ID,
    FORECAST_HORIZON_HOURS,
    LIVE_ENDPOINT,
    LIVE_SOURCE,
    MODEL_ID,
    MODEL_SHA256,
    PROVIDER_MODEL,
    PROVIDER_NAME,
)
from ml.streaming_inference.t2h_runtime import deterministic_t2h_forecast_id
from monitoring.evaluation_contract import LIVE_REFERENCE_SOURCE
from validation.prospective_t2h import (
    EXPECTED_SLOTS,
    REFERENCE_GRACE_PERIOD_SECONDS,
    _append_jsonl_once,
    _atomic_json,
    _forecast_record_sha256,
    _read_json,
    _read_jsonl,
    _receipt_for_forecast,
    _state_observation_for_revision,
    record_reference_revisions_from_spark,
    update_cohort_state,
)


UTC = timezone.utc
LOCATION_IDS = {f"VN_TEST_{index:02d}" for index in range(63)}
FEATURE_TIME = datetime(2026, 10, 3, 0, 0, tzinfo=UTC)
TARGET_TIME = FEATURE_TIME + timedelta(hours=FORECAST_HORIZON_HOURS)
REQUESTED_AT = FEATURE_TIME + timedelta(minutes=30)
INFERENCE_TIME = FEATURE_TIME + timedelta(hours=1, minutes=1)
RETRIEVED_FEATURE_AT = FEATURE_TIME + timedelta(hours=1, seconds=30)


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def forecast_for(location_id: str, *, target_time: datetime = TARGET_TIME) -> dict:
    feature_time = target_time - timedelta(hours=FORECAST_HORIZON_HOURS)
    inference_time = feature_time + timedelta(hours=1, minutes=1)
    retrieved_at = feature_time + timedelta(hours=1, seconds=30)
    event_id = f"source-{location_id}-{iso(feature_time)}"
    return {
        "forecast_id": deterministic_t2h_forecast_id(location_id, feature_time, target_time),
        "location_id": location_id,
        "feature_time": feature_time,
        "target_time": target_time,
        "prediction_temperature_c": 24.5,
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "feature_count": FEATURE_COUNT,
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
        "source_event_id": event_id,
        "inference_time": inference_time,
        "forecast_lead_seconds": (target_time - inference_time).total_seconds(),
        "execution_origin": "LIVE_PROSPECTIVE",
        "provider": PROVIDER_NAME,
        "provider_endpoint": LIVE_ENDPOINT,
        "provider_model": PROVIDER_MODEL,
        "source": LIVE_SOURCE,
        "source_timestamp": feature_time,
        "source_retrieved_at": retrieved_at,
    }


def receipt_for(forecast: dict, *, persisted_at: datetime | None = None) -> dict:
    return {
        "forecast_id": forecast["forecast_id"],
        "location_id": forecast["location_id"],
        "target_time": iso(forecast["target_time"]),
        "forecast_persisted_at": iso(persisted_at or (forecast["inference_time"] + timedelta(seconds=2))),
        "record_sha256": _forecast_record_sha256(forecast),
        "persistence_receipt_kind": "NEW_DELTA_WRITE",
    }


def observations_for(
    forecasts: list[dict],
    *,
    target_reference: bool,
    temperature: float = 22.0,
) -> list[dict]:
    observations: list[dict] = []
    for forecast in forecasts:
        location_id = forecast["location_id"]
        feature_time = forecast["feature_time"]
        event_times = [(feature_time, forecast["source_event_id"])]
        if target_reference:
            event_times.append((forecast["target_time"], f"target-{location_id}"))
        for event_time, event_id in event_times:
            if event_time == feature_time:
                retrieved_at = event_time + timedelta(hours=1, seconds=30)
            else:
                retrieved_at = event_time + timedelta(hours=1, seconds=30)
            observations.append(
                {
                    "event_id": event_id,
                    "event_type": "WEATHER_HOURLY",
                    "location_id": location_id,
                    "event_time": event_time,
                    "source": LIVE_REFERENCE_SOURCE,
                    "source_retrieved_at": retrieved_at,
                    "temperature_c": temperature,
                    "humidity_pct": 60.0,
                    "precipitation_mm": 0.0,
                    "pressure_hpa": 1010.0,
                    "wind_speed_kmh": 10.0,
                    "wind_gust_kmh": 12.0,
                    "weather_code": 1,
                    "latitude": 10.0,
                    "longitude": 106.0,
                    "provider": PROVIDER_NAME,
                    "provider_endpoint": LIVE_ENDPOINT,
                    "provider_model": PROVIDER_MODEL,
                }
            )
    return observations


def run_state(tmp_path, *, now: datetime, include_target_reference: bool) -> tuple[dict, dict, list[dict]]:
    state_dir = tmp_path / "prospective-run"
    state_dir.mkdir(parents=True, exist_ok=True)
    fingerprint = "frozen-test-contract-fingerprint"
    request = {
        "run_id": state_dir.name,
        "start_requested_at": iso(REQUESTED_AT),
        "forecast_grace_period_seconds": 15 * 60,
        "reference_grace_period_seconds": REFERENCE_GRACE_PERIOD_SECONDS,
        "contract_fingerprint": fingerprint,
    }
    _atomic_json(state_dir / "start_request.json", request)
    forecasts = [forecast_for(location_id) for location_id in sorted(LOCATION_IDS)]
    receipts = [receipt_for(row) for row in forecasts]
    observations = observations_for(forecasts, target_reference=include_target_reference)
    runtime = {"status": "PASS", "contract_fingerprint": fingerprint}
    status = update_cohort_state(
        state_dir=state_dir,
        now=now,
        forecasts=forecasts,
        receipts=receipts,
        observations=observations,
        revisions=[],
        rejected_observations=[],
        known_location_ids=LOCATION_IDS,
        runtime_contract=runtime,
    )
    return status, _read_json(state_dir / "cohort_state.json"), forecasts


def test_forecast_receipt_requires_matching_persisted_record_hash():
    forecast = forecast_for(sorted(LOCATION_IDS)[0])
    valid, errors = _receipt_for_forecast(forecast, {forecast["forecast_id"]: [receipt_for(forecast)]})
    assert valid is not None
    assert errors == []

    corrupt = receipt_for(forecast)
    corrupt["record_sha256"] = "0" * 64
    _, errors = _receipt_for_forecast(forecast, {forecast["forecast_id"]: [corrupt]})
    assert "PERSISTENCE_RECORD_HASH_MISMATCH" in errors


def test_target_window_freezes_on_first_complete_live_cycle_and_tracks_pending_target(tmp_path):
    now = TARGET_TIME - timedelta(minutes=30)
    status, state, forecasts = run_state(tmp_path, now=now, include_target_reference=False)

    manifest = state["manifest"]
    assert manifest["cohort_start_target_time"] == iso(TARGET_TIME)
    assert len(manifest["target_times"]) == 24
    assert manifest["target_times"] == [iso(TARGET_TIME + timedelta(hours=i)) for i in range(24)]
    assert status["status"] == "COLLECTING"
    assert status["pending_target"] >= 63
    assert status["valid_evaluation_count"] == 0
    first_slot = state["slots"][f"{iso(TARGET_TIME)}|{forecasts[0]['location_id']}"]
    assert first_slot["status"] == "PENDING_TARGET_TIME"
    assert status["expected_slots"] == EXPECTED_SLOTS


def test_evaluated_reference_is_first_wins_across_restart_and_revision(tmp_path):
    state_dir = tmp_path / "prospective-run"
    state_dir.mkdir(parents=True, exist_ok=True)
    fingerprint = "same-contract"
    _atomic_json(
        state_dir / "start_request.json",
        {
            "run_id": state_dir.name,
            "start_requested_at": iso(REQUESTED_AT),
            "forecast_grace_period_seconds": 15 * 60,
            "reference_grace_period_seconds": REFERENCE_GRACE_PERIOD_SECONDS,
            "contract_fingerprint": fingerprint,
        },
    )
    forecasts = [forecast_for(location_id) for location_id in sorted(LOCATION_IDS)]
    receipts = [receipt_for(row) for row in forecasts]
    observations = observations_for(forecasts, target_reference=True, temperature=22.0)
    runtime = {"status": "PASS", "contract_fingerprint": fingerprint}
    first_status = update_cohort_state(
        state_dir=state_dir,
        now=TARGET_TIME + timedelta(hours=2, seconds=1),
        forecasts=forecasts,
        receipts=receipts,
        observations=observations,
        revisions=[],
        rejected_observations=[],
        known_location_ids=LOCATION_IDS,
        runtime_contract=runtime,
    )
    first_state = _read_json(state_dir / "cohort_state.json")
    first_slot_key = f"{iso(TARGET_TIME)}|{forecasts[0]['location_id']}"
    original_evaluation = first_state["slots"][first_slot_key]["evaluation"]
    assert original_evaluation["status"] == "EVALUATED"
    assert first_status["cohort_id"] == first_state["manifest"]["cohort_id"]

    revision = {
        "revision_id": "revision-1",
        "source": LIVE_REFERENCE_SOURCE,
        "location_id": forecasts[0]["location_id"],
        "event_time": TARGET_TIME,
        "old_payload_sha256": "old",
        "new_payload_sha256": "new",
    }
    changed_observations = observations_for(forecasts, target_reference=True, temperature=26.0)
    second_status = update_cohort_state(
        state_dir=state_dir,
        now=TARGET_TIME + timedelta(hours=2, minutes=1),
        forecasts=forecasts,
        receipts=receipts,
        observations=changed_observations,
        revisions=[revision],
        rejected_observations=[],
        known_location_ids=LOCATION_IDS,
        runtime_contract=runtime,
    )
    second_state = _read_json(state_dir / "cohort_state.json")
    preserved = second_state["slots"][first_slot_key]
    assert second_status["cohort_id"] == first_status["cohort_id"]
    assert preserved["status"] == "EVALUATED"
    assert preserved["evaluation"]["reference_temperature_c"] == original_evaluation["reference_temperature_c"]
    assert second_status["reference_revision_count"] == 1


def test_missing_reference_waits_through_grace_then_becomes_terminal(tmp_path):
    now = TARGET_TIME + timedelta(seconds=REFERENCE_GRACE_PERIOD_SECONDS)
    status, state, forecasts = run_state(tmp_path, now=now, include_target_reference=False)
    first_slot = state["slots"][f"{iso(TARGET_TIME)}|{forecasts[0]['location_id']}"]
    assert first_slot["status"] == "MISSING_REFERENCE"
    assert status["missing_references"] == 63
    assert status["status"] == "COLLECTING"
    assert status["finalization_allowed"] is False


def test_jsonl_reader_ignores_only_an_uncommitted_partial_tail(tmp_path):
    path = tmp_path / "receipts.jsonl"
    path.write_text('{"id":"good"}\n{"id":"partial"', encoding="utf-8")
    assert _read_jsonl(path) == [{"id": "good"}]
    assert _append_jsonl_once(path, {"id": "next"}, id_field="id") is True
    assert _read_jsonl(path) == [{"id": "good"}, {"id": "next"}]


def test_record_reference_revisions_from_spark_qualifies_same_lineage_join(tmp_path):
    pytest.importorskip("pyspark.sql")
    from pyspark.sql import SparkSession, functions as F

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("prospective-reference-revision-self-join-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    event_time = FEATURE_TIME.replace(tzinfo=None)
    retrieved_at = event_time + timedelta(hours=1)
    old_row = {
        "location_id": "VN_TEST_00",
        "event_time": event_time,
        "event_id": "old-event",
        "city": "Test City",
        "latitude": 10.0,
        "longitude": 106.0,
        "temperature_c": 22.0,
        "humidity_pct": 60.0,
        "precipitation_mm": 0.0,
        "pressure_hpa": 1010.0,
        "wind_speed_kmh": 10.0,
        "wind_gust_kmh": 12.0,
        "weather_code": 1,
        "source": LIVE_REFERENCE_SOURCE,
        "payload_hash": "old-payload",
        "provider": PROVIDER_NAME,
        "provider_endpoint": LIVE_ENDPOINT,
        "provider_model": PROVIDER_MODEL,
        "source_retrieved_at": retrieved_at,
    }
    new_row = {
        **old_row,
        "event_id": "new-event",
        "temperature_c": 23.5,
        "humidity_pct": 61.0,
        "payload_hash": "new-payload",
        "source_retrieved_at": retrieved_at + timedelta(minutes=5),
    }
    state_columns = tuple(old_row)
    try:
        shared_source = spark.createDataFrame([old_row, new_row])
        previous_state = shared_source.filter(F.col("payload_hash") == "old-payload").select(*state_columns)
        conflict_frame = shared_source.filter(F.col("payload_hash") == "new-payload").select(*state_columns)
        assert previous_state.schema == conflict_frame.schema

        output_path = tmp_path / "reference_revisions.jsonl"
        assert record_reference_revisions_from_spark(
            conflict_frame,
            previous_state,
            output_path,
            state_columns=state_columns,
        ) == 1

        records = _read_jsonl(output_path)
        assert len(records) == 1
        revision = records[0]
        expected_old_hash = _state_observation_for_revision(old_row)["reference_payload_sha256"]
        expected_new_hash = _state_observation_for_revision(new_row)["reference_payload_sha256"]
        assert revision["first_event_id"] == "old-event"
        assert revision["later_event_id"] == "new-event"
        assert revision["old_payload_sha256"] == expected_old_hash
        assert revision["new_payload_sha256"] == expected_new_hash
        assert revision["old_payload_sha256"] != revision["new_payload_sha256"]
        assert revision["old_payload_hash"] == "old-payload"
        assert revision["new_payload_hash"] == "new-payload"
        assert revision["old_temperature_c"] == 22.0
        assert revision["new_temperature_c"] == 23.5
        assert revision["old_retrieved_at"] == iso(retrieved_at)
        assert revision["new_retrieved_at"] == iso(retrieved_at + timedelta(minutes=5))
        assert revision["location_id"] == "VN_TEST_00"
        assert revision["target_time"] == iso(FEATURE_TIME)
    finally:
        spark.stop()
