from __future__ import annotations

from argparse import Namespace
from datetime import datetime, timedelta, timezone
import multiprocessing
import os
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
import validation.prospective_t2h as prospective
from monitoring.evaluation_contract import LIVE_REFERENCE_SOURCE
from validation.prospective_t2h import (
    EXPECTED_SLOTS,
    EXPECTED_TARGET_HOURS,
    COHORT_PROTOCOL_ID,
    COMPLETE_COHORT_LABEL,
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


def _hold_readiness_lock(lock_path, acquired_queue, release_event):
    with prospective._exclusive_process_lock(lock_path) as acquired:
        acquired_queue.put(acquired)
        if acquired:
            release_event.wait(timeout=15)


def _crash_while_holding_readiness_lock(lock_path, acquired_event):
    lock_context = prospective._exclusive_process_lock(lock_path)
    acquired = lock_context.__enter__()
    acquired_event.set()
    os._exit(23 if acquired else 24)


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def start_request_for(run_id: str, contract_fingerprint: str) -> dict:
    return {
        "run_id": run_id,
        "start_requested_at": iso(REQUESTED_AT),
        "cohort_protocol_id": COHORT_PROTOCOL_ID,
        "expected_target_hours": EXPECTED_TARGET_HOURS,
        "expected_locations": len(LOCATION_IDS),
        "expected_slots": EXPECTED_SLOTS,
        "forecast_grace_period_seconds": 15 * 60,
        "reference_grace_period_seconds": REFERENCE_GRACE_PERIOD_SECONDS,
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_count": FEATURE_COUNT,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "provider_model": PROVIDER_MODEL,
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
        "contract_fingerprint": contract_fingerprint,
    }


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
    request = start_request_for(state_dir.name, fingerprint)
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
    assert EXPECTED_TARGET_HOURS == 168
    assert EXPECTED_SLOTS == 10_584
    assert COMPLETE_COHORT_LABEL == "COMPLETE_10584"
    assert manifest["cohort_protocol_id"] == COHORT_PROTOCOL_ID
    assert len(manifest["target_times"]) == 168
    assert manifest["target_times"] == [iso(TARGET_TIME + timedelta(hours=i)) for i in range(168)]
    assert manifest["cohort_end_target_time"] == iso(TARGET_TIME + timedelta(hours=167))
    assert manifest["expected_slots"] == 10_584
    assert len(state["slots"]) == 10_584
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
        start_request_for(state_dir.name, fingerprint),
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


def test_seven_day_window_does_not_finalize_after_first_day(tmp_path):
    status, state, _ = run_state(
        tmp_path,
        now=TARGET_TIME + timedelta(hours=24),
        include_target_reference=False,
    )
    assert state["manifest"]["expected_target_hours"] == 168
    assert status["status"] == "COLLECTING"
    assert status["expected_slots"] == 10_584
    assert status["terminal_slots"] < 10_584
    assert status["completed_target_hours"] < 168
    assert status["finalization_allowed"] is False


def test_seven_day_window_finalizes_with_missingness_after_all_deadlines(tmp_path):
    status, state, _ = run_state(
        tmp_path,
        now=TARGET_TIME + timedelta(hours=168),
        include_target_reference=False,
    )
    assert status["status"] == "FINALIZED"
    assert status["expected_slots"] == 10_584
    assert status["terminal_slots"] == 10_584
    assert status["completed_target_hours"] == 168
    assert status["finalization_allowed"] is True
    assert status["cohort_completeness_classification"] == "COMPLETE_WINDOW_WITH_MISSINGNESS"
    assert status["missing_forecasts"] == 10_584 - 63
    assert status["missing_references"] == 63
    assert len(state["slots"]) == 10_584


def test_legacy_24_hour_manifest_is_rejected_without_mutating_cohort_state(tmp_path):
    _, state, _ = run_state(
        tmp_path,
        now=TARGET_TIME - timedelta(minutes=30),
        include_target_reference=False,
    )
    run_dir = tmp_path / "prospective-run"
    state_path = run_dir / "cohort_state.json"
    previous_state_bytes = state_path.read_bytes()
    manifest = state["manifest"]
    legacy = {
        **manifest,
        "cohort_protocol_id": "T2H_LIVE_PROSPECTIVE_24H_V1",
        "target_times": manifest["target_times"][:24],
        "cohort_end_target_time": manifest["target_times"][23],
        "expected_target_hours": 24,
        "expected_forecasts": 63 * 24,
        "expected_slots": 63 * 24,
    }
    _atomic_json(run_dir / "cohort_manifest.json", legacy)

    result = update_cohort_state(
        state_dir=run_dir,
        now=TARGET_TIME + timedelta(hours=1),
        forecasts=[],
        receipts=[],
        observations=[],
        revisions=[],
        rejected_observations=[],
        known_location_ids=LOCATION_IDS,
        runtime_contract={
            "status": "PASS",
            "contract_fingerprint": "frozen-test-contract-fingerprint",
        },
    )
    assert result["status"] == "COHORT_PROTOCOL_DRIFT"
    assert result["finalization_allowed"] is False
    assert "INVALID_EXPECTED_TARGET_HOURS" in result["protocol_errors"]
    assert "INVALID_COHORT_PROTOCOL_ID" in result["protocol_errors"]
    assert state_path.read_bytes() == previous_state_bytes


@pytest.mark.parametrize(
    "drift",
    ["legacy_24_hour", "missing_protocol_id", "wrong_target_hours", "wrong_locations", "wrong_slots"],
)
def test_start_request_protocol_drift_is_rejected_before_manifest_or_state_creation(tmp_path, drift):
    run_dir = tmp_path / "prospective-run"
    run_dir.mkdir()
    request = start_request_for(run_dir.name, "frozen-test-contract-fingerprint")
    if drift == "legacy_24_hour":
        request.update(
            cohort_protocol_id="T2H_LIVE_PROSPECTIVE_24H_V1",
            expected_target_hours=24,
            expected_slots=63 * 24,
        )
    elif drift == "missing_protocol_id":
        request.pop("cohort_protocol_id")
    elif drift == "wrong_target_hours":
        request["expected_target_hours"] = 24
    elif drift == "wrong_locations":
        request["expected_locations"] = 62
    elif drift == "wrong_slots":
        request["expected_slots"] = 63 * 24
    _atomic_json(run_dir / "start_request.json", request)
    request_bytes = (run_dir / "start_request.json").read_bytes()

    result = update_cohort_state(
        state_dir=run_dir,
        now=TARGET_TIME,
        forecasts=[forecast_for(location_id) for location_id in sorted(LOCATION_IDS)],
        receipts=[],
        observations=[],
        revisions=[],
        rejected_observations=[],
        known_location_ids=LOCATION_IDS,
        runtime_contract={
            "status": "PASS",
            "contract_fingerprint": "frozen-test-contract-fingerprint",
        },
    )

    assert result["status"] == "COHORT_PROTOCOL_DRIFT"
    assert result["finalization_allowed"] is False
    assert (run_dir / "start_request.json").read_bytes() == request_bytes
    assert not (run_dir / "cohort_manifest.json").exists()
    assert not (run_dir / "cohort_state.json").exists()
    assert not (run_dir / "cohort_status.json").exists()


def test_freeze_cohort_manifest_rejects_legacy_request_before_freezing():
    request = start_request_for("legacy-run", "frozen-test-contract-fingerprint")
    request.update(
        cohort_protocol_id="T2H_LIVE_PROSPECTIVE_24H_V1",
        expected_target_hours=24,
        expected_slots=63 * 24,
    )
    forecasts = [forecast_for(location_id) for location_id in sorted(LOCATION_IDS)]

    with pytest.raises(ValueError, match="COHORT_PROTOCOL_DRIFT"):
        prospective.freeze_cohort_manifest(
            forecasts,
            [receipt_for(row) for row in forecasts],
            request=request,
            known_location_ids=LOCATION_IDS,
            frozen_at=TARGET_TIME,
        )


@pytest.mark.parametrize(
    "drift",
    ["legacy_24_hour", "missing_protocol_id", "wrong_target_hours", "wrong_locations", "wrong_slots"],
)
def test_resume_rejects_start_request_protocol_drift_before_docker(tmp_path, monkeypatch, capsys, drift):
    run_id = "prospective-run"
    run_dir = tmp_path / run_id
    run_dir.mkdir()
    request = start_request_for(run_id, "frozen-test-contract-fingerprint")
    if drift == "legacy_24_hour":
        request.update(
            cohort_protocol_id="T2H_LIVE_PROSPECTIVE_24H_V1",
            expected_target_hours=24,
            expected_slots=63 * 24,
        )
    elif drift == "missing_protocol_id":
        request.pop("cohort_protocol_id")
    elif drift == "wrong_target_hours":
        request["expected_target_hours"] = 24
    elif drift == "wrong_locations":
        request["expected_locations"] = 62
    elif drift == "wrong_slots":
        request["expected_slots"] = 63 * 24
    _atomic_json(run_dir / "start_request.json", request)
    _atomic_json(tmp_path / "active_run.json", {"run_id": run_id})
    before = {path.name: path.read_bytes() for path in run_dir.iterdir()}
    model_contract_calls = []
    docker_calls = []
    monkeypatch.setattr(prospective, "_default_model_contract", lambda: model_contract_calls.append(True))
    monkeypatch.setattr(
        prospective,
        "_run_compose",
        lambda candidate, **kwargs: docker_calls.append(candidate) or Namespace(returncode=0, stdout="", stderr=""),
    )

    result = prospective._resume(Namespace(), tmp_path)

    assert result == 2
    assert "COHORT_PROTOCOL_DRIFT" in capsys.readouterr().err
    assert model_contract_calls == []
    assert docker_calls == []
    assert {path.name: path.read_bytes() for path in run_dir.iterdir()} == before
    assert not (run_dir / "cohort_manifest.json").exists()
    assert not (run_dir / "cohort_state.json").exists()


@pytest.mark.parametrize("manifest_drift", ["wrong_expected_slots", "empty_manifest"])
def test_resume_rejects_manifest_drift_before_docker(tmp_path, monkeypatch, capsys, manifest_drift):
    run_id = "prospective-run"
    run_state(tmp_path, now=TARGET_TIME - timedelta(minutes=30), include_target_reference=False)
    run_dir = tmp_path / run_id
    manifest_path = run_dir / "cohort_manifest.json"
    manifest = _read_json(manifest_path)
    if manifest_drift == "wrong_expected_slots":
        manifest["expected_slots"] = 63 * 24
    else:
        manifest = {}
    _atomic_json(manifest_path, manifest)
    _atomic_json(tmp_path / "active_run.json", {"run_id": run_id})
    manifest_bytes = manifest_path.read_bytes()
    docker_calls = []
    monkeypatch.setattr(
        prospective,
        "_run_compose",
        lambda candidate, **kwargs: docker_calls.append(candidate) or Namespace(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(prospective, "_default_model_contract", lambda: {
        "status": "PASS",
        "contract_fingerprint": "frozen-test-contract-fingerprint",
    })

    result = prospective._resume(Namespace(), tmp_path)

    assert result == 2
    assert "COHORT_PROTOCOL_DRIFT" in capsys.readouterr().err
    assert docker_calls == []
    assert manifest_path.read_bytes() == manifest_bytes


def test_valid_resume_preserves_168_hour_window_and_canonical_state(tmp_path, monkeypatch):
    initial_time = TARGET_TIME - timedelta(minutes=30)
    status, state, _ = run_state(
        tmp_path,
        now=initial_time,
        include_target_reference=False,
    )
    run_id = "prospective-run"
    run_dir = tmp_path / run_id
    _atomic_json(tmp_path / "active_run.json", {"run_id": run_id})
    monkeypatch.setattr(prospective, "utc_now", lambda: initial_time)
    monkeypatch.setattr(prospective, "_default_model_contract", lambda: {
        "status": "PASS",
        "contract_fingerprint": state["manifest"]["contract_fingerprint"],
    })
    docker_calls = []
    monkeypatch.setattr(
        prospective,
        "_run_compose",
        lambda candidate, **kwargs: docker_calls.append(candidate) or Namespace(returncode=0, stdout="resumed", stderr=""),
    )
    paths = [
        run_dir / "start_request.json",
        run_dir / "cohort_manifest.json",
        run_dir / "cohort_state.json",
        run_dir / "cohort_status.json",
        tmp_path / "active_run.json",
    ]
    before = {path: path.read_bytes() for path in paths if path.exists()}

    assert prospective._resume(Namespace(), tmp_path) == 0

    assert status["status"] == "COLLECTING"
    assert docker_calls == [run_id]
    assert {path: path.read_bytes() for path in before} == before
    assert not (run_dir / "resume_events.jsonl").exists()


def test_resume_preserves_168_hour_manifest_and_accepted_forecast(tmp_path):
    status, state, forecasts = run_state(
        tmp_path,
        now=TARGET_TIME - timedelta(minutes=30),
        include_target_reference=False,
    )
    original_manifest = state["manifest"]
    run_dir = tmp_path / "prospective-run"
    updated = update_cohort_state(
        state_dir=run_dir,
        now=TARGET_TIME + timedelta(hours=12),
        forecasts=forecasts,
        receipts=[receipt_for(row) for row in forecasts],
        observations=observations_for(forecasts, target_reference=False),
        revisions=[],
        rejected_observations=[],
        known_location_ids=LOCATION_IDS,
        runtime_contract={
            "status": "PASS",
            "contract_fingerprint": "frozen-test-contract-fingerprint",
        },
    )
    persisted = _read_json(run_dir / "cohort_state.json")
    assert status["cohort_id"] == updated["cohort_id"]
    assert updated["status"] == "COLLECTING"
    assert persisted["manifest"] == original_manifest
    assert len(persisted["slots"]) == 10_584
    first_key = f"{iso(TARGET_TIME)}|{forecasts[0]['location_id']}"
    assert persisted["slots"][first_key]["forecast"]["forecast_id"] == forecasts[0]["forecast_id"]


def test_official_start_requires_new_protocol_preflight_and_readiness(tmp_path, monkeypatch):
    monkeypatch.setattr(prospective, "_default_model_contract", lambda: {
        "status": "PASS",
        "contract_fingerprint": "frozen-test-contract-fingerprint",
    })
    monkeypatch.setattr(prospective, "utc_now", lambda: TARGET_TIME)
    _atomic_json(tmp_path / "preflight_test_results.json", {
        "status": "PASS",
        "cohort_protocol_id": COHORT_PROTOCOL_ID,
    })
    readiness = {
        "status": "PASS",
        "checked_at": iso(TARGET_TIME),
        "contract_fingerprint": "frozen-test-contract-fingerprint",
        "forecast_count": 63,
        "positive_lead_count": 63,
        "target_offset_violations": 0,
        "monitoring_status": "PASS",
        "provider_model": PROVIDER_MODEL,
    }
    _atomic_json(tmp_path / "readiness.json", readiness)
    args = Namespace(
        run_id="must-not-start",
        forecast_grace_seconds=900,
        reference_grace_seconds=7200,
    )
    assert prospective._start(args, tmp_path) == 2
    assert not (tmp_path / "active_run.json").exists()

    readiness["cohort_protocol_id"] = COHORT_PROTOCOL_ID
    _atomic_json(tmp_path / "readiness.json", readiness)
    _atomic_json(tmp_path / "preflight_test_results.json", {
        "status": "PASS",
    })
    assert prospective._start(args, tmp_path) == 2
    assert not (tmp_path / "active_run.json").exists()


def _create_passed_readiness_attempt(state_root, monkeypatch):
    contract_fingerprint = "frozen-test-contract-fingerprint"
    readiness_run_id = "readiness-success-attempt"
    _atomic_json(state_root / "preflight_test_results.json", {
        "status": "PASS",
        "cohort_protocol_id": COHORT_PROTOCOL_ID,
    })
    monkeypatch.setattr(prospective, "_default_model_contract", lambda: {
        "status": "PASS",
        "contract_fingerprint": contract_fingerprint,
    })
    monkeypatch.setattr(prospective, "utc_now", lambda: TARGET_TIME)
    monkeypatch.setattr(
        prospective,
        "_stop_readiness_services",
        lambda: prospective.subprocess.CompletedProcess(["docker", "compose", "stop"], 0, "", ""),
    )

    def pass_readiness(run_id, *, state_root, bootstrap=False, services=None):
        assert run_id == readiness_run_id
        assert bootstrap is True
        request = _read_json(state_root / run_id / "readiness_request.json")
        _atomic_json(state_root / run_id / "readiness.json", {
            "status": "PASS",
            "run_id": run_id,
            "requested_at": request["requested_at"],
            "attempt_id": request["attempt_id"],
            "checked_at": iso(TARGET_TIME),
            "forecast_count": 63,
            "positive_lead_count": 63,
            "target_offset_violations": 0,
            "monitoring_status": "PASS",
            "provider_model": PROVIDER_MODEL,
        })
        return prospective.subprocess.CompletedProcess(["docker", "compose"], 0, "ready", "")

    monkeypatch.setattr(prospective, "_run_compose", pass_readiness)
    assert prospective._readiness(Namespace(run_id=readiness_run_id, wait_seconds=1), state_root) == 0
    attempt_id = _read_json(state_root / "readiness_latest_attempt.json")["attempt_id"]
    start_args = Namespace(
        run_id="must-not-start",
        forecast_grace_seconds=900,
        reference_grace_seconds=7200,
    )
    return attempt_id, start_args


def _assert_new_readiness_failure_invalidates_previous_pass(
    state_root,
    monkeypatch,
    *,
    failure_kind,
    expected_status,
    expected_result,
):
    previous_attempt_id, start_args = _create_passed_readiness_attempt(state_root, monkeypatch)
    second_run_id = f"readiness-{failure_kind}-attempt"
    in_progress_start_results = []

    def fail_followup(run_id, *, state_root, bootstrap=False, services=None):
        assert run_id == second_run_id
        assert bootstrap is True
        in_progress_start_results.append(prospective._start(start_args, state_root))
        if failure_kind == "bootstrap":
            return prospective.subprocess.CompletedProcess(["docker", "compose"], 1, "", "bootstrap failed")
        if failure_kind == "readiness":
            request = _read_json(state_root / run_id / "readiness_request.json")
            _atomic_json(state_root / run_id / "readiness.json", {
                "status": "FAIL",
                "reason": "READINESS_CYCLE_REJECTED",
                "run_id": run_id,
                "requested_at": request["requested_at"],
                "attempt_id": request["attempt_id"],
            })
        return prospective.subprocess.CompletedProcess(["docker", "compose"], 0, "started", "")

    monkeypatch.setattr(prospective, "_run_compose", fail_followup)
    wait_seconds = 0 if failure_kind == "incomplete" else 1
    result = prospective._readiness(Namespace(run_id=second_run_id, wait_seconds=wait_seconds), state_root)

    assert result == expected_result
    assert in_progress_start_results == [2]
    previous_result = _read_json(state_root / "readiness_attempts" / previous_attempt_id / "result.json")
    latest = _read_json(state_root / "readiness_latest_attempt.json")
    current = _read_json(state_root / "readiness.json")
    latest_result = _read_json(state_root / "readiness_attempts" / latest["attempt_id"] / "result.json")
    assert previous_result["status"] == "PASS"
    assert latest["attempt_id"] != previous_attempt_id
    assert current["attempt_id"] == latest["attempt_id"]
    assert current["status"] == expected_status
    assert latest_result["status"] == expected_status
    assert prospective._start(start_args, state_root) == 2
    assert not (state_root / "active_run.json").exists()
    assert not (state_root / start_args.run_id).exists()


def test_success_then_failed_bootstrap_invalidates_readiness_pass_and_denies_start(tmp_path, monkeypatch):
    _assert_new_readiness_failure_invalidates_previous_pass(
        tmp_path,
        monkeypatch,
        failure_kind="bootstrap",
        expected_status="FAIL",
        expected_result=1,
    )
    current = _read_json(tmp_path / "readiness.json")
    assert current["reason"] == "BOOTSTRAP_FAILED"


def test_success_then_failed_readiness_invalidates_readiness_pass_and_denies_start(tmp_path, monkeypatch):
    _assert_new_readiness_failure_invalidates_previous_pass(
        tmp_path,
        monkeypatch,
        failure_kind="readiness",
        expected_status="FAIL",
        expected_result=1,
    )


def test_concurrent_readiness_fails_closed_without_mutating_first_attempt(tmp_path, monkeypatch):
    context = multiprocessing.get_context("spawn")
    state_root = tmp_path / "shared-readiness-state"
    state_root.mkdir()
    protected_attempt_id = "first-active-attempt"
    protected_attempt_dir = state_root / "readiness_attempts" / protected_attempt_id
    protected_attempt_dir.mkdir(parents=True)
    evidence = {
        state_root / "readiness.json": {
            "status": "IN_PROGRESS",
            "run_id": "first-active-run",
            "attempt_id": protected_attempt_id,
        },
        state_root / "readiness_latest_attempt.json": {
            "status": "IN_PROGRESS",
            "run_id": "first-active-run",
            "attempt_id": protected_attempt_id,
        },
        protected_attempt_dir / "request.json": {
            "run_id": "first-active-run",
            "attempt_id": protected_attempt_id,
        },
        protected_attempt_dir / "result.json": {
            "status": "IN_PROGRESS",
            "run_id": "first-active-run",
            "attempt_id": protected_attempt_id,
        },
    }
    for path, record in evidence.items():
        _atomic_json(path, record)
    evidence_bytes_before = {path: path.read_bytes() for path in evidence}
    attempt_dirs_before = sorted(path.name for path in (state_root / "readiness_attempts").iterdir())

    acquired_queue = context.Queue()
    release_event = context.Event()
    first_process = context.Process(
        target=_hold_readiness_lock,
        args=(prospective._fresh_readiness_lock_path(state_root), acquired_queue, release_event),
    )
    first_process.start()
    try:
        assert acquired_queue.get(timeout=10) is True

        def unexpected_side_effect(*_args, **_kwargs):
            pytest.fail("A readiness attempt ran despite another process holding the lock")

        monkeypatch.setattr(prospective, "_default_model_contract", unexpected_side_effect)
        monkeypatch.setattr(prospective, "_begin_readiness_attempt", unexpected_side_effect)
        monkeypatch.setattr(prospective, "_run_compose", unexpected_side_effect)
        monkeypatch.setattr(prospective, "_stop_readiness_services", unexpected_side_effect)

        result = prospective._readiness(
            Namespace(run_id="second-contending-run", wait_seconds=1),
            state_root,
        )

        assert result == 2
        assert {path: path.read_bytes() for path in evidence} == evidence_bytes_before
        assert sorted(path.name for path in (state_root / "readiness_attempts").iterdir()) == attempt_dirs_before
        assert not (state_root / "second-contending-run").exists()
    finally:
        release_event.set()
        first_process.join(timeout=10)
        if first_process.is_alive():
            first_process.terminate()
            first_process.join(timeout=5)
        acquired_queue.close()
        acquired_queue.join_thread()
    assert first_process.exitcode == 0


def test_readiness_process_lock_is_shared_across_azure_state_roots(tmp_path, monkeypatch):
    monkeypatch.setattr(prospective, "is_azure_runtime", lambda *_args: True)

    first = prospective._fresh_readiness_lock_path(tmp_path / "one")
    second = prospective._fresh_readiness_lock_path(tmp_path / "two")

    assert first == second
    assert first == prospective.DEFAULT_STATE_ROOT / prospective.FRESH_READINESS_LOCK_NAME


def test_readiness_process_lock_is_released_after_process_crash(tmp_path):
    context = multiprocessing.get_context("spawn")
    lock_path = tmp_path / "readiness.lock"
    acquired_event = context.Event()
    crashed_process = context.Process(
        target=_crash_while_holding_readiness_lock,
        args=(lock_path, acquired_event),
    )
    crashed_process.start()
    assert acquired_event.wait(timeout=10)
    crashed_process.join(timeout=10)
    if crashed_process.is_alive():
        crashed_process.terminate()
        crashed_process.join(timeout=5)

    assert crashed_process.exitcode == 23
    with prospective._exclusive_process_lock(lock_path) as acquired:
        assert acquired is True


def test_success_then_failed_model_contract_preflight_invalidates_readiness_pass(tmp_path, monkeypatch):
    _, start_args = _create_passed_readiness_attempt(tmp_path, monkeypatch)
    contract_fingerprint = "frozen-test-contract-fingerprint"
    calls = iter(("FAIL", "PASS"))

    def contract_status_changes():
        return {
            "status": next(calls),
            "contract_fingerprint": contract_fingerprint,
            "reason": "test preflight failure",
        }

    monkeypatch.setattr(prospective, "_default_model_contract", contract_status_changes)
    result = prospective._readiness(
        Namespace(run_id="readiness-contract-failure-attempt", wait_seconds=1),
        tmp_path,
    )

    latest = _read_json(tmp_path / "readiness_latest_attempt.json")
    current = _read_json(tmp_path / "readiness.json")
    latest_result = _read_json(tmp_path / "readiness_attempts" / latest["attempt_id"] / "result.json")
    assert result == 1
    assert current["status"] == "FAIL"
    assert current["reason"] == "MODEL_CONTRACT_PREFLIGHT_FAILED"
    assert latest_result["status"] == "FAIL"
    assert prospective._start(start_args, tmp_path) == 2
    assert not (tmp_path / "active_run.json").exists()


def test_success_then_incomplete_readiness_invalidates_readiness_pass_and_denies_start(tmp_path, monkeypatch):
    _assert_new_readiness_failure_invalidates_previous_pass(
        tmp_path,
        monkeypatch,
        failure_kind="incomplete",
        expected_status="READINESS_INCOMPLETE",
        expected_result=2,
    )


def test_official_start_accepts_only_a_passed_latest_readiness_attempt(tmp_path, monkeypatch):
    _create_passed_readiness_attempt(tmp_path, monkeypatch)

    def no_op_compose(run_id, *, state_root, bootstrap=False, services=None):
        assert bootstrap is True
        return prospective.subprocess.CompletedProcess(["docker", "compose"], 0, "started", "")

    def write_test_start_request(*, state_root, run_id, **kwargs):
        run_state = state_root / run_id
        run_state.mkdir(parents=True, exist_ok=False)
        _atomic_json(run_state / "start_request.json", {"run_id": run_id})
        return run_state

    monkeypatch.setattr(prospective, "_run_compose", no_op_compose)
    monkeypatch.setattr(prospective, "_write_start_request", write_test_start_request)
    args = Namespace(run_id="test-only-start", forecast_grace_seconds=900, reference_grace_seconds=7200)

    assert prospective._start(args, tmp_path) == 0
    assert _read_json(tmp_path / "active_run.json")["run_id"] == "test-only-start"


def test_official_start_rejects_pass_from_mismatched_attempt_contract(tmp_path, monkeypatch):
    attempt_id, start_args = _create_passed_readiness_attempt(tmp_path, monkeypatch)
    result_path = tmp_path / "readiness_attempts" / attempt_id / "result.json"
    attempt_result = _read_json(result_path)
    attempt_result["cohort_protocol_id"] = "different-protocol"
    _atomic_json(result_path, attempt_result)

    assert prospective._start(start_args, tmp_path) == 2
    assert not (tmp_path / "active_run.json").exists()


def test_new_readiness_attempt_invalidates_previous_pass_before_archiving(tmp_path, monkeypatch):
    _, start_args = _create_passed_readiness_attempt(tmp_path, monkeypatch)
    original_archive = prospective._archive_current_readiness
    concurrent_start_results = []
    monkeypatch.setattr(
        prospective,
        "_archive_current_readiness",
        lambda state_root: (
            concurrent_start_results.append(prospective._start(start_args, state_root)),
            original_archive(state_root),
        ),
    )

    request = {
        "run_id": "readiness-in-progress-attempt",
        "requested_at": iso(TARGET_TIME),
        "attempt_id": "b" * 32,
        "contract_fingerprint": "frozen-test-contract-fingerprint",
    }
    prospective._begin_readiness_attempt(tmp_path, request)

    assert concurrent_start_results == [2]
    assert _read_json(tmp_path / "readiness.json")["status"] == "IN_PROGRESS"


def test_streaming_readiness_result_preserves_attempt_identity(tmp_path):
    state_dir = tmp_path / "streaming-readiness-attempt"
    state_dir.mkdir()
    request = {
        "run_id": "streaming-readiness-attempt",
        "attempt_id": "a" * 32,
        "requested_at": iso(REQUESTED_AT),
    }
    _atomic_json(state_dir / "readiness_request.json", request)

    result = prospective._readiness_update(
        state_dir=state_dir,
        forecasts=[],
        receipts=[],
        observations=[],
        known_location_ids=LOCATION_IDS,
        runtime_contract={"status": "PASS"},
        startup_validation_path=tmp_path / "startup_validation.json",
    )

    assert result is not None
    assert result["status"] == "WAITING"
    assert result["run_id"] == request["run_id"]
    assert result["attempt_id"] == request["attempt_id"]
    assert result["requested_at"] == request["requested_at"]
    assert _read_json(state_dir / "readiness.json") == result


def test_new_start_request_freezes_isolated_topic_cache_and_bootstrap_plan(tmp_path):
    run_id = "20261010T120000Z-prospective-live-t2h-v1"
    run_state = prospective._write_start_request(
        state_root=tmp_path,
        run_id=run_id,
        contract={"contract_fingerprint": "frozen-test-contract-fingerprint"},
        requested_at=REQUESTED_AT,
    )

    request = _read_json(run_state / "start_request.json")
    runtime_config = request["runtime_configuration"]
    assert runtime_config["input_topic"] == f"weather.hourly.observations.t2h.prospective.{run_id}.v1"
    assert run_id in runtime_config["producer_cache_path"]
    assert runtime_config["producer_history_hours"] == 48
    assert runtime_config["bootstrap_required"] is True
    assert request["model_sha256"] == MODEL_SHA256
    assert request["feature_list_sha256"] == FEATURE_LIST_SHA256
    assert request["forecast_horizon_hours"] == 2
    assert request["training_performed"] is False


def test_default_state_root_is_versioned_away_from_prior_24_hour_cohort():
    assert prospective.DEFAULT_STATE_ROOT.name == "prospective-live-t2h-168h-v1"
    assert prospective.DEFAULT_STATE_ROOT != prospective.REPOSITORY_ROOT / "data" / "runtime" / "prospective-live-t2h"


def test_resume_runtime_uses_saved_topic_and_cache_without_migrating_legacy_request(tmp_path, monkeypatch):
    run_id = "prior-run"
    run_dir = tmp_path / run_id
    run_dir.mkdir()
    _atomic_json(
        run_dir / "start_request.json",
        {
            "run_id": run_id,
            "runtime_configuration": {
                "input_topic": "weather.hourly.observations.t2h.live.v1",
                "producer_history_hours": 48,
            },
        },
    )
    captured = {}

    def fake_compose(**kwargs):
        captured.update(kwargs)
        return Namespace(returncode=0, stdout="resumed", stderr="")

    monkeypatch.setattr(prospective, "run_prospective_compose", fake_compose)
    prospective._run_compose(run_id, state_root=tmp_path)

    assert captured["runtime_configuration"]["input_topic"] == "weather.hourly.observations.t2h.live.v1"
    assert "producer_cache_path" not in captured["runtime_configuration"]
    assert captured["bootstrap"] is False
