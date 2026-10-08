from copy import deepcopy
from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path

from validation.runtime_stabilization_t2h import (
    build_extended_validation_protocol,
    classify_hourly_cycle_gap,
    summarize_issuance_slo,
    validate_extended_validation_protocol,
)


BASE = datetime(2026, 10, 5, 0, 0, tzinfo=timezone.utc)
PHASE16_WRITER_PATH = (
    Path(__file__).resolve().parents[1]
    / "results/runtime-stabilization-t2h/20261005T171923Z-runtime-stabilization-t2h-v1/write_phase16_artifacts.py"
)
PHASE16_WRITER_SPEC = importlib.util.spec_from_file_location("phase16_artifact_writer", PHASE16_WRITER_PATH)
assert PHASE16_WRITER_SPEC is not None and PHASE16_WRITER_SPEC.loader is not None
PHASE16_WRITER = importlib.util.module_from_spec(PHASE16_WRITER_SPEC)
PHASE16_WRITER_SPEC.loader.exec_module(PHASE16_WRITER)


def test_hourly_cycle_gap_at_sixty_minutes_is_normal():
    result = classify_hourly_cycle_gap(BASE, BASE + timedelta(minutes=60))

    assert result["classification"] == "ON_CADENCE"
    assert result["is_outage"] is False
    assert result["outage_start_estimate"] is None


def test_slightly_delayed_cycle_is_warning_inside_grace_period():
    result = classify_hourly_cycle_gap(BASE, BASE + timedelta(minutes=68))

    assert result["classification"] == "LATE_WITHIN_GRACE"
    assert result["late_by_seconds"] == 8 * 60
    assert result["is_outage"] is False


def test_missed_hourly_cycle_becomes_outage_after_grace():
    result = classify_hourly_cycle_gap(BASE, BASE + timedelta(minutes=76))

    assert result["classification"] == "MISSED_CYCLE"
    assert result["is_outage"] is True
    assert result["duration_seconds_estimate"] == 60


def test_process_restart_is_outage_even_when_hourly_gap_is_expected():
    process_started = BASE + timedelta(minutes=5)
    result = classify_hourly_cycle_gap(
        BASE,
        BASE + timedelta(minutes=60),
        process_restarted=True,
        process_started_at=process_started,
    )

    assert result["classification"] == "PROCESS_RESTART_OBSERVED"
    assert result["is_outage"] is True
    assert result["outage_start_estimate"] == process_started.isoformat(timespec="microseconds").replace("+00:00", "Z")
    assert "downtime_start_not_captured" in result["timing_quality"]


def test_issuance_slo_uses_persist_time_latency_and_frozen_cutoffs():
    result = summarize_issuance_slo([30, 60, 299, 301, 901], [0.1, -1.0, 5.0])

    assert result["issuance_definition"] == "forecast_persisted_at - (feature_time + 1 hour)"
    assert result["issuance_latency_p50_seconds"] == 299
    assert result["issuance_latency_p95_seconds"] == 901
    assert result["issuance_latency_max_seconds"] == 901
    assert result["positive_lead_pct"] == 200 / 3
    assert result["within_cutoff_pct"] == {"60": 40.0, "300": 60.0, "900": 80.0}


def test_extended_validation_protocol_is_fixed_and_does_not_start_cohort():
    protocol = build_extended_validation_protocol()

    assert protocol["expected_target_hours"] == 168
    assert protocol["expected_locations"] == 63
    assert protocol["expected_logical_slots"] == 10584
    assert protocol["cohort_created"] is False
    assert validate_extended_validation_protocol(protocol)["status"] == "PASS"

    drifted = deepcopy(protocol)
    drifted["expected_target_hours"] = 24
    drifted["contract"]["provider_model"] = "different_model"
    check = validate_extended_validation_protocol(drifted)
    assert check["status"] == "FAIL"
    assert "TARGET_HOURS_MUST_BE_168" in check["errors"]
    assert "CONTRACT_MISMATCH:provider_model" in check["errors"]


def test_phase16_readiness_gate_uses_validator_status_schema():
    assert PHASE16_WRITER._protocol_validation_passed({"status": "PASS", "checks_passed": 23}) is True
    assert PHASE16_WRITER._protocol_validation_passed({"status": "FAIL", "errors": ["invalid protocol"]}) is False
    assert PHASE16_WRITER._protocol_validation_passed({}) is False
