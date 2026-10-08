"""Runtime-only observability contracts for future T2H validation runs."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
from typing import Any, Iterable, Mapping

from ml.streaming_inference.t2h_contract import (
    FEATURE_COUNT,
    FEATURE_LIST_SHA256,
    FEATURE_SET_ID,
    FORECAST_HORIZON_HOURS,
    MODEL_ID,
    MODEL_SHA256,
    PROVIDER_MODEL,
    PROVIDER_NAME,
)
from monitoring.evaluation_contract import parse_utc_timestamp


EXPECTED_HOURLY_CADENCE_SECONDS = 60 * 60
DEFAULT_CADENCE_GRACE_SECONDS = 15 * 60
ISSUANCE_SLO_CUTOFFS_SECONDS = (60, 300, 900)


def _utc(value: Any) -> datetime:
    parsed = parse_utc_timestamp(value, assume_naive_utc=True)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def classify_hourly_cycle_gap(
    previous_update_at: Any,
    observed_at: Any,
    *,
    process_restarted: bool = False,
    process_started_at: Any | None = None,
    cadence_seconds: int = EXPECTED_HOURLY_CADENCE_SECONDS,
    grace_seconds: int = DEFAULT_CADENCE_GRACE_SECONDS,
) -> dict[str, Any]:
    """Classify an update gap against hourly input cadence and restart evidence."""
    if cadence_seconds <= 0 or grace_seconds < 0:
        raise ValueError("cadence must be positive and grace cannot be negative")
    previous = _utc(previous_update_at)
    observed = _utc(observed_at)
    gap_seconds = (observed - previous).total_seconds()
    if gap_seconds < 0:
        cadence_classification = "CLOCK_SKEW"
    elif gap_seconds <= cadence_seconds:
        cadence_classification = "ON_CADENCE"
    elif gap_seconds <= cadence_seconds + grace_seconds:
        cadence_classification = "LATE_WITHIN_GRACE"
    else:
        cadence_classification = "MISSED_CYCLE"

    is_missed_cycle = cadence_classification == "MISSED_CYCLE"
    is_outage = bool(process_restarted or is_missed_cycle)
    if process_restarted and is_missed_cycle:
        classification = "PROCESS_RESTART_AND_MISSED_CYCLE"
    elif process_restarted:
        classification = "PROCESS_RESTART_OBSERVED"
    else:
        classification = cadence_classification

    outage_start = None
    duration_seconds = None
    timing_quality = "not_an_outage"
    if is_missed_cycle:
        outage_start = previous + timedelta(seconds=cadence_seconds + grace_seconds)
        duration_seconds = max(0.0, gap_seconds - cadence_seconds - grace_seconds)
        timing_quality = "inferred_after_hourly_cadence_and_grace"
    elif process_restarted:
        restarted_at = _utc(process_started_at) if process_started_at is not None else None
        outage_start = restarted_at or previous
        # The prior process stop time is not persisted; do not claim a measured
        # downtime from the hourly silence around a restart.
        duration_seconds = 0.0
        timing_quality = "process_restart_observed; downtime_start_not_captured"

    return {
        "classification": classification,
        "cadence_classification": cadence_classification,
        "gap_seconds": max(0.0, gap_seconds),
        "expected_cadence_seconds": int(cadence_seconds),
        "grace_period_seconds": int(grace_seconds),
        "late_by_seconds": max(0.0, gap_seconds - cadence_seconds),
        "process_restarted": bool(process_restarted),
        "is_outage": is_outage,
        "outage_start_estimate": _iso_utc(outage_start) if outage_start is not None else None,
        "duration_seconds_estimate": duration_seconds,
        "timing_quality": timing_quality,
    }


def _nearest_rank(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def summarize_issuance_slo(
    issuance_latencies_seconds: Iterable[float],
    lead_seconds: Iterable[float],
    *,
    cutoffs_seconds: tuple[int, ...] = ISSUANCE_SLO_CUTOFFS_SECONDS,
) -> dict[str, Any]:
    """Summarize issuance latency and positive-lead rates without using lag proxies."""
    latencies = [float(value) for value in issuance_latencies_seconds]
    leads = [float(value) for value in lead_seconds]
    if any(not math.isfinite(value) for value in latencies + leads):
        raise ValueError("SLO inputs must be finite numbers")
    if any(cutoff <= 0 for cutoff in cutoffs_seconds):
        raise ValueError("SLO cutoffs must be positive")
    return {
        "issuance_definition": "forecast_persisted_at - (feature_time + 1 hour)",
        "issuance_latency_count": len(latencies),
        "issuance_latency_p50_seconds": _nearest_rank(latencies, 0.50),
        "issuance_latency_p95_seconds": _nearest_rank(latencies, 0.95),
        "issuance_latency_max_seconds": max(latencies) if latencies else None,
        "positive_lead_count": sum(value > 0 for value in leads),
        "lead_count": len(leads),
        "positive_lead_pct": (100.0 * sum(value > 0 for value in leads) / len(leads)) if leads else None,
        "within_cutoff_pct": {
            str(cutoff): (100.0 * sum(value <= cutoff for value in latencies) / len(latencies)) if latencies else None
            for cutoff in cutoffs_seconds
        },
        "cutoffs_seconds": list(cutoffs_seconds),
    }


def build_extended_validation_protocol() -> dict[str, Any]:
    """Describe the next seven-day cohort without creating or starting it."""
    target_hours = 7 * 24
    locations = 63
    return {
        "phase": "EXTENDED_PROSPECTIVE_VALIDATION_T2H_V1",
        "protocol_status": "PREPARED_NOT_STARTED",
        "cohort_created": False,
        "run_id": None,
        "cohort_id": None,
        "duration_days": 7,
        "expected_target_hours": target_hours,
        "expected_locations": locations,
        "expected_logical_slots": target_hours * locations,
        "statistical_unit": "target_hour",
        "contract": {
            "model_id": MODEL_ID,
            "model_sha256": MODEL_SHA256,
            "feature_set_id": FEATURE_SET_ID,
            "feature_count": FEATURE_COUNT,
            "feature_list_sha256": FEATURE_LIST_SHA256,
            "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
            "provider": PROVIDER_NAME,
            "provider_model": PROVIDER_MODEL,
            "safe_hour_semantics": "completed UTC hour H-1",
            "forecast_origin_required": "LIVE_PROSPECTIVE",
            "reference_policy": "first_wins; revisions are preserved as separate evidence",
            "baseline": "persistence",
        },
        "issuance_slo": {
            "definition": "forecast_persisted_at - (feature_time + 1 hour)",
            "report_percentiles": ["p50", "p95", "maximum"],
            "cutoffs_seconds": list(ISSUANCE_SLO_CUTOFFS_SECONDS),
            "primary_validity_requirement": "lead_seconds > 0",
            "thresholds_frozen_before_start": True,
        },
        "reference_revision_fields": [
            "old_payload_hash",
            "new_payload_hash",
            "old_temperature_c",
            "new_temperature_c",
            "old_retrieved_at",
            "new_retrieved_at",
            "location_id",
            "target_time",
        ],
        "forecast_diagnostic_context": [
            "forecast_id",
            "location_id",
            "feature_time",
            "target_time",
            "forecast_persisted_at",
            "forecast_lead_seconds",
            "model_sha256",
            "feature_list_sha256",
            "provider_model",
            "execution_origin",
            "source_event_id",
        ],
        "missingness_policy": {
            "missing_forecast_stays_missing": True,
            "late_retrospective_recovery_stays_late": True,
            "late_forecast_relabelled_live_prospective": False,
            "reference_conflict_is_terminal": True,
            "interpolation": False,
            "imputation_in_primary_metrics": False,
        },
        "new_run_storage_requirements": {
            "checkpoint_template": "data/checkpoints/t2h_v1_1/{new_run_id}/live",
            "checkpoint_must_be_new_and_empty": True,
            "result_directory_must_be_new": True,
            "immutable_manifest_before_start": True,
        },
        "readiness_gates": [
            "runtime_stack_healthy_after_at_least_3_consecutive_hourly_cycles",
            "docker_dns_and_dependency_healthchecks_pass",
            "stable_restart_counts_and_no_restart_loop",
            "readiness_cycle_covers_63_of_63_locations",
            "model_and_feature_hashes_match_frozen_contract",
            "provider_model_is_ecmwf_ifs",
            "new_checkpoint_is_empty_and_unique",
            "new_result_directory_and_immutable_manifest_exist",
        ],
    }


def validate_extended_validation_protocol(protocol: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the prepared protocol's fixed cohort size and frozen contract."""
    errors: list[str] = []
    contract = protocol.get("contract") if isinstance(protocol.get("contract"), Mapping) else {}
    slo = protocol.get("issuance_slo") if isinstance(protocol.get("issuance_slo"), Mapping) else {}
    missingness = protocol.get("missingness_policy") if isinstance(protocol.get("missingness_policy"), Mapping) else {}
    if protocol.get("duration_days") != 7:
        errors.append("DURATION_MUST_BE_7_DAYS")
    if protocol.get("expected_target_hours") != 168:
        errors.append("TARGET_HOURS_MUST_BE_168")
    if protocol.get("expected_locations") != 63:
        errors.append("LOCATION_COUNT_MUST_BE_63")
    if protocol.get("expected_logical_slots") != 168 * 63:
        errors.append("LOGICAL_SLOT_COUNT_MUST_BE_10584")
    if protocol.get("statistical_unit") != "target_hour":
        errors.append("STATISTICAL_UNIT_MUST_BE_TARGET_HOUR")
    expected_contract = {
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_count": FEATURE_COUNT,
        "feature_list_sha256": FEATURE_LIST_SHA256,
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
        "provider": PROVIDER_NAME,
        "provider_model": PROVIDER_MODEL,
        "forecast_origin_required": "LIVE_PROSPECTIVE",
    }
    errors.extend(f"CONTRACT_MISMATCH:{name}" for name, value in expected_contract.items() if contract.get(name) != value)
    if slo.get("cutoffs_seconds") != list(ISSUANCE_SLO_CUTOFFS_SECONDS) or not slo.get("thresholds_frozen_before_start"):
        errors.append("ISSUANCE_SLO_CUTOFFS_MUST_BE_FROZEN_60_300_900")
    if slo.get("primary_validity_requirement") != "lead_seconds > 0":
        errors.append("POSITIVE_LEAD_REQUIREMENT_MISSING")
    required_missingness = {
        "missing_forecast_stays_missing": True,
        "late_retrospective_recovery_stays_late": True,
        "late_forecast_relabelled_live_prospective": False,
        "reference_conflict_is_terminal": True,
        "interpolation": False,
        "imputation_in_primary_metrics": False,
    }
    errors.extend(f"MISSINGNESS_POLICY_MISMATCH:{name}" for name, value in required_missingness.items() if missingness.get(name) != value)
    if protocol.get("cohort_created") is not False or protocol.get("run_id") is not None or protocol.get("cohort_id") is not None:
        errors.append("PREPARATION_MUST_NOT_CREATE_A_COHORT")
    return {"status": "PASS" if not errors else "FAIL", "checks_passed": 23 - len(errors), "checks_total": 23, "errors": errors}
