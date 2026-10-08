"""Reproduce Phase 15's frozen-cohort T2H prospective review artifacts."""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import platform
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pyarrow
import pyarrow.parquet as parquet


RUN_ID = "20261003T165759Z-prospective-live-t2h-v1"
COHORT_ID = "prospective-t2h-20261003T170000Z"
MODEL_ID = "WEATHER_XGBOOST_GLOBAL_T2H_V1_1"
MODEL_SHA256 = "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a"
FEATURE_SET_ID = "WEATHER_FORECAST_FE_T2H_V1_1"
FEATURE_SHA256 = "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2"
FEATURE_COUNT = 73
PROVIDER = "Open-Meteo"
PROVIDER_MODEL = "ecmwf_ifs"
HORIZON_HOURS = 2
ORIGIN = "LIVE_PROSPECTIVE"
BOOTSTRAP_SEED = 20261005
BOOTSTRAP_RESAMPLES = 10_000
TERMINAL_STATES = {
    "EVALUATED",
    "FORECAST_MISSING",
    "MISSING_REFERENCE",
    "REFERENCE_CONFLICT",
    "INVALID",
}


def parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        result = datetime.fromisoformat(text)
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def timestamp_text(value: Any) -> str:
    return parse_timestamp(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return timestamp_text(value)
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=_jsonable) + "\n",
        encoding="utf-8",
    )


def primary_metric_rows(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Keep only frozen EVALUATED slots; missing/conflicted rows never enter metrics."""
    return [row for row in rows if row.get("status") == "EVALUATED"]


def recompute_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise ValueError("at least one EVALUATED row is required")
    truth = np.asarray([float(row["reference_temperature_c"]) for row in rows], dtype=np.float64)
    xgb = np.asarray([float(row["model_prediction_temperature_c"]) for row in rows], dtype=np.float64)
    persistence = np.asarray([float(row["persistence_prediction_temperature_c"]) for row in rows], dtype=np.float64)
    xgb_error = xgb - truth
    persistence_error = persistence - truth
    denominator = float(np.sum((truth - float(np.mean(truth))) ** 2))

    def one(prediction: np.ndarray, error: np.ndarray) -> dict[str, float | int]:
        squared = float(np.mean(error**2))
        return {
            "sample_count": int(len(error)),
            "mae_c": float(np.mean(np.abs(error))),
            "rmse_c": math.sqrt(squared),
            "r2": float(1.0 - float(np.sum(error**2)) / denominator) if denominator else 0.0,
            "bias_c": float(np.mean(error)),
        }

    xgb_metrics = one(xgb, xgb_error)
    persistence_metrics = one(persistence, persistence_error)
    mae_delta = float(xgb_metrics["mae_c"] - persistence_metrics["mae_c"])
    rmse_delta = float(xgb_metrics["rmse_c"] - persistence_metrics["rmse_c"])
    persistence_mae = float(persistence_metrics["mae_c"])
    persistence_rmse = float(persistence_metrics["rmse_c"])
    return {
        "unit": "degrees_celsius",
        "sample_count": len(rows),
        "xgboost": xgb_metrics,
        "persistence": persistence_metrics,
        "delta_xgb_minus_persistence": {"mae_c": mae_delta, "rmse_c": rmse_delta},
        "improvement_vs_persistence": {
            "mae_c": -mae_delta,
            "mae_pct": -mae_delta / persistence_mae * 100.0 if persistence_mae else None,
            "mae_skill": -mae_delta / persistence_mae if persistence_mae else None,
            "rmse_c": -rmse_delta,
            "rmse_pct": -rmse_delta / persistence_rmse * 100.0 if persistence_rmse else None,
        },
    }


def classify_skill(metrics: Mapping[str, Any]) -> str:
    xgb = metrics.get("xgboost") or {}
    persistence = metrics.get("persistence") or {}
    if not xgb or not persistence or int(metrics.get("sample_count", 0)) <= 0:
        return "INSUFFICIENT_VALID_EVALUATIONS"
    xgb_wins_mae = float(xgb["mae_c"]) < float(persistence["mae_c"])
    xgb_wins_rmse = float(xgb["rmse_c"]) < float(persistence["rmse_c"])
    if xgb_wins_mae and xgb_wins_rmse:
        return "POSITIVE_PROSPECTIVE_SKILL"
    if xgb_wins_mae or xgb_wins_rmse:
        return "MIXED_PROSPECTIVE_SKILL"
    return "NO_PROSPECTIVE_SKILL_VS_PERSISTENCE"


def validate_slot_grid(
    rows: Sequence[Mapping[str, Any]], expected_hours: Sequence[str], expected_locations: Sequence[str]
) -> dict[str, Any]:
    expected = {(hour, location) for hour in expected_hours for location in expected_locations}
    observed_counts = Counter(
        (timestamp_text(row["target_time"]), str(row["location_id"])) for row in rows
    )
    observed = set(observed_counts)
    missing = sorted(expected - observed)
    unexpected = sorted(observed - expected)
    duplicates = sorted(key for key, count in observed_counts.items() if count != 1)
    return {
        "expected_slots": len(expected),
        "observed_rows": len(rows),
        "unique_logical_slots": len(observed),
        "missing_slot_count": len(missing),
        "unexpected_slot_count": len(unexpected),
        "duplicate_logical_slot_count": len(duplicates),
        "passed": not missing and not unexpected and not duplicates and len(rows) == len(expected),
        "missing_slots": [{"target_time": key[0], "location_id": key[1]} for key in missing],
        "unexpected_slots": [{"target_time": key[0], "location_id": key[1]} for key in unexpected],
    }


def target_hour_block_bootstrap(
    rows: Sequence[Mapping[str, Any]], *, resamples: int = BOOTSTRAP_RESAMPLES, seed: int = BOOTSTRAP_SEED
) -> dict[str, Any]:
    if resamples < 1:
        raise ValueError("resamples must be positive")
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[timestamp_text(row["target_time"])].append(row)
    hour_names = sorted(groups)
    if not hour_names:
        raise ValueError("at least one target-hour block is required")
    # Additive sufficient statistics keep all locations from one target hour together.
    blocks: list[list[float]] = []
    for hour in hour_names:
        block = groups[hour]
        blocks.append(
            [
                sum(abs(float(row["model_error_c"])) for row in block),
                sum(float(row["model_error_c"]) ** 2 for row in block),
                sum(abs(float(row["persistence_error_c"])) for row in block),
                sum(float(row["persistence_error_c"]) ** 2 for row in block),
                len(block),
            ]
        )
    block_array = np.asarray(blocks, dtype=np.float64)
    generator = np.random.default_rng(seed)
    selection = generator.integers(0, len(block_array), size=(resamples, len(block_array)))
    aggregate = block_array[selection].sum(axis=1)
    n = aggregate[:, 4]
    xgb_mae = aggregate[:, 0] / n
    xgb_rmse = np.sqrt(aggregate[:, 1] / n)
    persistence_mae = aggregate[:, 2] / n
    persistence_rmse = np.sqrt(aggregate[:, 3] / n)
    delta_mae = xgb_mae - persistence_mae
    delta_rmse = xgb_rmse - persistence_rmse
    mae_skill = (persistence_mae - xgb_mae) / persistence_mae

    def interval(values: np.ndarray) -> list[float]:
        return [float(value) for value in np.quantile(values, [0.025, 0.975], method="linear")]

    point = recompute_metrics(rows)
    return {
        "method": "target_hour_block_bootstrap",
        "resampling_unit": "unique target_time; all location rows within the hour are kept together",
        "unique_target_hour_blocks": len(hour_names),
        "resamples": resamples,
        "seed": seed,
        "rng": "NumPy default_rng / PCG64",
        "point_estimates": {
            "delta_mae_c": point["delta_xgb_minus_persistence"]["mae_c"],
            "delta_rmse_c": point["delta_xgb_minus_persistence"]["rmse_c"],
            "mae_skill": point["improvement_vs_persistence"]["mae_skill"],
        },
        "confidence_intervals_95_percentile": {
            "delta_mae_c": interval(delta_mae),
            "delta_rmse_c": interval(delta_rmse),
            "mae_skill": interval(mae_skill),
        },
        "bootstrap_probability_xgboost_beats_persistence": {
            "mae": float(np.mean(delta_mae < 0.0)),
            "rmse": float(np.mean(delta_rmse < 0.0)),
        },
        "bootstrap_probability_delta_positive": {
            "mae": float(np.mean(delta_mae > 0.0)),
            "rmse": float(np.mean(delta_rmse > 0.0)),
        },
        "interpretation": (
            "Only the 20 observed target-hour blocks are resampled. The 63 locations within an hour are not "
            "treated as independent temporal observations; uncertainty remains substantial with this short window."
        ),
    }


def _group_records(rows: Sequence[Mapping[str, Any]], key_fn) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(key_fn(row))].append(row)
    return dict(sorted(grouped.items()))


def _metric_row(rows: Sequence[Mapping[str, Any]], group_name: str) -> dict[str, Any]:
    metrics = recompute_metrics(rows)
    errors = np.asarray([float(row["model_error_c"]) for row in rows], dtype=np.float64)
    absolute = np.abs(errors)
    persistence_errors = np.asarray([float(row["persistence_error_c"]) for row in rows], dtype=np.float64)
    return {
        group_name: "",
        "sample_count": len(rows),
        "xgb_mae_c": metrics["xgboost"]["mae_c"],
        "persistence_mae_c": metrics["persistence"]["mae_c"],
        "mae_skill": metrics["improvement_vs_persistence"]["mae_skill"],
        "delta_mae_xgb_minus_persistence_c": metrics["delta_xgb_minus_persistence"]["mae_c"],
        "xgb_rmse_c": metrics["xgboost"]["rmse_c"],
        "persistence_rmse_c": metrics["persistence"]["rmse_c"],
        "delta_rmse_xgb_minus_persistence_c": metrics["delta_xgb_minus_persistence"]["rmse_c"],
        "xgb_bias_c": metrics["xgboost"]["bias_c"],
        "persistence_bias_c": metrics["persistence"]["bias_c"],
        "mean_error_c": float(np.mean(errors)),
        "median_error_c": float(np.median(errors)),
        "p95_absolute_error_c": float(np.quantile(absolute, 0.95, method="linear")),
        "max_absolute_error_c": float(np.max(absolute)),
        "xgb_mae_win": metrics["xgboost"]["mae_c"] < metrics["persistence"]["mae_c"],
        "xgb_rmse_win": metrics["xgboost"]["rmse_c"] < metrics["persistence"]["rmse_c"],
    }


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    array = np.asarray(values, dtype=np.float64)
    if not len(array):
        return {"count": 0}
    quantiles = np.quantile(array, [0.05, 0.50, 0.95], method="linear")
    return {
        "count": int(len(array)),
        "mean": float(np.mean(array)),
        "median": float(quantiles[1]),
        "std_population": float(np.std(array, ddof=0)),
        "p05": float(quantiles[0]),
        "p95": float(quantiles[2]),
        "min": float(np.min(array)),
        "max": float(np.max(array)),
    }


def _percent(count: int, total: int) -> float:
    return 100.0 * count / total if total else 0.0


def _weather_group(code: float | int | None) -> str:
    if code is None:
        return "missing"
    value = int(code)
    if value == 0:
        return "clear (0)"
    if 1 <= value <= 48:
        return "cloud_or_fog (1-48)"
    if 51 <= value <= 67:
        return "drizzle_or_rain (51-67)"
    if 71 <= value <= 77:
        return "snow (71-77)"
    if 80 <= value <= 86:
        return "showers (80-86)"
    if 95 <= value <= 99:
        return "thunderstorm (95-99)"
    return "other_codes"


def _conditional_bias(rows: Sequence[Mapping[str, Any]], field: str, label_fn) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        value = row.get(field)
        if value is not None:
            grouped[label_fn(float(value))].append(row)
    result = {}
    for label, group in sorted(grouped.items()):
        metrics = recompute_metrics(group)
        result[label] = {
            "sample_count": len(group),
            "xgb_bias_c": metrics["xgboost"]["bias_c"],
            "xgb_mae_c": metrics["xgboost"]["mae_c"],
            "persistence_mae_c": metrics["persistence"]["mae_c"],
            "target_reference_mean_c": float(np.mean([float(row["reference_temperature_c"]) for row in group])),
        }
    return result


def _build_hour_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped = _group_records(rows, lambda row: timestamp_text(row["target_time"]))
    result: list[dict[str, Any]] = []
    for hour, group in grouped.items():
        row = _metric_row(group, "target_time")
        row["target_time"] = hour
        row["sample_count"] = len(group)
        result.append(row)
    return result


def _build_location_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped = _group_records(rows, lambda row: row["location_id"])
    result: list[dict[str, Any]] = []
    for location, group in grouped.items():
        row = _metric_row(group, "location_id")
        row["location_id"] = location
        result.append(row)
    for field, reverse in (
        ("mae_skill", True),
        ("mae_skill", False),
        ("xgb_bias_c", True),
        ("max_absolute_error_c", True),
    ):
        ordered = sorted(result, key=lambda row: (float(row[field]), row["location_id"]), reverse=reverse)
        rank_name = {
            ("mae_skill", True): "best_skill_rank",
            ("mae_skill", False): "worst_skill_rank",
            ("xgb_bias_c", True): "largest_positive_bias_rank",
            ("max_absolute_error_c", True): "largest_absolute_error_rank",
        }[(field, reverse)]
        for rank, row in enumerate(ordered, start=1):
            row[rank_name] = rank
    return sorted(result, key=lambda row: row["location_id"])


def _validate_contract(
    repo_root: Path,
    manifest: Mapping[str, Any],
    cohort_status: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    receipts: Sequence[Mapping[str, Any]],
    provider_validation: Mapping[str, Any],
    provenance_validation: Mapping[str, Any],
    model_validation: Mapping[str, Any],
) -> dict[str, Any]:
    checks: dict[str, Any] = {}

    def record(name: str, passed: bool, observed: Any, expected: Any = True, detail: str | None = None) -> None:
        checks[name] = {"passed": bool(passed), "observed": observed, "expected": expected}
        if detail:
            checks[name]["detail"] = detail

    expected_values = {
        "run_id": RUN_ID,
        "cohort_id": COHORT_ID,
        "model_id": MODEL_ID,
        "model_sha256": MODEL_SHA256,
        "feature_set_id": FEATURE_SET_ID,
        "feature_list_sha256": FEATURE_SHA256,
        "feature_count": FEATURE_COUNT,
        "forecast_horizon_hours": HORIZON_HOURS,
        "provider": PROVIDER,
        "provider_model": PROVIDER_MODEL,
        "execution_origin": ORIGIN,
    }
    manifest_values = {
        "run_id": manifest.get("run_id"),
        "cohort_id": manifest.get("cohort_id"),
        "model_id": manifest.get("model_id"),
        "model_sha256": manifest.get("model_sha256"),
        "feature_set_id": manifest.get("feature_set_id"),
        "feature_list_sha256": manifest.get("feature_list_sha256"),
        "feature_count": manifest.get("feature_count"),
        "forecast_horizon_hours": manifest.get("forecast_horizon_hours"),
        "provider": manifest.get("provider"),
        "provider_model": manifest.get("provider_model"),
        "execution_origin": manifest.get("execution_origin"),
    }
    record("manifest_contract", manifest_values == expected_values, manifest_values, expected_values)
    status_values = {key: cohort_status.get(key) for key in ("run_id", "cohort_id")}
    record("finalized_status_identity", status_values == {"run_id": RUN_ID, "cohort_id": COHORT_ID}, status_values)
    record("cohort_finalized", cohort_status.get("status") == "FINALIZED", cohort_status.get("status"), "FINALIZED")
    record("provider_contract", provider_validation.get("status") == "PASS" and provider_validation.get("provider_model") == PROVIDER_MODEL, provider_validation)
    record("model_validation", model_validation.get("status") == "PASS" and model_validation.get("model_sha256") == MODEL_SHA256, model_validation)
    record("provenance_validation", provenance_validation.get("status") == "PASS" and provenance_validation.get("invalid_provenance_slots") == 0, provenance_validation)

    expected_hours = [timestamp_text(value) for value in manifest.get("target_times", [])]
    expected_locations = list(manifest.get("canonical_locations", []))
    start = parse_timestamp("2026-10-03T17:00:00Z")
    generated_hours = [timestamp_text(start + timedelta(hours=i)) for i in range(24)]
    record("target_window", expected_hours == generated_hours and len(set(expected_hours)) == 24, expected_hours, generated_hours)
    record("location_catalog", len(expected_locations) == 63 and len(set(expected_locations)) == 63, {"count": len(expected_locations), "unique": len(set(expected_locations))}, {"count": 63, "unique": 63})
    dimensions = {
        "expected_target_hours": cohort_status.get("expected_target_hours"),
        "expected_locations": cohort_status.get("expected_locations"),
        "expected_slots": cohort_status.get("expected_slots"),
        "terminal_slots": cohort_status.get("terminal_slots"),
        "pending_target": cohort_status.get("pending_target"),
        "pending_reference": cohort_status.get("pending_reference"),
    }
    record("finalized_summary_dimensions", dimensions == {
        "expected_target_hours": 24, "expected_locations": 63, "expected_slots": 1512,
        "terminal_slots": 1512, "pending_target": 0, "pending_reference": 0,
    }, dimensions)
    grid = validate_slot_grid(rows, expected_hours, expected_locations)
    record("logical_slot_grid", grid["passed"], grid, {"passed": True})

    status_counts = dict(Counter(str(row.get("status")) for row in rows))
    frozen_counts = cohort_status.get("slot_status_counts", {})
    record("slot_status_counts", status_counts == frozen_counts, status_counts, frozen_counts)
    terminal_count = sum(count for status, count in status_counts.items() if status in TERMINAL_STATES)
    record("terminal_slots", terminal_count == 1512, terminal_count, 1512)
    nonterminal = [row for row in rows if row.get("status") not in TERMINAL_STATES]
    record("pending_slots", len(nonterminal) == 0, len(nonterminal), 0)

    evaluation_ids = [str(row["evaluation_id"]) for row in rows if row.get("evaluation_id")]
    forecast_ids = [str(row["forecast_id"]) for row in rows if row.get("forecast_id")]
    receipt_ids = [str(row["forecast_id"]) for row in receipts if row.get("forecast_id")]
    record("duplicate_evaluation_ids", len(evaluation_ids) == len(set(evaluation_ids)), len(evaluation_ids) - len(set(evaluation_ids)), 0)
    record("duplicate_forecast_ids", len(forecast_ids) == len(set(forecast_ids)), len(forecast_ids) - len(set(forecast_ids)), 0)
    record("duplicate_persistence_receipts", len(receipt_ids) == len(set(receipt_ids)), len(receipt_ids) - len(set(receipt_ids)), 0)
    receipt_hashes = [str(row["record_sha256"]) for row in receipts if row.get("record_sha256")]
    record("duplicate_persistence_receipt_hashes", len(receipt_hashes) == len(set(receipt_hashes)), len(receipt_hashes) - len(set(receipt_hashes)), 0)
    receipt_logical = [(str(row.get("target_time")), str(row.get("location_id"))) for row in receipts]
    record("duplicate_receipt_logical_forecasts", len(receipt_logical) == len(set(receipt_logical)), len(receipt_logical) - len(set(receipt_logical)), 0)

    receipt_by_id = {str(row["forecast_id"]): row for row in receipts if row.get("forecast_id")}
    accepted_ids = set(forecast_ids)
    matched = accepted_ids.issubset(receipt_by_id)
    unmatched = [row for row in receipts if str(row.get("forecast_id")) not in accepted_ids]
    slot_status = {(timestamp_text(row["target_time"]), str(row["location_id"])): row.get("status") for row in rows}
    grace_seconds = int(manifest.get("forecast_grace_period_seconds", 0))
    late_unmatched = []
    unmatched_explained = True
    for receipt in unmatched:
        key = (timestamp_text(receipt["target_time"]), str(receipt["location_id"]))
        boundary = parse_timestamp(receipt["feature_time"]) + timedelta(hours=1)
        persisted_at = parse_timestamp(receipt["forecast_persisted_at"])
        latency = (persisted_at - boundary).total_seconds()
        if slot_status.get(key) != "FORECAST_MISSING" or latency <= grace_seconds:
            unmatched_explained = False
        late_unmatched.append({
            "target_time": key[0], "location_id": key[1], "forecast_id": receipt.get("forecast_id"),
            "issuance_boundary": timestamp_text(boundary), "forecast_persisted_at": timestamp_text(persisted_at),
            "issuance_latency_seconds": latency, "grace_period_seconds": grace_seconds,
        })
    record("forecast_receipt_membership", matched and unmatched_explained, {
        "cohort_forecast_ids": len(accepted_ids), "matched_receipt_ids": len(accepted_ids & set(receipt_by_id)),
        "unmatched_receipts": len(unmatched), "unmatched_receipts_all_late_after_grace_in_missing_slots": unmatched_explained,
    }, {"unmatched_receipts": "only receipts later than frozen grace may remain outside terminal membership"})

    contract_violations = []
    for row in receipts:
        if any([
            row.get("model_id") != MODEL_ID,
            row.get("model_sha256") != MODEL_SHA256,
            row.get("feature_set_id") != FEATURE_SET_ID,
            row.get("feature_list_sha256") != FEATURE_SHA256,
            row.get("feature_count") != FEATURE_COUNT,
            row.get("provider") != PROVIDER,
            row.get("provider_model") != PROVIDER_MODEL,
            row.get("execution_origin") != ORIGIN,
            row.get("forecast_horizon_hours") != HORIZON_HOURS,
            (parse_timestamp(row["target_time"]) - parse_timestamp(row["feature_time"])).total_seconds() != HORIZON_HOURS * 3600,
            float(row.get("forecast_lead_seconds") or 0) <= 0,
        ]):
            contract_violations.append(str(row.get("forecast_id")))
    record("receipt_model_and_positive_lead_contract", not contract_violations, {"violation_count": len(contract_violations)}, 0)
    record("invalid_provenance_count", cohort_status.get("invalid_provenance") == 0, cohort_status.get("invalid_provenance"), 0)
    record("duplicate_validation_status", cohort_status.get("duplicate_validation", {}).get("status") == "PASS", cohort_status.get("duplicate_validation"))

    checksums_path = repo_root / "results" / "prospective-live-t2h" / RUN_ID / "checksums.json"
    checksum_manifest = json.loads(checksums_path.read_text(encoding="utf-8"))
    checksum_errors = []
    for item in checksum_manifest.get("files", []):
        path = checksums_path.parent / item["path"]
        if not path.is_file() or path.stat().st_size != item["bytes"] or sha256_file(path) != item["sha256"]:
            checksum_errors.append(item["path"])
    record("phase14_evidence_checksums", not checksum_errors, {"file_count": len(checksum_manifest.get("files", [])), "mismatch_paths": checksum_errors}, {"mismatch_paths": []})
    passed = all(value["passed"] for value in checks.values())
    return {
        "classification": "PASS" if passed else "COHORT_INTEGRITY_FAILURE",
        "checks_passed": sum(bool(value["passed"]) for value in checks.values()),
        "checks_total": len(checks),
        "checks": checks,
        "unmatched_late_receipts": late_unmatched,
    }


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        path.write_text("\n", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for name in row:
            if name not in fields:
                fields.append(name)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _distribution_report(rows: Sequence[Mapping[str, Any]], joined: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    errors = [float(row["model_error_c"]) for row in rows]
    absolute = [abs(value) for value in errors]
    p_errors = [float(row["persistence_error_c"]) for row in rows]
    top = sorted(joined, key=lambda row: (-abs(float(row["model_error_c"])), str(row["location_id"]), timestamp_text(row["target_time"])))[:10]
    top_records = []
    for row in top:
        top_records.append({
            key: row.get(key)
            for key in (
                "location_id", "feature_time", "target_time", "model_prediction_temperature_c",
                "reference_temperature_c", "persistence_prediction_temperature_c", "model_error_c",
                "persistence_error_c", "humidity_pct", "precipitation_mm", "weather_code",
                "pressure_hpa", "wind_speed_kmh", "wind_gust_kmh",
            )
        })
    temp_bins = lambda value: "<20" if value < 20 else "20-<25" if value < 25 else "25-<30" if value < 30 else ">=30"
    humidity_bins = lambda value: "<70" if value < 70 else "70-<85" if value < 85 else ">=85"
    return {
        "error_definition": "model_error_c = model_prediction_temperature_c - reference_temperature_c",
        "population_standard_deviation": True,
        "model_error_c": _distribution(errors),
        "absolute_model_error_c": _distribution(absolute),
        "persistence_error_c": _distribution(p_errors),
        "absolute_error_threshold_counts": {
            "model_abs_error_gt_3_c": sum(value > 3 for value in absolute),
            "model_abs_error_gt_5_c": sum(value > 5 for value in absolute),
            "persistence_abs_error_gt_3_c": sum(abs(value) > 3 for value in p_errors),
            "persistence_abs_error_gt_5_c": sum(abs(value) > 5 for value in p_errors),
        },
        "large_error_records_top_10": top_records,
        "conditional_bias_exploratory": {
            "covariate_semantics": "These are target-time canonical reference attributes in the frozen evaluation record, not a causal attribution or guaranteed representation of the 73 model input features.",
            "by_target_reference_temperature_c": _conditional_bias(joined, "reference_temperature_c", temp_bins),
            "by_target_reference_humidity_pct": _conditional_bias(joined, "humidity_pct", humidity_bins),
            "by_target_reference_precipitation": _conditional_bias(joined, "precipitation_mm", lambda value: "no_rain (0 mm)" if value <= 0 else "rain (>0 mm)"),
            "by_target_reference_weather_code": _conditional_bias(joined, "weather_code", _weather_group),
            "by_location": {
                row["location_id"]: {
                    "sample_count": row["sample_count"],
                    "xgb_bias_c": row["xgb_bias_c"],
                    "xgb_mae_c": row["xgb_mae_c"],
                    "persistence_mae_c": row["persistence_mae_c"],
                }
                for row in _build_location_rows(rows)
            },
            "by_target_hour": {
                row["target_time"]: {"sample_count": row["sample_count"], "xgb_bias_c": row["xgb_bias_c"]}
                for row in _build_hour_rows(rows)
            },
            "region": "Not analyzed; the frozen cohort contract has no region labels to reuse.",
        },
    }


def _markdown_report(
    integrity: Mapping[str, Any], aggregate: Mapping[str, Any], completeness: Mapping[str, Any],
    conflicts: Mapping[str, Any], locations: Sequence[Mapping[str, Any]], hours: Sequence[Mapping[str, Any]],
    error_report: Mapping[str, Any], operational: Mapping[str, Any], bootstrap: Mapping[str, Any],
    sensitivity: Mapping[str, Any], offline: Mapping[str, Any], decision: Mapping[str, Any], output_files: Sequence[str],
    cohort_integrity_summary: Mapping[str, Any],
    verification_summary: Mapping[str, Any] | None = None,
) -> str:
    top_locations = sorted(locations, key=lambda row: (-float(row["mae_skill"]), row["location_id"]))[:5]
    worst_locations = sorted(locations, key=lambda row: (float(row["mae_skill"]), row["location_id"]))[:5]
    worst_hours = sorted(hours, key=lambda row: (-float(row["xgb_mae_c"]), row["target_time"]))[:5]
    best_hours = sorted(hours, key=lambda row: (float(row["delta_mae_xgb_minus_persistence_c"]), row["target_time"]))[:5]
    verification = verification_summary or {}
    post_cohort = operational.get("post_cohort_runtime_snapshot", {})
    runtime_counts = post_cohort.get("container_restart_counts", {})
    stopped_dependencies = post_cohort.get("stopped_dependencies", [])
    log_signals = [str(signal).rstrip().rstrip(".") for signal in post_cohort.get("log_error_signals", [])]
    restart_increase = post_cohort.get("increase_since_previous_observation", {})
    git_status_entries = verification.get("git_status", [])
    lines = [
        "# Phase 15 — Prospective Validation Review & Model Decision V1 — T2H",
        "",
        f"Run `{RUN_ID}` · cohort `{COHORT_ID}` · frozen window `2026-10-03T17:00:00Z`–`2026-10-04T16:00:00Z`.",
        "",
        "## 1. Phase 14 cohort integrity",
        "",
        f"**{integrity['classification']}** ({integrity['checks_passed']}/{integrity['checks_total']} checks). Contract verified: `{cohort_integrity_summary['model_contract']['model_id']}` SHA `{cohort_integrity_summary['model_contract']['model_sha256']}`, `{cohort_integrity_summary['model_contract']['feature_set_id']}` ({cohort_integrity_summary['model_contract']['feature_count']} features) SHA `{cohort_integrity_summary['model_contract']['feature_list_sha256']}`, horizon {cohort_integrity_summary['model_contract']['forecast_horizon_hours']}h, `{cohort_integrity_summary['model_contract']['provider']}` / `{cohort_integrity_summary['model_contract']['provider_model']}`, origin `{cohort_integrity_summary['model_contract']['forecast_origin_required']}`. Final grid: 24 hours × 63 locations = 1,512 terminal slots; pending target/reference 0/0; duplicate evaluation IDs, forecast IDs, logical forecasts, and persistence receipts all 0; invalid provenance 0; recorded Phase 14 restart_count={cohort_integrity_summary['finalized_state']['restart_count']}. All 30 Phase 14 checksummed files reconcile.",
        "",
        "The receipt file contains 63 additional `NEW_DELTA_WRITE` receipts for target 14:00Z. They were persisted about 53 minutes after the 13:00Z issuance boundary, later than the frozen 900-second forecast grace. Each maps to an already-terminal `FORECAST_MISSING` slot and is therefore retained as late operational evidence, not admitted retroactively. This reconciles 1,386 immutable receipts with 1,323 cohort forecast memberships without changing Phase 14 classifications.",
        "",
        "## 2. Recomputed aggregate metrics",
        "",
        "Metrics below use only 1,260 `EVALUATED` rows; missing and reference-conflict rows are excluded.",
        "",
        "| Metric | XGBoost | Persistence | Δ XGB − persistence |",
        "|---|---:|---:|---:|",
        f"| MAE (°C) | {aggregate['xgboost']['mae_c']:.6f} | {aggregate['persistence']['mae_c']:.6f} | {aggregate['delta_xgb_minus_persistence']['mae_c']:+.6f} |",
        f"| RMSE (°C) | {aggregate['xgboost']['rmse_c']:.6f} | {aggregate['persistence']['rmse_c']:.6f} | {aggregate['delta_xgb_minus_persistence']['rmse_c']:+.6f} |",
        f"| R² | {aggregate['xgboost']['r2']:.6f} | {aggregate['persistence']['r2']:.6f} | — |",
        f"| Bias (°C) | {aggregate['xgboost']['bias_c']:+.6f} | {aggregate['persistence']['bias_c']:+.6f} | — |",
        "",
        f"MAE skill is {aggregate['improvement_vs_persistence']['mae_skill']:+.4%}; MAE improvement is {aggregate['improvement_vs_persistence']['mae_pct']:+.2f}%; RMSE improvement is {aggregate['improvement_vs_persistence']['rmse_pct']:+.2f}%. Independently recomputed values match the frozen Phase 14 summary within 1e−9.",
        "",
        "## 3. Completeness / missingness analysis",
        "",
        f"Evaluated: {completeness['status_counts'].get('EVALUATED', 0)}/1,512 ({completeness['rates_pct']['usable_primary_metric']:.2f}%). Missing forecast: {completeness['status_counts'].get('FORECAST_MISSING', 0)} ({completeness['rates_pct']['forecast_missing']:.2f}%). Reference conflict: {completeness['status_counts'].get('REFERENCE_CONFLICT', 0)} ({completeness['rates_pct']['reference_conflict']:.2f}%). Pending: 0.",
        "",
        f"Affected forecast-missing hours: {', '.join(completeness['hours_by_status'].get('FORECAST_MISSING', []))}. The 14:00Z block has late receipts after the grace deadline; 15:00Z and 16:00Z have no corresponding receipt. No missing slot is imputed or scored as a model error.",
        "",
        "## 4. Reference conflict analysis",
        "",
        f"{conflicts['reference_conflict_count']} slots share one revised feature hour ({conflicts['affected_feature_hours'][0] if conflicts['affected_feature_hours'] else 'none'}), corresponding to target hour {', '.join(conflicts['affected_target_hours'])}. The retained policy is first archived payload wins; later differing payloads are audited and do not replace it. All {conflicts['distinct_old_new_payload_hash_count']} old/new payload pairs have distinct hashes. Numeric temperature revision deltas cannot be recovered: retained revision rows contain hashes and event/ingestion timestamps, while conflict evaluation values are null. The review does not reclassify these slots.",
        "",
        "## 5. Per-location findings",
        "",
        f"XGBoost beats persistence on MAE at {aggregate['location_comparison']['xgb_mae_wins']}/63 locations; {aggregate['location_comparison']['equal_or_trails']} equal or trail. Best five by MAE skill: " + ", ".join(f"{r['location_id']} ({r['mae_skill']:+.3f})" for r in top_locations) + ".",
        "",
        "Worst five by MAE skill: " + ", ".join(f"{r['location_id']} ({r['mae_skill']:+.3f})" for r in worst_locations) + ". Location groupings are descriptive; no causal or unregistered regional attribution is made. Full 63-location metrics and rankings are in `per_location_metrics.csv`.",
        "",
        "## 6. Per-target-hour findings",
        "",
        f"There are {len(hours)} valid target-hour blocks, each with 63 locations. XGBoost wins MAE in {aggregate['hour_comparison']['mae_wins']} hours and RMSE in {aggregate['hour_comparison']['rmse_wins']}; it loses both in {aggregate['hour_comparison']['both_metrics_lost']} hours and wins both in {aggregate['hour_comparison']['both_metrics_won']}.",
        "",
        "Largest XGBoost MAE hours: " + ", ".join(f"{r['target_time']} ({r['xgb_mae_c']:.3f} °C)" for r in worst_hours) + ".",
        "",
        "Hours with the strongest relative MAE advantage over persistence: " + ", ".join(f"{r['target_time']} (Δ {r['delta_mae_xgb_minus_persistence_c']:+.3f} °C)" for r in best_hours) + ".",
        "",
        f"The aggregate loss is broad across hours rather than caused by one hour: every leave-one-hour-out aggregate still loses on both MAE and RMSE. The positive XGBoost bias also remains positive in every hour.",
        "",
        "## 7. Bias analysis",
        "",
        f"XGBoost bias is {aggregate['xgboost']['bias_c']:+.3f} °C versus persistence {aggregate['persistence']['bias_c']:+.3f} °C. All 20 valid target hours have positive XGBoost bias; leave-one-hour-out bias stays between {sensitivity['leave_one_hour_out']['xgb_bias_min_c']:.3f} and {sensitivity['leave_one_hour_out']['xgb_bias_max_c']:.3f} °C.",
        "",
        "Exploratory target-reference strata show higher mean XGBoost bias for cooler target temperatures and for rainy/high-humidity target conditions. These are target-time reference attributes, not proof of predictor attribution or causation. No post-hoc correction was applied. See `error_distribution.json` for bins and counts.",
        "",
        "## 8. Large-error analysis",
        "",
        f"Model-error distribution: mean {error_report['model_error_c']['mean']:+.3f} °C, median {error_report['model_error_c']['median']:+.3f} °C, p05 {error_report['model_error_c']['p05']:+.3f} °C, p95 {error_report['model_error_c']['p95']:+.3f} °C, range [{error_report['model_error_c']['min']:+.3f}, {error_report['model_error_c']['max']:+.3f}] °C. {error_report['absolute_error_threshold_counts']['model_abs_error_gt_3_c']} rows exceed 3 °C absolute error and {error_report['absolute_error_threshold_counts']['model_abs_error_gt_5_c']} exceed 5 °C. Top ten records include available target-reference weather context in `error_distribution.json`.",
        "",
        "## 9. Operational / issuance timing analysis",
        "",
        f"Recomputed lead is positive for {operational['forecast_lead_seconds']['positive_count']}/{operational['forecast_lead_seconds']['count']} receipts ({operational['forecast_lead_seconds']['positive_pct']:.2f}%); nonpositive lead violations={operational['forecast_lead_seconds']['nonpositive_count']}. Issuance latency is `forecast_persisted_at − (feature_time + 1h)`: across {operational['receipt_counts']['all_receipts']} preserved receipts, median {operational['issuance_latency_seconds']['median']:.1f}s, p95 {operational['issuance_latency_seconds']['p95']:.1f}s; {operational['issuance_latency_seconds']['late_after_grace_count']} exceeded the 900-second grace, including {operational['issuance_latency_seconds']['cohort_membership_late_after_grace_count']} cohort-member receipts. The frozen 60-second issuance SLO passed {operational['issuance_slo']['cycles_passed']}/{operational['issuance_slo']['hours']} cycles. Reference-arrival and evaluation lag are reported separately and are not used as issuance latency.",
        "",
        f"The frozen cohort records restart_count={operational['phase14_runtime']['restart_count']}; it retains all 21 inferred outage events. The detector flagged gaps above 15 minutes despite approximately hourly inference cadence, so raw event count is not reliability evidence: {operational['phase14_runtime']['cadence_like_gap_count']} intervals are similar to hourly cadence and {operational['phase14_runtime']['long_gap_count']} exceed that pattern. The known Phase 14 Spark self-join incident and earlier live producer HTTP 429 incident remain visible; the sealed local artifacts do not quantify 429-caused slots.",
        f"Read-only post-cohort Docker snapshot at {post_cohort.get('observed_at_utc', 'not captured')}: inference RestartCount={runtime_counts.get('streaming_inference_t2h_live', 'not captured')}, producer RestartCount={runtime_counts.get('live_hourly_producer_t2h', 'not captured')}; counts increased by +{restart_increase.get('streaming_inference_t2h_live', 'n/a')} and +{restart_increase.get('live_hourly_producer_t2h', 'n/a')} over {restart_increase.get('elapsed_seconds', 'n/a')}s since the preceding snapshot. The restart loop is unresolved. Stopped dependencies: {', '.join(stopped_dependencies) or 'none recorded'}. Current log signals: {'; '.join(log_signals) or 'not captured'}. This snapshot is separate from the finalized cohort and does not change its recorded restart_count.",
        "",
        "## 10. Target-hour block bootstrap",
        "",
        f"Using {bootstrap['resamples']:,} deterministic resamples, seed {bootstrap['seed']}, and {bootstrap['unique_target_hour_blocks']} target-hour blocks: ΔMAE 95% CI {bootstrap['confidence_intervals_95_percentile']['delta_mae_c']}; ΔRMSE CI {bootstrap['confidence_intervals_95_percentile']['delta_rmse_c']}; MAE-skill CI {bootstrap['confidence_intervals_95_percentile']['mae_skill']}. Bootstrap probability XGBoost beats persistence is {bootstrap['bootstrap_probability_xgboost_beats_persistence']['mae']:.1%} for MAE and {bootstrap['bootstrap_probability_xgboost_beats_persistence']['rmse']:.1%} for RMSE. The intervals include zero; 20 temporal blocks leave substantial uncertainty.",
        "",
        "## 11. Sensitivity analysis",
        "",
        f"Leave-one-hour-out classification remains `{sensitivity['leave_one_hour_out']['classification_counts']}` for all 20 exclusions. Across exclusions, ΔMAE ranges {sensitivity['leave_one_hour_out']['delta_mae_min_c']:+.3f} to {sensitivity['leave_one_hour_out']['delta_mae_max_c']:+.3f} °C and ΔRMSE {sensitivity['leave_one_hour_out']['delta_rmse_min_c']:+.3f} to {sensitivity['leave_one_hour_out']['delta_rmse_max_c']:+.3f} °C. Exploratory strata are specified in `error_distribution.json`; they do not change the primary result.",
        "",
        "## 12. Offline vs prospective comparison",
        "",
        "Offline TEST uses Open-Meteo Historical Forecast ECMWF IFS predictors with ERA5 target at feature_time+2h. Prospective evaluation uses the later live Open-Meteo ECMWF IFS forecast product as reference. These targets are not interchangeable and the prospective reference is not station truth.",
        "",
        f"Offline TEST XGB MAE/RMSE: {offline['offline_test']['xgboost']['mae_c']:.6f}/{offline['offline_test']['xgboost']['rmse_c']:.6f} °C; persistence: {offline['offline_test']['persistence']['mae_c']:.6f}/{offline['offline_test']['persistence']['rmse_c']:.6f} °C; bias XGB/persistence {offline['offline_test']['xgboost']['bias_c']:+.6f}/{offline['offline_test']['persistence']['bias_c']:+.6f} °C; location wins {offline['offline_test']['location_win_count']}/63. Prospective XGB MAE/RMSE: {aggregate['xgboost']['mae_c']:.6f}/{aggregate['xgboost']['rmse_c']:.6f} °C; persistence: {aggregate['persistence']['mae_c']:.6f}/{aggregate['persistence']['rmse_c']:.6f} °C; bias XGB/persistence {aggregate['xgboost']['bias_c']:+.6f}/{aggregate['persistence']['bias_c']:+.6f} °C; location wins {aggregate['location_comparison']['xgb_mae_wins']}/63. This is descriptive evidence only. Issuance-vintage mismatch, IFS evolution, target semantics, regime and calibration are hypotheses, not established causes.",
        "",
        "## 13. Scientific limitations",
        "",
        "One 24-hour cohort supplies only 20 evaluated target-hour blocks; 63 locations within each hour are spatial samples, not independent time blocks. Usable metric coverage is 83.33%. The operational reference is neither ERA5 nor station truth. Feature-time model inputs were not retained row-by-row in the evaluation dataset; target-reference weather fields support exploratory strata only. Numeric old/new temperatures for reference revisions were not retained, so their difference distribution cannot be computed. No delayed ERA5 data were fetched; the existing manifest remains `PREPARED_NOT_FETCHED`.",
        "",
        "## 14. Prospective classification",
        "",
        f"`{decision['prospective_skill_classification']}`. The point estimates lose on both MAE and RMSE, per the frozen classification rule.",
        "",
        "## 15. Model decision",
        "",
        f"`{decision['model_decision']}`. The result does not accept the model for production hardening. The decision reflects the broad positive bias and point-estimate losses, while the bootstrap intervals overlap zero, the prospective target differs from offline ERA5, only 20 hourly blocks are available, and the forecast/evaluation stream had material missingness and issuance delays.",
        "",
        "## 16. Retraining required/recommended?",
        "",
        f"{decision['retrain_status']}. No training or tuning was performed. If future evidence supports canonical retraining, it must be carried out on Google Colab GPU.",
        "",
        "## 17. Production hardening allowed?",
        "",
        f"`{str(decision['production_hardening_allowed']).lower()}`. The decision is not an explicit acceptance of the T2H model.",
        "",
        "## 18. Files created/changed",
        "",
        "New review-package artifacts:",
        "",
        *[f"- `{name}`" for name in output_files],
        "",
        "The review generator and focused tests are separate new files; no existing Phase 14 source/evidence, model, feature contract, or runtime state was edited.",
        "",
        "## 19. Test results",
        "",
        f"`compileall`: {verification.get('compileall', {}).get('summary', 'not recorded')}",
        f"`pytest -q`: {verification.get('pytest', {}).get('summary', 'not recorded')}",
        f"`git diff --check`: {verification.get('git_diff_check', {}).get('summary', 'not recorded')}",
        "The generator also runs cohort integrity and Phase 14 metric-reproduction assertions before writing artifacts.",
        "",
        "## 20. git diff --stat",
        "",
        "```text",
        str(verification.get("git_diff_stat", "The Phase 15 package is untracked and is not included in `git diff --stat`.")).strip(),
        "```",
        "No commit was made.",
        "",
        "## 21. git status",
        "",
        *(f"- `{entry}`" for entry in git_status_entries),
        "The pre-existing Phase 14 working-tree changes are shown alongside the new Phase 15 analysis and review package.",
        "",
    ]
    return "\n".join(lines)


def generate_review(
    repo_root: Path,
    output_dir: Path,
    *,
    bootstrap_resamples: int = BOOTSTRAP_RESAMPLES,
    verification_summary: Mapping[str, Any] | None = None,
    post_cohort_runtime_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if output_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing review package: {output_dir}")
    run_results = repo_root / "results" / "prospective-live-t2h" / RUN_ID
    spark_results = repo_root / "results" / "streaming-inference-t2h" / RUN_ID / "spark_live"
    runtime_dir = repo_root / "data" / "runtime" / "prospective-live-t2h" / RUN_ID
    offline_dir = repo_root / "results" / "modeling-t2h" / "20261003T122405Z-xgb-t2h-v1-1-colab"
    status = _read_json(run_results / "cohort_status.json")
    manifest = _read_json(run_results / "cohort_manifest.json")
    contract = _read_json(run_results / "model_contract_validation.json")
    provider_validation = _read_json(run_results / "provider_contract_validation.json")
    provenance = _read_json(run_results / "provenance_validation.json")
    revisions = _read_json(run_results / "reference_revision_audit.json")
    runtime = _read_json(run_results / "runtime_summary.json")
    slo = _read_json(run_results / "issuance_slo.json")
    delay_era5 = _read_json(run_results / "delayed_era5_manifest.json")
    checksums = _read_json(run_results / "checksums.json")
    evaluation_path = run_results / "prospective_evaluations.parquet"
    rows = parquet.read_table(evaluation_path).to_pylist()
    state = _read_json(runtime_dir / "cohort_state.json")
    receipts = [json.loads(line) for line in (spark_results / "forecast_persistence_receipts.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    revision_rows = [json.loads(line) for line in (spark_results / "reference_revisions.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    evaluated = primary_metric_rows(rows)

    feature_path = offline_dir / "feature_list.json"
    feature_doc = _read_json(feature_path)
    feature_names = tuple(feature_doc.get("ordered_model_features", []))
    feature_hash_payload = {
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "feature_count": len(feature_names),
        "ordered_model_features": list(feature_names),
        "hash_algorithm": "sha256 of this canonical JSON artifact, including identity and order",
    }
    feature_hash = hashlib.sha256((json.dumps(feature_hash_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")).hexdigest()
    model_path = offline_dir / "weather_forecast_xgboost_t2h_v1_1.json"
    computed_model_sha = sha256_file(model_path)
    contract = dict(contract)
    contract["actual_model_file_sha256"] = computed_model_sha
    contract["actual_feature_list_identity_sha256"] = feature_hash
    contract["actual_feature_count"] = len(feature_names)
    integrity = _validate_contract(repo_root, manifest, status, rows, receipts, provider_validation, provenance, contract)
    # Add independent artifact-level hashes to the checks even though the Phase 14 validator also passed.
    identity_ok = computed_model_sha == MODEL_SHA256 and feature_hash == FEATURE_SHA256 and len(feature_names) == FEATURE_COUNT
    integrity["checks"]["local_model_and_feature_artifacts"] = {
        "passed": identity_ok,
        "observed": {"model_sha256": computed_model_sha, "feature_list_sha256": feature_hash, "feature_count": len(feature_names)},
        "expected": {"model_sha256": MODEL_SHA256, "feature_list_sha256": FEATURE_SHA256, "feature_count": FEATURE_COUNT},
    }
    integrity["checks_total"] += 1
    integrity["checks_passed"] += int(identity_ok)
    if not identity_ok:
        integrity["classification"] = "COHORT_INTEGRITY_FAILURE"
    cohort_integrity_summary = {
        "classification": integrity["classification"],
        "checks_passed": integrity["checks_passed"],
        "checks_total": integrity["checks_total"],
        "checks": integrity["checks"],
        "model_contract": {
            "model_id": MODEL_ID,
            "model_sha256": computed_model_sha,
            "feature_set_id": FEATURE_SET_ID,
            "feature_count": len(feature_names),
            "feature_list_sha256": feature_hash,
            "forecast_horizon_hours": HORIZON_HOURS,
            "provider": PROVIDER,
            "provider_model": PROVIDER_MODEL,
            "forecast_origin_required": ORIGIN,
        },
        "target_window": {
            "start": timestamp_text(manifest["target_times"][0]),
            "end": timestamp_text(manifest["target_times"][-1]),
        },
        "expected_grid": {"target_hours": 24, "locations": 63, "slots": 1512},
        "finalized_state": {
            "status": status.get("status"),
            "cohort_completeness_classification": status.get("cohort_completeness_classification"),
            "terminal_slots": status.get("terminal_slots"),
            "pending_target": status.get("pending_target"),
            "pending_reference": status.get("pending_reference"),
            "duplicate_validation": status.get("duplicate_validation"),
            "invalid_provenance": provenance.get("invalid_provenance_slots"),
            "restart_count": runtime.get("restart_count"),
            "slot_status_counts": status.get("slot_status_counts"),
        },
        "phase14_checksums": {"declared_files": len(checksums.get("files", [])), "verified": True},
    }

    expected_metrics = status.get("interim_metrics", {})
    aggregate = recompute_metrics(evaluated)
    aggregate["phase14_reproduction"] = {
        "tolerance": 1e-9,
        "checks": {
            "xgb_mae": abs(aggregate["xgboost"]["mae_c"] - expected_metrics.get("model_mae", float("inf"))) <= 1e-9,
            "xgb_rmse": abs(aggregate["xgboost"]["rmse_c"] - expected_metrics.get("model_rmse", float("inf"))) <= 1e-9,
            "xgb_r2": abs(aggregate["xgboost"]["r2"] - expected_metrics.get("model_r2", float("inf"))) <= 1e-9,
            "xgb_bias": abs(aggregate["xgboost"]["bias_c"] - expected_metrics.get("model_bias", float("inf"))) <= 1e-9,
            "persistence_mae": abs(aggregate["persistence"]["mae_c"] - expected_metrics.get("persistence_mae", float("inf"))) <= 1e-9,
            "persistence_rmse": abs(aggregate["persistence"]["rmse_c"] - expected_metrics.get("persistence_rmse", float("inf"))) <= 1e-9,
            "persistence_r2": abs(aggregate["persistence"]["r2"] - expected_metrics.get("persistence_r2", float("inf"))) <= 1e-9,
            "persistence_bias": abs(aggregate["persistence"]["bias_c"] - expected_metrics.get("persistence_bias", float("inf"))) <= 1e-9,
        },
    }
    aggregate["phase14_reproduction"]["passed"] = all(aggregate["phase14_reproduction"]["checks"].values())
    aggregate["prospective_skill_classification"] = classify_skill(aggregate)

    hour_rows = _build_hour_rows(evaluated)
    location_rows = _build_location_rows(evaluated)
    hour_groups = _group_records(evaluated, lambda row: timestamp_text(row["target_time"]))
    hour_mae_wins = sum(row["xgb_mae_win"] for row in hour_rows)
    hour_rmse_wins = sum(row["xgb_rmse_win"] for row in hour_rows)
    both_won = sum(row["xgb_mae_win"] and row["xgb_rmse_win"] for row in hour_rows)
    both_lost = sum(not row["xgb_mae_win"] and not row["xgb_rmse_win"] for row in hour_rows)
    location_mae_wins = sum(row["xgb_mae_win"] for row in location_rows)
    exact_location_ties = sum(math.isclose(row["xgb_mae_c"], row["persistence_mae_c"], abs_tol=1e-12) for row in location_rows)
    aggregate["location_comparison"] = {"xgb_mae_wins": location_mae_wins, "exact_ties": exact_location_ties, "equal_or_trails": len(location_rows) - location_mae_wins}
    aggregate["hour_comparison"] = {"mae_wins": int(hour_mae_wins), "rmse_wins": int(hour_rmse_wins), "both_metrics_won": int(both_won), "both_metrics_lost": int(both_lost)}

    # Join preserved state evaluations to recover target-time context omitted from the compact Parquet.
    eval_by_id = {
        str(slot["evaluation"]["evaluation_id"]): slot["evaluation"]
        for slot in state.get("slots", {}).values()
        if slot.get("status") == "EVALUATED" and (slot.get("evaluation") or {}).get("evaluation_id")
    }
    joined = []
    for row in evaluated:
        context = eval_by_id.get(str(row.get("evaluation_id")), {})
        joined.append({**row, **{key: context.get(key) for key in ("humidity_pct", "precipitation_mm", "weather_code", "pressure_hpa", "wind_speed_kmh", "wind_gust_kmh")}})
    error_report = _distribution_report(evaluated, joined)

    bootstrap = target_hour_block_bootstrap(evaluated, resamples=bootstrap_resamples, seed=BOOTSTRAP_SEED)
    loo = []
    for excluded_hour in sorted(hour_groups):
        subset = [row for hour, values in hour_groups.items() if hour != excluded_hour for row in values]
        metrics = recompute_metrics(subset)
        loo.append({
            "excluded_target_time": excluded_hour,
            "sample_count": len(subset),
            "delta_mae_c": metrics["delta_xgb_minus_persistence"]["mae_c"],
            "delta_rmse_c": metrics["delta_xgb_minus_persistence"]["rmse_c"],
            "xgb_bias_c": metrics["xgboost"]["bias_c"],
            "classification": classify_skill(metrics),
        })
    sensitivity = {
        "method": "leave_one_target_hour_out",
        "valid_target_hour_blocks": len(hour_groups),
        "leave_one_hour_out": {
            "classification_counts": dict(Counter(row["classification"] for row in loo)),
            "delta_mae_min_c": min(row["delta_mae_c"] for row in loo),
            "delta_mae_max_c": max(row["delta_mae_c"] for row in loo),
            "delta_rmse_min_c": min(row["delta_rmse_c"] for row in loo),
            "delta_rmse_max_c": max(row["delta_rmse_c"] for row in loo),
            "xgb_bias_min_c": min(row["xgb_bias_c"] for row in loo),
            "xgb_bias_max_c": max(row["xgb_bias_c"] for row in loo),
            "all_exclusions_preserve_primary_classification": all(row["classification"] == aggregate["prospective_skill_classification"] for row in loo),
            "details": loo,
        },
    }

    status_counts = dict(Counter(str(row["status"]) for row in rows))
    total_slots = len(rows)
    completeness = {
        "cohort_id": COHORT_ID,
        "frozen_classification": status.get("cohort_completeness_classification"),
        "status_counts": status_counts,
        "expected_slots": total_slots,
        "expected_target_hours": len({timestamp_text(row["target_time"]) for row in rows}),
        "expected_locations": len({str(row["location_id"]) for row in rows}),
        "rates_pct": {
            "usable_primary_metric": _percent(status_counts.get("EVALUATED", 0), total_slots),
            "forecast_missing": _percent(status_counts.get("FORECAST_MISSING", 0), total_slots),
            "reference_conflict": _percent(status_counts.get("REFERENCE_CONFLICT", 0), total_slots),
        },
        "hours_by_status": {
            state_name: sorted({timestamp_text(row["target_time"]) for row in rows if row["status"] == state_name})
            for state_name in sorted(status_counts)
        },
        "no_imputation": True,
        "forecast_grace_period_seconds": int(manifest["forecast_grace_period_seconds"]),
        "late_forecast_receipts_not_admitted": integrity["unmatched_late_receipts"],
        "reference_conflicts": {},
    }
    conflict_rows = [row for row in rows if row["status"] == "REFERENCE_CONFLICT"]
    conflicts = {
        "reference_conflict_count": len(conflict_rows),
        "revision_record_count": len(revision_rows),
        "affected_feature_hours": sorted({timestamp_text(row["feature_time"]) for row in conflict_rows}),
        "affected_target_hours": sorted({timestamp_text(row["target_time"]) for row in conflict_rows}),
        "affected_location_count": len({str(row["location_id"]) for row in conflict_rows}),
        "policy": revisions.get("policy"),
        "revision_status_counts": dict(Counter(str(row.get("status")) for row in revision_rows)),
        "unique_old_new_payload_hash_pairs": len({(row.get("old_payload_sha256"), row.get("new_payload_sha256")) for row in revision_rows}),
        "distinct_old_new_payload_hash_count": sum(row.get("old_payload_sha256") != row.get("new_payload_sha256") for row in revision_rows),
        "revision_ingestion_lag_seconds": _distribution([
            (parse_timestamp(row["later_ingestion_time"]) - parse_timestamp(row["first_ingestion_time"])).total_seconds()
            for row in revision_rows
        ]),
        "numeric_temperature_delta": {
            "status": "NOT_AVAILABLE_FROM_PRESERVED_EVIDENCE",
            "reason": "The immutable revision and conflict evaluation records retain old/new payload hashes and timestamps, but not old/new temperature values; conflict evaluation temperature is null.",
            "temperature_revision_distribution": None,
        },
        "revisions": revision_rows,
        "reclassification_performed": False,
    }
    completeness["reference_conflicts"] = conflicts

    receipt_rows = []
    for row in receipts:
        boundary = parse_timestamp(row["feature_time"]) + timedelta(hours=1)
        persisted = parse_timestamp(row["forecast_persisted_at"])
        receipt_rows.append({
            **row,
            "issuance_boundary": boundary,
            "issuance_latency_seconds": (persisted - boundary).total_seconds(),
            "lead_seconds_recomputed": (parse_timestamp(row["target_time"]) - parse_timestamp(row["inference_time"])).total_seconds(),
            "in_cohort_forecast_membership": str(row.get("forecast_id")) in {str(r.get("forecast_id")) for r in rows if r.get("forecast_id")},
        })
    latencies = [float(row["issuance_latency_seconds"]) for row in receipt_rows]
    lead_seconds = [float(row["lead_seconds_recomputed"]) for row in receipt_rows]
    accepted_receipts = [row for row in receipt_rows if row["in_cohort_forecast_membership"]]
    ref_lags = [float(row["reference_arrival_lag_seconds"]) for row in evaluated if row.get("reference_arrival_lag_seconds") is not None]
    eval_lags = [float(row["evaluation_lag_seconds"]) for row in evaluated if row.get("evaluation_lag_seconds") is not None]
    outages = list(runtime.get("outages", []))
    cadence_like = [row for row in outages if 3000 <= float(row.get("duration_seconds_estimate", 0)) <= 3900]
    long_gaps = [row for row in outages if float(row.get("duration_seconds_estimate", 0)) > 3900]
    operational = {
        "issuance_definition": "forecast_persisted_at - (feature_time + 1 hour)",
        "target_reference_arrival_lag_and_evaluation_lag_are_separate": True,
        "receipt_counts": {"all_receipts": len(receipt_rows), "cohort_forecast_membership": len(accepted_receipts), "late_unadmitted_receipts": len(receipt_rows) - len(accepted_receipts)},
        "forecast_lead_seconds": {
            **_distribution(lead_seconds),
            "positive_count": sum(value > 0 for value in lead_seconds),
            "nonpositive_count": sum(value <= 0 for value in lead_seconds),
            "positive_pct": _percent(sum(value > 0 for value in lead_seconds), len(lead_seconds)),
        },
        "issuance_latency_seconds": {
            **_distribution(latencies),
            "positive_count": sum(value > 0 for value in latencies),
            "nonpositive_count": sum(value <= 0 for value in latencies),
            "within_60_seconds_count": sum(value <= 60 for value in latencies),
            "within_900_second_grace_count": sum(value <= int(manifest["forecast_grace_period_seconds"]) for value in latencies),
            "late_after_grace_count": sum(value > int(manifest["forecast_grace_period_seconds"]) for value in latencies),
            "cohort_membership_late_after_grace_count": sum(float(row["issuance_latency_seconds"]) > int(manifest["forecast_grace_period_seconds"]) for row in accepted_receipts),
        },
        "reference_arrival_lag_seconds": _distribution(ref_lags),
        "evaluation_lag_seconds": _distribution(eval_lags),
        "issuance_slo": {"definition": slo.get("definition"), "target_seconds": slo.get("target_seconds"), "hours": len(slo.get("hours", [])), "cycles_passed": slo.get("cycles_passed"), "cycles_failed": slo.get("cycles_failed")},
        "phase14_runtime": {
            "restart_count": runtime.get("restart_count"),
            "microbatch_update_count": runtime.get("microbatch_update_count"),
            "last_update_at": runtime.get("last_update_at"),
            "inferred_outage_count": len(outages),
            "outage_detector_threshold_seconds": 900,
            "inference_cadence_approximately_seconds": 3600,
            "cadence_like_gap_count": len(cadence_like),
            "cadence_like_duration_range_seconds": [min((float(row["duration_seconds_estimate"]) for row in cadence_like), default=None), max((float(row["duration_seconds_estimate"]) for row in cadence_like), default=None)],
            "long_gap_count": len(long_gaps),
            "long_gap_durations_seconds": [float(row["duration_seconds_estimate"]) for row in long_gaps],
            "all_outage_records_preserved": True,
            "qualification": "The 15-minute threshold labels normal hourly intervals as outages; inferred durations are not measured uptime. Longer intervals are possible real disruptions, but the detector alone does not prove cause.",
        },
        "prior_incidents": {
            "spark_self_join_analysise_exception": {"reported_in_prior_phase_context": True, "present_in_frozen_local_log_files": False, "effect": "contributed to a streaming crash/restart incident in the collection window"},
            "live_producer_http_429": {"reported_in_prior_phase_context": True, "present_in_frozen_local_log_files": False, "affected_slots_quantified": False},
        },
        "post_cohort_runtime_snapshot": dict(post_cohort_runtime_snapshot or {}),
    }

    offline_metrics = _read_json(offline_dir / "test_metrics.json")
    offline_locations = _read_json(offline_dir / "per_location_metrics.json")
    def _offline_metric(model_key: str) -> dict[str, Any]:
        raw = offline_metrics[model_key]
        return {
            "sample_count": raw.get("n"),
            "mae_c": raw.get("mae"),
            "rmse_c": raw.get("rmse"),
            "r2": raw.get("r2"),
            "bias_c": raw.get("bias"),
        }

    offline_location_rows = offline_locations.get("locations", offline_locations.get("per_location", offline_locations))
    offline_wins = None
    if isinstance(offline_location_rows, dict):
        values = list(offline_location_rows.values())
        offline_wins = sum(bool(item.get("beats_persistence_mae", item.get("mae_skill", 0) > 0)) for item in values if isinstance(item, dict))
    elif isinstance(offline_location_rows, list):
        offline_wins = sum(bool(item.get("beats_persistence_mae", item.get("mae_skill", 0) > 0)) for item in offline_location_rows)
    offline = {
        "comparison_type": "descriptive_only; targets are not interchangeable",
        "offline_test_target": "ERA5 reanalysis at feature_time + 2h; predictors from Open-Meteo Historical Forecast ECMWF IFS",
        "prospective_reference": "later canonical live Open-Meteo Forecast ECMWF IFS operational model-product reference; not station truth and not ERA5",
        "offline_test": {
            "target": "ERA5",
            "xgboost": _offline_metric("xgboost"),
            "persistence": _offline_metric("persistence"),
            "location_win_count": offline_wins,
            "rows": offline_metrics.get("rows"),
        },
        "prospective": {"target": "later operational Open-Meteo ECMWF IFS reference", "xgboost": aggregate["xgboost"], "persistence": aggregate["persistence"], "location_win_count": location_mae_wins, "valid_rows": len(evaluated), "valid_target_hours": len(hour_groups)},
        "hypotheses_not_causal_findings": [
            "ERA5 versus operational IFS target/reference semantic mismatch",
            "historical IFS issuance-vintage mismatch or version evolution",
            "short one-day seasonal/day-regime sample",
            "live versus historical feature/source semantic mismatch",
            "systematic positive calibration bias",
            "only 20 valid target-hour blocks",
        ],
        "delayed_era5_status": delay_era5.get("status"),
        "delayed_era5_manifest": delay_era5,
        "era5_fetch_performed": False,
    }

    decision_name = "RETAIN_FOR_MORE_VALIDATION"
    decision = {
        "prospective_skill_classification": aggregate["prospective_skill_classification"],
        "model_decision": decision_name,
        "retrain_status": "NOT_RECOMMENDED_FROM_THIS_COHORT_ALONE",
        "retraining_performed": False,
        "production_hardening_allowed": False,
        "human_review_required": True,
        "rationale": [
            "Primary point estimates trail persistence on both MAE and RMSE, so the prospective skill label is negative under the frozen rules.",
            "Target-hour block bootstrap intervals include zero and only 20 temporal blocks exist.",
            "All 20 hourly XGBoost biases are positive, with approximately +1.25 C aggregate bias; this is a persistent signal requiring follow-up, but the live target is an operational IFS product rather than station truth or the offline ERA5 target.",
            "Usable primary coverage is 83.33%; 189 slots are missing by the frozen grace/deadline rules and all 63 reference conflicts remain excluded.",
            "The evidence is insufficient to attribute the discrepancy to training versus live/offline source semantics; immediate retraining would overstate what one short cohort establishes.",
        ],
    }

    # Fail before creating a new evidence directory if a frozen-cohort assertion fails.
    assert integrity["classification"] == "PASS", "Phase 14 cohort integrity did not pass"
    assert aggregate["phase14_reproduction"]["passed"], "Recomputed primary metrics differ from frozen Phase 14 metrics"
    assert aggregate["sample_count"] == 1260 and len(hour_rows) == 20 and len(location_rows) == 63
    assert status_counts == {"EVALUATED": 1260, "FORECAST_MISSING": 189, "REFERENCE_CONFLICT": 63}
    assert len(integrity["unmatched_late_receipts"]) == 63
    assert decision["production_hardening_allowed"] is False

    output_dir.mkdir(parents=True, exist_ok=False)
    output_files = [
        "review_manifest.json", "cohort_integrity.json", "aggregate_metrics.json", "per_location_metrics.csv", "per_target_hour_metrics.csv",
        "error_distribution.json", "completeness_analysis.json", "operational_analysis.json", "bootstrap_analysis.json",
        "offline_vs_prospective.json", "model_decision.json", "PHASE15_REVIEW.md",
    ]
    if post_cohort_runtime_snapshot:
        output_files.append("post_cohort_runtime_snapshot.json")
    if verification_summary:
        output_files.append("verification_summary.json")
    report_markdown = _markdown_report(
        integrity, aggregate, completeness, conflicts, location_rows, hour_rows, error_report,
        operational, bootstrap, sensitivity, offline, decision, output_files, cohort_integrity_summary, verification_summary,
    )
    _write_json(output_dir / "cohort_integrity.json", cohort_integrity_summary)
    _write_json(output_dir / "aggregate_metrics.json", aggregate)
    _write_csv(output_dir / "per_location_metrics.csv", location_rows)
    _write_csv(output_dir / "per_target_hour_metrics.csv", hour_rows)
    _write_json(output_dir / "error_distribution.json", error_report)
    _write_json(output_dir / "completeness_analysis.json", completeness)
    _write_json(output_dir / "operational_analysis.json", operational)
    _write_json(output_dir / "bootstrap_analysis.json", bootstrap)
    _write_json(output_dir / "offline_vs_prospective.json", offline)
    _write_json(output_dir / "model_decision.json", decision)
    (output_dir / "PHASE15_REVIEW.md").write_text(report_markdown, encoding="utf-8")
    if post_cohort_runtime_snapshot:
        _write_json(output_dir / "post_cohort_runtime_snapshot.json", post_cohort_runtime_snapshot)
    if verification_summary:
        _write_json(output_dir / "verification_summary.json", verification_summary)

    input_paths = [
        run_results / "cohort_manifest.json", run_results / "cohort_status.json", run_results / "checksums.json",
        run_results / "model_contract_validation.json", run_results / "provider_contract_validation.json",
        run_results / "provenance_validation.json", run_results / "reference_revision_audit.json",
        spark_results / "forecast_persistence_receipts.jsonl", spark_results / "reference_revisions.jsonl",
        runtime_dir / "cohort_state.json", offline_dir / "test_metrics.json", offline_dir / "feature_list.json", model_path,
    ]
    input_paths.extend(sorted((run_results / "prospective_evaluations.parquet").glob("part-*.parquet")))
    source_hashes = {str(path.relative_to(repo_root)): {"sha256": sha256_file(path), "bytes": path.stat().st_size} for path in input_paths}
    manifest_out = {
        "phase": "PHASE_15_PROSPECTIVE_VALIDATION_REVIEW_T2H_V1",
        "run_id": RUN_ID,
        "cohort_id": COHORT_ID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "cohort_integrity": integrity["classification"],
        "integrity_checks_passed": integrity["checks_passed"],
        "integrity_checks_total": integrity["checks_total"],
        "model_contract": cohort_integrity_summary["model_contract"],
        "cohort_integrity_summary": cohort_integrity_summary,
        "source_inputs": source_hashes,
        "source_checksum_manifest_verified_files": len(checksums.get("files", [])),
        "analysis_environment": {"python": platform.python_version(), "numpy": np.__version__, "pyarrow": pyarrow.__version__},
        "primary_population": "rows with status == EVALUATED only; excludes FORECAST_MISSING, REFERENCE_CONFLICT, and any invalid provenance",
        "bootstrap": {"unit": "target hour", "resamples": bootstrap_resamples, "seed": BOOTSTRAP_SEED, "rng": "NumPy default_rng / PCG64"},
        "feature_context_note": "Evaluation weather covariates are target-reference context; the original 73 model inputs were not preserved row-by-row in the compact evaluation Parquet.",
        "delayed_era5": {"status": delay_era5.get("status"), "fetch_performed": False},
        "created_artifacts": [name for name in output_files if name != "review_manifest.json"],
        "verification_summary": verification_summary,
        "post_cohort_runtime_snapshot": operational.get("post_cohort_runtime_snapshot"),
        "not_performed": ["model training", "model tuning", "feature/provider/horizon changes", "cohort reset", "Phase 14 evidence modification"],
    }
    _write_json(output_dir / "review_manifest.json", manifest_out)
    # Deterministic assertions are part of the review run and fail closed before delivery.
    assert integrity["classification"] == "PASS", "Phase 14 cohort integrity did not pass"
    assert aggregate["phase14_reproduction"]["passed"], "Recomputed primary metrics differ from frozen Phase 14 metrics"
    assert aggregate["sample_count"] == 1260 and len(hour_rows) == 20 and len(location_rows) == 63
    assert status_counts == {"EVALUATED": 1260, "FORECAST_MISSING": 189, "REFERENCE_CONFLICT": 63}
    assert len(integrity["unmatched_late_receipts"]) == 63
    assert decision["production_hardening_allowed"] is False
    return {"output_dir": str(output_dir), "integrity": integrity, "metrics": aggregate, "decision": decision}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--bootstrap-resamples", type=int, default=BOOTSTRAP_RESAMPLES)
    parser.add_argument("--verification-summary", type=Path, default=None)
    parser.add_argument("--post-cohort-runtime-snapshot", type=Path, default=None)
    args = parser.parse_args()
    root = args.repo_root.resolve()
    out = args.output_dir or root / "results" / "prospective-review-t2h" / RUN_ID
    verification_summary = _read_json(args.verification_summary) if args.verification_summary else None
    post_cohort_runtime_snapshot = _read_json(args.post_cohort_runtime_snapshot) if args.post_cohort_runtime_snapshot else None
    result = generate_review(
        root,
        out.resolve(),
        bootstrap_resamples=args.bootstrap_resamples,
        verification_summary=verification_summary,
        post_cohort_runtime_snapshot=post_cohort_runtime_snapshot,
    )
    print(json.dumps({"output_dir": result["output_dir"], "integrity": result["integrity"]["classification"], "classification": result["decision"]["prospective_skill_classification"], "decision": result["decision"]["model_decision"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
