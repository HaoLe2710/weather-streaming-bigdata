from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone

from spark.jobs import verify_t2h_forecast_delta as audit


RUN_ID = "20261010T111433Z-prospective-live-t2h-v1"
COHORT_ID = "prospective-t2h-20261010T120000Z"
NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


def _status(count: int = 126, *, updated_at: datetime = NOW, run_id: str = RUN_ID):
    return {
        "status": "COLLECTING",
        "run_id": run_id,
        "cohort_id": COHORT_ID,
        "last_update_at": updated_at.isoformat(),
        "prospective_forecast_count": count,
    }


def _snapshot(version: int = 8, count: int = 126, **extra):
    return {
        "version": version,
        "live_forecast_count": count,
        "hard_failures": [],
        "receipt_integrity": {"status": "PASS", "missing": 0, "extra": 0, "duplicates": 0, "mismatched": 0, "invalid": 0},
        **extra,
    }


def _checkpoint(status: str, *, offset_batch_id: int | None = None, committed_batch_id: int | None = None):
    if offset_batch_id is None:
        offset_batch_id = 16 if status == "PENDING" else 15
    if committed_batch_id is None:
        committed_batch_id = 15 if status == "PENDING" else offset_batch_id
    return {
        "status": status,
        "latest_offset_batch_id": offset_batch_id,
        "latest_committed_batch_id": committed_batch_id,
        "pending_batch_id": offset_batch_id if status == "PENDING" else None,
        "in_flight": status == "PENDING",
    }


def _verify(statuses, snapshots, checkpoints, **overrides):
    status_values = deque(statuses)
    snapshot_values = deque(snapshots)
    checkpoint_values = deque(checkpoints)
    last_status = statuses[-1]
    last_snapshot = snapshots[-1]
    last_checkpoint = checkpoints[-1]
    clock = [0.0]

    def pop_or_last(values, last):
        return values.popleft() if len(values) > 1 else last

    return audit._verify_live_forecast_count_snapshot(
        read_status=lambda: pop_or_last(status_values, last_status),
        read_snapshot=lambda: pop_or_last(snapshot_values, last_snapshot),
        read_checkpoint=lambda: pop_or_last(checkpoint_values, last_checkpoint),
        expected_run_id=RUN_ID,
        expected_cohort_id=COHORT_ID,
        now=NOW,
        max_attempts=overrides.pop("max_attempts", 3),
        deadline_seconds=overrides.pop("deadline_seconds", 100),
        poll_interval_seconds=overrides.pop("poll_interval_seconds", 0.01),
        max_poll_interval_seconds=overrides.pop("max_poll_interval_seconds", 0.05),
        sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        monotonic=overrides.pop("monotonic", lambda: clock[0]),
        **overrides,
    )


def test_snapshot_consistent_expected_count_passes():
    result = _verify([_status(126)], [_snapshot(version=8, count=126)], [_checkpoint("SETTLED")])

    assert result["status"] == "PASS"
    assert result["classification"] == "FORECAST_COUNT_SNAPSHOT_CONSISTENT"
    assert result["expected_live_forecasts"] == result["observed_live_forecasts"] == 126
    assert result["delta_version"] == 8


def test_status_counter_behind_one_pending_63_row_batch_retries_fresh_snapshot_then_passes():
    result = _verify(
        [_status(126), _status(189)],
        [_snapshot(version=8, count=189), _snapshot(version=9, count=189)],
        [_checkpoint("PENDING"), _checkpoint("SETTLED"), _checkpoint("SETTLED"), _checkpoint("SETTLED")],
    )

    assert result["status"] == "PASS"
    assert result["expected_live_forecasts"] == result["observed_live_forecasts"] == 189
    assert result["delta_version"] == 9
    assert [attempt["status"] for attempt in result["attempts"]] == ["TRANSIENT_CANDIDATE", "PASS"]
    assert result["attempts"][0]["delta_version"] != result["attempts"][1]["delta_version"]


def test_status_counter_that_stays_behind_fails_after_bounded_attempts():
    result = _verify(
        [_status(126), _status(126)],
        [_snapshot(version=8, count=189), _snapshot(version=9, count=189)],
        [_checkpoint("PENDING"), _checkpoint("SETTLED"), _checkpoint("SETTLED"), _checkpoint("SETTLED")],
        max_attempts=2,
    )

    assert result["status"] == "FAIL"
    assert result["classification"] == "FORECAST_COUNT_NOT_CONSISTENT_BEFORE_DEADLINE"
    assert result["observed_live_forecasts"] == 189
    assert result["expected_live_forecasts"] == 126


def test_delta_count_permanently_below_expected_fails_without_retry():
    result = _verify([_status(126)], [_snapshot(version=8, count=63)], [_checkpoint("SETTLED")])

    assert result["status"] == "FAIL"
    assert result["classification"] == "FORECAST_COUNT_MISSING_ROWS"
    assert len(result["attempts"]) == 1


def test_unexplained_extra_forecasts_fail_without_weakening_exact_equality():
    result = _verify([_status(126)], [_snapshot(version=8, count=127)], [_checkpoint("SETTLED")])

    assert result["status"] == "FAIL"
    assert result["classification"] == "FORECAST_COUNT_UNEXPECTED_EXTRA_ROWS"
    assert result["expected_live_forecasts"] == 126
    assert result["observed_live_forecasts"] == 127


def test_duplicate_forecast_ids_or_logical_keys_are_not_retryable_count_mismatches():
    result = _verify(
        [_status(126)],
        [_snapshot(version=8, count=189, hard_failures=["forecast_id_duplicates_zero"])],
        [_checkpoint("PENDING")],
    )

    assert result["status"] == "FAIL"
    assert result["classification"] == "FORECAST_SNAPSHOT_HAS_HARD_FAILURES"
    assert result["attempts"][0]["hard_failures"] == ["forecast_id_duplicates_zero"]


def test_invalid_provider_contract_fails_even_when_count_has_inflight_mismatch():
    result = _verify(
        [_status(126)],
        [_snapshot(version=8, count=189, hard_failures=["provider_contract_violations_zero"])],
        [_checkpoint("PENDING")],
    )

    assert result["status"] == "FAIL"
    assert len(result["attempts"]) == 1
    assert "provider_contract_violations_zero" in result["hard_failures"]


def test_missing_stale_or_wrong_run_status_never_passes():
    missing = _verify([None], [_snapshot()], [_checkpoint("SETTLED")])
    stale = _verify(
        [_status(126, updated_at=NOW - timedelta(hours=2))],
        [_snapshot()],
        [_checkpoint("SETTLED")],
    )
    wrong_run = _verify([_status(126, run_id="other-run")], [_snapshot()], [_checkpoint("SETTLED")])

    assert missing["status"] in {"FAIL", "UNKNOWN"}
    assert missing["classification"] == "COHORT_STATUS_MISSING_OR_INVALID"
    assert stale["status"] == "FAIL"
    assert stale["classification"] == "COHORT_STATUS_STALE"
    assert wrong_run["status"] == "FAIL"
    assert wrong_run["classification"] == "COHORT_STATUS_RUN_ID_MISMATCH"


def test_receipt_hash_mismatch_fails_even_when_count_matches():
    result = _verify(
        [_status(126)],
        [_snapshot(version=8, count=126, receipt_integrity={"status": "FAIL", "missing": 0, "extra": 0, "duplicates": 0, "mismatched": 1, "invalid": 0})],
        [_checkpoint("SETTLED")],
    )

    assert result["status"] == "FAIL"
    assert result["classification"] == "FORECAST_PERSISTENCE_RECEIPTS_INVALID"
    assert result["receipt_integrity"]["mismatched"] == 1


def test_changed_delta_versions_are_retained_per_attempt_for_deterministic_evidence():
    result = _verify(
        [_status(126), _status(189)],
        [_snapshot(version=12, count=189), _snapshot(version=14, count=189)],
        [_checkpoint("PENDING"), _checkpoint("SETTLED"), _checkpoint("SETTLED"), _checkpoint("SETTLED")],
    )

    assert result["status"] == "PASS"
    assert [attempt["delta_version"] for attempt in result["attempts"]] == [12, 14]


def test_matching_counts_do_not_pass_while_checkpoint_batch_is_still_pending():
    result = _verify(
        [_status(126), _status(189)],
        [_snapshot(version=8, count=126), _snapshot(version=9, count=189)],
        [_checkpoint("PENDING"), _checkpoint("PENDING"), _checkpoint("SETTLED", offset_batch_id=16), _checkpoint("SETTLED", offset_batch_id=16)],
    )

    assert result["status"] == "PASS"
    assert result["expected_live_forecasts"] == result["observed_live_forecasts"] == 189
    assert [attempt["classification"] for attempt in result["attempts"]] == [
        "CHECKPOINT_CHANGED_DURING_DELTA_SNAPSHOT",
        "FORECAST_COUNT_SNAPSHOT_CONSISTENT",
    ]


def test_checkpoint_advancing_during_an_equal_snapshot_forces_fresh_status_and_delta_read():
    result = _verify(
        [_status(126), _status(189)],
        [_snapshot(version=20, count=126), _snapshot(version=21, count=189)],
        [
            _checkpoint("SETTLED", offset_batch_id=15),
            _checkpoint("SETTLED", offset_batch_id=16),
            _checkpoint("SETTLED", offset_batch_id=16),
            _checkpoint("SETTLED", offset_batch_id=16),
        ],
    )

    assert result["status"] == "PASS"
    assert [attempt["delta_version"] for attempt in result["attempts"]] == [20, 21]
    assert result["attempts"][0]["classification"] == "CHECKPOINT_CHANGED_DURING_DELTA_SNAPSHOT"


def test_equal_counts_with_unknown_checkpoint_fail_closed():
    result = _verify([_status(126)], [_snapshot(version=8, count=126)], [{"status": "UNKNOWN"}])

    assert result["status"] == "FAIL"
    assert result["classification"] == "FORECAST_COUNT_CHECKPOINT_STATE_UNKNOWN"


def test_equal_counts_with_checkpoint_never_settling_fail_by_attempt_bound():
    result = _verify(
        [_status(126)],
        [_snapshot(version=8, count=126)],
        [_checkpoint("PENDING")],
        max_attempts=2,
        deadline_seconds=0.02,
    )

    assert result["status"] == "FAIL"
    assert result["classification"] == "FORECAST_COUNT_NOT_CONSISTENT_BEFORE_DEADLINE"
    assert len(result["attempts"]) == 1
