from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Callable, Mapping


EXPECTED_STATE_LOCATIONS = 63
EXPECTED_STATE_HISTORY_HOURS = 49
TRANSIENT_STATE_ROWS_PER_LOCATION = EXPECTED_STATE_HISTORY_HOURS + 1
MAX_STATE_AUDIT_ATTEMPTS = 2
STATE_AUDIT_PENDING_DEADLINE_SECONDS = 300.0
STATE_AUDIT_CHECKPOINT_POLL_SECONDS = 5.0
STATE_AUDIT_MAX_CHECKPOINT_POLL_SECONDS = 30.0
LIVE_COUNT_EXPECTED_BATCH_ROWS = 63
LIVE_COUNT_MAX_ATTEMPTS = 3
LIVE_COUNT_RETRY_DEADLINE_SECONDS = 300.0
LIVE_COUNT_CHECKPOINT_POLL_SECONDS = 5.0
LIVE_COUNT_MAX_CHECKPOINT_POLL_SECONDS = 30.0
LIVE_COHORT_STATUS_MAX_AGE_SECONDS = 90 * 60


def _parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _verify_live_forecast_count_snapshot(
    *,
    read_status: Callable[[], Mapping[str, Any] | None],
    read_snapshot: Callable[[], Mapping[str, Any]],
    read_checkpoint: Callable[[], Mapping[str, Any]],
    expected_run_id: str,
    expected_cohort_id: str | None,
    now: datetime,
    max_attempts: int = LIVE_COUNT_MAX_ATTEMPTS,
    deadline_seconds: float = LIVE_COUNT_RETRY_DEADLINE_SECONDS,
    poll_interval_seconds: float = LIVE_COUNT_CHECKPOINT_POLL_SECONDS,
    max_poll_interval_seconds: float = LIVE_COUNT_MAX_CHECKPOINT_POLL_SECONDS,
    max_status_age_seconds: int = LIVE_COHORT_STATUS_MAX_AGE_SECONDS,
    attempt_log_path: str | Path | None = None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    timestamp: Callable[[], str] = lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
) -> dict[str, Any]:
    """Compare a fresh status snapshot to a pinned Delta version.

    Only an excess of exactly one 63-location microbatch can be retried, and
    only while the checkpoint proves that batch is pending or just settled.
    Each retry calls ``read_snapshot`` again and therefore must pin a fresh
    Delta version. Expected counts are never changed to match observed rows.
    """
    if max_attempts < 1 or deadline_seconds < 0 or poll_interval_seconds <= 0 or max_poll_interval_seconds < poll_interval_seconds:
        raise ValueError("invalid bounded live forecast count verification configuration")
    now_utc = now.replace(tzinfo=timezone.utc) if now.tzinfo is None or now.utcoffset() is None else now.astimezone(timezone.utc)
    deadline = monotonic() + deadline_seconds
    attempts: list[dict[str, Any]] = []
    saw_transient_candidate = False
    settled_recheck_due = False
    last_status: Mapping[str, Any] | None = None
    last_snapshot: Mapping[str, Any] | None = None
    last_progress: Mapping[str, Any] | None = None

    def finish(status: str, classification: str, reason: str, *, expected: int | None, observed: int | None,
               snapshot: Mapping[str, Any] | None, progress: Mapping[str, Any] | None) -> dict[str, Any]:
        return {
            "status": status,
            "classification": classification,
            "reason": reason,
            "expected_live_forecasts": expected,
            "observed_live_forecasts": observed,
            "delta_version": None if not isinstance(snapshot, Mapping) else snapshot.get("version"),
            "cohort_status_snapshot": None if not isinstance(last_status, Mapping) else {
                "run_id": last_status.get("run_id"),
                "cohort_id": last_status.get("cohort_id"),
                "status": last_status.get("status"),
                "last_update_at": last_status.get("last_update_at"),
                "prospective_forecast_count": last_status.get("prospective_forecast_count"),
            },
            "checkpoint_progress": dict(progress) if isinstance(progress, Mapping) else None,
            "receipt_integrity": None if not isinstance(snapshot, Mapping) else snapshot.get("receipt_integrity"),
            "hard_failures": [] if not isinstance(snapshot, Mapping) else list(snapshot.get("hard_failures") or []),
            "attempts": attempts,
        }

    def log_attempt(row: dict[str, Any]) -> None:
        attempts.append(row)
        _append_jsonl(attempt_log_path, row)

    def checkpoint_snapshot_is_stable(before: Any, after: Any) -> bool:
        if not isinstance(before, Mapping) or not isinstance(after, Mapping):
            return False
        if before.get("status") != "SETTLED" or after.get("status") != "SETTLED":
            return False
        identity_keys = ("latest_offset_batch_id", "latest_committed_batch_id")
        return all(
            isinstance(before.get(key), int)
            and not isinstance(before.get(key), bool)
            and before.get(key) == after.get(key)
            for key in identity_keys
        )

    for attempt_number in range(1, max_attempts + 1):
        if attempt_number > 1 and monotonic() >= deadline:
            expected = last_status.get("prospective_forecast_count") if isinstance(last_status, Mapping) else None
            observed_value = last_snapshot.get("live_forecast_count") if isinstance(last_snapshot, Mapping) else None
            observed = observed_value if isinstance(observed_value, int) and not isinstance(observed_value, bool) else None
            return finish(
                "FAIL",
                "FORECAST_COUNT_NOT_CONSISTENT_BEFORE_DEADLINE",
                "cohort count and pinned Delta snapshot did not reach a stable committed checkpoint before the verification deadline",
                expected=expected if isinstance(expected, int) and not isinstance(expected, bool) else None,
                observed=observed,
                snapshot=last_snapshot,
                progress=last_progress,
            )
        started_at = timestamp()
        status_snapshot = read_status()
        last_status = status_snapshot if isinstance(status_snapshot, Mapping) else None
        expected: int | None = None
        status_error: tuple[str, str] | None = None
        if not isinstance(status_snapshot, Mapping):
            status_error = ("COHORT_STATUS_MISSING_OR_INVALID", "cohort status is missing or unreadable")
        elif status_snapshot.get("run_id") != expected_run_id:
            status_error = ("COHORT_STATUS_RUN_ID_MISMATCH", "cohort status run_id does not match the requested run")
        elif expected_cohort_id is not None and status_snapshot.get("cohort_id") != expected_cohort_id:
            status_error = ("COHORT_STATUS_COHORT_ID_MISMATCH", "cohort status cohort_id does not match the active manifest")
        elif status_snapshot.get("status") != "COLLECTING":
            status_error = ("COHORT_STATUS_NOT_COLLECTING", "cohort status is not COLLECTING")
        else:
            count_value = status_snapshot.get("prospective_forecast_count")
            if isinstance(count_value, bool) or not isinstance(count_value, int) or count_value < 0:
                status_error = ("COHORT_STATUS_FORECAST_COUNT_INVALID", "prospective_forecast_count must be a non-negative integer")
            else:
                expected = count_value
            updated_at = _parse_utc(status_snapshot.get("last_update_at"))
            if updated_at is None:
                status_error = ("COHORT_STATUS_UPDATE_TIMESTAMP_INVALID", "cohort status last_update_at is missing or invalid")
            elif (now_utc - updated_at).total_seconds() > max_status_age_seconds:
                status_error = ("COHORT_STATUS_STALE", "cohort status last_update_at exceeds the allowed freshness window")
            elif updated_at > now_utc + timedelta(minutes=5):
                status_error = ("COHORT_STATUS_TIMESTAMP_IN_FUTURE", "cohort status last_update_at is too far in the future")

        checkpoint_before = read_checkpoint()
        snapshot = read_snapshot()
        checkpoint_after = read_checkpoint()
        last_snapshot = snapshot if isinstance(snapshot, Mapping) else None
        last_progress = checkpoint_after if isinstance(checkpoint_after, Mapping) else None
        observed_value = snapshot.get("live_forecast_count") if isinstance(snapshot, Mapping) else None
        observed = observed_value if isinstance(observed_value, int) and not isinstance(observed_value, bool) and observed_value >= 0 else None
        hard_failures = list(snapshot.get("hard_failures") or []) if isinstance(snapshot, Mapping) else ["DELTA_SNAPSHOT_INVALID"]
        version_value = snapshot.get("version") if isinstance(snapshot, Mapping) else None
        if isinstance(version_value, bool) or not isinstance(version_value, int) or version_value < 0:
            hard_failures.append("DELTA_SNAPSHOT_VERSION_INVALID")
        receipt_integrity = snapshot.get("receipt_integrity") if isinstance(snapshot, Mapping) else None
        receipt_shape_valid = (
            isinstance(receipt_integrity, Mapping)
            and all(isinstance(receipt_integrity.get(name), int) and not isinstance(receipt_integrity.get(name), bool) and receipt_integrity.get(name) >= 0
                    for name in ("missing", "extra", "duplicates", "mismatched", "invalid"))
            and receipt_integrity.get("status") in {"PASS", "FAIL"}
        )
        if not receipt_shape_valid:
            hard_failures.append("PERSISTENCE_RECEIPT_AUDIT_INVALID_OR_MISSING")
        log_base = {
            "event": "FORECAST_COUNT_SNAPSHOT_ATTEMPT",
            "attempt_number": attempt_number,
            "started_at": started_at,
            "finished_at": timestamp(),
            "run_id": expected_run_id,
            "cohort_id": expected_cohort_id,
            "expected_live_forecasts": expected,
            "observed_live_forecasts": observed,
            "delta_version": snapshot.get("version") if isinstance(snapshot, Mapping) else None,
            "checkpoint_before": dict(checkpoint_before) if isinstance(checkpoint_before, Mapping) else None,
            "checkpoint_after": dict(checkpoint_after) if isinstance(checkpoint_after, Mapping) else None,
            "receipt_integrity": dict(receipt_integrity) if isinstance(receipt_integrity, Mapping) else None,
            "hard_failures": sorted(set(str(value) for value in hard_failures)),
        }
        if status_error:
            classification, reason = status_error
            log_attempt({**log_base, "status": "FAIL", "classification": classification, "reason": reason})
            return finish("UNKNOWN" if classification == "COHORT_STATUS_MISSING_OR_INVALID" else "FAIL", classification, reason,
                          expected=expected, observed=observed, snapshot=snapshot, progress=checkpoint_after)
        if observed is None:
            log_attempt({**log_base, "status": "FAIL", "classification": "DELTA_LIVE_FORECAST_COUNT_INVALID", "reason": "Delta snapshot live forecast count is missing or invalid"})
            return finish("FAIL", "DELTA_LIVE_FORECAST_COUNT_INVALID", "Delta snapshot live forecast count is missing or invalid",
                          expected=expected, observed=None, snapshot=snapshot, progress=checkpoint_after)
        if hard_failures:
            reason = "Delta snapshot contains forecast contract, duplicate, or receipt integrity failures"
            log_attempt({**log_base, "status": "FAIL", "classification": "FORECAST_SNAPSHOT_HAS_HARD_FAILURES", "reason": reason})
            return finish("FAIL", "FORECAST_SNAPSHOT_HAS_HARD_FAILURES", reason,
                          expected=expected, observed=observed, snapshot=snapshot, progress=checkpoint_after)

        receipt_is_clean = receipt_integrity.get("status") == "PASS" and all(
            receipt_integrity[name] == 0 for name in ("missing", "extra", "duplicates", "mismatched", "invalid")
        )
        if observed == expected and receipt_is_clean:
            if checkpoint_snapshot_is_stable(checkpoint_before, checkpoint_after):
                log_attempt({**log_base, "status": "PASS", "classification": "FORECAST_COUNT_SNAPSHOT_CONSISTENT", "reason": "status and pinned Delta snapshot counts and persistence receipts agree across a stable committed checkpoint"})
                return finish("PASS", "FORECAST_COUNT_SNAPSHOT_CONSISTENT", "status and pinned Delta snapshot counts and persistence receipts agree across a stable committed checkpoint",
                              expected=expected, observed=observed, snapshot=snapshot, progress=checkpoint_after)

            checkpoint_statuses = {
                progress.get("status")
                for progress in (checkpoint_before, checkpoint_after)
                if isinstance(progress, Mapping)
            }
            if not checkpoint_statuses or not checkpoint_statuses.issubset({"PENDING", "SETTLED"}):
                reason = "checkpoint state was unknown or inconsistent while pinning an otherwise count-matching Delta snapshot"
                log_attempt({**log_base, "status": "FAIL", "classification": "FORECAST_COUNT_CHECKPOINT_STATE_UNKNOWN", "reason": reason})
                return finish("FAIL", "FORECAST_COUNT_CHECKPOINT_STATE_UNKNOWN", reason,
                              expected=expected, observed=observed, snapshot=snapshot, progress=checkpoint_after)

            reason = "counts matched while the Spark checkpoint was pending or advanced during the Delta snapshot; retrying with fresh status and Delta snapshots"
            log_attempt({**log_base, "status": "TRANSIENT_CANDIDATE", "classification": "CHECKPOINT_CHANGED_DURING_DELTA_SNAPSHOT", "reason": reason})
            if attempt_number >= max_attempts or monotonic() >= deadline:
                return finish("FAIL", "FORECAST_COUNT_CHECKPOINT_NOT_STABLE_BEFORE_DEADLINE", reason,
                              expected=expected, observed=observed, snapshot=snapshot, progress=checkpoint_after)
            saw_transient_candidate = True
            progress = checkpoint_after
            interval = poll_interval_seconds
            while isinstance(progress, Mapping) and progress.get("status") == "PENDING" and monotonic() < deadline:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                sleep(min(interval, remaining))
                progress = read_checkpoint()
                last_progress = progress
                interval = min(interval * 2, max_poll_interval_seconds)
            if not isinstance(progress, Mapping) or progress.get("status") not in {"SETTLED", "PENDING"}:
                return finish("FAIL", "FORECAST_COUNT_CHECKPOINT_STATE_UNKNOWN",
                              "checkpoint state became unknown during bounded live count verification",
                              expected=expected, observed=observed, snapshot=snapshot, progress=progress)
            if progress.get("status") == "SETTLED":
                settled_recheck_due = True
            continue
        if observed < expected:
            log_attempt({**log_base, "status": "FAIL", "classification": "FORECAST_COUNT_MISSING_ROWS", "reason": "pinned Delta snapshot has fewer live forecasts than the cohort status snapshot"})
            return finish("FAIL", "FORECAST_COUNT_MISSING_ROWS", "pinned Delta snapshot has fewer live forecasts than the cohort status snapshot",
                          expected=expected, observed=observed, snapshot=snapshot, progress=checkpoint_after)
        if observed == expected and not receipt_is_clean:
            log_attempt({**log_base, "status": "FAIL", "classification": "FORECAST_PERSISTENCE_RECEIPTS_INVALID", "reason": "persistence receipts do not exactly and validly cover the pinned live forecast snapshot"})
            return finish("FAIL", "FORECAST_PERSISTENCE_RECEIPTS_INVALID", "persistence receipts do not exactly and validly cover the pinned live forecast snapshot",
                          expected=expected, observed=observed, snapshot=snapshot, progress=checkpoint_after)
        difference = observed - expected
        checkpoints = [checkpoint_before, checkpoint_after]
        has_pending = any(progress.get("status") == "PENDING" for progress in checkpoints if isinstance(progress, Mapping))
        settled_during_attempt = (
            isinstance(checkpoint_before, Mapping)
            and checkpoint_before.get("status") == "PENDING"
            and isinstance(checkpoint_after, Mapping)
            and checkpoint_after.get("status") == "SETTLED"
        )
        missing_only_for_extra_batch = (
            receipt_integrity["missing"] in {0, difference}
            and receipt_integrity["extra"] == 0
            and receipt_integrity["duplicates"] == 0
            and receipt_integrity["mismatched"] == 0
            and receipt_integrity["invalid"] == 0
        )
        is_single_batch_candidate = (
            difference == LIVE_COUNT_EXPECTED_BATCH_ROWS
            and missing_only_for_extra_batch
            and (has_pending or settled_during_attempt or settled_recheck_due)
        )
        if not is_single_batch_candidate:
            classification = "FORECAST_COUNT_NOT_CONSISTENT_BEFORE_DEADLINE" if saw_transient_candidate else "FORECAST_COUNT_UNEXPECTED_EXTRA_ROWS"
            reason = "cohort status did not catch up to the one pending 63-location batch before the bounded verification ended" if saw_transient_candidate else "Delta snapshot has unexplained extra live forecasts; exact count equality is required"
            log_attempt({**log_base, "status": "FAIL", "classification": classification, "reason": reason})
            return finish("FAIL", classification, reason, expected=expected, observed=observed, snapshot=snapshot, progress=checkpoint_after)
        if settled_recheck_due and not has_pending and not settled_during_attempt:
            reason = "cohort status did not catch up after the pending 63-location batch settled"
            log_attempt({**log_base, "status": "FAIL", "classification": "FORECAST_COUNT_NOT_CONSISTENT_BEFORE_DEADLINE", "reason": reason})
            return finish("FAIL", "FORECAST_COUNT_NOT_CONSISTENT_BEFORE_DEADLINE", reason,
                          expected=expected, observed=observed, snapshot=snapshot, progress=checkpoint_after)

        saw_transient_candidate = True
        log_attempt({**log_base, "status": "TRANSIENT_CANDIDATE", "classification": "ONE_PENDING_63_FORECAST_BATCH", "reason": "exactly one 63-location batch is ahead of cohort status while checkpoint is pending or just settled"})
        if attempt_number >= max_attempts or monotonic() >= deadline:
            return finish("FAIL", "FORECAST_COUNT_NOT_CONSISTENT_BEFORE_DEADLINE",
                          "cohort status did not catch up to the one pending 63-location batch before the bounded verification ended",
                          expected=expected, observed=observed, snapshot=snapshot, progress=checkpoint_after)
        if settled_during_attempt:
            settled_recheck_due = True
        progress = checkpoint_after
        interval = poll_interval_seconds
        while isinstance(progress, Mapping) and progress.get("status") == "PENDING" and monotonic() < deadline:
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            sleep(min(interval, remaining))
            progress = read_checkpoint()
            last_progress = progress
            interval = min(interval * 2, max_poll_interval_seconds)
        if isinstance(progress, Mapping) and progress.get("status") == "SETTLED":
            settled_recheck_due = True
        if not isinstance(progress, Mapping) or progress.get("status") not in {"SETTLED", "PENDING"}:
            return finish("FAIL", "FORECAST_COUNT_CHECKPOINT_STATE_UNKNOWN",
                          "checkpoint state became unknown during bounded live count verification",
                          expected=expected, observed=observed, snapshot=snapshot, progress=progress)

    return finish("FAIL", "FORECAST_COUNT_NOT_CONSISTENT_BEFORE_DEADLINE",
                  "cohort status did not catch up to the one pending 63-location batch before the bounded verification ended",
                  expected=None, observed=None, snapshot=last_snapshot, progress=last_progress)


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    if hasattr(value, "item"):
        return value.item()
    return value


def _latest_checkpoint_batch(log_path: Path) -> int | None:
    """Return the highest Spark checkpoint batch id without changing the checkpoint."""
    batch_ids: list[int] = []
    try:
        for entry in log_path.iterdir():
            if not entry.is_file():
                continue
            match = re.fullmatch(r"(\d+)(?:\.compact)?", entry.name)
            if match:
                batch_ids.append(int(match.group(1)))
    except OSError:
        return None
    return max(batch_ids) if batch_ids else None


def _checkpoint_progress(checkpoint_path: str | Path | None) -> dict[str, Any]:
    """Read offsets/commits markers; an offset ahead of its commit is in flight."""
    if checkpoint_path is None:
        return {
            "status": "UNKNOWN",
            "reason": "checkpoint_path_not_configured",
            "latest_offset_batch_id": None,
            "latest_committed_batch_id": None,
            "pending_batch_id": None,
            "in_flight": None,
        }
    root = Path(checkpoint_path)
    offset_batch_id = _latest_checkpoint_batch(root / "offsets")
    committed_batch_id = _latest_checkpoint_batch(root / "commits")
    if offset_batch_id is None:
        return {
            "status": "UNKNOWN",
            "reason": "offset_log_missing_or_empty",
            "checkpoint_path": str(root),
            "latest_offset_batch_id": None,
            "latest_committed_batch_id": committed_batch_id,
            "pending_batch_id": None,
            "in_flight": None,
        }
    if committed_batch_id is not None and committed_batch_id > offset_batch_id:
        return {
            "status": "INCONSISTENT",
            "reason": "commit_log_ahead_of_offset_log",
            "checkpoint_path": str(root),
            "latest_offset_batch_id": offset_batch_id,
            "latest_committed_batch_id": committed_batch_id,
            "pending_batch_id": None,
            "in_flight": None,
        }
    pending_batch_id = offset_batch_id if committed_batch_id is None or offset_batch_id > committed_batch_id else None
    return {
        "status": "PENDING" if pending_batch_id is not None else "SETTLED",
        "reason": "offset_ahead_of_commit" if pending_batch_id is not None else "latest_offset_committed",
        "checkpoint_path": str(root),
        "latest_offset_batch_id": offset_batch_id,
        "latest_committed_batch_id": committed_batch_id,
        "pending_batch_id": pending_batch_id,
        "in_flight": pending_batch_id is not None,
    }


def _state_history_checks(state_report: dict[str, Any]) -> dict[str, bool]:
    return {
        "state_has_63_locations": state_report["locations"] == EXPECTED_STATE_LOCATIONS,
        "state_location_ids_non_null": state_report["null_location_rows"] == 0,
        "state_history_is_49_rows_per_location": state_report["rows_per_location_values"] == [EXPECTED_STATE_HISTORY_HOURS],
        "state_duplicate_location_hours_zero": state_report["duplicate_location_hours"] == 0,
        "state_history_is_hourly_contiguous": state_report["hourly_gap_rows"] == 0,
    }


def _audit_status(checks: dict[str, bool]) -> str:
    return "PASS" if all(checks.values()) else "FAIL"


def _retryable_retention_window(
    state_report: dict[str, Any],
    state_checks: dict[str, bool],
    checkpoint_samples: list[dict[str, Any]],
) -> bool:
    """Recognize only the exact MERGE-before-retention-DELETE intermediate shape."""
    failed_state_checks = {name for name, passed in state_checks.items() if not passed}
    has_pending_batch = any(sample.get("status") == "PENDING" for sample in checkpoint_samples)
    return bool(
        failed_state_checks == {"state_history_is_49_rows_per_location"}
        and state_report.get("locations") == EXPECTED_STATE_LOCATIONS
        and state_report.get("rows") == EXPECTED_STATE_LOCATIONS * TRANSIENT_STATE_ROWS_PER_LOCATION
        and state_report.get("rows_per_location_values") == [TRANSIENT_STATE_ROWS_PER_LOCATION]
        and state_report.get("duplicate_location_hours") == 0
        and state_report.get("hourly_gap_rows") == 0
        and has_pending_batch
    )


def _append_jsonl(path: str | Path | None, record: dict[str, Any]) -> None:
    if path is None:
        return
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n")


def _audit_state_history(
    read_state: Any,
    read_checkpoint: Any,
    *,
    audit_id: str,
    attempt_log_path: str | Path | None = None,
    max_attempts: int = MAX_STATE_AUDIT_ATTEMPTS,
    deadline_seconds: float = STATE_AUDIT_PENDING_DEADLINE_SECONDS,
    poll_interval_seconds: float = STATE_AUDIT_CHECKPOINT_POLL_SECONDS,
    max_poll_interval_seconds: float = STATE_AUDIT_MAX_CHECKPOINT_POLL_SECONDS,
    sleep: Any = time.sleep,
    monotonic: Any = time.monotonic,
    timestamp: Any = lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
) -> dict[str, Any]:
    """Audit state history; retry only after a proven in-flight batch settles."""
    if max_attempts < 1 or deadline_seconds < 0 or poll_interval_seconds <= 0 or max_poll_interval_seconds < poll_interval_seconds:
        raise ValueError("invalid bounded state audit retry configuration")
    attempts: list[dict[str, Any]] = []
    last_progress: dict[str, Any] = {}

    def append_attempt(record: dict[str, Any]) -> None:
        attempts.append(record)
        _append_jsonl(attempt_log_path, record)

    for attempt_number in range(1, max_attempts + 1):
        started_at = timestamp()
        progress_before = read_checkpoint()
        state_report = read_state()
        progress_after = read_checkpoint()
        last_progress = progress_after
        state_checks = _state_history_checks(state_report)
        passed = all(state_checks.values())
        transient = _retryable_retention_window(
            state_report,
            state_checks,
            [progress_before, progress_after],
        )
        record = {
            "event": "STATE_AUDIT_ATTEMPT",
            "audit_id": audit_id,
            "attempt_number": attempt_number,
            "started_at": started_at,
            "finished_at": timestamp(),
            "status": "PASS" if passed else "TRANSIENT_CANDIDATE" if transient else "FAIL",
            "classification": "STATE_HISTORY_VALID" if passed else "TRANSIENT_MERGE_BEFORE_RETENTION_DELETE" if transient else "STATE_HISTORY_CONTRACT_FAILED",
            "reason": "all_state_history_checks_passed" if passed else "checkpoint_has_uncommitted_batch_during_exact_50_row_retention_window" if transient else "one_or_more_state_history_invariants_failed",
            "state": state_report,
            "checks": state_checks,
            "checkpoint_before": progress_before,
            "checkpoint_after": progress_after,
        }
        append_attempt(record)
        if passed:
            return {
                "state": state_report,
                "checks": state_checks,
                "attempts": attempts,
                "classification": "STATE_HISTORY_VALID",
                "checkpoint_progress": progress_after,
                "status": "PASS",
            }
        if not transient:
            return {
                "state": state_report,
                "checks": state_checks,
                "attempts": attempts,
                "classification": "STATE_HISTORY_CONTRACT_FAILED",
                "checkpoint_progress": progress_after,
                "status": "FAIL",
            }
        if attempt_number >= max_attempts:
            return {
                "state": state_report,
                "checks": state_checks,
                "attempts": attempts,
                "classification": "TRANSIENT_RETRY_LIMIT_EXCEEDED",
                "checkpoint_progress": progress_after,
                "status": "FAIL",
            }

        settled = progress_after.get("status") == "SETTLED"
        deadline = monotonic() + deadline_seconds
        wait_interval = poll_interval_seconds
        while monotonic() < deadline:
            if settled:
                break
            progress = read_checkpoint()
            last_progress = progress
            if progress.get("status") == "SETTLED":
                settled = True
                break
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            sleep(min(wait_interval, remaining))
            wait_interval = min(wait_interval * 2, max_poll_interval_seconds)
        if not settled:
            timeout_record = {
                "event": "STATE_AUDIT_RETRY_WAIT",
                "audit_id": audit_id,
                "attempt_number": attempt_number,
                "started_at": timestamp(),
                "finished_at": timestamp(),
                "status": "FAIL",
                "classification": "TRANSIENT_STATE_WINDOW_TIMEOUT",
                "reason": "checkpoint_batch_did_not_commit_before_retry_deadline",
                "state": state_report,
                "checks": state_checks,
                "checkpoint_after": last_progress,
            }
            append_attempt(timeout_record)
            return {
                "state": state_report,
                "checks": state_checks,
                "attempts": attempts,
                "classification": "TRANSIENT_STATE_WINDOW_TIMEOUT",
                "checkpoint_progress": last_progress,
                "status": "FAIL",
            }

    raise AssertionError("bounded state audit loop exited unexpectedly")


def _state_history_report(states: Any) -> dict[str, Any]:
    from pyspark.sql import Window, functions as F

    state_rows = states.count()
    location_counts = states.groupBy("location_id").count().collect()
    null_location_rows = sum(int(row["count"]) for row in location_counts if row["location_id"] is None)
    state_locations = {
        str(row["location_id"]): int(row["count"])
        for row in location_counts
        if row["location_id"] is not None
    }
    state_duplicate_keys = state_rows - states.select("location_id", "event_time").distinct().count()
    window = Window.partitionBy("location_id").orderBy("event_time")
    ordered = states.select("location_id", "event_time").withColumn("_previous_event_time", F.lag("event_time").over(window))
    hourly_gap_rows = ordered.filter(
        F.col("event_time").isNull()
        | (
            F.col("_previous_event_time").isNotNull()
            & ((F.col("event_time").cast("long") - F.col("_previous_event_time").cast("long")) != 3600)
        )
    ).count()
    return {
        "rows": state_rows,
        "locations": len(state_locations),
        "null_location_rows": null_location_rows,
        "rows_per_location_values": sorted(set(state_locations.values())),
        "duplicate_location_hours": state_duplicate_keys,
        "hourly_gap_rows": hourly_gap_rows,
    }


def _load_persistence_receipts(path: str | Path | None) -> tuple[list[dict[str, Any]], int]:
    if path is None:
        return [], 0
    rows: list[dict[str, Any]] = []
    invalid_rows = 0
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    invalid_rows += 1
                    continue
                if not isinstance(row, dict):
                    invalid_rows += 1
                    continue
                rows.append(row)
    except FileNotFoundError:
        return [], 0
    return rows, invalid_rows


def _receipt_integrity_for_live_forecasts(
    live_forecasts: list[dict[str, Any]],
    receipt_rows: list[dict[str, Any]],
    invalid_receipt_rows: int,
) -> dict[str, Any]:
    from validation.prospective_t2h import _receipt_for_forecast, _receipt_map

    receipt_map = _receipt_map(receipt_rows)
    live_ids = {str(row.get("forecast_id") or "") for row in live_forecasts}
    receipt_ids = [str(row.get("forecast_id") or "") for row in receipt_rows if row.get("forecast_id")]
    duplicate_receipts = len(receipt_ids) - len(set(receipt_ids))
    missing = 0
    mismatched = 0
    for forecast in live_forecasts:
        _receipt, errors = _receipt_for_forecast(forecast, receipt_map)
        if "MISSING_PERSISTENCE_RECEIPT" in errors:
            missing += 1
        elif errors:
            mismatched += 1
    extra = len(set(receipt_ids) - live_ids)
    result = {
        "status": "PASS" if not (missing or extra or duplicate_receipts or mismatched or invalid_receipt_rows) else "FAIL",
        "missing": missing,
        "extra": extra,
        "duplicates": duplicate_receipts,
        "mismatched": mismatched,
        "invalid": invalid_receipt_rows,
        "live_forecast_ids": len(live_ids),
        "receipt_ids": len(set(receipt_ids)),
    }
    return result


def _audit_forecast_delta_snapshot(
    spark: Any,
    forecast_path: str,
    *,
    version: int,
    receipts: list[dict[str, Any]] | None,
    invalid_receipt_rows: int = 0,
) -> dict[str, Any]:
    """Audit one immutable Delta version; returned metrics all share that version."""
    from pyspark.sql import functions as F

    forecasts = (
        spark.read.format("delta")
        .option("versionAsOf", int(version))
        .load(forecast_path)
        .cache()
    )
    try:
        total_rows = forecasts.count()
        duplicate_ids = total_rows - forecasts.select("forecast_id").distinct().count()
        duplicate_logical_keys = (
            forecasts.groupBy("model_id", "location_id", "feature_time", "target_time")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        origin_counts = {row["execution_origin"]: int(row["count"]) for row in forecasts.groupBy("execution_origin").count().collect()}
        location_counts = {
            str(row["location_id"]): int(row["count"])
            for row in forecasts.filter(F.col("execution_origin") == "REPLAY_VALIDATION").groupBy("location_id").count().collect()
        }
        live_location_counts = {
            str(row["location_id"]): int(row["count"])
            for row in forecasts.filter(F.col("execution_origin") == "LIVE_PROSPECTIVE").groupBy("location_id").count().collect()
        }
        invalid_target_offsets = forecasts.filter(
            (F.col("target_time").cast("long") - F.col("feature_time").cast("long")) != 7200
        ).count()
        invalid_contract_rows = forecasts.filter(
            (F.col("model_id") != "WEATHER_XGBOOST_GLOBAL_T2H_V1_1")
            | (F.col("model_sha256") != "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a")
            | (F.col("feature_set_id") != "WEATHER_FORECAST_FE_T2H_V1_1")
            | (F.col("feature_list_sha256") != "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2")
            | (F.col("feature_count") != 73)
            | (F.col("forecast_horizon_hours") != 2)
            | (F.col("provider_model") != "ecmwf_ifs")
        ).count()
        invalid_provider_rows = forecasts.filter(
            (
                (F.col("execution_origin") == "LIVE_PROSPECTIVE")
                & (
                    (F.col("provider") != "Open-Meteo")
                    | (F.col("provider_endpoint") != "https://api.open-meteo.com/v1/forecast")
                    | (F.col("source") != "OPEN_METEO_LIVE_HOURLY")
                )
            )
            | (
                (F.col("execution_origin") == "REPLAY_VALIDATION")
                & (
                    (F.col("provider_endpoint") != "https://historical-forecast-api.open-meteo.com/v1/forecast")
                    | (F.col("source") != "OPEN_METEO_HISTORICAL_FORECAST")
                )
            )
        ).count()
        invalid_live_leads = forecasts.filter(
            (F.col("execution_origin") == "LIVE_PROSPECTIVE")
            & (
                F.col("forecast_lead_seconds").isNull()
                | (F.col("forecast_lead_seconds") <= 0)
                | (
                    F.abs(
                        F.col("forecast_lead_seconds")
                        - (F.col("target_time").cast("double") - F.col("inference_time").cast("double"))
                    )
                    > 0.001
                )
            )
        ).count()
        lead_stats_row = (
            forecasts.filter(F.col("execution_origin") == "LIVE_PROSPECTIVE")
            .agg(
                F.count(F.lit(1)).alias("count"),
                F.min("forecast_lead_seconds").alias("min"),
                F.avg("forecast_lead_seconds").alias("mean"),
                F.expr("percentile_approx(forecast_lead_seconds, 0.5, 10000)").alias("median"),
                F.expr("percentile_approx(forecast_lead_seconds, 0.95, 10000)").alias("p95"),
                F.max("forecast_lead_seconds").alias("max"),
            )
            .first()
        )
        lead_stats = {name: _json_value(lead_stats_row[name]) for name in ("count", "min", "mean", "median", "p95", "max")}
        live_forecasts = [
            row.asDict(recursive=True)
            for row in forecasts.filter(F.col("execution_origin") == "LIVE_PROSPECTIVE").collect()
        ]
        receipt_integrity = (
            _receipt_integrity_for_live_forecasts(live_forecasts, receipts, invalid_receipt_rows)
            if receipts is not None
            else None
        )
        sample = forecasts.orderBy("execution_origin", "location_id", "feature_time").limit(1).first()
        sample_record = None if sample is None else {name: _json_value(value) for name, value in sample.asDict().items()}
        checks = {
            "forecast_id_duplicates_zero": duplicate_ids == 0,
            "logical_forecast_duplicates_zero": duplicate_logical_keys == 0,
            "target_offset_violations_zero": invalid_target_offsets == 0,
            "contract_violations_zero": invalid_contract_rows == 0,
            "provider_contract_violations_zero": invalid_provider_rows == 0,
            "live_nonpositive_leads_zero": invalid_live_leads == 0,
            "replay_location_coverage_valid": origin_counts.get("REPLAY_VALIDATION", 0) == 0 or len(location_counts) == 63,
            "replay_rows_match_expected": True,
            "live_rows_match_expected": True,
        }
        state_hard_checks = {
            "forecast_id_duplicates_zero",
            "logical_forecast_duplicates_zero",
            "target_offset_violations_zero",
            "contract_violations_zero",
            "provider_contract_violations_zero",
            "live_nonpositive_leads_zero",
            "replay_location_coverage_valid",
        }
        hard_failures = sorted(name for name in state_hard_checks if checks[name] is not True)
        if isinstance(receipt_integrity, Mapping) and (
            receipt_integrity["extra"]
            or receipt_integrity["duplicates"]
            or receipt_integrity["mismatched"]
            or receipt_integrity["invalid"]
        ):
            hard_failures.append("persistence_receipts_integrity")
        return {
            "version": int(version),
            "total_forecast_rows": total_rows,
            "origin_counts": origin_counts,
            "replay_location_count": len(location_counts),
            "replay_rows_per_location_values": sorted(set(location_counts.values())),
            "live_location_count": len(live_location_counts),
            "live_rows_per_location_values": sorted(set(live_location_counts.values())),
            "live_forecast_count": int(origin_counts.get("LIVE_PROSPECTIVE", 0)),
            "forecast_id_duplicates": duplicate_ids,
            "logical_forecast_duplicates": duplicate_logical_keys,
            "target_offset_violations": invalid_target_offsets,
            "contract_violations": invalid_contract_rows,
            "provider_contract_violations": invalid_provider_rows,
            "nonpositive_live_leads": invalid_live_leads,
            "live_forecast_lead_seconds": lead_stats,
            "receipt_integrity": receipt_integrity,
            "hard_failures": hard_failures,
            "checks": checks,
            "sample_forecast": sample_record,
        }
    finally:
        forecasts.unpersist()


def _latest_forecast_delta_version(spark: Any, forecast_path: str) -> int:
    from delta.tables import DeltaTable

    row = DeltaTable.forPath(spark, forecast_path).history(1).select("version").first()
    if row is None or row["version"] is None:
        raise RuntimeError("forecast Delta table has no readable commit version")
    return int(row["version"])


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description="Read-only audit of T2H forecast and state Delta tables")
    parser.add_argument("--forecast-path", required=True)
    parser.add_argument("--state-path")
    parser.add_argument("--checkpoint-path")
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--attempt-log-jsonl")
    parser.add_argument("--expected-replay-forecasts", type=int, default=None)
    parser.add_argument("--expected-live-forecasts", type=int, default=None)
    parser.add_argument("--cohort-status-path")
    parser.add_argument("--expected-run-id")
    parser.add_argument("--expected-cohort-id")
    parser.add_argument("--receipts-path")
    args = parser.parse_args(argv)
    if args.cohort_status_path and (not args.expected_run_id or not args.receipts_path):
        parser.error("--cohort-status-path requires --expected-run-id and --receipts-path")

    spark = (
        SparkSession.builder.appName("verify-weather-forecast-t2h-v1-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        snapshots: dict[int, dict[str, Any]] = {}

        def read_cohort_status() -> Mapping[str, Any] | None:
            try:
                payload = json.loads(Path(args.cohort_status_path).read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError, TypeError):
                return None
            return payload if isinstance(payload, Mapping) else None

        def read_fresh_forecast_snapshot() -> dict[str, Any]:
            # Receipt JSONL is read before pinning Delta. Receipt append follows
            # the Delta MERGE, so this ordering cannot include receipts newer
            # than the version captured immediately afterward.
            if args.receipts_path:
                receipts, invalid_receipt_rows = _load_persistence_receipts(args.receipts_path)
            else:
                receipts, invalid_receipt_rows = None, 0
            version = _latest_forecast_delta_version(spark, args.forecast_path)
            snapshot = _audit_forecast_delta_snapshot(
                spark,
                args.forecast_path,
                version=version,
                receipts=receipts,
                invalid_receipt_rows=invalid_receipt_rows,
            )
            snapshots[version] = snapshot
            return snapshot

        if args.cohort_status_path:
            forecast_count_snapshot_audit = _verify_live_forecast_count_snapshot(
                read_status=read_cohort_status,
                read_snapshot=read_fresh_forecast_snapshot,
                read_checkpoint=lambda: _checkpoint_progress(args.checkpoint_path),
                expected_run_id=args.expected_run_id,
                expected_cohort_id=args.expected_cohort_id,
                now=datetime.now(timezone.utc),
                attempt_log_path=args.attempt_log_jsonl,
            )
            delta_version = forecast_count_snapshot_audit.get("delta_version")
            snapshot_metrics = snapshots.get(delta_version, {})
        else:
            # Replay and non-cohort callers retain one exact Delta version and
            # the historical scalar equality contract.
            snapshot_metrics = read_fresh_forecast_snapshot()
            delta_version = snapshot_metrics.get("version")
            observed_live = snapshot_metrics.get("live_forecast_count")
            expected_live = args.expected_live_forecasts
            consistent = expected_live is None or observed_live == expected_live
            if args.expected_live_forecasts is not None and args.receipts_path:
                integrity = snapshot_metrics["receipt_integrity"]
                consistent = consistent and integrity.get("status") == "PASS"
            forecast_count_snapshot_audit = {
                "status": "PASS" if consistent else "FAIL",
                "classification": "FORECAST_COUNT_SNAPSHOT_CONSISTENT" if consistent else "FORECAST_COUNT_SNAPSHOT_MISMATCH",
                "reason": "exact counts and configured receipts agree" if consistent else "exact expected and observed forecast counts or persistence receipts differ",
                "expected_live_forecasts": expected_live,
                "observed_live_forecasts": observed_live,
                "delta_version": delta_version,
                "attempts": [],
                "receipt_integrity": snapshot_metrics.get("receipt_integrity") if args.receipts_path else None,
                "hard_failures": snapshot_metrics.get("hard_failures", []),
            }

        total_rows = snapshot_metrics.get("total_forecast_rows", 0)
        origin_counts = snapshot_metrics.get("origin_counts", {})
        location_count = snapshot_metrics.get("replay_location_count", 0)
        replay_rows_per_location_values = snapshot_metrics.get("replay_rows_per_location_values", [])
        live_location_count = snapshot_metrics.get("live_location_count", 0)
        live_rows_per_location_values = snapshot_metrics.get("live_rows_per_location_values", [])
        duplicate_ids = snapshot_metrics.get("forecast_id_duplicates", 0)
        duplicate_logical_keys = snapshot_metrics.get("logical_forecast_duplicates", 0)
        invalid_target_offsets = snapshot_metrics.get("target_offset_violations", 0)
        invalid_contract_rows = snapshot_metrics.get("contract_violations", 0)
        invalid_provider_rows = snapshot_metrics.get("provider_contract_violations", 0)
        invalid_live_leads = snapshot_metrics.get("nonpositive_live_leads", 0)
        lead_stats = snapshot_metrics.get("live_forecast_lead_seconds", {"count": 0, "min": None, "mean": None, "median": None, "p95": None, "max": None})
        sample_record = snapshot_metrics.get("sample_forecast")
        receipt_integrity = snapshot_metrics.get("receipt_integrity")

        state_audit = None
        state_report = None
        if args.state_path:
            def read_state_snapshot() -> dict[str, Any]:
                states = spark.read.format("delta").load(args.state_path).cache()
                try:
                    return _state_history_report(states)
                finally:
                    states.unpersist()

            state_audit = _audit_state_history(
                read_state_snapshot,
                lambda: _checkpoint_progress(args.checkpoint_path),
                audit_id=Path(args.output_json).stem,
                attempt_log_path=args.attempt_log_jsonl,
            )
            state_report = state_audit["state"]

        checks = {
            "forecast_id_duplicates_zero": duplicate_ids == 0,
            "logical_forecast_duplicates_zero": duplicate_logical_keys == 0,
            "target_offset_violations_zero": invalid_target_offsets == 0,
            "contract_violations_zero": invalid_contract_rows == 0,
            "provider_contract_violations_zero": invalid_provider_rows == 0,
            "live_nonpositive_leads_zero": invalid_live_leads == 0,
            "replay_location_coverage_valid": origin_counts.get("REPLAY_VALIDATION", 0) == 0 or len(location_counts) == 63,
            "replay_rows_match_expected": args.expected_replay_forecasts is None or origin_counts.get("REPLAY_VALIDATION", 0) == args.expected_replay_forecasts,
            "live_rows_match_expected": forecast_count_snapshot_audit.get("status") == "PASS",
        }
        if args.receipts_path:
            checks["persistence_receipts_match_forecast_snapshot"] = isinstance(receipt_integrity, Mapping) and receipt_integrity.get("status") == "PASS"
        if state_report is not None:
            checks.update(state_audit["checks"])
        report = {
            "status": _audit_status(checks),
            "forecast_path": args.forecast_path,
            "total_forecast_rows": total_rows,
            "origin_counts": origin_counts,
            "replay_location_count": location_count,
            "replay_rows_per_location_values": replay_rows_per_location_values,
            "live_location_count": live_location_count,
            "live_rows_per_location_values": live_rows_per_location_values,
            "forecast_id_duplicates": duplicate_ids,
            "logical_forecast_duplicates": duplicate_logical_keys,
            "target_offset_violations": invalid_target_offsets,
            "contract_violations": invalid_contract_rows,
            "provider_contract_violations": invalid_provider_rows,
            "nonpositive_live_leads": invalid_live_leads,
            "live_forecast_lead_seconds": lead_stats,
            "forecast_delta_version": delta_version,
            "watchdog_expected_live_forecasts": args.expected_live_forecasts,
            "cohort_status_snapshot": forecast_count_snapshot_audit.get("cohort_status_snapshot"),
            "forecast_count_snapshot_audit": forecast_count_snapshot_audit,
            "persistence_receipt_integrity": receipt_integrity,
            "state": state_report,
            "state_audit_classification": None if state_audit is None else state_audit["classification"],
            "state_audit_attempts": [] if state_audit is None else state_audit["attempts"],
            "checkpoint_progress": None if state_audit is None else state_audit["checkpoint_progress"],
            "checks": checks,
            "sample_forecast": sample_record,
        }
        output = Path(args.output_json)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False))
        return 0 if report["status"] == "PASS" else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
