from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

import pytest

from ops.azure_t2h import watchdog
from spark.jobs import verify_t2h_forecast_delta as audit


RUN_ID = "20261010T111433Z-prospective-live-t2h-v1"
COHORT_ID = "prospective-t2h-20261010T120000Z"
NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


def _state_report(
    rows_per_location: int = 49,
    *,
    duplicate_hours: int = 0,
    hourly_gaps: int = 0,
    locations: int = 63,
    null_location_rows: int = 0,
):
    return {
        "rows": locations * rows_per_location,
        "locations": locations,
        "null_location_rows": null_location_rows,
        "rows_per_location_values": [rows_per_location],
        "duplicate_location_hours": duplicate_hours,
        "hourly_gap_rows": hourly_gaps,
    }


def _checkpoint(status: str, offset: int | None = 15, commit: int | None = 15):
    pending = offset if status == "PENDING" else None
    return {
        "status": status,
        "latest_offset_batch_id": offset,
        "latest_committed_batch_id": commit,
        "pending_batch_id": pending,
        "in_flight": pending is not None,
    }


def test_checkpoint_progress_detects_uncommitted_batch_without_mutating_checkpoint(tmp_path):
    checkpoint = tmp_path / "checkpoint"
    (checkpoint / "offsets").mkdir(parents=True)
    (checkpoint / "commits").mkdir()
    (checkpoint / "offsets" / "16").write_text("offset", encoding="utf-8")
    (checkpoint / "commits" / "15").write_text("commit", encoding="utf-8")

    progress = audit._checkpoint_progress(checkpoint)

    assert progress["status"] == "PENDING"
    assert progress["pending_batch_id"] == 16
    assert (checkpoint / "offsets" / "16").read_text(encoding="utf-8") == "offset"
    assert sorted(path.name for path in (checkpoint / "offsets").iterdir()) == ["16"]
    assert sorted(path.name for path in (checkpoint / "commits").iterdir()) == ["15"]


def test_checkpoint_progress_recognizes_compacted_log_ids(tmp_path):
    checkpoint = tmp_path / "checkpoint"
    (checkpoint / "offsets").mkdir(parents=True)
    (checkpoint / "commits").mkdir()
    (checkpoint / "offsets" / "20.compact").write_text("offsets", encoding="utf-8")
    (checkpoint / "commits" / "20").write_text("commits", encoding="utf-8")

    assert audit._checkpoint_progress(checkpoint)["status"] == "SETTLED"


def test_49_row_bootstrap_passes_without_checkpoint_evidence():
    report = audit._audit_state_history(
        lambda: _state_report(),
        lambda: audit._checkpoint_progress(None),
        audit_id="bootstrap",
        sleep=lambda _: None,
    )

    assert report["status"] == "PASS"
    assert report["classification"] == "STATE_HISTORY_VALID"
    assert report["checks"] == {
        "state_has_63_locations": True,
        "state_location_ids_non_null": True,
        "state_history_is_49_rows_per_location": True,
        "state_duplicate_location_hours_zero": True,
        "state_history_is_hourly_contiguous": True,
    }
    assert len(report["attempts"]) == 1


def test_stable_49_row_history_passes_with_settled_checkpoint():
    report = audit._audit_state_history(
        lambda: _state_report(),
        lambda: _checkpoint("SETTLED"),
        audit_id="stable-49",
        sleep=lambda _: None,
    )

    assert report["status"] == "PASS"
    assert report["checkpoint_progress"]["status"] == "SETTLED"


def test_exact_50_row_inflight_shape_is_classified_as_transient_candidate():
    state = _state_report(50)
    checks = audit._state_history_checks(state)

    assert audit._retryable_retention_window(state, checks, [_checkpoint("PENDING", 16, 15)]) is True
    assert audit._retryable_retention_window(state, checks, [_checkpoint("SETTLED")]) is False


def test_state_retry_does_not_override_frozen_forecast_and_provenance_checks():
    checks = {
        "state_history_is_49_rows_per_location": True,
        "logical_forecast_duplicates_zero": False,
        "target_offset_violations_zero": False,
        "contract_violations_zero": False,
        "provider_contract_violations_zero": False,
    }

    assert audit._audit_status(checks) == "FAIL"


def test_transient_50_rows_then_49_passes_and_first_failed_attempt_is_appended(tmp_path):
    attempt_log = tmp_path / "delta_audit_attempts.jsonl"
    historical_failure = {"audit_id": "prior-hour", "attempt_number": 1, "status": "FAIL", "reason": "old evidence"}
    attempt_log.write_text(json.dumps(historical_failure) + "\n", encoding="utf-8")
    states = iter([_state_report(50), _state_report(49)])
    progress = iter([
        _checkpoint("PENDING", 16, 15),
        _checkpoint("PENDING", 16, 15),
        _checkpoint("PENDING", 16, 15),
        _checkpoint("SETTLED", 16, 16),
        _checkpoint("SETTLED", 16, 16),
        _checkpoint("SETTLED", 16, 16),
    ])
    timestamps = iter(f"2026-10-10T12:00:0{n}Z" for n in range(6))

    result = audit._audit_state_history(
        lambda: next(states),
        lambda: next(progress),
        audit_id="hour-12",
        attempt_log_path=attempt_log,
        sleep=lambda _: None,
        timestamp=lambda: next(timestamps),
    )

    rows = [json.loads(line) for line in attempt_log.read_text(encoding="utf-8").splitlines()]
    assert result["status"] == "PASS"
    assert result["state"]["rows_per_location_values"] == [49]
    assert [row["status"] for row in rows] == ["FAIL", "TRANSIENT_CANDIDATE", "PASS"]
    assert rows[0] == historical_failure
    assert rows[1]["state"]["rows"] == 3_150
    assert rows[1]["checkpoint_before"]["pending_batch_id"] == 16
    assert rows[2]["checks"]["state_history_is_49_rows_per_location"] is True


def test_stable_50_rows_fail_without_retry():
    state_reads = []
    result = audit._audit_state_history(
        lambda: state_reads.append(1) or _state_report(50),
        lambda: _checkpoint("SETTLED"),
        audit_id="stable-50",
        sleep=lambda _: None,
    )

    assert result["status"] == "FAIL"
    assert result["classification"] == "STATE_HISTORY_CONTRACT_FAILED"
    assert len(state_reads) == 1


def test_50_rows_that_remain_after_pending_batch_commits_are_final_failure():
    states = iter([_state_report(50), _state_report(50)])
    progress = iter([
        _checkpoint("PENDING", 16, 15),
        _checkpoint("PENDING", 16, 15),
        _checkpoint("SETTLED", 16, 16),
        _checkpoint("SETTLED", 16, 16),
        _checkpoint("SETTLED", 16, 16),
    ])

    result = audit._audit_state_history(
        lambda: next(states),
        lambda: next(progress),
        audit_id="settled-still-50",
        sleep=lambda _: None,
    )

    assert result["status"] == "FAIL"
    assert result["classification"] == "STATE_HISTORY_CONTRACT_FAILED"
    assert [attempt["status"] for attempt in result["attempts"]] == ["TRANSIENT_CANDIDATE", "FAIL"]


def test_pending_50_rows_timeout_as_fail_with_bounded_polling():
    elapsed = [0.0]
    state_reads = []
    result = audit._audit_state_history(
        lambda: state_reads.append(1) or _state_report(50),
        lambda: _checkpoint("PENDING", 16, 15),
        audit_id="timeout-50",
        deadline_seconds=2,
        poll_interval_seconds=1,
        sleep=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds),
        monotonic=lambda: elapsed[0],
    )

    assert result["status"] == "FAIL"
    assert result["classification"] == "TRANSIENT_STATE_WINDOW_TIMEOUT"
    assert len(state_reads) == 1
    assert len(result["attempts"]) == 2
    assert result["attempts"][-1]["reason"] == "checkpoint_batch_did_not_commit_before_retry_deadline"


def test_pending_checkpoint_polling_uses_capped_exponential_backoff():
    elapsed = [0.0]
    waits = []
    result = audit._audit_state_history(
        lambda: _state_report(50),
        lambda: _checkpoint("PENDING", 16, 15),
        audit_id="backoff-50",
        deadline_seconds=40,
        poll_interval_seconds=5,
        max_poll_interval_seconds=30,
        sleep=lambda seconds: waits.append(seconds) or elapsed.__setitem__(0, elapsed[0] + seconds),
        monotonic=lambda: elapsed[0],
    )

    assert result["classification"] == "TRANSIENT_STATE_WINDOW_TIMEOUT"
    assert waits == [5, 10, 20, 5]
    assert sum(waits) == 40
    assert max(waits) <= 30


def test_duplicate_keys_block_retry_even_if_checkpoint_is_pending():
    state_reads = []
    result = audit._audit_state_history(
        lambda: state_reads.append(1) or _state_report(50, duplicate_hours=1),
        lambda: _checkpoint("PENDING", 16, 15),
        audit_id="duplicate-50",
        sleep=lambda _: None,
    )

    assert result["status"] == "FAIL"
    assert result["classification"] == "STATE_HISTORY_CONTRACT_FAILED"
    assert len(state_reads) == 1


def test_missing_location_fails_closed():
    result = audit._audit_state_history(
        lambda: _state_report(49, locations=62),
        lambda: _checkpoint("PENDING", 16, 15),
        audit_id="missing-location",
        sleep=lambda _: None,
    )

    assert result["status"] == "FAIL"
    assert result["checks"]["state_has_63_locations"] is False


def test_missing_hour_and_null_location_fail_and_cannot_be_retried_as_retention_window():
    gap_report = _state_report(hourly_gaps=1)
    gap_checks = audit._state_history_checks(gap_report)
    null_report = _state_report(null_location_rows=1)
    null_checks = audit._state_history_checks(null_report)

    assert gap_checks["state_history_is_hourly_contiguous"] is False
    assert audit._retryable_retention_window(
        {**gap_report, "rows": 3_150, "rows_per_location_values": [50]},
        {**gap_checks, "state_history_is_49_rows_per_location": False},
        [_checkpoint("PENDING", 16, 15)],
    ) is False
    assert null_checks["state_location_ids_non_null"] is False


@pytest.fixture(scope="module")
def local_spark():
    pytest.importorskip("pyspark.sql")
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    session = (
        SparkSession.builder.master("local[2]")
        .appName("hourly-delta-audit-state-history-regression")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session, StructType([
        StructField("location_id", StringType(), False),
        StructField("event_time", TimestampType(), True),
    ])
    session.stop()


def test_spark_state_history_requires_49_contiguous_hourly_rows_per_location(local_spark):
    spark, schema = local_spark
    base = datetime(2026, 10, 1, tzinfo=timezone.utc).replace(tzinfo=None)
    rows = [
        (f"VN_{location:02}", base + timedelta(hours=hour))
        for location in range(63)
        for hour in range(49)
        if not (location == 0 and hour == 24)
    ]
    state = audit._state_history_report(spark.createDataFrame(rows, schema))
    checks = audit._state_history_checks(state)

    assert state["locations"] == 63
    assert state["rows_per_location_values"] == [48, 49]
    assert state["hourly_gap_rows"] == 1
    assert checks["state_history_is_hourly_contiguous"] is False
    assert checks["state_history_is_49_rows_per_location"] is False


def test_watchdog_audit_command_is_read_only_and_uses_run_checkpoint_and_attempt_log(tmp_path):
    captured = {}
    audit_path = tmp_path / "delta_audit.json"

    def runner(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        audit_path.write_text(json.dumps({
            "status": "PASS",
            "checks": {
                "forecast_id_duplicates_zero": True,
                "logical_forecast_duplicates_zero": True,
                "target_offset_violations_zero": True,
                "contract_violations_zero": True,
                "provider_contract_violations_zero": True,
                "live_nonpositive_leads_zero": True,
                "replay_location_coverage_valid": True,
                "replay_rows_match_expected": True,
                "live_rows_match_expected": True,
                "persistence_receipts_match_forecast_snapshot": True,
                "state_has_63_locations": True,
                "state_location_ids_non_null": True,
                "state_history_is_49_rows_per_location": True,
                "state_duplicate_location_hours_zero": True,
                "state_history_is_hourly_contiguous": True,
            },
            "state_audit_classification": "STATE_HISTORY_VALID",
            "state": _state_report(),
            "state_audit_attempts": [{"status": "PASS"}],
            "checkpoint_progress": _checkpoint("SETTLED"),
            "forecast_count_snapshot_audit": {"status": "PASS", "classification": "FORECAST_COUNT_SNAPSHOT_CONSISTENT"},
        }), encoding="utf-8")
        audit_id = command[command.index("--audit-id") + 1]
        state_dir = Path(command[command.index("--state-dir") + 1])
        job_path = audit_path.parent / "delta_audit_processes" / f"{audit_id}.json"
        job_path.parent.mkdir(parents=True)
        job_path.write_text(json.dumps({
            "audit_id": audit_id,
            "status": "SUCCEEDED",
            "returncode": 0,
            "process_identity": {"pid": 11, "process_group_id": 11, "session_id": 11, "start_time_ticks": 123},
            "cleanup": {"confirmed_terminated": True},
            "log_path": str(state_dir / "delta_audit_processes" / f"{audit_id}.log"),
        }), encoding="utf-8")
        # A detached exec can launch the process and then lose its host-side
        # client; persisted, run-unique supervisor evidence remains authoritative.
        return subprocess.CompletedProcess(command, 1, "", "client disconnected after detach")

    result = watchdog._delta_audit(
        tmp_path,
        RUN_ID,
        {},
        expected_live_forecasts=63,
        audit_path=audit_path,
        runner=runner,
        cohort_id=COHORT_ID,
    )

    command = captured["command"]
    assert result["status"] == "PASS"
    assert command[command.index("exec") + 1:command.index("exec") + 3] == ["-T", "-d"]
    supervisor_index = command.index(watchdog.CONTAINER_AUDIT_SUPERVISOR_PATH)
    assert command[supervisor_index + 1] == "run"
    assert command[supervisor_index + 2] == "--state-dir"
    assert command[command.index("--timeout-seconds") + 1] == "900.0"
    assert command[command.index("--cohort-status-path") + 1] == f"/opt/project/prospective-runtime/{RUN_ID}/cohort_status.json"
    assert command[command.index("--expected-cohort-id") + 1] == COHORT_ID
    assert command[command.index("--checkpoint-path") + 1] == f"/opt/project/data/checkpoints/t2h_v1_1/{RUN_ID}/live"
    assert command[command.index("--attempt-log-jsonl") + 1].endswith("/delta_audit_attempts.jsonl")
    assert any("spark-submit" in part for part in command)
    assert not any(value in {"start", "restart", "up", "down", "resume"} for value in command)
    assert captured["kwargs"]["timeout"] == 30
    assert result["process_cleanup"]["confirmed_terminated"] is True
    lifecycle = [json.loads(line) for line in (audit_path.parent / "delta_audit_host_lifecycle.jsonl").read_text(encoding="utf-8").splitlines()]
    assert any(row["event"] == "AUDIT_DISPATCH_REJECTED" for row in lifecycle)
    assert any(row["event"] == "AUDIT_PROCESS_COMPLETED" for row in lifecycle)


def _write_monitor_fixture(tmp_path: Path, now: datetime = NOW) -> tuple[Path, Path, dict[str, bytes]]:
    state_root = tmp_path / "state"
    run_state = state_root / RUN_ID
    run_state.mkdir(parents=True)
    (state_root / "active_run.json").write_text(json.dumps({"run_id": RUN_ID}), encoding="utf-8")
    request = {
        "run_id": RUN_ID,
        "cohort_protocol_id": watchdog.PROTOCOL_ID,
        "expected_target_hours": 168,
        "expected_locations": 63,
        "expected_slots": 10_584,
        "model_sha256": watchdog.MODEL_SHA256,
        "feature_list_sha256": watchdog.FEATURE_LIST_SHA256,
        "provider_model": "ecmwf_ifs",
        "forecast_horizon_hours": 2,
        "runtime_configuration": {
            "bootstrap_required": True,
            "input_topic": f"weather.hourly.observations.t2h.prospective.{RUN_ID}.v1",
            "producer_cache_path": f"/opt/project/results/prospective-live-t2h/{RUN_ID}/runtime/producer_cache/published_hours.json",
        },
    }
    manifest = {**request, "cohort_id": COHORT_ID, "cohort_start_target_time": "2026-10-10T12:00:00Z", "cohort_end_target_time": "2026-10-17T11:00:00Z"}
    status = {
        "run_id": RUN_ID,
        "cohort_id": COHORT_ID,
        "status": "COLLECTING",
        "expected_slots": 10_584,
        "microbatch_update_count": 7,
        "last_update_at": now.isoformat(),
        "prospective_forecast_count": 441,
        "valid_evaluation_count": 315,
        "restart_count": 0,
        "reference_conflicts": 0,
        "reference_conflict_key_count": 0,
        "invalid_provenance": 0,
        "duplicate_validation": {"status": "PASS"},
        "provenance_validation": {"blocking_violations": []},
    }
    for name, value in (("start_request.json", request), ("cohort_manifest.json", manifest), ("cohort_status.json", status)):
        (run_state / name).write_text(json.dumps(value), encoding="utf-8")
    result_dir = tmp_path / "results" / "prospective-live-t2h" / RUN_ID / "runtime"
    result_dir.mkdir(parents=True)
    producer_path = result_dir / "producer_runtime.json"
    producer_path.write_text(json.dumps({"status": "PASS", "poll_results": [{"delivery_failures": [], "producer_flush_remaining": 0}]}), encoding="utf-8")
    snapshots = {path.name: path.read_bytes() for path in [run_state / "start_request.json", run_state / "cohort_manifest.json", run_state / "cohort_status.json"]}
    return state_root, result_dir.parent, snapshots


def _patch_healthy_monitor(monkeypatch):
    monkeypatch.setattr(watchdog, "collect_container_states", lambda *args, **kwargs: {
        service: {"status": "running", "health": "healthy", "restart_count": 0}
        for service in watchdog.SERVICES
    })
    monkeypatch.setattr(watchdog, "recover_infrastructure", lambda *args, **kwargs: [])


def test_5_minute_monitor_keeps_failed_hourly_audit_visible(tmp_path, monkeypatch):
    state_root, result_root, _ = _write_monitor_fixture(tmp_path)
    _patch_healthy_monitor(monkeypatch)
    ops_dir = result_root / "runtime" / "ops"
    ops_dir.mkdir(parents=True)
    prior_audit = {
        "status": "FAIL",
        "checked_at": NOW.isoformat(),
        "classification": "STATE_HISTORY_CONTRACT_FAILED",
        "evidence_path": "delta_audit_20261010T120000Z.json",
        "checks": {"state_history_is_49_rows_per_location": False},
    }
    (ops_dir / "hourly_reports.jsonl").write_text(json.dumps({
        "hour_key": NOW.strftime("%Y-%m-%dT%H:00Z"),
        "checked_at": NOW.isoformat(),
        "run_id": RUN_ID,
        "status": "FAIL",
        "hourly_delta_audit": prior_audit,
    }) + "\n", encoding="utf-8")
    monkeypatch.setattr(watchdog, "_delta_audit", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("5-minute cycle must not rerun Spark audit")))

    report = watchdog.monitor_once(tmp_path, state_root, now=NOW + timedelta(minutes=5), run_delta_audit=True)
    next_hour_skipped = watchdog.monitor_once(
        tmp_path,
        state_root,
        now=NOW + timedelta(hours=1),
        run_delta_audit=False,
    )
    next_five_minute = watchdog.monitor_once(
        tmp_path,
        state_root,
        now=NOW + timedelta(hours=1, minutes=5),
        run_delta_audit=True,
    )

    assert report["status"] == "FAIL"
    assert "HOURLY_DELTA_AUDIT_FAILED" in report["data_errors"]
    assert report["hourly_delta_audit"]["status"] == "UNRESOLVED"
    assert next_hour_skipped["status"] == "FAIL"
    assert "HOURLY_DELTA_AUDIT_FAILED" in next_hour_skipped["data_errors"]
    assert next_five_minute["status"] == "FAIL"
    assert "HOURLY_DELTA_AUDIT_FAILED" in next_five_minute["data_errors"]
    saved = [json.loads(line) for line in (ops_dir / "monitor.jsonl").read_text(encoding="utf-8").splitlines()]
    assert all(item["status"] == "FAIL" for item in saved)
    hourly = [json.loads(line) for line in (ops_dir / "hourly_reports.jsonl").read_text(encoding="utf-8").splitlines()]
    assert hourly[-1]["hourly_delta_audit"]["status"] == "UNRESOLVED"
    assert not (ops_dir / "delta_audit_attempts.jsonl").exists()


def test_new_hourly_pass_resolves_but_does_not_delete_prior_audit_failure(tmp_path, monkeypatch):
    state_root, result_root, immutable_inputs = _write_monitor_fixture(tmp_path)
    _patch_healthy_monitor(monkeypatch)
    ops_dir = result_root / "runtime" / "ops"
    ops_dir.mkdir(parents=True)
    prior_audit = {
        "status": "FAIL",
        "checked_at": NOW.isoformat(),
        "classification": "TRANSIENT_STATE_WINDOW_TIMEOUT",
        "evidence_path": "delta_audit_20261010T120000Z.json",
        "checks": {"state_history_is_49_rows_per_location": False},
    }
    (ops_dir / "hourly_reports.jsonl").write_text(json.dumps({
        "hour_key": NOW.strftime("%Y-%m-%dT%H:00Z"),
        "checked_at": NOW.isoformat(),
        "run_id": RUN_ID,
        "status": "FAIL",
        "hourly_delta_audit": prior_audit,
    }) + "\n", encoding="utf-8")
    monkeypatch.setattr(watchdog, "_delta_audit", lambda *args, **kwargs: {
        "status": "PASS",
        "classification": "STATE_HISTORY_VALID",
        "checked_at": (NOW + timedelta(hours=1)).isoformat(),
        "evidence_path": "delta_audit_20261010T130000Z.json",
        "checks": {"state_history_is_49_rows_per_location": True},
    })

    report = watchdog.monitor_once(tmp_path, state_root, now=NOW + timedelta(hours=1), run_delta_audit=True)

    assert report["status"] == "PASS"
    assert report["hourly_delta_audit"]["status"] == "PASS"
    assert report["run_id"] == RUN_ID
    assert report["cohort_id"] == COHORT_ID
    assert report["restart_count"] == 0
    assert report["model_sha256"] == "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a"
    assert report["feature_list_sha256"] == "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2"
    hourly = [json.loads(line) for line in (ops_dir / "hourly_reports.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [row["hourly_delta_audit"]["status"] for row in hourly] == ["FAIL", "PASS"]
    run_state = state_root / RUN_ID
    for filename, contents in immutable_inputs.items():
        assert (run_state / filename).read_bytes() == contents


def _hold_audit_lock(lock_path: str, ready_path: str, release_path: str) -> None:
    with watchdog._exclusive_audit_lock(Path(lock_path)) as acquired:
        Path(ready_path).write_text("1" if acquired else "0", encoding="utf-8")
        while acquired and not Path(release_path).exists():
            time.sleep(0.02)


def test_audit_process_lock_rejects_concurrent_process_without_changing_attempt_log(tmp_path):
    lock_path = tmp_path / "delta_audit.lock"
    attempt_log = tmp_path / "delta_audit_attempts.jsonl"
    attempt_log.write_text('{"status":"first-attempt"}\n', encoding="utf-8")
    ready_path = tmp_path / "holder-ready"
    release_path = tmp_path / "holder-release"
    script = (
        "from pathlib import Path\n"
        "import sys,time\n"
        "from ops.azure_t2h.watchdog import _exclusive_audit_lock\n"
        "with _exclusive_audit_lock(Path(sys.argv[1])) as acquired:\n"
        " Path(sys.argv[2]).write_text('1' if acquired else '0')\n"
        " while acquired and not Path(sys.argv[3]).exists(): time.sleep(.02)\n"
    )
    process = subprocess.Popen([sys.executable, "-c", script, str(lock_path), str(ready_path), str(release_path)])
    try:
        deadline = time.monotonic() + 10
        while not ready_path.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready_path.read_text(encoding="utf-8") == "1"
        original_attempts = attempt_log.read_bytes()
        with watchdog._exclusive_audit_lock(lock_path) as acquired:
            assert acquired is False
        assert attempt_log.read_bytes() == original_attempts
    finally:
        release_path.touch()
        process.wait(timeout=10)


def test_audit_process_lock_is_released_after_holder_crashes(tmp_path):
    lock_path = tmp_path / "delta_audit.lock"
    ready_path = tmp_path / "holder-ready"
    release_path = tmp_path / "holder-release"
    script = (
        "from pathlib import Path\n"
        "import sys,time\n"
        "from ops.azure_t2h.watchdog import _exclusive_audit_lock\n"
        "with _exclusive_audit_lock(Path(sys.argv[1])) as acquired:\n"
        " Path(sys.argv[2]).write_text('1' if acquired else '0')\n"
        " while acquired and not Path(sys.argv[3]).exists(): time.sleep(.02)\n"
    )
    process = subprocess.Popen([sys.executable, "-c", script, str(lock_path), str(ready_path), str(release_path)])
    try:
        deadline = time.monotonic() + 10
        while not ready_path.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready_path.read_text(encoding="utf-8") == "1"
        process.terminate()
        process.wait(timeout=10)
        deadline = time.monotonic() + 5
        acquired = False
        while time.monotonic() < deadline and not acquired:
            with watchdog._exclusive_audit_lock(lock_path) as locked:
                acquired = locked
            if not acquired:
                time.sleep(0.05)
        assert acquired is True
    finally:
        if process.poll() is None:
            release_path.touch()
            process.wait(timeout=10)


def test_monitor_lock_contention_does_not_run_audit_or_recover_services(tmp_path, monkeypatch):
    state_root, result_root, _ = _write_monitor_fixture(tmp_path)
    _patch_healthy_monitor(monkeypatch)

    @contextmanager
    def lock_denied(_path):
        yield False

    monkeypatch.setattr(watchdog, "_exclusive_audit_lock", lock_denied)
    monkeypatch.setattr(watchdog, "_delta_audit", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("lock loser must not run Spark")))
    recovery_calls = []
    monkeypatch.setattr(
        watchdog,
        "recover_infrastructure",
        lambda *args, **kwargs: recovery_calls.append(kwargs["data_errors"]) or [],
    )

    report = watchdog.monitor_once(tmp_path, state_root, now=NOW, run_delta_audit=True)
    ops_dir = result_root / "runtime" / "ops"

    assert report["status"] == "FAIL"
    assert report["hourly_delta_audit"]["classification"] == "AUDIT_ALREADY_RUNNING"
    assert "HOURLY_DELTA_AUDIT_COULD_NOT_RUN" in report["infrastructure_errors"]
    assert "AUDIT_LOCK_HELD" in report["recovery_eligibility"]["blocking_reasons"]
    assert recovery_calls == []
    assert not (ops_dir / "delta_audit_attempts.jsonl").exists()


def _transient_retention_audit(classification="TRANSIENT_STATE_WINDOW_TIMEOUT"):
    state = _state_report(50)
    state_checks = audit._state_history_checks(state)
    checkpoint = _checkpoint("PENDING", 16, 15)
    audit_id = "delta_audit_20261010T120000Z"

    def candidate(attempt_number):
        return {
            "event": "STATE_AUDIT_ATTEMPT",
            "audit_id": audit_id,
            "attempt_number": attempt_number,
            "status": "TRANSIENT_CANDIDATE",
            "classification": "TRANSIENT_MERGE_BEFORE_RETENTION_DELETE",
            "reason": "checkpoint_has_uncommitted_batch_during_exact_50_row_retention_window",
            "state": dict(state),
            "checks": dict(state_checks),
            "checkpoint_before": dict(checkpoint),
            "checkpoint_after": dict(checkpoint),
        }

    if classification == "TRANSIENT_RETRY_LIMIT_EXCEEDED":
        attempts = [candidate(1), candidate(2)]
    else:
        attempts = [
            candidate(1),
            {
                "event": "STATE_AUDIT_RETRY_WAIT",
                "audit_id": audit_id,
                "attempt_number": 1,
                "status": "FAIL",
                "classification": "TRANSIENT_STATE_WINDOW_TIMEOUT",
                "reason": "checkpoint_batch_did_not_commit_before_retry_deadline",
                "state": dict(state),
                "checks": dict(state_checks),
                "checkpoint_after": dict(checkpoint),
            },
        ]

    checks = {name: True for name in watchdog.HOURLY_AUDIT_FORECAST_CHECKS}
    checks.update(state_checks)
    return {
        "status": "FAIL",
        "checked_at": NOW.isoformat(),
        "classification": classification,
        "evidence_path": "delta_audit_20261010T120000Z.json",
        "attempt_log_path": "delta_audit_attempts.jsonl",
        "checks": checks,
        "state": state,
        "state_audit_attempts": attempts,
        "checkpoint_progress": dict(checkpoint),
        "forecast_count_snapshot_audit": {
            "status": "PASS",
            "classification": "FORECAST_COUNT_SNAPSHOT_CONSISTENT",
            "expected_live_forecasts": 441,
            "observed_live_forecasts": 441,
            "delta_version": 21,
            "hard_failures": [],
            "receipt_integrity": {
                "status": "PASS",
                "missing": 0,
                "extra": 0,
                "duplicates": 0,
                "mismatched": 0,
                "invalid": 0,
            },
        },
    }


def _write_prior_failed_hourly_audit(
    tmp_path,
    result_root,
    *,
    classification="TRANSIENT_STATE_WINDOW_TIMEOUT",
    checks=None,
    audit_evidence=None,
):
    ops_dir = result_root / "runtime" / "ops"
    ops_dir.mkdir(parents=True, exist_ok=True)
    historical_audit = dict(audit_evidence or _transient_retention_audit())
    historical_audit["classification"] = classification
    if checks is not None:
        historical_audit["checks"] = checks
    (ops_dir / "hourly_reports.jsonl").write_text(json.dumps({
        "hour_key": NOW.strftime("%Y-%m-%dT%H:00Z"),
        "checked_at": NOW.isoformat(),
        "run_id": RUN_ID,
        "status": "FAIL",
        "hourly_delta_audit": historical_audit,
    }) + "\n", encoding="utf-8")
    return ops_dir


def _carried_audit(audit_evidence):
    report = {"data_errors": [], "infrastructure_errors": [], "status": "PASS"}
    watchdog._carry_unresolved_hourly_delta_audit(report, audit_evidence)
    return report["hourly_delta_audit"]


@pytest.mark.parametrize(
    "classification",
    ["TRANSIENT_STATE_WINDOW_TIMEOUT", "TRANSIENT_RETRY_LIMIT_EXCEEDED"],
)
def test_carried_audit_allows_only_fully_evidenced_transient_retention(classification):
    carried = _carried_audit(_transient_retention_audit(classification))

    decision = watchdog._carried_audit_recovery_decision(carried)

    assert carried["state"] == _state_report(50)
    assert len(carried["state_audit_attempts"]) >= 2
    assert decision["eligible"] is True
    assert decision["reason"] == "evidenced_transient_retention_window_only"


@pytest.mark.parametrize(
    "corruption",
    [
        "duplicate_location_hours",
        "missing_location",
        "null_location_id",
        "history_gap",
        "forecast_contract_violation",
        "duplicate_forecast_key",
        "receipt_integrity_failure",
        "missing_current_check",
        "missing_attempt_evidence",
        "unknown_classification",
        "state_contract_failure_classification",
    ],
)
def test_carried_audit_rejects_all_non_transient_or_malformed_evidence(corruption):
    evidence = _transient_retention_audit()
    if corruption == "duplicate_location_hours":
        evidence["state"]["duplicate_location_hours"] = 1
        evidence["checks"]["state_duplicate_location_hours_zero"] = False
    elif corruption == "missing_location":
        evidence["state"].update({"locations": 62, "rows": 3_100})
        evidence["checks"]["state_has_63_locations"] = False
    elif corruption == "null_location_id":
        evidence["state"]["null_location_rows"] = 1
        evidence["checks"]["state_location_ids_non_null"] = False
    elif corruption == "history_gap":
        evidence["state"]["hourly_gap_rows"] = 1
        evidence["checks"]["state_history_is_hourly_contiguous"] = False
    elif corruption == "forecast_contract_violation":
        evidence["checks"]["contract_violations_zero"] = False
    elif corruption == "duplicate_forecast_key":
        evidence["checks"]["logical_forecast_duplicates_zero"] = False
    elif corruption == "receipt_integrity_failure":
        evidence["forecast_count_snapshot_audit"]["receipt_integrity"]["duplicates"] = 1
    elif corruption == "missing_current_check":
        evidence["checks"].pop("persistence_receipts_match_forecast_snapshot")
    elif corruption == "missing_attempt_evidence":
        evidence.pop("state_audit_attempts")
    elif corruption == "unknown_classification":
        evidence["classification"] = "UNRECOGNIZED_STATE_CLASSIFICATION"
    elif corruption == "state_contract_failure_classification":
        evidence["classification"] = "STATE_HISTORY_CONTRACT_FAILED"

    decision = watchdog._carried_audit_recovery_decision(_carried_audit(evidence))

    assert decision["eligible"] is False


def _patch_monitor_states(monkeypatch, states):
    monkeypatch.setattr(watchdog, "collect_container_states", lambda *args, **kwargs: states)


def test_historical_hourly_failure_remains_alerted_but_does_not_restart_healthy_services(tmp_path, monkeypatch):
    state_root, result_root, _ = _write_monitor_fixture(tmp_path)
    states = {service: {"status": "running", "health": "healthy", "restart_count": 0} for service in watchdog.SERVICES}
    _patch_monitor_states(monkeypatch, states)
    ops_dir = _write_prior_failed_hourly_audit(tmp_path, result_root)
    calls = []

    report = watchdog.monitor_once(
        tmp_path,
        state_root,
        now=NOW + timedelta(minutes=5),
        run_delta_audit=False,
        runner=lambda command, **kwargs: calls.append(command) or subprocess.CompletedProcess(command, 0, "", ""),
    )

    assert report["hourly_delta_audit"]["status"] == "UNRESOLVED"
    assert "HOURLY_DELTA_AUDIT_FAILED" in report["data_errors"]
    assert report["recovery_actions"] == []
    assert calls == []
    assert len((ops_dir / "alerts.jsonl").read_text(encoding="utf-8").splitlines()) == 1


@pytest.mark.parametrize(
    ("service", "state"),
    [
        ("live-hourly-producer-t2h", {"status": "exited", "health": None, "restart_count": 0}),
        ("streaming-inference-t2h-live", {"status": "running", "health": "unhealthy", "restart_count": 1}),
    ],
)
def test_historical_hourly_failure_allows_eligible_infrastructure_recovery(tmp_path, monkeypatch, service, state):
    state_root, result_root, _ = _write_monitor_fixture(tmp_path)
    states = {name: {"status": "running", "health": "healthy", "restart_count": 0} for name in watchdog.SERVICES}
    states[service] = state
    _patch_monitor_states(monkeypatch, states)
    _write_prior_failed_hourly_audit(tmp_path, result_root)
    commands = []

    report = watchdog.monitor_once(
        tmp_path,
        state_root,
        now=NOW + timedelta(minutes=5),
        run_delta_audit=False,
        runner=lambda command, **kwargs: commands.append(command) or subprocess.CompletedProcess(command, 0, "started", ""),
    )

    assert "HOURLY_DELTA_AUDIT_FAILED" in report["data_errors"]
    assert report["recovery_eligibility"]["eligible"] is True
    assert report["recovery_actions"][0]["service"] == service
    assert report["recovery_actions"][0]["status"] == "PASS"
    assert len(commands) == 1


def test_current_data_integrity_violation_blocks_recovery_despite_historical_audit_failure(tmp_path, monkeypatch):
    state_root, result_root, _ = _write_monitor_fixture(tmp_path)
    status_path = state_root / RUN_ID / "cohort_status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["reference_conflicts"] = 1
    status_path.write_text(json.dumps(status), encoding="utf-8")
    states = {service: {"status": "running", "health": "healthy", "restart_count": 0} for service in watchdog.SERVICES}
    states["streaming-inference-t2h-live"] = {"status": "running", "health": "unhealthy", "restart_count": 0}
    _patch_monitor_states(monkeypatch, states)
    _write_prior_failed_hourly_audit(tmp_path, result_root)
    calls = []

    report = watchdog.monitor_once(
        tmp_path,
        state_root,
        now=NOW + timedelta(minutes=5),
        run_delta_audit=False,
        runner=lambda command, **kwargs: calls.append(command) or subprocess.CompletedProcess(command, 0, "", ""),
    )

    assert any(error.startswith("COHORT_REFERENCE_CONFLICTS_NONZERO:") for error in report["current_data_errors"])
    assert report["recovery_eligibility"]["eligible"] is False
    assert report["recovery_actions"] == []
    assert calls == []


def test_invalid_live_forecast_counter_is_a_current_blocking_data_error(tmp_path, monkeypatch):
    state_root, _, _ = _write_monitor_fixture(tmp_path)
    status_path = state_root / RUN_ID / "cohort_status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["prospective_forecast_count"] = True
    status_path.write_text(json.dumps(status), encoding="utf-8")
    states = {service: {"status": "running", "health": "healthy", "restart_count": 0} for service in watchdog.SERVICES}
    states["streaming-inference-t2h-live"] = {"status": "running", "health": "unhealthy", "restart_count": 0}
    _patch_monitor_states(monkeypatch, states)
    recovery_calls = []
    monkeypatch.setattr(
        watchdog,
        "recover_infrastructure",
        lambda *args, **kwargs: recovery_calls.append(kwargs["data_errors"]) or [],
    )

    report = watchdog.monitor_once(tmp_path, state_root, now=NOW, run_delta_audit=False)

    assert "COHORT_PROSPECTIVE_FORECAST_COUNT_INVALID" in report["current_data_errors"]
    assert report["recovery_eligibility"]["eligible"] is False
    assert "COHORT_PROSPECTIVE_FORECAST_COUNT_INVALID" in report["recovery_eligibility"]["blocking_reasons"]
    assert report["recovery_actions"] == []
    assert recovery_calls == []


def test_unresolved_audit_with_unknown_classification_blocks_recovery(tmp_path, monkeypatch):
    state_root, result_root, _ = _write_monitor_fixture(tmp_path)
    states = {service: {"status": "running", "health": "healthy", "restart_count": 0} for service in watchdog.SERVICES}
    states["live-hourly-producer-t2h"] = {"status": "exited", "health": None, "restart_count": 0}
    _patch_monitor_states(monkeypatch, states)
    _write_prior_failed_hourly_audit(tmp_path, result_root, classification=None)
    calls = []

    report = watchdog.monitor_once(
        tmp_path,
        state_root,
        now=NOW + timedelta(minutes=5),
        run_delta_audit=False,
        runner=lambda command, **kwargs: calls.append(command) or subprocess.CompletedProcess(command, 0, "", ""),
    )

    assert report["recovery_eligibility"]["eligible"] is False
    assert "HOURLY_DELTA_AUDIT_CLASSIFICATION_UNVERIFIED" in report["recovery_eligibility"]["blocking_reasons"]
    assert report["recovery_actions"] == []
    assert calls == []


def test_audit_lock_contention_blocks_unhealthy_service_recovery(tmp_path, monkeypatch):
    state_root, _, _ = _write_monitor_fixture(tmp_path)
    states = {service: {"status": "running", "health": "healthy", "restart_count": 0} for service in watchdog.SERVICES}
    states["streaming-inference-t2h-live"] = {"status": "running", "health": "unhealthy", "restart_count": 0}
    _patch_monitor_states(monkeypatch, states)

    @contextmanager
    def lock_denied(_path):
        yield False

    monkeypatch.setattr(watchdog, "_exclusive_audit_lock", lock_denied)
    monkeypatch.setattr(watchdog, "_delta_audit", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("lock loser must not run Spark")))
    calls = []
    report = watchdog.monitor_once(
        tmp_path,
        state_root,
        now=NOW,
        runner=lambda command, **kwargs: calls.append(command) or subprocess.CompletedProcess(command, 0, "", ""),
    )

    assert "AUDIT_LOCK_HELD" in report["recovery_eligibility"]["blocking_reasons"]
    assert report["recovery_actions"] == []
    assert calls == []


@pytest.mark.parametrize("guard_status", ["ACTIVE", "UNVERIFIED"])
def test_container_audit_process_active_or_unverified_blocks_infrastructure_recovery(tmp_path, monkeypatch, guard_status):
    state_root, _, _ = _write_monitor_fixture(tmp_path)
    states = {service: {"status": "running", "health": "healthy", "restart_count": 0} for service in watchdog.SERVICES}
    states["streaming-inference-t2h-live"] = {"status": "running", "health": "unhealthy", "restart_count": 1}
    _patch_monitor_states(monkeypatch, states)
    guard = {
        "status": guard_status,
        "classification": "AUDIT_PROCESS_STILL_RUNNING" if guard_status == "ACTIVE" else "AUDIT_PROCESS_IDENTITY_UNVERIFIED",
        "audit_id": "delta_audit_active",
        "process_identity": {"pid": 1234, "process_group_id": 1234, "session_id": 1234},
    }
    monkeypatch.setattr(watchdog, "_audit_process_guard", lambda *args, **kwargs: dict(guard))
    recovery_calls = []
    monkeypatch.setattr(
        watchdog,
        "recover_infrastructure",
        lambda *args, **kwargs: recovery_calls.append(kwargs["data_errors"]) or [],
    )

    report = watchdog.monitor_once(tmp_path, state_root, now=NOW, run_delta_audit=False)

    assert report["recovery_eligibility"]["eligible"] is False
    assert "AUDIT_PROCESS_ACTIVE_OR_UNVERIFIED" in report["recovery_eligibility"]["blocking_reasons"]
    assert report["audit_process_guard"]["before_recovery"]["status"] == guard_status
    assert report["recovery_actions"] == []
    assert recovery_calls == []


def test_inference_unavailable_skips_delta_audit_without_turning_infra_failure_into_data_blocker(tmp_path, monkeypatch):
    state_root, _, _ = _write_monitor_fixture(tmp_path)
    states = {service: {"status": "running", "health": "healthy", "restart_count": 0} for service in watchdog.SERVICES}
    states["streaming-inference-t2h-live"] = {"status": "exited", "health": None, "restart_count": 1}
    _patch_monitor_states(monkeypatch, states)
    monkeypatch.setattr(
        watchdog,
        "_delta_audit",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cannot execute spark-submit in a stopped inference container")),
    )
    recovery_calls = []
    monkeypatch.setattr(
        watchdog,
        "recover_infrastructure",
        lambda *args, **kwargs: recovery_calls.append(kwargs["data_errors"]) or [],
    )

    report = watchdog.monitor_once(tmp_path, state_root, now=NOW, run_delta_audit=True)

    assert report["hourly_delta_audit"]["status"] == "SKIPPED"
    assert report["hourly_delta_audit"]["origin"] == "INFRASTRUCTURE_UNAVAILABLE"
    assert report["recovery_eligibility"]["eligible"] is True
    assert recovery_calls == [[]]


def test_skipped_audit_keeps_previous_hourly_failure_visible_and_append_only(tmp_path, monkeypatch):
    state_root, result_root, _ = _write_monitor_fixture(tmp_path)
    states = {service: {"status": "running", "health": "healthy", "restart_count": 0} for service in watchdog.SERVICES}
    states["streaming-inference-t2h-live"] = {"status": "exited", "health": None, "restart_count": 1}
    _patch_monitor_states(monkeypatch, states)
    ops_dir = _write_prior_failed_hourly_audit(tmp_path, result_root)
    rows = [json.loads(line) for line in (ops_dir / "hourly_reports.jsonl").read_text(encoding="utf-8").splitlines()]
    rows[0]["hour_key"] = (NOW - timedelta(hours=1)).strftime("%Y-%m-%dT%H:00Z")
    rows[0]["checked_at"] = (NOW - timedelta(hours=1)).isoformat()
    (ops_dir / "hourly_reports.jsonl").write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    recovery_calls = []
    monkeypatch.setattr(
        watchdog,
        "recover_infrastructure",
        lambda *args, **kwargs: recovery_calls.append(kwargs["data_errors"]) or [],
    )

    report = watchdog.monitor_once(tmp_path, state_root, now=NOW, run_delta_audit=True)

    assert report["hourly_delta_audit"]["status"] == "UNRESOLVED"
    assert report["hourly_delta_audit"]["origin"] == "CARRIED_FORWARD"
    assert report["hourly_delta_audit"]["latest_attempt"]["origin"] == "INFRASTRUCTURE_UNAVAILABLE"
    assert "HOURLY_DELTA_AUDIT_FAILED" in report["data_errors"]
    assert report["recovery_eligibility"]["eligible"] is True
    assert recovery_calls == [[]]
    saved = [json.loads(line) for line in (ops_dir / "hourly_reports.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(saved) == 2
    assert saved[-1]["hourly_delta_audit"]["latest_attempt"]["status"] == "SKIPPED"
