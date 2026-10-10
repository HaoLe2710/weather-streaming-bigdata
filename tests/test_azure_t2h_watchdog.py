from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import subprocess

import pytest

from ops.azure_t2h import watchdog


RUN_ID = "20261010T120000Z-prospective-live-t2h-v1"
NOW = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _isolate_tmp_repository_tests_from_deployment_environment(monkeypatch):
    monkeypatch.delenv("WEATHER_AZURE_RUNTIME", raising=False)
    monkeypatch.delenv("COMPOSE_FILE", raising=False)


def _contract(run_id: str = RUN_ID) -> dict:
    return {
        "run_id": run_id,
        "cohort_protocol_id": watchdog.PROTOCOL_ID,
        "expected_target_hours": 168,
        "expected_locations": 63,
        "expected_slots": 10_584,
        "model_sha256": watchdog.MODEL_SHA256,
        "feature_list_sha256": watchdog.FEATURE_LIST_SHA256,
        "provider_model": watchdog.PROVIDER_MODEL,
        "forecast_horizon_hours": 2,
        "runtime_configuration": {
            "bootstrap_required": True,
            "input_topic": f"weather.hourly.observations.t2h.prospective.{run_id}.v1",
            "producer_cache_path": f"/opt/project/results/prospective-live-t2h/{run_id}/runtime/producer_cache/published_hours.json",
        },
    }


def test_data_findings_catches_conflicts_gaps_provenance_and_zero_forecasts():
    request = _contract()
    status = {
        "run_id": RUN_ID,
        "expected_slots": 10_584,
        "restart_count": 2,
        "last_update_at": NOW.isoformat(),
        "status": "COLLECTING",
        "reference_conflicts": 1,
        "reference_conflict_key_count": 1,
        "invalid_provenance": 1,
        "duplicate_validation": {"duplicate_forecast_ids": 1},
        "provenance_validation": {"blocking_violations": ["INVALID_PROVENANCE_SLOTS"]},
    }
    batches = [{
        "batch_id": 9,
        "source_rows": 63,
        "feature_ready_rows": 0,
        "new_forecast_rows": 0,
        "duplicate_conflict_keys": 567,
        "gap_in_history_rows": 1_575,
        "invalid_provenance_rows": 1,
    }]

    errors, _ = watchdog._data_findings(
        request=request,
        manifest=None,
        cohort_status=status,
        inference_batches=batches,
        receipts=[],
        producer_runtime=None,
        now=NOW,
    )

    assert "INFERENCE_DUPLICATE_CONFLICT_KEYS_BATCH_9:567" in errors
    assert "INFERENCE_GAP_IN_HISTORY_ROWS_BATCH_9:1575" in errors
    assert "INFERENCE_INVALID_PROVENANCE_ROWS_BATCH_9:1" in errors
    assert "LATEST_BATCH_HAS_ZERO_READY_FEATURES_AND_FORECASTS" in errors
    assert "COHORT_REFERENCE_CONFLICTS_NONZERO:1" in errors
    assert "COHORT_INVALID_PROVENANCE_NONZERO:1" in errors
    assert "COHORT_DUPLICATE_FORECAST_IDS_NONZERO:1" in errors
    assert "COHORT_PROVENANCE_BLOCKING_VIOLATIONS" in errors


def test_isolation_contract_and_malformed_batch_metrics_fail_closed():
    request = _contract()
    request["runtime_configuration"]["input_topic"] = "weather.hourly.observations.t2h.live.v1"
    request["runtime_configuration"]["producer_cache_path"] = "/tmp/shared-cache.json"
    errors, _ = watchdog._data_findings(
        request=request,
        manifest=None,
        cohort_status=None,
        inference_batches=[{"batch_id": "bad", "source_rows": "not-an-int"}],
        receipts=[],
        producer_runtime=None,
        now=NOW,
    )
    assert "RUN_TOPIC_NOT_ISOLATED" in errors
    assert "PRODUCER_CACHE_NOT_RUN_SCOPED" in errors
    assert "LATEST_INFERENCE_BATCH_METRICS_INVALID" in errors


def test_recovery_is_disabled_for_data_errors(tmp_path):
    calls = []
    actions = watchdog.recover_infrastructure(
        tmp_path,
        RUN_ID,
        {},
        {"broker": {"status": "exited", "health": None, "restart_count": 3}},
        data_errors=["INFERENCE_GAP_IN_HISTORY_ROWS_BATCH_1:2"],
        action_log=tmp_path / "restart_actions.jsonl",
        now=NOW,
        runner=lambda *args, **kwargs: calls.append(args),
        sleep=lambda _: None,
    )
    assert actions == []
    assert calls == []
    assert not (tmp_path / "restart_actions.jsonl").exists()


def test_recovery_retries_transient_infrastructure_and_counts_one_action(tmp_path):
    replies = [
        subprocess.CompletedProcess([], 1, "", "Cannot connect to the Docker daemon"),
        subprocess.CompletedProcess([], 0, "started", ""),
    ]
    waits = []
    action_log = tmp_path / "restart_actions.jsonl"
    actions = watchdog.recover_infrastructure(
        tmp_path,
        RUN_ID,
        {},
        {"broker": {"status": "exited", "health": None, "restart_count": 2}},
        data_errors=[],
        action_log=action_log,
        now=NOW,
        runner=lambda *args, **kwargs: replies.pop(0),
        sleep=waits.append,
    )
    assert actions[0]["operation"] == "start"
    assert actions[0]["status"] == "PASS"
    assert actions[0]["command_attempts"] == 2
    assert waits == [1]
    logged = [json.loads(line) for line in action_log.read_text(encoding="utf-8").splitlines()]
    assert len(logged) == 2
    assert len({item["action_id"] for item in logged}) == 1
    assert watchdog._restart_budget(logged, "broker", NOW + timedelta(minutes=1)) == 1


def test_recovery_does_not_act_on_unverified_or_missing_containers(tmp_path):
    calls = []
    actions = watchdog.recover_infrastructure(
        tmp_path,
        RUN_ID,
        {},
        {
            "broker": {"status": "unknown", "health": None, "error": "Docker unavailable"},
            "spark-worker": {"status": "missing", "health": None},
        },
        data_errors=[],
        action_log=tmp_path / "restart_actions.jsonl",
        now=NOW,
        runner=lambda *args, **kwargs: calls.append(args),
        sleep=lambda _: None,
    )
    assert calls == []
    assert {action["status"] for action in actions} == {
        "CONTAINER_STATE_UNVERIFIED",
        "MISSING_CONTAINER_REQUIRES_OPERATOR",
    }


def test_monitor_is_noop_without_official_cohort(tmp_path):
    report = watchdog.monitor_once(
        tmp_path,
        tmp_path / "state",
        runner=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not call Docker")),
        run_delta_audit=False,
    )
    assert report["status"] == "NO_OFFICIAL_COHORT"
    assert report["cohort_started"] is False


def test_watchdog_does_not_restart_services_or_reaudit_a_finalized_cohort(tmp_path):
    state_root = tmp_path / "state"
    run_state = state_root / RUN_ID
    run_state.mkdir(parents=True)
    (state_root / "active_run.json").write_text(json.dumps({"run_id": RUN_ID}), encoding="utf-8")
    (run_state / "cohort_status.json").write_text(
        json.dumps({
            "status": "FINALIZED",
            "run_id": RUN_ID,
            "cohort_id": "prospective-t2h-frozen-t0",
            "cohort_start_target_time": "2026-10-03T17:00:00Z",
            "restart_count": 3,
        }),
        encoding="utf-8",
    )
    report = watchdog.monitor_once(
        tmp_path,
        state_root,
        runner=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("finalized cohort must not call Docker")),
    )
    assert report["status"] == "COHORT_FINALIZED"
    assert report["cohort_id"] == "prospective-t2h-frozen-t0"
    assert report["restart_count"] == 3
    assert report["monitoring_actions_taken"] is False


def test_alert_log_records_only_transitions(tmp_path):
    ops_dir = tmp_path / "ops"
    report = {
        "checked_at": NOW.isoformat(),
        "run_id": RUN_ID,
        "restart_count": 3,
        "data_errors": ["INFERENCE_GAP_IN_HISTORY_ROWS_BATCH_1:4"],
        "infrastructure_errors": [],
    }
    watchdog._update_alert_evidence(ops_dir, report)
    watchdog._update_alert_evidence(ops_dir, report)
    assert len((ops_dir / "alerts.jsonl").read_text(encoding="utf-8").splitlines()) == 1

    watchdog._update_alert_evidence(ops_dir, {**report, "data_errors": []})
    events = [json.loads(line)["event"] for line in (ops_dir / "alerts.jsonl").read_text(encoding="utf-8").splitlines()]
    assert events == ["OPEN", "RESOLVED"]


def test_monitor_appends_hourly_daily_and_restart_evidence_without_reset(tmp_path, monkeypatch):
    monitor_now = datetime.now(timezone.utc)
    state_root = tmp_path / "data" / "runtime" / "prospective-live-t2h"
    run_state = state_root / RUN_ID
    run_state.mkdir(parents=True)
    (state_root / "active_run.json").write_text(json.dumps({"run_id": RUN_ID}), encoding="utf-8")
    request = _contract()
    (run_state / "start_request.json").write_text(json.dumps(request), encoding="utf-8")
    status = {
        "run_id": RUN_ID,
        "cohort_id": "prospective-t2h-fixed-t0",
        "status": "COLLECTING",
        "expected_slots": 10_584,
        "microbatch_update_count": 12,
        "last_update_at": monitor_now.isoformat(),
        "prospective_forecast_count": 126,
        "valid_evaluation_count": 63,
        "restart_count": 4,
        "reference_conflicts": 0,
        "reference_conflict_key_count": 0,
        "invalid_provenance": 0,
        "duplicate_validation": {"status": "PASS"},
        "provenance_validation": {"blocking_violations": []},
    }
    (run_state / "cohort_status.json").write_text(json.dumps(status), encoding="utf-8")
    manifest = {**request, "cohort_id": "prospective-t2h-fixed-t0"}
    (run_state / "cohort_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    result_dir = tmp_path / "results" / "prospective-live-t2h" / RUN_ID / "runtime"
    result_dir.mkdir(parents=True)
    (result_dir / "producer_runtime.json").write_text(
        json.dumps({"status": "PASS", "poll_results": [{"delivery_failures": [], "producer_flush_remaining": 0}]}),
        encoding="utf-8",
    )
    (result_dir / "producer_runtime.json").touch()
    monkeypatch.setattr(
        watchdog,
        "collect_container_states",
        lambda *args, **kwargs: {
            service: {"status": "running", "health": "healthy", "restart_count": 0}
            for service in watchdog.SERVICES
        },
    )

    report = watchdog.monitor_once(tmp_path, state_root, now=monitor_now, run_delta_audit=False)

    assert report["status"] == "PASS"
    assert report["restart_count"] == 4
    assert report["cohort_id"] == "prospective-t2h-fixed-t0"
    assert report["model_sha256"] == watchdog.MODEL_SHA256
    ops_dir = tmp_path / "results" / "prospective-live-t2h" / RUN_ID / "runtime" / "ops"
    saved = json.loads((ops_dir / "monitor.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert saved["restart_count"] == 4
    assert (ops_dir / "hourly_reports.jsonl").exists()
    assert (ops_dir / "daily_reports.jsonl").exists()
    assert not (ops_dir / "alerts.jsonl").exists()


def test_stale_cohort_heartbeat_is_infrastructure_recovery_not_data_rewrite(tmp_path, monkeypatch):
    monitor_now = datetime.now(timezone.utc)
    state_root = tmp_path / "data" / "runtime" / "prospective-live-t2h"
    run_state = state_root / RUN_ID
    run_state.mkdir(parents=True)
    (state_root / "active_run.json").write_text(json.dumps({"run_id": RUN_ID}), encoding="utf-8")
    request = _contract()
    (run_state / "start_request.json").write_text(json.dumps(request), encoding="utf-8")
    status = {
        "run_id": RUN_ID,
        "cohort_id": "prospective-t2h-fixed-t0",
        "status": "COLLECTING",
        "expected_slots": 10_584,
        "microbatch_update_count": 12,
        "last_update_at": (monitor_now - timedelta(hours=2)).isoformat(),
        "prospective_forecast_count": 126,
        "valid_evaluation_count": 63,
        "restart_count": 5,
        "reference_conflicts": 0,
        "reference_conflict_key_count": 0,
        "invalid_provenance": 0,
        "duplicate_validation": {"status": "PASS"},
        "provenance_validation": {"blocking_violations": []},
    }
    manifest = {**request, "cohort_id": "prospective-t2h-fixed-t0"}
    (run_state / "cohort_status.json").write_text(json.dumps(status), encoding="utf-8")
    (run_state / "cohort_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    result_dir = tmp_path / "results" / "prospective-live-t2h" / RUN_ID / "runtime"
    result_dir.mkdir(parents=True)
    (result_dir / "producer_runtime.json").write_text(
        json.dumps({"status": "PASS", "poll_results": [{"delivery_failures": [], "producer_flush_remaining": 0}]}),
        encoding="utf-8",
    )
    calls = []
    monkeypatch.setattr(
        watchdog,
        "collect_container_states",
        lambda *args, **kwargs: {
            service: {"status": "running", "health": "healthy", "restart_count": 0}
            for service in watchdog.SERVICES
        },
    )

    report = watchdog.monitor_once(
        tmp_path,
        state_root,
        now=monitor_now,
        runner=lambda command, **kwargs: calls.append(command) or subprocess.CompletedProcess(command, 0, "restarted", ""),
        run_delta_audit=False,
    )

    assert report["data_errors"] == []
    assert "COHORT_UPDATE_STALE_OVER_90_MINUTES" in report["infrastructure_errors"]
    assert [action["service"] for action in report["recovery_actions"]] == ["streaming-inference-t2h-live"]
    assert report["recovery_actions"][0]["operation"] == "restart"
    assert report["restart_count"] == 5
    assert len(calls) == 1
