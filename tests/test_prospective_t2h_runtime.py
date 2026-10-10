from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

from validation import prospective_t2h_runtime as runtime


RUN_ID = "20261010T120000Z-prospective-live-t2h-v1"
TOPIC = f"weather.hourly.observations.t2h.prospective.{RUN_ID}.v1"


@pytest.fixture(autouse=True)
def _isolate_tmp_repository_tests_from_deployment_environment(monkeypatch):
    monkeypatch.delenv("WEATHER_AZURE_RUNTIME", raising=False)
    monkeypatch.delenv("COMPOSE_FILE", raising=False)


def bootstrap_summary(topic: str = TOPIC, *, hours: int = 49, cache_seeded: bool = True) -> dict:
    rows = 63 * hours
    return {
        "status": "PASS",
        "topic": topic,
        "catalog_location_count": 63,
        "history_hours_requested": hours - 1,
        "poll_results": [
            {
                "topic": topic,
                "successful_locations": 63,
                "failed_locations": {},
                "bootstrap_observation_hours_requested": hours,
                "bootstrap_observation_hours_accepted": hours,
                "events_built": rows,
                "unique_location_hour_keys": rows,
                "events_enqueued": rows,
                "events_delivered": rows,
                "delivery_failures": [],
                "producer_flush_remaining": 0,
                "history_gap_locations": [],
                "cache_seeded": cache_seeded,
                "future_provider_rows_filtered": 0,
            }
        ],
    }


def test_runtime_configuration_is_run_scoped_and_keeps_frozen_history_contract():
    config = runtime.cohort_runtime_configuration(RUN_ID)

    assert config["input_topic"] == TOPIC
    assert RUN_ID in config["producer_cache_path"]
    assert config["producer_history_hours"] == 48
    assert config["bootstrap_required"] is True


@pytest.mark.parametrize("run_id", ["../escape", "run/id", "", "."])
def test_runtime_configuration_rejects_unsafe_run_ids(run_id):
    with pytest.raises(ValueError, match="run_id"):
        runtime.cohort_runtime_configuration(run_id)


def test_bootstrap_validation_requires_complete_49_hour_history_and_cache_seed():
    passed = bootstrap_summary()
    assert runtime._bootstrap_errors(passed, topic=TOPIC, history_hours=48) == []

    missing_cache = bootstrap_summary(cache_seeded=False)
    assert "PRODUCER_BOOTSTRAP_CACHE_NOT_SEEDED" in runtime._bootstrap_errors(
        missing_cache,
        topic=TOPIC,
        history_hours=48,
    )

    incomplete = bootstrap_summary(hours=48)
    errors = runtime._bootstrap_errors(incomplete, topic=TOPIC, history_hours=48)
    assert "PRODUCER_BOOTSTRAP_INCOMPLETE_HOURLY_HISTORY" in errors
    assert "PRODUCER_BOOTSTRAP_EXPECTED_SLOT_COUNT_MISMATCH" in errors


def test_compose_resume_passes_the_frozen_topic_and_cache_path(monkeypatch, tmp_path):
    runtime_config = runtime.cohort_runtime_configuration(RUN_ID)
    calls = []

    def fake_run(repository_root, env, arguments, *, timeout=None):
        calls.append({"env": dict(env), "arguments": list(arguments)})
        return subprocess.CompletedProcess(list(arguments), 0, "started", "")

    monkeypatch.setattr(runtime, "_run", fake_run)
    monkeypatch.setattr(runtime, "_wait_for_healthy_infrastructure", lambda *args, **kwargs: (True, "healthy"))
    result = runtime.run_prospective_compose(
        repository_root=tmp_path,
        state_root=tmp_path / "state",
        run_id=RUN_ID,
        runtime_configuration=runtime_config,
        bootstrap=False,
    )

    assert result.returncode == 0
    assert calls[0]["env"]["WEATHER_INFERENCE_RUN_ID"] == RUN_ID
    assert calls[0]["env"]["WEATHER_PROSPECTIVE_INPUT_TOPIC"] == TOPIC
    assert calls[0]["env"]["WEATHER_PROSPECTIVE_PRODUCER_CACHE_PATH"] == runtime_config["producer_cache_path"]
    assert calls[-1]["arguments"] == [
        "up",
        "-d",
        "live-hourly-producer-t2h",
        "streaming-inference-t2h-live",
    ]


def test_infrastructure_retry_is_bounded_to_transient_errors(monkeypatch, tmp_path):
    replies = [
        subprocess.CompletedProcess([], 1, "", "Cannot connect to the Docker daemon"),
        subprocess.CompletedProcess([], 0, "up", ""),
    ]
    waits = []
    monkeypatch.setattr(runtime, "_run", lambda *args, **kwargs: replies.pop(0))

    result = runtime._run_infrastructure_command(tmp_path, {}, ["up", "-d"], sleep=waits.append)

    assert result.returncode == 0
    assert waits == [1]

    replies = [subprocess.CompletedProcess([], 1, "", "invalid Compose configuration")]
    waits.clear()
    monkeypatch.setattr(runtime, "_run", lambda *args, **kwargs: replies.pop(0))
    result = runtime._run_infrastructure_command(tmp_path, {}, ["up", "-d"], sleep=waits.append)
    assert result.returncode == 1
    assert waits == []


def test_infrastructure_health_gate_probes_existing_kafka_and_spark_services(monkeypatch, tmp_path):
    commands = []

    def fake_run(repository_root, env, arguments, *, timeout=None):
        commands.append((list(arguments), timeout))
        return subprocess.CompletedProcess(list(arguments), 0, "healthy", "")

    monkeypatch.setattr(runtime, "_run", fake_run)
    healthy, detail = runtime._wait_for_healthy_infrastructure(
        tmp_path,
        {},
        timeout_seconds=1,
        sleep=lambda _: None,
    )

    assert healthy is True
    assert "Kafka broker" in detail
    assert commands[0][0][:3] == ["exec", "-T", "broker"]
    assert commands[1][0][:3] == ["exec", "-T", "spark-master"]
    assert [item[1] for item in commands] == [20, 20]


def test_topic_record_count_uses_bounded_compose_runner(monkeypatch, tmp_path):
    captured = {}

    def fake_retry(repository_root, env, arguments, *, sleep, timeout=None):
        captured["arguments"] = list(arguments)
        captured["timeout"] = timeout
        return subprocess.CompletedProcess(list(arguments), 0, '{"record_count":42}\n', "")

    monkeypatch.setattr(runtime, "_run_infrastructure_command", fake_retry)
    count, error = runtime._topic_record_count(tmp_path, {}, TOPIC, sleep=lambda _: None)

    assert (count, error) == (42, "")
    assert captured["timeout"] == 45
    assert captured["arguments"][:4] == ["run", "--rm", "--no-deps", "--entrypoint"]
    assert captured["arguments"][-1] == TOPIC


def test_bootstrap_receipt_is_written_once_and_reused_without_republishing(monkeypatch, tmp_path):
    config = runtime.cohort_runtime_configuration(RUN_ID)
    state_root = tmp_path / "data" / "runtime" / "prospective-live-t2h"
    run_state = state_root / RUN_ID
    run_state.mkdir(parents=True)
    ensure_calls = []
    run_calls = []

    monkeypatch.setattr(
        runtime,
        "_ensure_topic",
        lambda *args, **kwargs: ensure_calls.append(args[-1]) or subprocess.CompletedProcess([], 0, "created", ""),
    )
    counts = iter(((0, ""), (100, "")))
    monkeypatch.setattr(runtime, "_topic_record_count", lambda *args, **kwargs: next(counts))

    def fake_run(repository_root, env, arguments, *, timeout=None):
        run_calls.append(list(arguments))
        summary_path = tmp_path / "results" / "prospective-live-t2h" / RUN_ID / "runtime" / "producer_bootstrap_attempt_1.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(bootstrap_summary()), encoding="utf-8")
        return subprocess.CompletedProcess(list(arguments), 0, "bootstrapped", "")

    monkeypatch.setattr(runtime, "_run", fake_run)
    first, _ = runtime._run_bootstrap(tmp_path, state_root, RUN_ID, {}, config)
    assert first.returncode == 0
    assert json.loads((run_state / "bootstrap_status.json").read_text(encoding="utf-8"))["status"] == "PASS"
    assert (run_state / "bootstrap_attempts.jsonl").exists()

    monkeypatch.setattr(
        runtime,
        "_ensure_topic",
        lambda *args: (_ for _ in ()).throw(AssertionError("successful bootstrap must not be repeated")),
    )
    second, _ = runtime._run_bootstrap(tmp_path, state_root, RUN_ID, {}, config)
    assert second.returncode == 0
    assert len(run_calls) == 1
    assert ensure_calls == [TOPIC]


def test_bootstrap_refuses_to_replay_a_topic_with_partial_records(monkeypatch, tmp_path):
    config = runtime.cohort_runtime_configuration(RUN_ID)
    state_root = tmp_path / "state"
    run_state = state_root / RUN_ID
    run_state.mkdir(parents=True)
    monkeypatch.setattr(
        runtime,
        "_ensure_topic",
        lambda *args, **kwargs: subprocess.CompletedProcess([], 0, "exists", ""),
    )
    monkeypatch.setattr(runtime, "_topic_record_count", lambda *args, **kwargs: (9, ""))
    monkeypatch.setattr(
        runtime,
        "_run",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("partial topic must not be republished")),
    )

    result, _ = runtime._run_bootstrap(tmp_path, state_root, RUN_ID, {}, config)

    assert result.returncode != 0
    marker = json.loads((run_state / "bootstrap_status.json").read_text(encoding="utf-8"))
    assert marker["reason"] == "BOOTSTRAP_TOPIC_NONEMPTY_WITHOUT_SUCCESS_EVIDENCE"
    assert marker["topic_record_count"] == 9
