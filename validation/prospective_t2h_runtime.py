from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Any, Mapping, Sequence

from validation.azure_compose import (
    AzureComposeConfigurationError,
    azure_compose_environment,
    is_azure_runtime,
    validate_azure_compose_configuration,
)


EXPECTED_LOCATIONS = 63
DEFAULT_BOOTSTRAP_HISTORY_HOURS = 48
MAX_INFRASTRUCTURE_ATTEMPTS = 3
HEALTH_WAIT_SECONDS = 300
HEALTH_POLL_SECONDS = 5
LEGACY_INPUT_TOPIC = "weather.hourly.observations.t2h.live.v1"
INFRASTRUCTURE_RETRY_MARKERS = (
    "cannot connect to the docker daemon",
    "error during connect",
    "connection refused",
    "connection reset",
    "context deadline exceeded",
    "timed out",
    "timeoutexpired",
    "temporary failure in name resolution",
    "network is unreachable",
    "no such host",
)


def cohort_runtime_configuration(
    run_id: str,
    *,
    history_hours: int = DEFAULT_BOOTSTRAP_HISTORY_HOURS,
) -> dict[str, Any]:
    """Return the immutable Kafka/cache identity and warmup plan for one run."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id) or run_id in {".", ".."}:
        raise ValueError("run_id must contain only letters, digits, dot, underscore, or hyphen")
    if history_hours < 24:
        raise ValueError("bootstrap history must cover the frozen 24-hour feature window")
    return {
        "input_topic": f"weather.hourly.observations.t2h.prospective.{run_id}.v1",
        "producer_cache_path": (
            f"/opt/project/results/prospective-live-t2h/{run_id}/runtime/producer_cache/published_hours.json"
        ),
        "producer_history_hours": int(history_hours),
        "bootstrap_required": True,
    }


def _compose_prefix() -> list[str]:
    return ["docker", "compose", "--profile", "t2h-live"]


def _compose_environment(
    repository_root: Path,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    return azure_compose_environment(
        repository_root,
        environ,
        required=is_azure_runtime(repository_root, environ),
    )


def _run(
    repository_root: Path,
    env: Mapping[str, str],
    arguments: Sequence[str],
    *,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    compose_env = _compose_environment(repository_root, env)
    return subprocess.run(
        [*_compose_prefix(), *arguments],
        cwd=repository_root,
        env=compose_env,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout,
    )


def _combined_result(
    args: Sequence[str],
    returncode: int,
    outputs: Sequence[subprocess.CompletedProcess[str]],
    extra_error: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        list(args),
        returncode,
        "\n".join(part.stdout for part in outputs if part.stdout),
        "\n".join([*(part.stderr for part in outputs if part.stderr), extra_error]).strip(),
    )


def _is_transient_infrastructure_error(completed: subprocess.CompletedProcess[str]) -> bool:
    message = f"{completed.stdout}\n{completed.stderr}".lower()
    return any(marker in message for marker in INFRASTRUCTURE_RETRY_MARKERS)


def _run_infrastructure_command(
    repository_root: Path,
    env: Mapping[str, str],
    arguments: Sequence[str],
    *,
    sleep=time.sleep,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    last: subprocess.CompletedProcess[str] | None = None
    for attempt in range(MAX_INFRASTRUCTURE_ATTEMPTS):
        try:
            last = _run(repository_root, env, arguments, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired) as exc:
            last = subprocess.CompletedProcess(list(arguments), 1, "", f"{type(exc).__name__}: {exc}")
        if last.returncode == 0 or not _is_transient_infrastructure_error(last):
            return last
        if attempt + 1 < MAX_INFRASTRUCTURE_ATTEMPTS:
            sleep(2**attempt)
    assert last is not None
    return last


def _wait_for_healthy_infrastructure(
    repository_root: Path,
    env: Mapping[str, str],
    *,
    timeout_seconds: int = HEALTH_WAIT_SECONDS,
    sleep=time.sleep,
) -> tuple[bool, str]:
    deadline = time.monotonic() + timeout_seconds
    last_details: list[str] = []
    spark_probe = "\n".join((
        "import json, urllib.request",
        "data = json.load(urllib.request.urlopen('http://127.0.0.1:8080/json/', timeout=5))",
        "assert data.get('url') == 'spark://spark-master:7077'",
        "assert any(worker.get('state') == 'ALIVE' for worker in data.get('workers', []))",
    ))
    while True:
        try:
            broker = _run(
                repository_root,
                env,
                ["exec", "-T", "broker", "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "broker:19092"],
                timeout=20,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            broker = subprocess.CompletedProcess(["broker-health"], 1, "", f"{type(exc).__name__}: {exc}")
        try:
            spark = _run(
                repository_root,
                env,
                ["exec", "-T", "spark-master", "/usr/local/bin/python3.12", "-c", spark_probe],
                timeout=20,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            spark = subprocess.CompletedProcess(["spark-health"], 1, "", f"{type(exc).__name__}: {exc}")
        if broker.returncode == 0 and spark.returncode == 0:
            return True, "Kafka broker, Spark master, and Spark worker are responsive"
        last_details = [
            f"Kafka probe: {broker.stderr[-1000:] or broker.stdout[-1000:]}",
            f"Spark probe: {spark.stderr[-1000:] or spark.stdout[-1000:]}",
        ]
        if time.monotonic() >= deadline:
            return False, "timed out waiting for healthy broker, Spark master, and Spark worker; " + "; ".join(last_details)
        sleep(HEALTH_POLL_SECONDS)


def _ensure_topic(
    repository_root: Path,
    env: Mapping[str, str],
    topic: str,
    *,
    sleep=time.sleep,
) -> subprocess.CompletedProcess[str]:
    return _run_infrastructure_command(
        repository_root,
        env,
        [
            "exec", "-T", "broker",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "broker:19092",
            "--create", "--if-not-exists", "--topic", topic,
            "--partitions", "3", "--replication-factor", "1",
        ],
        sleep=sleep,
        timeout=45,
    )


def _topic_record_count(
    repository_root: Path,
    env: Mapping[str, str],
    topic: str,
    *,
    sleep=time.sleep,
) -> tuple[int | None, str]:
    script = "\n".join(
        (
            "import json, os, sys",
            "from confluent_kafka import Consumer, TopicPartition",
            "topic = sys.argv[1]",
            "consumer = Consumer({'bootstrap.servers': os.getenv('WEATHER_LIVE_HOURLY_BOOTSTRAP_SERVERS', 'broker:19092'), 'group.id': 'prospective-topic-audit', 'enable.auto.commit': False})",
            "metadata = consumer.list_topics(topic=topic, timeout=10).topics.get(topic)",
            "if metadata is None or metadata.error is not None:",
            "    print(json.dumps({'record_count': None, 'error': 'topic metadata unavailable'}))",
            "else:",
            "    offsets = [consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=10, cached=False) for partition_id in metadata.partitions]",
            "    print(json.dumps({'record_count': sum(max(0, high - low) for low, high in offsets)}))",
            "consumer.close()",
        )
    )
    command = [
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "python",
        "live-hourly-producer-t2h",
        "-c",
        script,
        topic,
    ]
    completed = _run_infrastructure_command(
        repository_root,
        env,
        command,
        sleep=sleep,
        timeout=45,
    )
    if completed.returncode != 0:
        return None, completed.stderr[-4000:] or "Kafka topic offset inspection failed"
    for line in reversed(completed.stdout.splitlines()):
        try:
            result = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(result, dict) and isinstance(result.get("record_count"), int):
            return result["record_count"], ""
        if isinstance(result, dict) and result.get("error"):
            return None, str(result["error"])
    return None, "Kafka topic offset inspection returned no JSON record count"


def _bootstrap_errors(
    summary: Any,
    *,
    topic: str,
    history_hours: int,
) -> list[str]:
    errors: list[str] = []
    if not isinstance(summary, Mapping) or summary.get("status") != "PASS":
        return ["PRODUCER_BOOTSTRAP_SUMMARY_NOT_PASS"]
    if summary.get("topic") != topic:
        errors.append("PRODUCER_BOOTSTRAP_TOPIC_MISMATCH")
    if summary.get("catalog_location_count") != EXPECTED_LOCATIONS:
        errors.append("PRODUCER_BOOTSTRAP_LOCATION_COUNT_MISMATCH")
    if summary.get("history_hours_requested") != history_hours:
        errors.append("PRODUCER_BOOTSTRAP_HISTORY_HOURS_MISMATCH")
    polls = summary.get("poll_results")
    if not isinstance(polls, list) or len(polls) != 1 or not isinstance(polls[0], Mapping):
        return [*errors, "PRODUCER_BOOTSTRAP_POLL_SUMMARY_INVALID"]
    poll = polls[0]
    expected_hours = history_hours + 1
    expected_rows = EXPECTED_LOCATIONS * expected_hours
    if poll.get("successful_locations") != EXPECTED_LOCATIONS or poll.get("failed_locations"):
        errors.append("PRODUCER_BOOTSTRAP_LOCATION_FAILURE")
    if poll.get("bootstrap_observation_hours_requested") != expected_hours:
        errors.append("PRODUCER_BOOTSTRAP_WINDOW_SIZE_MISMATCH")
    if poll.get("bootstrap_observation_hours_accepted") != expected_hours:
        errors.append("PRODUCER_BOOTSTRAP_INCOMPLETE_HOURLY_HISTORY")
    if poll.get("events_built") != expected_rows or poll.get("unique_location_hour_keys") != expected_rows:
        errors.append("PRODUCER_BOOTSTRAP_EXPECTED_SLOT_COUNT_MISMATCH")
    if poll.get("events_delivered") != expected_rows or poll.get("events_enqueued") != expected_rows:
        errors.append("PRODUCER_BOOTSTRAP_KAFKA_DELIVERY_INCOMPLETE")
    if poll.get("delivery_failures") or poll.get("producer_flush_remaining") != 0:
        errors.append("PRODUCER_BOOTSTRAP_KAFKA_FLUSH_FAILED")
    if poll.get("history_gap_locations"):
        errors.append("PRODUCER_BOOTSTRAP_HISTORY_GAPS")
    if poll.get("cache_seeded") is not True:
        errors.append("PRODUCER_BOOTSTRAP_CACHE_NOT_SEEDED")
    try:
        future_rows_filtered = int(poll.get("future_provider_rows_filtered", 0) or 0)
    except (TypeError, ValueError):
        future_rows_filtered = -1
    if future_rows_filtered < 0:
        errors.append("PRODUCER_BOOTSTRAP_INVALID_FILTER_METRIC")
    return sorted(set(errors))


def _append_jsonl(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _run_bootstrap(
    repository_root: Path,
    state_root: Path,
    run_id: str,
    env: Mapping[str, str],
    runtime_configuration: Mapping[str, Any],
    *,
    sleep=time.sleep,
) -> tuple[subprocess.CompletedProcess[str], list[subprocess.CompletedProcess[str]]]:
    topic = str(runtime_configuration.get("input_topic") or "")
    history_hours = int(runtime_configuration.get("producer_history_hours", DEFAULT_BOOTSTRAP_HISTORY_HOURS))
    run_state = state_root / run_id
    marker_path = run_state / "bootstrap_status.json"
    marker = _read_json(marker_path)
    if isinstance(marker, Mapping) and marker.get("status") == "PASS" and marker.get("input_topic") == topic:
        record_count, count_error = _topic_record_count(repository_root, env, topic, sleep=sleep)
        if record_count is None or record_count <= 0:
            failure = {
                "status": "FAIL",
                "run_id": run_id,
                "input_topic": topic,
                "reason": "BOOTSTRAP_TOPIC_MISSING_AFTER_SUCCESS_RECEIPT",
                "topic_record_count": record_count,
                "topic_record_count_error": count_error or None,
                "prior_success_receipt": dict(marker),
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }
            _atomic_json(marker_path, failure)
            _append_jsonl(run_state / "bootstrap_attempts.jsonl", failure)
            failed = subprocess.CompletedProcess(["producer-bootstrap"], 1, "", failure["reason"])
            return failed, [failed]
        skipped = subprocess.CompletedProcess(["producer-bootstrap"], 0, "verified prior bootstrap PASS; reusing frozen topic", "")
        return skipped, [skipped]

    topic_ready = _ensure_topic(repository_root, env, topic, sleep=sleep)
    steps = [topic_ready]
    if topic_ready.returncode != 0:
        return topic_ready, steps
    record_count, count_error = _topic_record_count(repository_root, env, topic, sleep=sleep)
    if record_count is None:
        failed = subprocess.CompletedProcess(["kafka-topic-audit"], 1, "", count_error)
        _atomic_json(marker_path, {
            "status": "FAIL",
            "run_id": run_id,
            "input_topic": topic,
            "reason": "TOPIC_OFFSETS_UNVERIFIABLE",
            "detail": count_error,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        })
        _append_jsonl(run_state / "bootstrap_attempts.jsonl", _read_json(marker_path))
        return failed, [*steps, failed]
    if record_count != 0:
        detail = f"refusing to bootstrap non-empty run topic ({record_count} Kafka records) without a prior PASS receipt"
        failed = subprocess.CompletedProcess(["producer-bootstrap"], 1, "", detail)
        _atomic_json(marker_path, {
            "status": "FAIL",
            "run_id": run_id,
            "input_topic": topic,
            "reason": "BOOTSTRAP_TOPIC_NONEMPTY_WITHOUT_SUCCESS_EVIDENCE",
            "topic_record_count": record_count,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        })
        _append_jsonl(run_state / "bootstrap_attempts.jsonl", _read_json(marker_path))
        return failed, [*steps, failed]

    previous_attempts = int(marker.get("attempt_count", 0)) if isinstance(marker, Mapping) else 0
    if previous_attempts >= MAX_INFRASTRUCTURE_ATTEMPTS:
        failed = subprocess.CompletedProcess(["producer-bootstrap"], 1, "", "bootstrap attempt limit reached; operator review required")
        return failed, [*steps, failed]
    attempt = previous_attempts + 1
    attempt_path = repository_root / "results" / "prospective-live-t2h" / run_id / "runtime" / f"producer_bootstrap_attempt_{attempt}.json"
    container_attempt_path = f"/opt/project/results/prospective-live-t2h/{run_id}/runtime/producer_bootstrap_attempt_{attempt}.json"
    _atomic_json(marker_path, {
        "status": "RUNNING",
        "run_id": run_id,
        "input_topic": topic,
        "attempt_count": attempt,
        "started_at": datetime.now(timezone.utc).isoformat(),
    })
    command = [
        "run",
        "--rm",
        "--no-deps",
        "live-hourly-producer-t2h",
        "--mode",
        "bootstrap",
        "--history-hours",
        str(history_hours),
        "--summary-json",
        container_attempt_path,
    ]
    producer = _run(repository_root, env, command, timeout=900)
    steps.append(producer)
    summary = _read_json(attempt_path)
    errors = _bootstrap_errors(summary, topic=topic, history_hours=history_hours)
    if producer.returncode != 0 and not errors:
        errors.append("PRODUCER_BOOTSTRAP_PROCESS_FAILED")
    if errors:
        record_count_after, count_error_after = _topic_record_count(repository_root, env, topic, sleep=sleep)
        failure = {
            "status": "FAIL",
            "run_id": run_id,
            "input_topic": topic,
            "attempt_count": attempt,
            "reason": "PRODUCER_BOOTSTRAP_VALIDATION_FAILED",
            "errors": errors,
            "producer_summary_path": str(attempt_path),
            "topic_record_count_after_attempt": record_count_after,
            "topic_record_count_error": count_error_after or None,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        _atomic_json(marker_path, failure)
        _append_jsonl(run_state / "bootstrap_attempts.jsonl", failure)
        failure_result = subprocess.CompletedProcess(command, 1, producer.stdout, "; ".join(errors))
        return failure_result, steps

    passed = {
        "status": "PASS",
        "run_id": run_id,
        "input_topic": topic,
        "attempt_count": attempt,
        "history_hours": history_hours,
        "expected_history_rows": EXPECTED_LOCATIONS * (history_hours + 1),
        "producer_summary_path": str(attempt_path),
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    _atomic_json(marker_path, passed)
    _append_jsonl(run_state / "bootstrap_attempts.jsonl", passed)
    return producer, steps


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def run_prospective_compose(
    *,
    repository_root: Path,
    state_root: Path,
    run_id: str,
    runtime_configuration: Mapping[str, Any],
    services: Sequence[str] | None = None,
    bootstrap: bool = False,
    sleep=time.sleep,
) -> subprocess.CompletedProcess[str]:
    """Start/resume T2H services without reusing another run's topic or producer cache."""
    env = _compose_environment(repository_root)
    if is_azure_runtime(repository_root, env):
        try:
            env = validate_azure_compose_configuration(repository_root, env)
        except AzureComposeConfigurationError as exc:
            return subprocess.CompletedProcess(
                ["docker", "compose", "config", "--quiet"],
                2,
                "",
                str(exc),
            )
    env["WEATHER_INFERENCE_RUN_ID"] = run_id
    topic = runtime_configuration.get("input_topic")
    if isinstance(topic, str) and topic:
        env["WEATHER_PROSPECTIVE_INPUT_TOPIC"] = topic
    cache_path = runtime_configuration.get("producer_cache_path")
    if isinstance(cache_path, str) and cache_path:
        env["WEATHER_PROSPECTIVE_PRODUCER_CACHE_PATH"] = cache_path

    if not bootstrap:
        infrastructure = _run_infrastructure_command(
            repository_root,
            env,
            ["up", "-d", "broker", "spark-master", "spark-worker"],
            sleep=sleep,
        )
        steps = [infrastructure]
        if infrastructure.returncode != 0:
            return _combined_result(infrastructure.args, infrastructure.returncode, steps)
        healthy, health_detail = _wait_for_healthy_infrastructure(repository_root, env, sleep=sleep)
        if not healthy:
            failure = subprocess.CompletedProcess(["infrastructure-health"], 1, "", health_detail)
            return _combined_result(failure.args, 1, [*steps, failure])
        live = _run_infrastructure_command(
            repository_root,
            env,
            ["up", "-d", *(services or (
                "live-hourly-producer-t2h",
                "streaming-inference-t2h-live",
            ))],
            sleep=sleep,
        )
        return _combined_result(live.args, live.returncode, [*steps, live])

    steps: list[subprocess.CompletedProcess[str]] = []
    infrastructure = _run_infrastructure_command(
        repository_root,
        env,
        ["up", "-d", "broker", "spark-master", "spark-worker"],
        sleep=sleep,
    )
    steps.append(infrastructure)
    if infrastructure.returncode != 0:
        return _combined_result(infrastructure.args, infrastructure.returncode, steps)
    healthy, health_detail = _wait_for_healthy_infrastructure(repository_root, env, sleep=sleep)
    if not healthy:
        failure = subprocess.CompletedProcess(["infrastructure-health"], 1, "", health_detail)
        return _combined_result(failure.args, 1, [*steps, failure])

    bootstrap_result, bootstrap_steps = _run_bootstrap(
        repository_root,
        state_root,
        run_id,
        env,
        runtime_configuration,
        sleep=sleep,
    )
    steps.extend(bootstrap_steps)
    if bootstrap_result.returncode != 0:
        return _combined_result(bootstrap_result.args, bootstrap_result.returncode, steps)

    services_to_start = services or ("streaming-inference-t2h-live", "live-hourly-producer-t2h")
    live_start = _run_infrastructure_command(
        repository_root,
        env,
        ["up", "-d", *services_to_start],
        sleep=sleep,
    )
    steps.append(live_start)
    return _combined_result(live_start.args, live_start.returncode, steps)
