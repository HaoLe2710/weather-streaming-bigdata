"""Run one isolated correctness benchmark from Windows, PowerShell, or CI."""

import argparse
import ast
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from urllib.request import urlopen
import uuid


REPO_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = REPO_ROOT / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from benchmark_config import (  # noqa: E402
    B0_CORRECTNESS,
    SCALABILITY_BENCHMARK,
    SCENARIO_SLUGS,
    THROUGHPUT_BASELINE,
    BenchmarkConfig,
    normalize_scenario,
    read_json,
    utc_now,
    write_json,
)
from performance_metrics import (  # noqa: E402
    DEFAULT_WARMUP_MIN_BATCHES,
    DEFAULT_WARMUP_SECONDS,
    analyze_run_artifacts,
    aggregate_repetitions,
    kafka_source_offsets,
    latest_spark_end_offsets,
    read_jsonl,
    resource_peaks,
    summarize_progress,
)
from scalability_metrics import (  # noqa: E402
    ScalabilityConfig,
    aggregate_scalability_runs,
    partition_distribution,
    scalability_resource_metrics,
    summarize_progress_percentiles,
    validate_runtime_allocation,
)
from telemetry import DockerResourceSampler, KafkaLagSampler  # noqa: E402


RESULTS_ROOT = Path("results") / "benchmarks"
RESULTS_CONTAINER_ROOT = "/opt/project/results/benchmarks"
DELTA_REQUIREMENTS = REPO_ROOT / "spark" / "requirements-benchmark.txt"
SIMULATOR = REPO_ROOT / "simulator" / "historical_stream_simulator.py"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
DELTA_SESSION_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def _run(command, *, capture_output=False, timeout=900):
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=capture_output,
        timeout=timeout,
    )


def _docker_compose(*args, capture_output=False, timeout=900):
    return _run(
        ["docker", "compose", *args],
        capture_output=capture_output,
        timeout=timeout,
    )


def _compose_exec_prefix() -> list[str]:
    """Prefer the Compose executable directly so Windows does not orphan its CLI plugin."""
    executable = shutil.which("docker-compose")
    return [executable] if executable else ["docker", "compose"]


def _scalability_generator_limit_reason(
    simulator_summary: dict | None,
    *,
    generator_rate_valid: bool,
) -> str | None:
    if simulator_summary is None:
        return None
    if simulator_summary.get("producer_delivery_complete") is not True:
        remaining = simulator_summary.get("producer_flush_remaining")
        detail = f"; queued messages remaining={remaining}" if remaining is not None else ""
        return f"Simulator could not flush every queued Kafka message{detail}."
    if not generator_rate_valid:
        return "This run's measured generator rate fell outside its calibrated +/-10% band."
    return None


def _cleanup_temporary_directory(path: Path, attempts: int = 4) -> str | None:
    delays = (0.1, 0.25, 0.5, 1.0)
    for attempt in range(attempts):
        try:
            shutil.rmtree(path)
            return None
        except FileNotFoundError:
            return None
        except OSError as exc:
            if attempt + 1 == attempts:
                return f"{type(exc).__name__}: {exc}"
            time.sleep(delays[min(attempt, len(delays) - 1)])
    return None


def _git_commit() -> str:
    return _run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
    ).stdout.strip()


def _require_clean_worktree() -> None:
    status = _run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        capture_output=True,
    ).stdout.strip()
    code_paths = [
        line for line in status.splitlines()
        if not line[3:].replace("\\", "/").startswith("results/benchmarks/")
    ]
    if code_paths:
        raise RuntimeError(
            "Run benchmarks from a clean committed tree so the manifest can "
            "identify the tested code. Commit or stash these paths first:\n"
            f"{chr(10).join(code_paths)}"
        )


def _require_services() -> None:
    output = _docker_compose(
        "ps",
        "--services",
        "--status",
        "running",
        capture_output=True,
    ).stdout
    running = {line.strip() for line in output.splitlines() if line.strip()}
    required = {"broker", "spark-master", "spark-worker"}
    missing = sorted(required - running)
    if missing:
        raise RuntimeError(
            "Required Docker Compose services are not running: "
            f"{', '.join(missing)}. Start them with: "
            "docker compose up --build -d broker spark-master spark-worker"
        )


def _compose_service_containers(service: str) -> list[str]:
    output = _docker_compose("ps", "-q", service, capture_output=True).stdout
    container_ids = [line.strip() for line in output.splitlines() if line.strip()]
    names = []
    for container_id in container_ids:
        name = _run(
            ["docker", "inspect", "--format", "{{.Name}}", container_id],
            capture_output=True,
            timeout=15,
        ).stdout.strip().lstrip("/")
        if name:
            names.append(name)
    return sorted(names)


def _compose_images() -> dict:
    result = _docker_compose(
        "config",
        "--format",
        "json",
        capture_output=True,
    )
    return json.loads(result.stdout)


def _runtime_versions() -> dict[str, str | None]:
    spark_output = _docker_compose(
        "exec",
        "-T",
        "spark-master",
        SPARK_SUBMIT,
        "--version",
        capture_output=True,
    )
    spark_text = (spark_output.stdout or "") + (spark_output.stderr or "")
    spark_match = re.search(r"\bversion\s+([0-9]+(?:\.[0-9]+)+)", spark_text)
    spark_version = spark_match.group(1) if spark_match else None

    images = _compose_images().get("services", {})
    kafka_image = images.get("broker", {}).get("image", "")
    kafka_version = kafka_image.rsplit(":", 1)[-1] if ":" in kafka_image else None

    delta_output = _docker_compose(
        "exec",
        "-T",
        "spark-master",
        "python3",
        "-c",
        (
            "import importlib.metadata as m; "
            "print(m.version('delta-spark'))"
        ),
        capture_output=True,
    )
    delta_version = delta_output.stdout.strip() or None
    missing = [
        name
        for name, value in (
            ("Spark", spark_version),
            ("Kafka", kafka_version),
            ("Delta Spark", delta_version),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Could not determine runtime version(s): " + ", ".join(missing)
        )
    return {
        "spark": spark_version,
        "kafka": kafka_version,
        "delta": delta_version,
    }


def _new_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{uuid.uuid4().hex[:8]}"


def _container_artifact_path(path: Path) -> str:
    relative = path.resolve().relative_to(REPO_ROOT.resolve())
    return "/opt/project/" + relative.as_posix()


def _ensure_topic(config: BenchmarkConfig) -> None:
    readiness_command = [
        "exec",
        "-T",
        "broker",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "broker:19092",
        "--list",
    ]
    deadline = time.monotonic() + 90
    last_error = None
    while time.monotonic() < deadline:
        try:
            _docker_compose(
                *readiness_command,
                capture_output=True,
                timeout=15,
            )
            break
        except subprocess.CalledProcessError as exc:
            last_error = (exc.stderr or exc.stdout or str(exc)).strip()
            time.sleep(2)
    else:
        raise RuntimeError(
            "Kafka broker did not become ready within 90 seconds. "
            f"Last error: {last_error or 'no response'}"
        )

    _docker_compose(
        "exec", "-T", "broker", "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "broker:19092", "--create",
        "--topic", config.topic,
        "--partitions", str(config.topic_partitions),
        "--replication-factor", "1",
    )
    described = _docker_compose(
        "exec",
        "-T",
        "broker",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "broker:19092",
        "--describe",
        "--topic",
        config.topic,
        capture_output=True,
    ).stdout
    if not re.search(
        rf"PartitionCount:\s*{config.topic_partitions}\b",
        described,
    ):
        raise RuntimeError(
            f"Kafka topic {config.topic} was not created with "
            f"{config.topic_partitions} partition(s):\n"
            f"{described}"
        )


def _spark_submit(
    script: Path,
    environment: dict[str, str],
    spark_version: str,
    *,
    fixed_throughput_baseline: bool = False,
) -> None:
    command = _spark_command(
        script,
        environment,
        spark_version,
        fixed_throughput_baseline=fixed_throughput_baseline,
    )
    print(f"[SPARK] Running {script.name}")
    _run(command)


def _spark_command(
    script: Path,
    environment: dict[str, str],
    spark_version: str,
    *,
    fixed_throughput_baseline: bool = False,
) -> list[str]:
    command = [*_compose_exec_prefix(), "exec", "-T"]
    for name, value in sorted(environment.items()):
        command.extend(["-e", f"{name}={value}"])
    command.extend([
        "spark-master",
        SPARK_SUBMIT,
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--conf",
        "spark.jars.ivy=/tmp/spark-ivy",
        "--packages",
        ",".join([
            _delta_coordinate(),
            f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}",
        ]),
        "--conf",
        f"spark.sql.extensions={DELTA_SESSION_EXTENSION}",
        "--conf",
        f"spark.sql.catalog.spark_catalog={DELTA_CATALOG}",
        "--conf",
        "spark.sql.session.timeZone=UTC",
        "--conf",
        "spark.driver.host=spark-master",
        "--conf",
        "spark.driver.bindAddress=0.0.0.0",
    ])
    if environment.get("BENCHMARK_SCENARIO") == SCALABILITY_BENCHMARK:
        memory_mb = int(environment.get("SPARK_EXECUTOR_MEMORY_MB", "1024"))
        command.extend([
            "--conf", f"spark.cores.max={environment.get('SPARK_CORES_MAX', '1')}",
            "--conf", f"spark.executor.cores={environment.get('SPARK_EXECUTOR_CORES', '1')}",
            "--conf", f"spark.executor.memory={memory_mb}m",
            "--conf", f"spark.sql.shuffle.partitions={environment.get('SPARK_SQL_SHUFFLE_PARTITIONS', '1')}",
        ])
    elif fixed_throughput_baseline:
        command.extend([
            "--conf", "spark.cores.max=1",
            "--conf", "spark.executor.cores=1",
            "--conf", "spark.executor.memory=1g",
            "--conf", "spark.sql.shuffle.partitions=1",
        ])
    command.append(_container_artifact_path(script))
    return command


def _start_spark_stream(
    script: Path,
    environment: dict[str, str],
    spark_version: str,
    log_path: Path,
) -> subprocess.Popen:
    command = _spark_command(
        script,
        environment,
        spark_version,
        fixed_throughput_baseline=True,
    )
    output = log_path.open("w", encoding="utf-8")
    try:
        process = subprocess.Popen(
            command,
            cwd=REPO_ROOT,
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
        )
    finally:
        output.close()
    return process


def _tail_text(path: Path, line_count: int = 80) -> str:
    if not path.exists():
        return "(no Spark log was written)"
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-line_count:])


def _wait_for_query_start(
    progress_paths: dict[str, Path],
    processes: dict[str, subprocess.Popen],
    log_paths: dict[str, Path],
    timeout_seconds: float = 180.0,
) -> dict[str, dict]:
    deadline = time.monotonic() + timeout_seconds
    expected = set(progress_paths)
    started: dict[str, dict] = {}
    while time.monotonic() < deadline:
        for stage, path in progress_paths.items():
            for row in read_jsonl(path):
                if row.get("event_type") == "query_started":
                    started[stage] = row
                    break
        if expected <= started.keys():
            return started
        for stage, process in processes.items():
            return_code = process.poll()
            if return_code is not None:
                raise RuntimeError(
                    f"Spark {stage} stream exited before query start (code {return_code}).\n"
                    f"{_tail_text(log_paths[stage])}"
                )
        time.sleep(0.25)
    missing = sorted(expected - started.keys())
    details = "\n\n".join(
        f"--- {stage} ---\n{_tail_text(log_paths[stage])}"
        for stage in missing
    )
    raise TimeoutError(
        f"Spark query start event missing for: {', '.join(missing)}.\n{details}"
    )


def _spark_worker_executor_snapshot() -> list[dict]:
    code = (
        "import json,urllib.request;"
        "m=json.load(urllib.request.urlopen('http://spark-master:8080/json/',timeout=3));"
        "print(json.dumps(["
        "{'id':p.get('id'),'executors':["
        "{'id':e.get('id'),'app_id':e.get('appid'),'cores':e.get('cores'),"
        "'memory_mb':e.get('memory')} for e in p.get('executors',[])]} "
        "for w in m.get('workers',[]) if w.get('state')=='ALIVE' "
        "for p in [json.load(urllib.request.urlopen(w['webuiaddress']+'/json/',timeout=3))]"
        "]))"
    )
    completed = _docker_compose(
        "exec", "-T", "spark-master", "python3", "-c", code,
        capture_output=True,
        timeout=15,
    )
    result = json.loads(completed.stdout)
    if not isinstance(result, list):
        raise ValueError("Spark worker UI did not return a worker list.")
    return result


def _spark_active_apps(run_id: str, *, include_executors: bool = False) -> dict:
    try:
        with urlopen("http://localhost:8080/json/", timeout=3) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
    apps = []
    all_apps = payload.get("activeapps", [])
    for app in all_apps:
        if run_id not in str(app.get("name", "")):
            continue
        apps.append({
            "id": app.get("id"),
            "name": app.get("name"),
            "cores": app.get("cores"),
            "memory_per_executor_mb": app.get("memoryperexecutor"),
            "state": app.get("state"),
        })
    workers = [
        {
            "id": worker.get("id"),
            "host": worker.get("host"),
            "webuiaddress": worker.get("webuiaddress"),
            "state": worker.get("state"),
            "cores_available": worker.get("cores"),
            "cores_used": worker.get("coresused"),
            "memory_available_mb": worker.get("memory"),
            "memory_used_mb": worker.get("memoryused"),
        }
        for worker in payload.get("workers", [])
    ]
    other_apps = [
        {
            "id": app.get("id"),
            "name": app.get("name"),
            "cores": app.get("cores"),
            "state": app.get("state"),
        }
        for app in all_apps
        if run_id not in str(app.get("name", ""))
    ]
    result = {"apps": apps, "workers": workers, "other_active_apps": other_apps}
    if include_executors:
        try:
            worker_executor_rows = _spark_worker_executor_snapshot()
            app_names = {
                app.get("id"): app.get("name")
                for app in all_apps
                if app.get("id") and app.get("name")
            }
            executor_by_app: dict[str, list[dict]] = {}
            executor_by_worker: dict[str, list[dict]] = {}
            for worker_row in worker_executor_rows:
                worker_id = str(worker_row.get("id"))
                worker_executors = []
                for executor in worker_row.get("executors", []):
                    app_id = executor.get("app_id")
                    details = {
                        "executor_id": executor.get("id"),
                        "app_id": app_id,
                        "application": app_names.get(app_id),
                        "worker_id": worker_id,
                        "cores": executor.get("cores"),
                        "memory_mb": executor.get("memory_mb"),
                    }
                    worker_executors.append(details)
                    if details["application"]:
                        executor_by_app.setdefault(details["application"], []).append(details)
                executor_by_worker[worker_id] = worker_executors
            for worker in result["workers"]:
                worker["executors"] = executor_by_worker.get(str(worker.get("id")), [])
            for app in result["apps"]:
                executors = executor_by_app.get(str(app.get("name")), [])
                app["executors"] = executors
                master_cores = app.get("cores")
                observed_executor_cores = [
                    int(executor["cores"])
                    for executor in executors
                    if isinstance(executor.get("cores"), (int, float))
                    and not isinstance(executor.get("cores"), bool)
                ]
                if isinstance(master_cores, (int, float)) and not isinstance(master_cores, bool):
                    app["actual_allocated_cores"] = int(master_cores)
                    app["actual_allocated_cores_source"] = "spark_master_active_apps.cores"
                elif observed_executor_cores:
                    app["actual_allocated_cores"] = sum(observed_executor_cores)
                    app["actual_allocated_cores_source"] = "worker_executor_cores"
                else:
                    app["actual_allocated_cores"] = None
                    app["actual_allocated_cores_source"] = None
                app["actual_executor_count"] = len(executors)
                app["actual_executor_cores"] = [executor.get("cores") for executor in executors]
                app["actual_executor_memory_mb"] = [executor.get("memory_mb") for executor in executors]
                app["assigned_worker_ids"] = sorted({executor["worker_id"] for executor in executors})
        except Exception as exc:
            result["executor_error"] = f"{type(exc).__name__}: {exc}"
    return result


class SparkClusterSampler:
    def __init__(self, run_id: str, *, interval_seconds: float = 10.0):
        self.run_id = run_id
        self.interval_seconds = interval_seconds
        self.samples: list[dict] = []
        self.last_error: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            name="spark-cluster-sampler",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=15)

    def sample_now(self, *, phase: str = "during_replay") -> None:
        captured = utc_now()
        try:
            observation = _spark_active_apps(self.run_id, include_executors=True)
            self.samples.append({
                "captured_at_utc": captured,
                "phase": phase,
                "observation": observation,
            })
            if observation.get("executor_error"):
                self.last_error = observation["executor_error"]
            elif observation.get("error"):
                self.last_error = observation["error"]
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.samples.append({"captured_at_utc": captured, "error": self.last_error})

    def _run(self) -> None:
        while not self._stop.is_set():
            self.sample_now()
            self._stop.wait(self.interval_seconds)


def _wait_for_scalability_allocation(
    *,
    run_id: str,
    config: ScalabilityConfig,
    timeout_seconds: float = 90,
) -> tuple[dict, dict]:
    deadline = time.monotonic() + timeout_seconds
    last_observation: dict = {}
    last_validation: dict = {"passed": False, "errors": ["Spark allocation was not observed."]}
    while time.monotonic() < deadline:
        last_observation = _spark_active_apps(run_id, include_executors=True)
        last_validation = validate_runtime_allocation(
            last_observation,
            run_id=run_id,
            config=config,
        )
        if last_validation["passed"]:
            return last_observation, last_validation
        time.sleep(1)
    return last_observation, last_validation


def _progress_input_counts(progress_paths: dict[str, Path]) -> dict[str, int]:
    events = []
    for path in progress_paths.values():
        events.extend(read_jsonl(path))
    totals = summarize_progress(events)
    return {
        "bronze": totals["bronze_input_records"],
        "silver": totals["silver_input_records"],
    }


def _wait_for_stream_drain(
    *,
    config: BenchmarkConfig,
    simulator_summary: dict,
    progress_paths: dict[str, Path],
    sampler: KafkaLagSampler,
    processes: dict[str, subprocess.Popen],
    timeout_seconds: float,
) -> tuple[bool, str | None, dict[str, int]]:
    produced = int(simulator_summary.get("kafka_messages", 0))
    deadline = time.monotonic() + timeout_seconds
    stable_polls = 0
    while time.monotonic() < deadline:
        for stage, process in processes.items():
            return_code = process.poll()
            if return_code is not None:
                return False, f"Spark {stage} stream exited with code {return_code} before drain.", _progress_input_counts(progress_paths)
        counts = _progress_input_counts(progress_paths)
        current_lag = sampler.latest_sample()
        lag = current_lag.get("lag_records") if current_lag else None
        if counts["bronze"] >= produced and counts["silver"] >= produced and lag == 0:
            stable_polls += 1
            if stable_polls >= 2:
                return True, None, counts
        else:
            stable_polls = 0
        time.sleep(0.5)
    return False, f"Drain timeout after {timeout_seconds:g} seconds.", _progress_input_counts(progress_paths)


def _stop_stream_processes(
    processes: dict[str, subprocess.Popen],
    stop_signal: Path,
    *,
    run_id: str,
    timeout_seconds: float = 90.0,
) -> dict[str, int | None]:
    stop_signal.parent.mkdir(parents=True, exist_ok=True)
    stop_signal.touch(exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    next_app_poll = 0.0
    observation: dict = {}
    while True:
        now = time.monotonic()
        if now >= next_app_poll:
            observation = _spark_active_apps(run_id)
            next_app_poll = now + 1.0
        launchers_running = any(
            process.poll() is None for process in processes.values()
        )
        apps_known_stopped = (
            not observation.get("error") and not observation.get("apps")
        )
        if not launchers_running and apps_known_stopped:
            break
        if now >= deadline:
            break
        time.sleep(min(0.25, max(0.0, deadline - now)))

    for process in processes.values():
        if process.poll() is None:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=15,
                )
            else:
                process.terminate()
    return_codes = {}
    for stage, process in processes.items():
        try:
            return_codes[stage] = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=15,
                )
            else:
                process.kill()
            return_codes[stage] = process.wait(timeout=15)

    verify_deadline = time.monotonic() + 5.0
    while True:
        observation = _spark_active_apps(run_id)
        if not observation.get("error") and not observation.get("apps"):
            return return_codes
        if time.monotonic() >= verify_deadline:
            details = observation.get("error") or json.dumps(
                observation.get("apps", []), sort_keys=True
            )
            raise RuntimeError(
                "Spark applications for this run remained active after the stop signal: "
                + details
            )
        time.sleep(0.25)


def _merge_progress_files(
    progress_paths: dict[str, Path],
    output_path: Path,
) -> list[dict]:
    rows = []
    for path in progress_paths.values():
        rows.extend(read_jsonl(path))
    rows.sort(key=lambda row: str(row.get("captured_at_utc", "")))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as output:
        for row in rows:
            output.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    for path in progress_paths.values():
        path.unlink(missing_ok=True)
    return rows


def _spark_offset_summary(rows: list[dict], topic: str) -> dict:
    progress_rows = [
        row.get("progress")
        for row in rows
        if row.get("stage") == "bronze"
        and row.get("event_type") == "progress"
        and isinstance(row.get("progress"), dict)
    ]
    progress_rows.sort(key=lambda progress: int(progress.get("batchId", -1)))
    first_offsets = {}
    for progress in progress_rows:
        first_offsets = kafka_source_offsets(progress, topic, "startOffset")
        if first_offsets:
            break
    return {
        "spark_start_offsets": first_offsets,
        "spark_end_offsets": latest_spark_end_offsets(rows, topic),
    }


def _find_calibration(rate: int, git_commit: str) -> tuple[dict, Path]:
    candidates = []
    for namespace in ("throughput", "scalability"):
        experiments_root = REPO_ROOT / RESULTS_ROOT / namespace / "experiments"
        if experiments_root.exists():
            candidates.extend(experiments_root.glob("*/summary.json"))
    candidates.sort(key=lambda path: path.parent.name, reverse=True)
    for path in candidates:
        try:
            summary = read_json(path)
        except (OSError, ValueError):
            continue
        if summary.get("experiment_type") != "load_generator_calibration":
            continue
        calibration_commit = str(summary.get("git_commit") or "")
        compatible, compatibility_reason = _calibration_source_compatibility(
            calibration_commit,
            git_commit,
            summary.get("calibration_source_signature"),
        )
        if not compatible:
            continue
        for item in summary.get("rates", []):
            if int(item.get("requested_rate_msgs_sec", -1)) == rate:
                if not item.get("calibrated"):
                    raise RuntimeError(
                        f"Rate {rate} msg/s did not pass generator calibration: "
                        f"actual={item.get('actual_generated_msgs_sec')}, "
                        f"deviation={item.get('deviation_percent')}%. "
                        "Do not use this rate as a pipeline workload."
                    )
                selected = dict(item)
                selected["calibration_git_commit"] = calibration_commit
                selected["calibration_reused_from_previous_commit"] = calibration_commit != git_commit
                selected["calibration_compatibility_reason"] = compatibility_reason
                return selected, path
    raise RuntimeError(
        f"No compatible generator calibration for {rate} msg/s is available at "
        f"Git commit {git_commit}; simulator or workload configuration changed. "
        "Run `python benchmark/run_benchmark.py --scenario scalability "
        "--calibrate-load-generator --calibration-rates " + str(rate) + " first."
    )


def _simulator_command_ast(source: str) -> str:
    tree = ast.parse(source)
    node = next(
        (
            item for item in tree.body
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
            and item.name == "_simulator_command"
        ),
        None,
    )
    if node is None:
        raise ValueError("Could not locate _simulator_command in benchmark runner source.")
    return ast.dump(node, include_attributes=False)


def _calibration_source_signature() -> str:
    runner_source = Path(__file__).read_text(encoding="utf-8")
    calibration_inputs = [
        SIMULATOR.read_bytes(),
        (JOBS_DIR / "benchmark_config.py").read_bytes(),
        _simulator_command_ast(runner_source).encode("utf-8"),
    ]
    digest = hashlib.sha256()
    for content in calibration_inputs:
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _calibration_source_compatibility(
    calibration_commit: str,
    current_commit: str,
    stored_signature: str | None,
) -> tuple[bool, str]:
    if not calibration_commit or not current_commit:
        return False, "Calibration or current Git commit is missing."
    if calibration_commit == current_commit:
        if stored_signature and stored_signature != _calibration_source_signature():
            return False, "Stored calibration fingerprint does not match current source."
        return True, "Calibration and benchmark use the same Git commit."

    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", calibration_commit, current_commit],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if ancestor.returncode != 0:
        return False, "Calibration commit is not an ancestor of the current commit."

    if stored_signature:
        if stored_signature != _calibration_source_signature():
            return False, "Simulator workload fingerprint changed after calibration."
        return True, "Simulator workload fingerprint matches the saved calibration."

    changed_inputs = subprocess.run(
        [
            "git", "diff", "--quiet", f"{calibration_commit}..{current_commit}",
            "--", "simulator/historical_stream_simulator.py",
            "spark/jobs/benchmark_config.py",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if changed_inputs.returncode != 0:
        return False, "Simulator or benchmark workload configuration changed after calibration."

    try:
        prior_runner = subprocess.run(
            ["git", "show", f"{calibration_commit}:benchmark/run_benchmark.py"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        current_runner = Path(__file__).read_text(encoding="utf-8")
        if _simulator_command_ast(prior_runner) != _simulator_command_ast(current_runner):
            return False, "The simulator command changed after calibration."
    except (OSError, subprocess.CalledProcessError, ValueError, SyntaxError):
        return False, "Could not verify the simulator command at the calibration commit."
    return True, "Simulator, workload configuration, and simulator command are unchanged."


def run_load_generator_calibration(args) -> int:
    if args.max_source_events not in (None, 10_000):
        raise ValueError("Load calibration uses a fixed 10,000 source events per target rate.")
    _require_clean_worktree()
    _require_services()
    versions = _runtime_versions()
    git_commit = _git_commit()
    experiment_id = _new_run_id()
    scenario = normalize_scenario(args.scenario)
    if scenario not in {THROUGHPUT_BASELINE, SCALABILITY_BENCHMARK}:
        raise ValueError("Load calibration requires throughput or scalability scenario.")
    experiment_dir = REPO_ROOT / RESULTS_ROOT / SCENARIO_SLUGS[scenario] / "experiments" / experiment_id
    experiment_dir.mkdir(parents=True, exist_ok=False)
    source = Path(args.source).resolve()
    if not source.is_dir():
        raise FileNotFoundError(f"Historical source directory not found: {source}")
    rates = []
    tolerance = 0.10
    requested_rates = getattr(args, "calibration_rates", None) or (100, 500, 1_000, 2_000, 5_000)
    if not requested_rates or any(rate <= 0 or rate > 10_000 for rate in requested_rates):
        raise ValueError("Calibration rates must be positive and no greater than 10,000 msg/s.")

    for requested_rate in requested_rates:
        run_id = _new_run_id()
        config = BenchmarkConfig.for_scenario(
            scenario,
            run_id,
            source_record_limit=10_000,
            requested_replay_rate=requested_rate,
            seed=args.seed,
            experiment_id=experiment_id if scenario == SCALABILITY_BENCHMARK else None,
        )
        run_dir = REPO_ROOT / RESULTS_ROOT / config.slug / config.run_id
        run_dir.mkdir(parents=True, exist_ok=False)
        artifacts = _artifact_paths(config, run_dir)
        manifest = _make_manifest(
            config,
            git_commit=git_commit,
            source=source,
            artifact_paths=artifacts,
            versions=versions,
        )
        manifest["measurement_mode"] = "load_generator_calibration"
        infrastructure_key = "scalability" if scenario == SCALABILITY_BENCHMARK else "throughput_baseline"
        manifest[infrastructure_key]["spark_pipeline_executed"] = False
        manifest[infrastructure_key]["calibration_tolerance_fraction"] = tolerance
        manifest["artifacts"].pop("spark_progress", None)
        manifest["artifacts"].pop("resource_metrics", None)
        manifest["artifacts"].pop("kafka_lag", None)
        manifest["artifacts"].pop("delta_metrics", None)
        manifest["artifacts"].pop("spark_cluster_snapshot", None)
        write_json(artifacts["manifest"], manifest)
        print(f"[CALIBRATION] requested={requested_rate} msg/s run_id={run_id}")

        try:
            _ensure_topic(config)
            _run(
                _simulator_command(config, source, artifacts["simulator"]),
                timeout=600,
            )
            simulator = read_json(artifacts["simulator"])
            simulator["git_commit"] = git_commit
            write_json(artifacts["simulator"], simulator)
            actual = simulator.get("actual_generated_msgs_sec")
            deviation = (
                abs(float(actual) - requested_rate) / requested_rate
                if actual is not None and requested_rate > 0
                else None
            )
            calibrated = bool(
                simulator.get("producer_delivery_complete")
                and deviation is not None
                and deviation <= tolerance
            )
            result = {
                "scenario": scenario,
                "run_id": run_id,
                "git_commit": git_commit,
                "requested_rate_msgs_sec": requested_rate,
                "actual_generated_msgs_sec": actual,
                "actual_generation_rate_including_flush": simulator.get("actual_generation_rate"),
                "deviation_percent": deviation * 100 if deviation is not None else None,
                "calibration_tolerance_percent": tolerance * 100,
                "calibrated": calibrated,
                "source_records": simulator.get("source_records"),
                "produced_messages": simulator.get("kafka_messages"),
                "producer_elapsed_seconds": simulator.get("producer_elapsed_seconds"),
                "generation_elapsed_seconds": simulator.get("generation_elapsed_seconds"),
                "producer_flush_remaining": simulator.get("producer_flush_remaining"),
                "system_throughput_measured": False,
            }
            write_json(artifacts["result"], result)
            manifest["status"] = "CALIBRATED" if calibrated else "UNSUPPORTED_RATE"
            manifest["finished_at"] = utc_now()
            manifest["result_metrics"] = result
            write_json(artifacts["manifest"], manifest)
            rates.append(result)
            actual_text = f"{actual:.2f}" if actual is not None else "n/a"
            deviation_text = (
                f"{result['deviation_percent']:.2f}%"
                if result["deviation_percent"] is not None
                else "n/a"
            )
            print(
                f"[CALIBRATION] actual={actual_text} msg/s "
                f"deviation={deviation_text} supported={calibrated}"
            )
        except Exception as exc:
            manifest["status"] = "FAILED"
            manifest["finished_at"] = utc_now()
            manifest["error"] = {"type": type(exc).__name__, "message": str(exc)}
            write_json(artifacts["manifest"], manifest)
            raise

    summary = {
        "schema_version": 2,
        "experiment_type": "load_generator_calibration",
        "experiment_id": experiment_id,
        "created_at": utc_now(),
        "git_commit": git_commit,
        "calibration_source_signature": _calibration_source_signature(),
        "calibration_source_files": [
            "simulator/historical_stream_simulator.py",
            "spark/jobs/benchmark_config.py",
            "benchmark/run_benchmark.py::_simulator_command",
        ],
        "scenario": scenario,
        "source_record_limit": 10_000,
        "fault_rates": {
            "duplicate": 0,
            "invalid": 0,
            "late": 0,
            "out_of_order": 0,
        },
        "tolerance_fraction": tolerance,
        "tolerance_reason": (
            "A 10% band accepts short-run rate-limiter and producer-flush jitter "
            "while rejecting a materially different offered workload."
        ),
        "rates": rates,
    }
    summary_path = experiment_dir / "summary.json"
    write_json(summary_path, summary)
    print(f"[CALIBRATION] summary={summary_path.relative_to(REPO_ROOT)}")
    return 0


def _run_throughput_iteration(
    *,
    config: BenchmarkConfig,
    source: Path,
    versions: dict[str, str | None],
    git_commit: str,
    calibration: dict,
    calibration_path: Path,
    warmup_seconds: float,
    warmup_min_batches: int,
    drain_timeout_seconds: float,
) -> dict:
    run_dir = REPO_ROOT / RESULTS_ROOT / config.slug / config.run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    artifacts = _artifact_paths(config, run_dir)
    is_scalability = config.scenario == SCALABILITY_BENCHMARK
    infrastructure_key = "scalability" if is_scalability else "throughput_baseline"
    manifest = _make_manifest(
        config,
        git_commit=git_commit,
        source=source,
        artifact_paths=artifacts,
        versions=versions,
    )
    manifest["measurement_mode"] = (
        "one_factor_at_a_time_scalability"
        if is_scalability
        else "fixed_configuration_throughput"
    )
    manifest["load_generator_calibration"] = {
        "summary": calibration_path.relative_to(REPO_ROOT).as_posix(),
        "calibrated_rate": calibration.get("requested_rate_msgs_sec"),
        "calibration_actual_rate_msgs_sec": calibration.get("actual_generated_msgs_sec"),
        "tolerance_percent": calibration.get("calibration_tolerance_percent"),
        "calibration_git_commit": calibration.get("calibration_git_commit"),
        "reused_from_previous_commit": calibration.get(
            "calibration_reused_from_previous_commit", False
        ),
        "compatibility_reason": calibration.get("calibration_compatibility_reason"),
    }
    manifest["warmup_policy"] = {
        "name": "elapsed_time_and_completed_batches",
        "minimum_warmup_seconds": warmup_seconds,
        "minimum_completed_batches_per_query": warmup_min_batches,
    }
    manifest["saturation_definition"] = {
        "sustainable": "all expected records reach Bronze and Silver, final Kafka-to-Bronze source lag is zero, and both stream applications exit normally",
        "under_capacity_lag_slope_max_records_per_second": (
            "max(10, 2% of actual offered rate)"
        ),
        "under_capacity_pipeline_rate_min_fraction_of_actual": 0.95,
        "under_capacity_drain_max_seconds": "max(15, 25% of replay duration)",
        "saturated_lag_slope_min_fraction_of_actual": 0.10,
        "saturated_pipeline_rate_max_fraction_of_actual": 0.90,
        "startup_peak_lag_is_not_a_capacity_threshold": True,
        "drain_timeout_seconds": drain_timeout_seconds,
        "classes": ["UNDER_CAPACITY", "NEAR_CAPACITY", "SATURATED", "FAILED"],
    }
    write_json(artifacts["manifest"], manifest)

    progress_paths = {
        "bronze": artifacts["spark_progress_bronze_temp"],
        "silver": artifacts["spark_progress_silver_temp"],
    }
    stop_signal = run_dir / "stop.signal"
    processes: dict[str, subprocess.Popen] = {}
    process_return_codes: dict[str, int | None] = {}
    resource_sampler = None
    lag_sampler = None
    cluster_sampler = None
    simulator_summary = None
    runtime_observation = None
    runtime_validation = None
    runtime_allocation_invalid = False
    failure_reason = None
    failure_kind = None
    stream_shutdown_confirmed = False
    spark_shutdown_observation = None
    drain_complete = False
    drain_seconds = None
    stream_started_monotonic = None
    stream_started_at_utc = None
    run_finished_monotonic = None
    delta_metrics = None
    metrics_error = None
    log_tails = {}

    temporary_log_dir = None
    with tempfile.TemporaryDirectory(
        prefix="weather-throughput-", ignore_cleanup_errors=True
    ) as temp_dir:
        temp_root = Path(temp_dir)
        temporary_log_dir = temp_root
        log_paths = {
            "bronze": temp_root / "spark-bronze.log",
            "silver": temp_root / "spark-silver.log",
        }
        try:
            _ensure_topic(config)
            initializer_environment = config.spark_environment("check_throughput")
            initializer_environment["APP_NAME"] = f"InitThroughputBronze-{config.run_id}"
            _spark_submit(
                JOBS_DIR / "initialize_throughput_delta.py",
                initializer_environment,
                versions["spark"],
                fixed_throughput_baseline=True,
            )
            infrastructure = manifest[infrastructure_key]
            infrastructure["bronze_delta_initialized_empty_before_streaming"] = True
            write_json(artifacts["manifest"], manifest)
            container_limits = infrastructure["docker_container_limits"]
            worker_containers = _compose_service_containers("spark-worker")
            if not worker_containers:
                worker_containers = ["weather-spark-worker"]
            resource_sampler = DockerResourceSampler(
                artifacts["resource_metrics"],
                interval_seconds=1.0,
                container_limits=container_limits,
                host_logical_cpu_count=infrastructure.get(
                    "host_logical_cpu_count"
                ),
                worker_containers=worker_containers,
            )
            lag_sampler = KafkaLagSampler(
                artifacts["kafka_lag"],
                progress_paths["bronze"],
                bootstrap_servers="localhost:9092",
                topic=config.topic,
                partitions=config.topic_partitions,
                run_id=config.run_id,
                interval_seconds=1.0,
            )
            resource_sampler.start()
            lag_sampler.start()

            processes["bronze"] = _start_spark_stream(
                JOBS_DIR / "bronze_weather_stream.py",
                config.spark_environment("bronze"),
                versions["spark"],
                log_paths["bronze"],
            )
            processes["silver"] = _start_spark_stream(
                JOBS_DIR / "silver_weather_stream.py",
                config.spark_environment("silver"),
                versions["spark"],
                log_paths["silver"],
            )
            _wait_for_query_start(progress_paths, processes, log_paths)
            if is_scalability:
                requested_config = ScalabilityConfig(
                    partitions=config.topic_partitions,
                    bronze_cores=config.bronze_cores_max,
                    silver_cores=config.silver_cores_max,
                    workers=config.requested_worker_count,
                    worker_cores_each=config.worker_cores_each,
                    worker_memory_mb_each=config.worker_memory_mb_each,
                    executor_cores=config.executor_cores,
                    executor_memory_mb=config.executor_memory_mb,
                    shuffle_partitions=config.shuffle_partitions,
                    trigger_interval=config.trigger_interval,
                ).validate()
                runtime_observation, runtime_validation = _wait_for_scalability_allocation(
                    run_id=config.run_id,
                    config=requested_config,
                )
                runtime_allocation_invalid = not runtime_validation["passed"]
                manifest["runtime_validation"] = runtime_validation
            else:
                runtime_observation = _spark_active_apps(config.run_id)
            manifest["observed_spark_runtime"] = runtime_observation
            runtime_errors = []
            if is_scalability:
                runtime_errors.extend((runtime_validation or {}).get("errors", []))
            if runtime_observation.get("error"):
                runtime_errors.append(runtime_observation["error"])
            if not is_scalability:
                expected_app_names = {
                    f"WeatherBronzeStreaming-{config.run_id}",
                    f"WeatherSilverStreaming-{config.run_id}",
                }
                observed_apps = runtime_observation.get("apps", [])
                observed_app_names = {app.get("name") for app in observed_apps}
                if observed_app_names != expected_app_names or len(observed_apps) != 2:
                    runtime_errors.append(
                        "Expected exactly the run-scoped Bronze and Silver streaming applications."
                    )
                for app in observed_apps:
                    if app.get("cores") != 1:
                        runtime_errors.append(
                            f"{app.get('name')} requested {app.get('cores')} cores; expected 1."
                        )
                    raw_memory = app.get("memory_per_executor_mb")
                    observed_memory = (
                        raw_memory
                        if isinstance(raw_memory, (int, float))
                        else _memory_to_mb(raw_memory)
                    )
                    if observed_memory != 1024:
                        runtime_errors.append(
                            f"{app.get('name')} executor memory is {observed_memory} MB; expected 1024 MB."
                        )
                workers = [
                    worker
                    for worker in runtime_observation.get("workers", [])
                    if worker.get("state") == "ALIVE"
                ]
                if len(workers) != 1:
                    runtime_errors.append(
                        f"Expected one ALIVE Spark worker; observed {len(workers)}."
                    )
                for worker in workers:
                    if worker.get("state") != "ALIVE":
                        runtime_errors.append(
                            f"Spark worker state is {worker.get('state')}; expected ALIVE."
                        )
                    if worker.get("cores_available") != 4:
                        runtime_errors.append(
                            f"Spark worker reports {worker.get('cores_available')} cores; expected 4."
                        )
                    if worker.get("memory_available_mb") != 4096:
                        runtime_errors.append(
                            "Spark worker reports "
                            f"{worker.get('memory_available_mb')} MB; expected 4096 MB."
                        )
                if runtime_observation.get("other_active_apps"):
                    runtime_errors.append("Unrelated Spark applications are active on the master.")
                manifest["runtime_validation"] = {
                    "passed": not runtime_errors,
                    "errors": runtime_errors,
                    "expected_worker_count": 1,
                    "expected_worker_cores": 4,
                    "expected_worker_memory_mb": 4096,
                    "expected_streaming_app_count": 2,
                    "expected_cores_per_application": 1,
                    "expected_executor_memory_mb_per_application": 1024,
                }
            write_json(artifacts["manifest"], manifest)
            if runtime_errors:
                if is_scalability:
                    runtime_allocation_invalid = True
                    failure_kind = "allocation_mismatch"
                    failure_reason = "Actual Spark worker/executor allocation does not match the requested scalability configuration."
                raise RuntimeError(
                    "Spark runtime does not match the requested benchmark allocation: "
                    + "; ".join(runtime_errors)
                    + ". Observed: "
                    + json.dumps(runtime_observation, sort_keys=True)
                )
            stream_started_monotonic = time.monotonic()
            stream_started_at_utc = utc_now()
            manifest["stream_started_at_utc"] = stream_started_at_utc
            write_json(artifacts["manifest"], manifest)
            print(
                f"[{'SCALABILITY' if is_scalability else 'THROUGHPUT'}] streaming apps started; requested="
                f"{config.requested_replay_rate} msg/s, events={config.source_record_limit:,}"
            )

            if is_scalability:
                cluster_sampler = SparkClusterSampler(config.run_id)
                cluster_sampler.sample_now(phase="before_replay")
                cluster_sampler.start()
            try:
                _run(
                    _simulator_command(config, source, artifacts["simulator"]),
                    timeout=max(
                        900,
                        int(config.source_record_limit / max(config.requested_replay_rate, 1) * 3) + 180,
                    ),
                )
            finally:
                if cluster_sampler is not None:
                    cluster_sampler.stop()
            simulator_summary = read_json(artifacts["simulator"])
            simulator_summary["git_commit"] = git_commit
            write_json(artifacts["simulator"], simulator_summary)
            if simulator_summary.get("run_id") != config.run_id:
                raise RuntimeError("Simulator emitted a different run_id.")
            if not simulator_summary.get("producer_delivery_complete"):
                raise RuntimeError("Simulator did not flush every queued Kafka message.")

            generation_start = simulator_summary.get("generation_start_time")
            generation_end = simulator_summary.get("generation_end_time")
            generation_rate = simulator_summary.get("actual_generated_msgs_sec")
            tolerance = float(calibration.get("calibration_tolerance_percent", 10)) / 100
            measured_deviation = (
                abs(float(generation_rate) - config.requested_replay_rate)
                / config.requested_replay_rate
                if generation_rate is not None and config.requested_replay_rate > 0
                else None
            )
            manifest["load_generator_calibration"]["this_run_actual_rate_msgs_sec"] = generation_rate
            manifest["load_generator_calibration"]["this_run_deviation_percent"] = (
                measured_deviation * 100 if measured_deviation is not None else None
            )
            manifest["load_generator_calibration"]["this_run_within_tolerance"] = (
                measured_deviation is not None and measured_deviation <= tolerance
            )
            write_json(artifacts["manifest"], manifest)

            drain_started = time.monotonic()
            drain_complete, drain_message, counts = _wait_for_stream_drain(
                config=config,
                simulator_summary=simulator_summary,
                progress_paths=progress_paths,
                sampler=lag_sampler,
                processes=processes,
                timeout_seconds=drain_timeout_seconds,
            )
            drain_seconds = time.monotonic() - drain_started
            if not drain_complete:
                failure_reason = drain_message
                failure_kind = "drain_timeout" if drain_message and drain_message.startswith("Drain timeout") else "stream_failure"
            else:
                print(
                    f"[THROUGHPUT] drained: bronze={counts['bronze']:,}, "
                    f"silver={counts['silver']:,} in {drain_seconds:.1f}s"
                )
            lag_sampler.sample_now(timeout_seconds=10)
        except Exception as exc:
            if is_scalability and runtime_allocation_invalid:
                errors = (runtime_validation or {}).get("errors", [])
                failure_reason = "Actual Spark allocation did not match the requested configuration: " + "; ".join(errors)
                failure_kind = "allocation_mismatch"
            elif is_scalability and simulator_summary is not None and simulator_summary.get("producer_delivery_complete") is not True:
                failure_reason = _scalability_generator_limit_reason(
                    simulator_summary,
                    generator_rate_valid=False,
                )
                failure_kind = "load_generator_limited"
            else:
                failure_reason = f"{type(exc).__name__}: {exc}"
                failure_kind = "run_failure"
            for stage, log_path in log_paths.items():
                log_tails[stage] = _tail_text(log_path)
        finally:
            if lag_sampler is not None:
                try:
                    lag_sampler.sample_now(timeout_seconds=10)
                except Exception as exc:
                    log_tails["kafka_lag_sampler"] = f"{type(exc).__name__}: {exc}"
            try:
                process_return_codes = _stop_stream_processes(
                    processes,
                    stop_signal,
                    run_id=config.run_id,
                )
                spark_shutdown_observation = _spark_active_apps(config.run_id)
                stream_shutdown_confirmed = bool(
                    not spark_shutdown_observation.get("error")
                    and not spark_shutdown_observation.get("apps")
                    and all(code is not None for code in process_return_codes.values())
                )
                if not stream_shutdown_confirmed:
                    raise RuntimeError(
                        "Spark Master did not confirm that this run's applications stopped."
                    )
            except Exception as exc:
                process_return_codes = {
                    stage: process.poll() for stage, process in processes.items()
                }
                shutdown_error = f"Stream shutdown failed: {type(exc).__name__}: {exc}"
                failure_reason = (
                    f"{failure_reason}; {shutdown_error}"
                    if failure_reason
                    else shutdown_error
                )
                failure_kind = "stream_failure"
                spark_shutdown_observation = _spark_active_apps(config.run_id)
                stream_shutdown_confirmed = bool(
                    not spark_shutdown_observation.get("error")
                    and not spark_shutdown_observation.get("apps")
                    and all(code is not None for code in process_return_codes.values())
                )
            if resource_sampler is not None:
                resource_sampler.stop()
            if lag_sampler is not None:
                lag_sampler.stop()
            if cluster_sampler is not None:
                cluster_sampler.stop()
                write_json(artifacts["spark_cluster_snapshot"], {
                    "schema_version": 1,
                    "run_id": config.run_id,
                    "sampling_interval_seconds": cluster_sampler.interval_seconds,
                    "samples": cluster_sampler.samples,
                    "last_error": cluster_sampler.last_error,
                })
            if stream_shutdown_confirmed:
                stop_signal.unlink(missing_ok=True)
            run_finished_monotonic = time.monotonic()
            for stage, log_path in log_paths.items():
                if log_path.exists():
                    log_tails[stage] = _tail_text(log_path)

    progress_rows = _merge_progress_files(
        progress_paths,
        artifacts["spark_progress"],
    )
    if (
        all(process_return_codes.get(stage) == 0 for stage in ("bronze", "silver"))
        and simulator_summary is not None
        and config.paths.bronze
        and config.paths.silver
    ):
        try:
            checker_environment = config.spark_environment("check_throughput")
            checker_environment["RESULT_PATH"] = _container_artifact_path(artifacts["delta_metrics"])
            _spark_submit(
                JOBS_DIR / "check_throughput_metrics.py",
                checker_environment,
                versions["spark"],
                fixed_throughput_baseline=True,
            )
            delta_metrics = read_json(artifacts["delta_metrics"])
        except Exception as exc:
            metrics_error = f"{type(exc).__name__}: {exc}"
            for stage, log_path in log_paths.items():
                log_tails[f"{stage}_metrics"] = _tail_text(log_path)

    temporary_log_cleanup_error = (
        _cleanup_temporary_directory(temporary_log_dir)
        if temporary_log_dir is not None
        else None
    )

    lag_samples = read_jsonl(artifacts["kafka_lag"])
    resource_samples = read_jsonl(artifacts["resource_metrics"])
    analysis = analyze_run_artifacts(
        progress_rows=progress_rows,
        lag_samples=lag_samples,
        resource_samples=resource_samples,
        simulator_summary=simulator_summary or {},
        delta_metrics=delta_metrics,
        manifest=manifest,
        legacy_result={
            "run_id": config.run_id,
            "requested_rate_msgs_sec": config.requested_replay_rate,
            "drain_seconds": drain_seconds,
            "drain_complete": drain_complete,
        },
        minimum_warmup_seconds=warmup_seconds,
        minimum_completed_batches=warmup_min_batches,
        drain_complete=drain_complete,
        stream_process_return_codes=process_return_codes,
        failure_reason=(
            failure_reason
            if failure_kind in {"run_failure", "stream_failure"}
            else None
        ),
        metrics_error=metrics_error,
    )
    progress_summary = summarize_progress(
        progress_rows,
        steady_state_start=analysis["steady_state_start"],
        generation_end_time=analysis["steady_state_end"],
    )
    progress_percentiles = summarize_progress_percentiles(
        progress_rows,
        steady_state_start=analysis["steady_state_start"],
        generation_end_time=analysis["steady_state_end"],
    ) if is_scalability else {}
    resource_scalability = scalability_resource_metrics(resource_samples) if is_scalability else None
    maximum_lag = analysis["production_peak_kafka_to_bronze_lag"]
    final_lag = analysis["final_source_lag"]
    offsets = _spark_offset_summary(progress_rows, config.topic)
    final_kafka_offsets = next(
        (
            row.get("kafka_latest_offsets")
            for row in reversed(lag_samples)
            if isinstance(row.get("kafka_latest_offsets"), dict)
            and row.get("kafka_latest_offsets")
        ),
        {},
    )
    final_kafka_low_offsets = next(
        (
            row.get("kafka_low_offsets")
            for row in lag_samples
            if isinstance(row.get("kafka_low_offsets"), dict)
            and row.get("kafka_low_offsets")
        ),
        {},
    )
    partition_counts = {
        key: max(0, int(offset) - int(final_kafka_low_offsets.get(key, 0)))
        for key, offset in final_kafka_offsets.items()
    }
    partition_metrics = (
        partition_distribution(partition_counts, config.topic_partitions)
        if is_scalability
        else None
    )
    produced = int((simulator_summary or {}).get("kafka_messages", 0))
    bronze_processed = int(progress_summary.get("bronze_input_records", 0))
    silver_processed = int(progress_summary.get("silver_input_records", 0))
    bronze_delta = int((delta_metrics or {}).get("bronze_records", 0))
    generator_rate = (simulator_summary or {}).get("actual_generated_msgs_sec")
    generator_deviation = (
        abs(float(generator_rate) - config.requested_replay_rate) / config.requested_replay_rate
        if generator_rate is not None and config.requested_replay_rate > 0
        else None
    )
    generator_valid = (
        generator_deviation is not None
        and generator_deviation <= float(calibration.get("calibration_tolerance_percent", 10)) / 100
    )
    correctness_checks = (delta_metrics or {}).get("correctness_checks", {})
    correctness_passed = bool(
        delta_metrics
        and (delta_metrics.get("correctness_passed") is True)
        and all(correctness_checks.values())
    )
    capacity = analysis["capacity_classification"]
    capacity_reason = analysis["capacity_reason"]
    generator_limit_reason = _scalability_generator_limit_reason(
        simulator_summary,
        generator_rate_valid=generator_valid,
    ) if is_scalability else None
    if is_scalability and runtime_allocation_invalid:
        status = "INVALID_FOR_COMPARISON"
        capacity = None
        capacity_reason = "Observed Spark worker/executor allocation differed from the requested scalability configuration."
    elif not stream_shutdown_confirmed:
        status = "FAILED"
        capacity = "FAILED"
        capacity_reason = "Spark streaming applications or their launchers did not stop cleanly."
    elif is_scalability and generator_limit_reason is not None:
        status = "LOAD_GENERATOR_LIMITED"
        capacity = None
        capacity_reason = generator_limit_reason
    elif is_scalability and delta_metrics is not None and not correctness_passed:
        status = "INVALID_CORRECTNESS"
        capacity = None
        capacity_reason = "Bronze/Silver data correctness checks did not all pass."
    elif simulator_summary is not None and not generator_valid:
        status = "LOAD_GENERATOR_LIMITED" if is_scalability else "INVALID_GENERATOR"
        capacity = None
        capacity_reason = "This run's measured generator rate fell outside its calibrated +/-10% band."
    elif capacity == "FAILED":
        status = "FAILED"
        if metrics_error:
            failure_reason = failure_reason or f"Delta metrics collection failed: {metrics_error}"
    elif capacity == "SATURATED":
        status = "SATURATED"
    else:
        status = capacity

    bronze_summary = progress_summary.get("bronze", {})
    silver_summary = progress_summary.get("silver", {})
    duration_seconds = (
        run_finished_monotonic - stream_started_monotonic
        if run_finished_monotonic is not None and stream_started_monotonic is not None
        else None
    )
    valid_for_comparison = bool(
        is_scalability
        and status in {"UNDER_CAPACITY", "NEAR_CAPACITY"}
        and generator_valid
        and (runtime_validation or {}).get("passed") is True
        and correctness_passed
        and analysis.get("pipeline_completed") is True
        and final_lag == 0
        and metrics_error is None
        and stream_shutdown_confirmed
    )
    result = {
        **analysis,
        "schema_version": 2,
        "scenario": config.scenario,
        "run_id": config.run_id,
        "experiment_id": config.experiment_id,
        "config_id": config.config_id,
        "git_commit": git_commit,
        "status": status,
        "valid_for_comparison": valid_for_comparison,
        "correctness_passed": correctness_passed,
        "correctness_checks": correctness_checks,
        "capacity_classification": capacity,
        "capacity_reason": capacity_reason,
        "sustainable": status in {"UNDER_CAPACITY", "NEAR_CAPACITY"},
        "requested_rate_msgs_sec": config.requested_replay_rate,
        "actual_generated_msgs_sec": generator_rate,
        "actual_generation_rate_including_flush": (simulator_summary or {}).get("actual_generation_rate"),
        "generator_deviation_percent": generator_deviation * 100 if generator_deviation is not None else None,
        "generator_calibrated": generator_valid,
        "input_records": (simulator_summary or {}).get("source_records"),
        "produced_messages": produced,
        "producer_delivery_complete": (simulator_summary or {}).get("producer_delivery_complete"),
        "bronze_records": bronze_delta if delta_metrics else None,
        "processed_records": silver_processed,
        "silver_output_records": (delta_metrics or {}).get("silver_records"),
        "dlq_output_records": (delta_metrics or {}).get("dlq_records"),
        "producer_elapsed_seconds": (simulator_summary or {}).get("producer_elapsed_seconds"),
        "generation_elapsed_seconds": (simulator_summary or {}).get("generation_elapsed_seconds"),
        "producer_flush_remaining": (simulator_summary or {}).get("producer_flush_remaining"),
        "duration_seconds": duration_seconds,
        "duration_definition": "First streaming query start through confirmed drain and clean stream shutdown.",
        "drain_seconds": drain_seconds,
        "drain_complete": analysis["pipeline_completed"],
        "stream_drain_complete": drain_complete,
        "bronze_input_records": bronze_processed,
        "silver_input_records": silver_processed,
        "avg_input_rows_per_sec": bronze_summary.get("avg_input_rows_per_sec"),
        "peak_input_rows_per_sec": bronze_summary.get("peak_input_rows_per_sec"),
        "avg_processed_rows_per_sec": bronze_summary.get("avg_processed_rows_per_sec"),
        "peak_processed_rows_per_sec": bronze_summary.get("peak_processed_rows_per_sec"),
        "silver_avg_processed_rows_per_sec": silver_summary.get("avg_processed_rows_per_sec"),
        "silver_peak_processed_rows_per_sec": silver_summary.get("peak_processed_rows_per_sec"),
        "bronze_processed_rate_p50": progress_percentiles.get("bronze", {}).get("processed_rate_p50"),
        "bronze_processed_rate_p95": progress_percentiles.get("bronze", {}).get("processed_rate_p95"),
        "silver_processed_rate_p50": progress_percentiles.get("silver", {}).get("processed_rate_p50"),
        "silver_processed_rate_p95": progress_percentiles.get("silver", {}).get("processed_rate_p95"),
        "progress_percentile_metrics": progress_percentiles,
        "included_bronze_batches": bronze_summary.get("steady_state_sample_count"),
        "included_silver_batches": silver_summary.get("steady_state_sample_count"),
        "warmup_batches_excluded_per_query": None,
        "avg_batch_duration_ms": bronze_summary.get("avg_batch_duration_ms"),
        "p95_batch_duration_ms": bronze_summary.get("p95_batch_duration_ms"),
        "max_batch_duration_ms": bronze_summary.get("max_batch_duration_ms"),
        "batch_duration_components": bronze_summary.get("duration_components"),
        "max_kafka_lag": maximum_lag,
        "max_kafka_lag_during_production": analysis["production_peak_kafka_to_bronze_lag"],
        "final_kafka_lag": final_lag,
        "kafka_topic": config.topic,
        "kafka_partition_count": config.topic_partitions,
        "kafka_lag_definition": "For each sample, sum(max(0, Kafka high/latest offset - Spark Kafka source end offset)) across the run topic partitions.",
        **offsets,
        "final_kafka_latest_offsets": final_kafka_offsets,
        "final_kafka_low_offsets": final_kafka_low_offsets,
        "kafka_partition_distribution": partition_metrics,
        "latency_count": analysis["replay_to_bronze_latency_count"],
        "latency_min_ms": analysis["replay_to_bronze_latency_min_ms"],
        "latency_avg_ms": analysis["replay_to_bronze_latency_avg_ms"],
        "latency_p50_ms": analysis["replay_to_bronze_latency_p50_ms"],
        "latency_p95_ms": analysis["replay_to_bronze_latency_p95_ms"],
        "latency_p99_ms": analysis["replay_to_bronze_latency_p99_ms"],
        "latency_max_ms": analysis["replay_to_bronze_latency_max_ms"],
        "latency_definition": analysis["replay_to_bronze_latency_definition"],
        "latency_percentile_method": analysis["replay_to_bronze_latency_percentile_method"],
        "spark_worker_peak_cpu_percent": analysis["worker_cpu_peak"],
        "spark_worker_peak_memory_mb": analysis["worker_memory_peak_mb"],
        "broker_peak_cpu_percent": analysis["broker_cpu_peak"],
        "broker_peak_memory_mb": analysis["broker_memory_peak_mb"],
        "resource_sample_count": len(resource_samples),
        "kafka_lag_sample_count": len(lag_samples),
        "fixed_infrastructure": manifest.get("throughput_baseline"),
        "scalability_infrastructure": manifest.get("scalability"),
        "scalability_runtime_validation": runtime_validation,
        "scalability_resource_metrics": resource_scalability,
        "spark_cluster_snapshot": artifacts["spark_cluster_snapshot"].relative_to(REPO_ROOT).as_posix() if is_scalability else None,
        "observed_spark_runtime": runtime_observation,
        "stream_process_return_codes": process_return_codes,
        "stream_shutdown_confirmed": stream_shutdown_confirmed,
        "spark_shutdown_observation": spark_shutdown_observation,
        "temporary_log_cleanup_error": temporary_log_cleanup_error,
        "null_metric_reasons": {
            "latency": metrics_error or (
                f"Latency was measured for {analysis['replay_to_bronze_latency_count'] or 0} "
                f"of {produced} produced messages."
                if not analysis["latency_measurements_complete"]
                else None
            ),
            "kafka_lag": (
                "No comparable Kafka latest offsets and Spark source end offsets were captured."
                if maximum_lag is None or final_lag is None
                else None
            ),
            "resources": (
                "Docker stats did not provide numeric CPU and memory samples for both containers."
                if not analysis["resource_measurements_complete"]
                else None
            ),
        },
        "errors": {
            "run": failure_reason,
            "metrics": metrics_error,
            "resource_sampler": resource_sampler.last_error if resource_sampler else "sampler did not start",
            "kafka_lag_sampler": lag_sampler.last_error if lag_sampler else "sampler did not start",
            "spark_log_tails": log_tails,
        },
    }
    write_json(artifacts["result"], result)
    manifest["status"] = status
    manifest["finished_at"] = utc_now()
    manifest["result_metrics"] = result
    manifest["stream_shutdown_confirmed"] = stream_shutdown_confirmed
    manifest["stop_signal_retained"] = bool(stop_signal.exists())
    manifest["spark_shutdown_observation"] = spark_shutdown_observation
    if failure_reason:
        manifest["error"] = {"type": failure_kind, "message": failure_reason}
    if temporary_log_cleanup_error:
        manifest["temporary_log_cleanup_error"] = temporary_log_cleanup_error
    write_json(artifacts["manifest"], manifest)
    print(
        f"[{'SCALABILITY' if is_scalability else 'THROUGHPUT'}] run={config.run_id} status={status} "
        f"processed={int(result['processed_records'] or 0):,} "
        f"max_lag={result['max_kafka_lag']} final_lag={result['final_kafka_lag']}"
    )
    return result


def run_throughput_benchmark(args) -> int:
    if args.calibrate_load_generator:
        return run_load_generator_calibration(args)
    if args.rate <= 0:
        raise ValueError("Throughput --rate must be a positive messages/second target.")
    if args.repetitions <= 0:
        raise ValueError("--repetitions must be positive.")
    if args.warmup_seconds < 0:
        raise ValueError("--warmup-seconds must be zero or greater.")
    if args.warmup_min_batches < 1:
        raise ValueError("--warmup-min-batches must be at least one.")
    if args.drain_timeout <= 0:
        raise ValueError("--drain-timeout must be positive.")

    source_limit = (
        args.max_source_events
        if args.max_source_events is not None
        else 50_000
    )
    source = Path(args.source).resolve()
    if not source.is_dir():
        raise FileNotFoundError(f"Historical source directory not found: {source}")
    if importlib.util.find_spec("confluent_kafka") is None:
        raise RuntimeError(
            "The current Python interpreter lacks confluent-kafka. Install the "
            "repository dependency with: python -m pip install -r producer/requirements.txt"
        )

    _require_clean_worktree()
    _require_services()
    versions = _runtime_versions()
    git_commit = _git_commit()
    calibration, calibration_path = _find_calibration(args.rate, git_commit)
    experiment_id = _new_run_id()
    experiment_dir = REPO_ROOT / RESULTS_ROOT / "throughput" / "experiments" / experiment_id
    experiment_dir.mkdir(parents=True, exist_ok=False)
    run_results = []

    for repetition in range(1, args.repetitions + 1):
        if args.run_id:
            run_id = args.run_id if args.repetitions == 1 else f"{args.run_id}-r{repetition}"
        else:
            run_id = _new_run_id()
        config = BenchmarkConfig.for_scenario(
            THROUGHPUT_BASELINE,
            run_id,
            source_record_limit=source_limit,
            requested_replay_rate=args.rate,
            seed=args.seed,
        )
        print(
            f"[THROUGHPUT] experiment={experiment_id} "
            f"rate={args.rate} repetition={repetition}/{args.repetitions}"
        )
        result = _run_throughput_iteration(
            config=config,
            source=source,
            versions=versions,
            git_commit=git_commit,
            calibration=calibration,
            calibration_path=calibration_path,
            warmup_seconds=args.warmup_seconds,
            warmup_min_batches=args.warmup_min_batches,
            drain_timeout_seconds=args.drain_timeout,
        )
        run_results.append(result)
        if result.get("status") in {"FAILED", "INVALID_GENERATOR"}:
            break
        if result.get("status") == "SATURATED" and not result.get("pipeline_completed"):
            print("[THROUGHPUT] pipeline did not drain; stopping further repetitions at this rate")
            break

    aggregate = aggregate_repetitions(run_results)
    summary = {
        "schema_version": 2,
        "experiment_type": "throughput_repetitions",
        "experiment_id": experiment_id,
        "created_at": utc_now(),
        "scenario": THROUGHPUT_BASELINE,
        "git_commit": git_commit,
        "source_record_limit": source_limit,
        "requested_repetitions": args.repetitions,
        "completed_repetitions": len(run_results),
        "warmup_policy": {
            "minimum_warmup_seconds": args.warmup_seconds,
            "minimum_completed_batches_per_query": args.warmup_min_batches,
            "steady_state_start_rule": "Later of elapsed warm-up and completed-batch cutoffs for both main queries.",
            "steady_state_end_rule": "Simulator generation end; post-replay drain excluded.",
        },
        "drain_timeout_seconds": args.drain_timeout,
        "calibration_summary": calibration_path.relative_to(REPO_ROOT).as_posix(),
        "runs": [
            {
                "run_id": result.get("run_id"),
                "status": result.get("status"),
                "result": f"results/benchmarks/throughput/{result.get('run_id')}/result.json",
            }
            for result in run_results
        ],
        "aggregate": aggregate,
    }
    summary_path = experiment_dir / "summary.json"
    write_json(summary_path, summary)
    print(f"[THROUGHPUT] summary={summary_path.relative_to(REPO_ROOT)}")
    if any(result.get("status") in {"FAILED", "INVALID_GENERATOR"} for result in run_results):
        return 2
    return 0


def run_scalability_benchmark(args) -> int:
    if args.calibrate_load_generator:
        return run_load_generator_calibration(args)
    if args.rate <= 0:
        raise ValueError("Scalability --rate must be a positive messages/second target.")
    if args.repetitions <= 0:
        raise ValueError("--repetitions must be positive.")
    if args.warmup_seconds < 0:
        raise ValueError("--warmup-seconds must be zero or greater.")
    if args.warmup_min_batches < 1:
        raise ValueError("--warmup-min-batches must be at least one.")
    if args.drain_timeout <= 0:
        raise ValueError("--drain-timeout must be positive.")

    source_limit = args.max_source_events if args.max_source_events is not None else 500_000
    source = Path(args.source).resolve()
    if not source.is_dir():
        raise FileNotFoundError(f"Historical source directory not found: {source}")
    if importlib.util.find_spec("confluent_kafka") is None:
        raise RuntimeError(
            "The current Python interpreter lacks confluent-kafka. Install the "
            "repository dependency with: python -m pip install -r producer/requirements.txt"
        )

    _require_clean_worktree()
    _require_services()
    versions = _runtime_versions()
    git_commit = _git_commit()
    calibration, calibration_path = _find_calibration(args.rate, git_commit)
    experiment_id = args.experiment_id or _new_run_id()
    validated_config = BenchmarkConfig.for_scenario(
        SCALABILITY_BENCHMARK,
        "scalability-validation",
        source_record_limit=source_limit,
        requested_replay_rate=args.rate,
        seed=args.seed,
        topic_partitions=args.partitions,
        config_id=args.config_id,
        experiment_id=experiment_id,
        workers=args.workers,
        worker_cores_each=args.worker_cores,
        worker_memory_mb_each=args.worker_memory_mb,
        bronze_cores_max=args.bronze_cores,
        silver_cores_max=args.silver_cores,
        executor_cores=args.executor_cores,
        executor_memory_mb=args.executor_memory_mb,
        shuffle_partitions=args.shuffle_partitions,
        trigger_interval=args.trigger_interval,
    )
    experiment_dir = REPO_ROOT / RESULTS_ROOT / "scalability" / "experiments" / experiment_id
    experiment_dir.mkdir(parents=True, exist_ok=False)
    configuration = {
        "partitions": args.partitions,
        "workers": args.workers,
        "worker_cores_each": args.worker_cores,
        "worker_memory_mb_each": args.worker_memory_mb,
        "bronze_cores_max": args.bronze_cores,
        "silver_cores_max": args.silver_cores,
        "executor_cores": args.executor_cores,
        "executor_memory_mb": args.executor_memory_mb,
        "shuffle_partitions": args.shuffle_partitions,
        "trigger_interval": args.trigger_interval,
    }
    experiment_manifest = {
        "schema_version": 1,
        "experiment_type": "scalability_configuration_repetitions",
        "experiment_id": experiment_id,
        "created_at": utc_now(),
        "git_commit": git_commit,
        "scenario": SCALABILITY_BENCHMARK,
        "config_id": validated_config.config_id,
        "configuration": configuration,
        "requested_rate_msgs_sec": args.rate,
        "source_record_limit": source_limit,
        "calibration_summary": calibration_path.relative_to(REPO_ROOT).as_posix(),
    }
    write_json(experiment_dir / "experiment_manifest.json", experiment_manifest)
    run_results = []
    for repetition in range(1, args.repetitions + 1):
        if args.run_id:
            run_id = args.run_id if args.repetitions == 1 else f"{args.run_id}-r{repetition}"
        else:
            run_id = _new_run_id()
        config = BenchmarkConfig.for_scenario(
            SCALABILITY_BENCHMARK,
            run_id,
            source_record_limit=source_limit,
            requested_replay_rate=args.rate,
            seed=args.seed,
            topic_partitions=args.partitions,
            config_id=args.config_id,
            experiment_id=experiment_id,
            workers=args.workers,
            worker_cores_each=args.worker_cores,
            worker_memory_mb_each=args.worker_memory_mb,
            bronze_cores_max=args.bronze_cores,
            silver_cores_max=args.silver_cores,
            executor_cores=args.executor_cores,
            executor_memory_mb=args.executor_memory_mb,
            shuffle_partitions=args.shuffle_partitions,
            trigger_interval=args.trigger_interval,
        )
        print(
            f"[SCALABILITY] experiment={experiment_id} config={config.config_id} "
            f"rate={args.rate} repetition={repetition}/{args.repetitions}"
        )
        result = _run_throughput_iteration(
            config=config,
            source=source,
            versions=versions,
            git_commit=git_commit,
            calibration=calibration,
            calibration_path=calibration_path,
            warmup_seconds=args.warmup_seconds,
            warmup_min_batches=args.warmup_min_batches,
            drain_timeout_seconds=args.drain_timeout,
        )
        run_results.append(result)
        if result.get("status") in {"FAILED", "INVALID_FOR_COMPARISON", "INVALID_CORRECTNESS", "LOAD_GENERATOR_LIMITED"}:
            break
    aggregate = aggregate_scalability_runs(run_results)
    summary = {
        "schema_version": 1,
        "experiment_type": "scalability_configuration_repetitions",
        "experiment_id": experiment_id,
        "created_at": utc_now(),
        "scenario": SCALABILITY_BENCHMARK,
        "git_commit": git_commit,
        "configuration": configuration,
        "config_id": args.config_id or (
            f"p{args.partitions}-b{args.bronze_cores}-s{args.silver_cores}-w{args.workers}"
        ),
        "requested_rate_msgs_sec": args.rate,
        "source_record_limit": source_limit,
        "requested_repetitions": args.repetitions,
        "completed_repetitions": len(run_results),
        "warmup_policy": {
            "minimum_warmup_seconds": args.warmup_seconds,
            "minimum_completed_batches_per_query": args.warmup_min_batches,
            "steady_state_end_rule": "Simulator generation end; post-replay drain excluded.",
        },
        "calibration_summary": calibration_path.relative_to(REPO_ROOT).as_posix(),
        "runs": [
            {
                "run_id": result.get("run_id"),
                "status": result.get("status"),
                "valid_for_comparison": result.get("valid_for_comparison"),
                "result": f"results/benchmarks/scalability/{result.get('run_id')}/result.json",
            }
            for result in run_results
        ],
        "aggregate": aggregate,
    }
    write_json(experiment_dir / "summary.json", summary)
    print(f"[SCALABILITY] summary={(experiment_dir / 'summary.json').relative_to(REPO_ROOT)}")
    return 2 if any(
        result.get("status") in {"FAILED", "INVALID_FOR_COMPARISON", "INVALID_CORRECTNESS", "LOAD_GENERATOR_LIMITED"}
        for result in run_results
    ) else 0


def _delta_coordinate() -> str:
    match = re.search(
        r"^delta-spark==([0-9]+(?:\.[0-9]+)+)\s*$",
        DELTA_REQUIREMENTS.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if not match:
        raise RuntimeError(
            f"Could not resolve the pinned Delta version from {DELTA_REQUIREMENTS}."
        )
    return f"io.delta:delta-spark_2.13:{match.group(1)}"


def _simulator_command(
    config: BenchmarkConfig,
    source: Path,
    summary_path: Path,
) -> list[str]:
    return [
        sys.executable,
        str(SIMULATOR),
        "--bootstrap-servers",
        "localhost:9092",
        "--dataset",
        "benchmark-20",
        "--source",
        str(source),
        *config.simulator_arguments(),
        "--scenario",
        config.scenario,
        "--summary-json",
        str(summary_path),
    ]


def _artifact_paths(config: BenchmarkConfig, run_dir: Path) -> dict[str, Path]:
    artifacts = {
        "manifest": run_dir / "manifest.json",
        "result": run_dir / "result.json",
        "simulator": run_dir / "simulator.json",
    }
    if config.scenario in {THROUGHPUT_BASELINE, SCALABILITY_BENCHMARK}:
        artifacts.update({
            "spark_progress": run_dir / "spark_progress.jsonl",
            "spark_progress_bronze_temp": run_dir / "spark_progress_bronze.tmp.jsonl",
            "spark_progress_silver_temp": run_dir / "spark_progress_silver.tmp.jsonl",
            "resource_metrics": run_dir / "resource_metrics.jsonl",
            "kafka_lag": run_dir / "kafka_lag.jsonl",
            "delta_metrics": run_dir / "delta_metrics.json",
        })
    if config.scenario == SCALABILITY_BENCHMARK:
        artifacts["spark_cluster_snapshot"] = run_dir / "spark_cluster_snapshot.json"
    return artifacts


def _memory_to_mb(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value // (1024 * 1024)
    match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([kmgt](?:i?b)?|b)?\s*", str(value), re.I)
    if not match:
        return None
    amount = float(match.group(1))
    unit = (match.group(2) or "mb").lower()
    factors = {
        "b": 1 / (1024 * 1024),
        "kb": 1 / 1024,
        "kib": 1 / 1024,
        "mb": 1,
        "mib": 1,
        "gb": 1024,
        "gib": 1024,
        "tb": 1024 * 1024,
        "tib": 1024 * 1024,
        "k": 1 / 1024,
        "m": 1,
        "g": 1024,
        "t": 1024 * 1024,
    }
    return round(amount * factors[unit]) if unit in factors else None


def _container_hard_limits(containers: list[str]) -> dict[str, dict[str, int | None]]:
    limits = {}
    for name in containers:
        try:
            completed = _run(
                [
                    "docker", "inspect", "--format",
                    "{{json .HostConfig}}",
                    name,
                ],
                capture_output=True,
                timeout=15,
            )
            host_config = json.loads(completed.stdout)
            memory_bytes = int(host_config.get("Memory") or 0)
            nano_cpus = int(host_config.get("NanoCpus") or 0)
            cpu_quota = int(host_config.get("CpuQuota") or 0)
            cpu_period = int(host_config.get("CpuPeriod") or 0)
            cpu_limit_cores = (
                nano_cpus / 1_000_000_000
                if nano_cpus > 0
                else cpu_quota / cpu_period
                if cpu_quota > 0 and cpu_period > 0
                else None
            )
            limits[name] = {
                "configured_memory_limit_mb": memory_bytes // (1024 * 1024) if memory_bytes else None,
                "configured_cpu_limit_cores": cpu_limit_cores,
                "configured_cpu_quota_us": cpu_quota or None,
                "configured_cpu_period_us": cpu_period or None,
                "configured_cpuset_cpus": host_config.get("CpusetCpus") or None,
            }
        except Exception as exc:
            limits[name] = {
                "configured_memory_limit_mb": None,
                "configured_cpu_limit_cores": None,
                "configured_cpu_quota_us": None,
                "configured_cpu_period_us": None,
                "configured_cpuset_cpus": None,
                "inspection_error": f"{type(exc).__name__}: {exc}",
            }
    return limits


def _throughput_infrastructure(versions: dict[str, str | None]) -> dict:
    services = _compose_images().get("services", {})
    worker = services.get("spark-worker", {})
    broker = services.get("broker", {})
    worker_environment = worker.get("environment", {})
    worker_cores = int(worker_environment.get("SPARK_WORKER_CORES", 4))
    worker_memory = _memory_to_mb(worker_environment.get("SPARK_WORKER_MEMORY", "4g"))
    worker_containers = _compose_service_containers("spark-worker")
    if not worker_containers:
        worker_containers = ["weather-spark-worker"]
    containers = _container_hard_limits([*worker_containers, "weather-kafka"])
    first_worker_limits = containers.get(worker_containers[0])
    if first_worker_limits is not None:
        containers.setdefault("weather-spark-worker", first_worker_limits)
    return {
        "measurement_scope": "Kafka -> Bronze -> Silver and DLQ; Gold excluded",
        "host_logical_cpu_count": os.cpu_count(),
        "kafka": {
            "image": broker.get("image"),
            "version": versions.get("kafka"),
            "partition_count": 1,
            "replication_factor": 1,
            "configured_container_limits": containers.get("weather-kafka"),
        },
        "spark": {
            "image": worker.get("image"),
            "version": versions.get("spark"),
            "delta_version": versions.get("delta"),
            "worker_count": len(worker_containers),
            "worker_cores_available": worker_cores,
            "worker_memory_mb_available": worker_memory,
            "worker_containers": worker_containers,
            "streaming_applications": 2,
            "executor_cores_per_application": 1,
            "cores_max_per_application": 1,
            "executor_memory_mb_per_application": 1024,
            "total_requested_executor_cores": 2,
            "shuffle_partitions": 1,
            "trigger_interval_seconds": 1,
            "deployment_mode": "client",
            "configured_container_limits": first_worker_limits,
        },
        "docker_container_limits": containers,
        "resource_sampling": {
            "docker_stats_reported_memory_limit_is_host_capacity_when_container_limit_is_unset": True,
            "hard_container_memory_limit_configured": any(
                item.get("configured_memory_limit_mb") is not None
                for item in containers.values()
            ),
        },
    }


def _scalability_infrastructure(
    config: BenchmarkConfig,
    versions: dict[str, str | None],
) -> dict:
    infrastructure = _throughput_infrastructure(versions)
    worker_containers = infrastructure["spark"].get("worker_containers", [])
    infrastructure["kafka"]["partition_count"] = config.topic_partitions
    infrastructure["spark"].update({
        "worker_count_requested": config.requested_worker_count,
        "worker_count_registered_by_compose": len(worker_containers),
        "worker_cores_each_requested": config.worker_cores_each,
        "worker_memory_mb_each_requested": config.worker_memory_mb_each,
        "bronze_cores_max_requested": config.bronze_cores_max,
        "silver_cores_max_requested": config.silver_cores_max,
        "bronze_executor_cores_requested": config.executor_cores,
        "silver_executor_cores_requested": config.executor_cores,
        "bronze_executor_memory_mb_requested": config.executor_memory_mb,
        "silver_executor_memory_mb_requested": config.executor_memory_mb,
        "total_requested_executor_cores": config.bronze_cores_max + config.silver_cores_max,
        "shuffle_partitions": config.shuffle_partitions,
        "trigger_interval": config.trigger_interval,
        "deployment_mode": "client",
    })
    infrastructure["scalability_config"] = {
        "config_id": config.config_id,
        "experiment_id": config.experiment_id,
        "topic_partitions": config.topic_partitions,
        "workers": config.requested_worker_count,
        "worker_cores_each": config.worker_cores_each,
        "worker_memory_mb_each": config.worker_memory_mb_each,
        "bronze_cores_max": config.bronze_cores_max,
        "silver_cores_max": config.silver_cores_max,
        "executor_cores": config.executor_cores,
        "executor_memory_mb": config.executor_memory_mb,
        "shuffle_partitions": config.shuffle_partitions,
        "trigger_interval": config.trigger_interval,
    }
    return infrastructure


def _make_manifest(
    config: BenchmarkConfig,
    *,
    git_commit: str,
    source: Path,
    artifact_paths: dict[str, Path],
    versions: dict[str, str | None],
) -> dict:
    manifest = {
        "schema_version": 1,
        "scenario": config.scenario,
        "run_id": config.run_id,
        "created_at": utc_now(),
        "status": "RUNNING",
        "git_commit": git_commit,
        "source": {
            "path": str(source.resolve()),
            "record_limit": config.source_record_limit,
        },
        "kafka": {
            "topic": config.topic,
            "partition_count": config.topic_partitions,
        },
        "parameters": config.simulator_defaults(),
        "paths": config.paths.as_dict(),
        "checkpoints": {
            "bronze": config.paths.bronze_checkpoint,
            "silver": config.paths.silver_checkpoint,
            "dlq": config.paths.dlq_checkpoint,
            "gold": config.paths.gold_checkpoint,
        },
        "software_versions": versions,
        "artifacts": {
            "manifest": artifact_paths["manifest"].relative_to(REPO_ROOT).as_posix(),
            "result": artifact_paths["result"].relative_to(REPO_ROOT).as_posix(),
            "input_reference": artifact_paths["simulator"].relative_to(REPO_ROOT).as_posix(),
        },
    }
    if config.scenario == THROUGHPUT_BASELINE:
        manifest["throughput_baseline"] = _throughput_infrastructure(versions)
        manifest["artifacts"].update({
            name: path.relative_to(REPO_ROOT).as_posix()
            for name, path in artifact_paths.items()
            if name in {"spark_progress", "resource_metrics", "kafka_lag", "delta_metrics"}
        })
    elif config.scenario == SCALABILITY_BENCHMARK:
        manifest["scalability"] = _scalability_infrastructure(config, versions)
        manifest["measurement_mode"] = "one_factor_at_a_time_scalability"
        manifest["artifacts"].update({
            name: path.relative_to(REPO_ROOT).as_posix()
            for name, path in artifact_paths.items()
            if name in {
                "spark_progress", "resource_metrics", "kafka_lag", "delta_metrics",
                "spark_cluster_snapshot",
            }
        })
    return manifest


def run_benchmark(args) -> int:
    scenario = normalize_scenario(args.scenario)
    if args.rate is None:
        args.rate = 5_000 if scenario == SCALABILITY_BENCHMARK else 1_000
    if args.max_source_events is not None and args.max_source_events <= 0:
        raise ValueError("--max-source-events must be a positive integer.")
    if scenario == THROUGHPUT_BASELINE:
        return run_throughput_benchmark(args)
    if scenario == SCALABILITY_BENCHMARK:
        return run_scalability_benchmark(args)
    if args.calibrate_load_generator:
        raise ValueError("--calibrate-load-generator requires a performance scenario.")
    run_id = args.run_id or _new_run_id()
    config = BenchmarkConfig.for_scenario(
        scenario,
        run_id,
        source_record_limit=(
            args.max_source_events
            if args.max_source_events is not None
            else 10_000
        ),
        requested_replay_rate=args.rate,
        seed=args.seed,
    )
    source = Path(args.source).resolve()
    if not source.is_dir():
        raise FileNotFoundError(f"Historical source directory not found: {source}")
    if importlib.util.find_spec("confluent_kafka") is None:
        raise RuntimeError(
            "The current Python interpreter lacks confluent-kafka. Install the "
            "repository dependency with: python -m pip install -r producer/requirements.txt"
        )

    _require_clean_worktree()
    _require_services()
    versions = _runtime_versions()
    run_dir = REPO_ROOT / RESULTS_ROOT / config.slug / config.run_id
    if run_dir.exists():
        raise FileExistsError(
            f"Run artifact directory already exists; choose a fresh run_id: {run_dir}"
        )
    run_dir.mkdir(parents=True, exist_ok=False)

    artifacts = _artifact_paths(config, run_dir)
    manifest = _make_manifest(
        config,
        git_commit=_git_commit(),
        source=source,
        artifact_paths=artifacts,
        versions=versions,
    )
    write_json(artifacts["manifest"], manifest)
    print(f"[RUN] scenario={config.scenario} run_id={config.run_id}")
    print(f"[RUN] topic={config.topic} partitions={config.topic_partitions}")

    try:
        _ensure_topic(config)
        print("[SIMULATOR] Replaying historical records")
        _run(_simulator_command(config, source, artifacts["simulator"]))
        simulator_summary = read_json(artifacts["simulator"])
        simulator_summary["git_commit"] = manifest["git_commit"]
        write_json(artifacts["simulator"], simulator_summary)
        if simulator_summary.get("run_id") != config.run_id:
            raise RuntimeError("Simulator emitted a different run_id.")
        if not simulator_summary.get("producer_delivery_complete"):
            raise RuntimeError("Simulator did not flush every Kafka message.")

        input_reference = _container_artifact_path(artifacts["simulator"])
        result_path = _container_artifact_path(artifacts["result"])
        if scenario == B0_CORRECTNESS:
            _spark_submit(
                JOBS_DIR / "bronze_weather_stream.py",
                config.spark_environment("bronze"),
                versions["spark"],
            )
            _spark_submit(
                JOBS_DIR / "silver_weather_stream.py",
                config.spark_environment("silver"),
                versions["spark"],
            )
            checker_environment = config.spark_environment("check_b0")
            checker_environment.update({
                "INPUT_REFERENCE_PATH": input_reference,
                "RESULT_PATH": result_path,
            })
            _spark_submit(
                JOBS_DIR / "check_benchmark_silver.py",
                checker_environment,
                versions["spark"],
            )
        else:
            _spark_submit(
                JOBS_DIR / "gold_kafka_watermark_benchmark.py",
                config.spark_environment("gold"),
                versions["spark"],
            )
            checker_environment = config.spark_environment("check_watermark")
            checker_environment.update({
                "INPUT_REFERENCE_PATH": input_reference,
                "RESULT_PATH": result_path,
            })
            _spark_submit(
                JOBS_DIR / "check_gold_watermark_benchmark.py",
                checker_environment,
                versions["spark"],
            )

        result = read_json(artifacts["result"])
        if result.get("run_id") != config.run_id:
            raise RuntimeError("Checker result run_id does not match the run.")
        manifest["status"] = result.get("status", "UNKNOWN")
        manifest["finished_at"] = utc_now()
        manifest["result_metrics"] = result.get("metrics", {})
        manifest["assertions"] = result.get("assertions", {})
        write_json(artifacts["manifest"], manifest)
        print(f"[RUN] status={manifest['status']}")
        print(f"[RUN] manifest={artifacts['manifest'].relative_to(REPO_ROOT)}")
        print(f"[RUN] result={artifacts['result'].relative_to(REPO_ROOT)}")
        return 0 if manifest["status"] == "PASS" else 2
    except Exception as exc:
        manifest["status"] = "FAILED"
        manifest["finished_at"] = utc_now()
        manifest["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        write_json(artifacts["manifest"], manifest)
        raise


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a correctness, fixed-throughput, or scalability benchmark."
    )
    parser.add_argument(
        "--scenario",
        required=True,
        choices=("b0", "wm10m", "throughput", "scalability"),
        help="b0 correctness, wm10m correctness, fixed throughput, or scalability.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional unique run identity. A timestamped UUID is generated by default.",
    )
    parser.add_argument(
        "--source",
        default=str(REPO_ROOT / "data" / "historical" / "raw"),
        help="Historical JSONL.GZ source directory.",
    )
    parser.add_argument(
        "--max-source-events",
        type=int,
        default=None,
        help="Bounded source records (10,000 for correctness; 50,000 for throughput by default).",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=None,
        help="Requested simulator message rate; this is load generation, not throughput measurement.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )
    parser.add_argument(
        "--calibrate-load-generator",
        action="store_true",
        help="Run the bounded simulator-only calibration at 100/500/1,000/2,000/5,000 msg/s.",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=2,
        help="Independent runs at the selected throughput rate.",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=DEFAULT_WARMUP_SECONDS,
        help="Minimum elapsed warm-up before steady-state metrics; both queries must also complete the minimum batch count.",
    )
    parser.add_argument(
        "--warmup-min-batches",
        type=int,
        default=DEFAULT_WARMUP_MIN_BATCHES,
        help="Minimum completed micro-batches required for each main query before steady-state selection.",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=300.0,
        help="Maximum seconds to wait after replay ends for Bronze/Silver and Kafka lag to drain.",
    )
    parser.add_argument("--partitions", type=int, default=1, help="Kafka partitions for a scalability run.")
    parser.add_argument("--bronze-cores", type=int, default=1, help="spark.cores.max for Bronze.")
    parser.add_argument("--silver-cores", type=int, default=1, help="spark.cores.max for Silver.")
    parser.add_argument("--workers", type=int, default=1, help="Expected Spark worker count.")
    parser.add_argument("--worker-cores", type=int, default=4, help="Cores available on each Spark worker.")
    parser.add_argument("--worker-memory-mb", type=int, default=4096, help="Memory available on each Spark worker, in MiB.")
    parser.add_argument("--executor-cores", type=int, default=1, help="spark.executor.cores requested by each streaming app.")
    parser.add_argument("--executor-memory-mb", type=int, default=1024, help="spark.executor.memory requested by each streaming app, in MiB.")
    parser.add_argument("--shuffle-partitions", type=int, default=1, help="spark.sql.shuffle.partitions.")
    parser.add_argument("--trigger-interval", default="1 second", help="Structured Streaming trigger interval.")
    parser.add_argument("--config-id", default=None, help="Optional canonical pP-bB-sS-wW ID, validated against supplied dimensions.")
    parser.add_argument("--experiment-id", default=None, help="Optional experiment group ID.")
    parser.add_argument("--calibration-rates", nargs="+", type=int, default=None, help="Optional simulator calibration targets (maximum 10,000 msg/s).")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        raise SystemExit(run_benchmark(parse_args()))
    except subprocess.CalledProcessError as exc:
        command = " ".join(str(part) for part in exc.cmd)
        print(
            f"Benchmark command failed with exit code {exc.returncode}: {command}",
            file=sys.stderr,
        )
        raise SystemExit(exc.returncode or 1)
