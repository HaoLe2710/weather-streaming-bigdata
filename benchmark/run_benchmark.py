"""Run one isolated correctness benchmark from Windows, PowerShell, or CI."""

import argparse
from datetime import datetime, timezone
import importlib.util
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen
import uuid


REPO_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = REPO_ROOT / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from benchmark_config import (  # noqa: E402
    B0_CORRECTNESS,
    THROUGHPUT_BASELINE,
    BenchmarkConfig,
    normalize_scenario,
    read_json,
    utc_now,
    write_json,
)
from performance_metrics import (  # noqa: E402
    add_warmup_markers,
    aggregate_repetitions,
    classify_capacity,
    kafka_source_offsets,
    lag_peak,
    latest_spark_end_offsets,
    production_peak_lag,
    read_jsonl,
    resource_peaks,
    summarize_progress,
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
    if not re.search(r"PartitionCount:\s*1\b", described):
        raise RuntimeError(
            f"Kafka topic {config.topic} was not created with one partition:\n"
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
    command = ["docker", "compose", "exec", "-T"]
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
    if fixed_throughput_baseline:
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


def _spark_active_apps(run_id: str) -> dict:
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
    return {"apps": apps, "workers": workers, "other_active_apps": other_apps}


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
    processes: dict[str, subprocess.Popen], stop_signal: Path, timeout_seconds: float = 90.0
) -> dict[str, int | None]:
    stop_signal.parent.mkdir(parents=True, exist_ok=True)
    stop_signal.touch(exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline and any(
        process.poll() is None for process in processes.values()
    ):
        time.sleep(0.25)
    for process in processes.values():
        if process.poll() is None:
            process.terminate()
    return_codes = {}
    for stage, process in processes.items():
        try:
            return_codes[stage] = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            return_codes[stage] = process.wait(timeout=15)
    return return_codes


def _merge_progress_files(
    progress_paths: dict[str, Path],
    output_path: Path,
    warmup_batches: int,
) -> list[dict]:
    rows = []
    for path in progress_paths.values():
        rows.extend(read_jsonl(path))
    rows = add_warmup_markers(rows, warmup_batches)
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
    experiments_root = REPO_ROOT / RESULTS_ROOT / "throughput" / "experiments"
    candidates = sorted(
        experiments_root.glob("*/summary.json"),
        key=lambda path: path.parent.name,
        reverse=True,
    ) if experiments_root.exists() else []
    for path in candidates:
        try:
            summary = read_json(path)
        except (OSError, ValueError):
            continue
        if (
            summary.get("experiment_type") != "load_generator_calibration"
            or summary.get("git_commit") != git_commit
        ):
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
                return item, path
    raise RuntimeError(
        f"No generator calibration for {rate} msg/s matches Git commit {git_commit}. "
        "Run `python benchmark/run_benchmark.py --scenario throughput --calibrate-load-generator` first."
    )


def run_load_generator_calibration(args) -> int:
    if args.max_source_events not in (None, 10_000):
        raise ValueError("Load calibration uses a fixed 10,000 source events per target rate.")
    _require_clean_worktree()
    _require_services()
    versions = _runtime_versions()
    git_commit = _git_commit()
    experiment_id = _new_run_id()
    experiment_dir = REPO_ROOT / RESULTS_ROOT / "throughput" / "experiments" / experiment_id
    experiment_dir.mkdir(parents=True, exist_ok=False)
    source = Path(args.source).resolve()
    if not source.is_dir():
        raise FileNotFoundError(f"Historical source directory not found: {source}")
    rates = []
    tolerance = 0.10

    for requested_rate in (100, 500, 1_000, 2_000, 5_000):
        run_id = _new_run_id()
        config = BenchmarkConfig.for_scenario(
            THROUGHPUT_BASELINE,
            run_id,
            source_record_limit=10_000,
            requested_replay_rate=requested_rate,
            seed=args.seed,
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
        manifest["throughput_baseline"]["spark_pipeline_executed"] = False
        manifest["throughput_baseline"]["calibration_tolerance_fraction"] = tolerance
        manifest["artifacts"].pop("spark_progress", None)
        manifest["artifacts"].pop("resource_metrics", None)
        manifest["artifacts"].pop("kafka_lag", None)
        manifest["artifacts"].pop("delta_metrics", None)
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
                "scenario": THROUGHPUT_BASELINE,
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
        "schema_version": 1,
        "experiment_type": "load_generator_calibration",
        "experiment_id": experiment_id,
        "created_at": utc_now(),
        "git_commit": git_commit,
        "scenario": THROUGHPUT_BASELINE,
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
    warmup_batches: int,
    drain_timeout_seconds: float,
) -> dict:
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
    manifest["measurement_mode"] = "fixed_configuration_throughput"
    manifest["load_generator_calibration"] = {
        "summary": calibration_path.relative_to(REPO_ROOT).as_posix(),
        "calibrated_rate": calibration.get("requested_rate_msgs_sec"),
        "calibration_actual_rate_msgs_sec": calibration.get("actual_generated_msgs_sec"),
        "tolerance_percent": calibration.get("calibration_tolerance_percent"),
    }
    manifest["warmup_batches_excluded_per_query"] = warmup_batches
    manifest["saturation_definition"] = {
        "sustainable": "all producer messages counted by Bronze and Silver, final sampled Kafka lag is zero, and both stream applications exit normally",
        "under_capacity_backlog_limit_records": max(1, math.ceil(config.requested_replay_rate * 2)),
        "under_capacity_backlog_limit_reason": "two one-second trigger intervals of backlog",
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
    simulator_summary = None
    runtime_observation = None
    failure_reason = None
    failure_kind = None
    drain_complete = False
    drain_seconds = None
    stream_started_monotonic = None
    run_finished_monotonic = None
    delta_metrics = None
    metrics_error = None
    log_tails = {}

    with tempfile.TemporaryDirectory(prefix="weather-throughput-") as temp_dir:
        temp_root = Path(temp_dir)
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
            manifest["throughput_baseline"]["bronze_delta_initialized_empty_before_streaming"] = True
            write_json(artifacts["manifest"], manifest)
            container_limits = manifest["throughput_baseline"]["docker_container_limits"]
            configured_memory = {
                name: item.get("configured_memory_limit_mb")
                for name, item in container_limits.items()
            }
            resource_sampler = DockerResourceSampler(
                artifacts["resource_metrics"],
                interval_seconds=1.0,
                configured_memory_limits_mb=configured_memory,
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
            runtime_observation = _spark_active_apps(config.run_id)
            manifest["observed_spark_runtime"] = runtime_observation
            runtime_errors = []
            if runtime_observation.get("error"):
                runtime_errors.append(runtime_observation["error"])
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
            workers = runtime_observation.get("workers", [])
            if len(workers) != 1:
                runtime_errors.append(
                    f"Expected one Spark worker; observed {len(workers)}."
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
                raise RuntimeError(
                    "Spark runtime does not match the fixed throughput baseline: "
                    + "; ".join(runtime_errors)
                    + ". Observed: "
                    + json.dumps(runtime_observation, sort_keys=True)
                )
            stream_started_monotonic = time.monotonic()
            print(
                f"[THROUGHPUT] streaming apps started; requested="
                f"{config.requested_replay_rate} msg/s, events={config.source_record_limit:,}"
            )

            _run(
                _simulator_command(config, source, artifacts["simulator"]),
                timeout=max(
                    900,
                    int(config.source_record_limit / max(config.requested_replay_rate, 1) * 3) + 180,
                ),
            )
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
                process_return_codes = _stop_stream_processes(processes, stop_signal)
            except Exception as exc:
                process_return_codes = {
                    stage: process.poll() for stage, process in processes.items()
                }
                failure_reason = failure_reason or f"Stream shutdown failed: {type(exc).__name__}: {exc}"
                failure_kind = "stream_failure"
            if resource_sampler is not None:
                resource_sampler.stop()
            if lag_sampler is not None:
                lag_sampler.stop()
            stop_signal.unlink(missing_ok=True)
            run_finished_monotonic = time.monotonic()
            for stage, log_path in log_paths.items():
                if log_path.exists():
                    log_tails[stage] = _tail_text(log_path)

    progress_rows = _merge_progress_files(
        progress_paths,
        artifacts["spark_progress"],
        warmup_batches,
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

    progress_summary = summarize_progress(progress_rows)
    lag_samples = read_jsonl(artifacts["kafka_lag"])
    resource_samples = read_jsonl(artifacts["resource_metrics"])
    maximum_lag, final_lag = lag_peak(lag_samples)
    replay_peak_lag = production_peak_lag(
        lag_samples,
        simulator_summary.get("generation_start_time") if simulator_summary else None,
        simulator_summary.get("generation_end_time") if simulator_summary else None,
    )
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
    peaks = resource_peaks(resource_samples)
    worker_peaks = peaks.get("weather-spark-worker", {})
    broker_peaks = peaks.get("weather-kafka", {})

    produced = int((simulator_summary or {}).get("kafka_messages", 0))
    bronze_processed = int(progress_summary.get("bronze_input_records", 0))
    silver_processed = int(progress_summary.get("silver_input_records", 0))
    bronze_delta = int((delta_metrics or {}).get("bronze_records", 0))
    all_processed = (
        produced > 0
        and bronze_processed >= produced
        and silver_processed >= produced
        and bronze_delta >= produced
        and final_lag == 0
    )
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
    terminated_failures = [
        row for row in progress_rows
        if row.get("event_type") == "query_terminated" and row.get("exception")
    ]
    process_failed = failure_kind in {"run_failure", "stream_failure"} or any(
        process_return_codes.get(stage) not in (0, None)
        for stage in ("bronze", "silver")
    ) or bool(terminated_failures)
    resource_failed = any(
        not isinstance(container_peaks.get(metric), (int, float))
        for container_peaks in (worker_peaks, broker_peaks)
        for metric in ("peak_cpu_percent", "peak_memory_mb")
    )
    latency_count = (delta_metrics or {}).get("latency_count")
    latency_complete = isinstance(latency_count, int) and latency_count >= produced
    measurement_failed = bool(metrics_error) or not any(
        isinstance(row.get("lag_records"), int) for row in lag_samples
    ) or final_lag is None or resource_failed or not latency_complete
    capacity, capacity_reason = classify_capacity(
        failed=process_failed or measurement_failed,
        all_records_processed=all_processed,
        final_lag=final_lag,
        production_peak_lag=replay_peak_lag,
        requested_rate=config.requested_replay_rate,
        trigger_interval_seconds=1.0,
    )
    if simulator_summary is not None and not generator_valid:
        status = "INVALID_GENERATOR"
        capacity = None
        capacity_reason = "This run's measured generator rate fell outside its calibrated +/-10% band."
    elif process_failed or measurement_failed:
        status = "FAILED"
        if metrics_error:
            failure_reason = failure_reason or f"Delta metrics collection failed: {metrics_error}"
    elif not drain_complete or not all_processed:
        status = "SATURATED"
        if failure_kind == "drain_timeout":
            capacity = "SATURATED"
            capacity_reason = failure_reason
    else:
        status = capacity

    bronze_summary = progress_summary.get("bronze", {})
    silver_summary = progress_summary.get("silver", {})
    duration_seconds = (
        run_finished_monotonic - stream_started_monotonic
        if run_finished_monotonic is not None and stream_started_monotonic is not None
        else None
    )
    result = {
        "schema_version": 1,
        "scenario": THROUGHPUT_BASELINE,
        "run_id": config.run_id,
        "git_commit": git_commit,
        "status": status,
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
        "drain_complete": drain_complete,
        "bronze_input_records": bronze_processed,
        "silver_input_records": silver_processed,
        "avg_input_rows_per_sec": bronze_summary.get("avg_input_rows_per_sec"),
        "peak_input_rows_per_sec": bronze_summary.get("peak_input_rows_per_sec"),
        "avg_processed_rows_per_sec": bronze_summary.get("avg_processed_rows_per_sec"),
        "peak_processed_rows_per_sec": bronze_summary.get("peak_processed_rows_per_sec"),
        "silver_avg_processed_rows_per_sec": silver_summary.get("avg_processed_rows_per_sec"),
        "silver_peak_processed_rows_per_sec": silver_summary.get("peak_processed_rows_per_sec"),
        "included_bronze_batches": bronze_summary.get("included_batches"),
        "included_silver_batches": silver_summary.get("included_batches"),
        "warmup_batches_excluded_per_query": warmup_batches,
        "avg_batch_duration_ms": bronze_summary.get("avg_batch_duration_ms"),
        "p95_batch_duration_ms": bronze_summary.get("p95_batch_duration_ms"),
        "max_batch_duration_ms": bronze_summary.get("max_batch_duration_ms"),
        "batch_duration_components": bronze_summary.get("duration_components"),
        "max_kafka_lag": maximum_lag,
        "max_kafka_lag_during_production": replay_peak_lag,
        "final_kafka_lag": final_lag,
        "kafka_topic": config.topic,
        "kafka_partition_count": config.topic_partitions,
        "kafka_lag_definition": "For each sample, sum(max(0, Kafka high/latest offset - Spark Kafka source end offset)) across the run topic partitions.",
        **offsets,
        "final_kafka_latest_offsets": final_kafka_offsets,
        "latency_count": (delta_metrics or {}).get("latency_count"),
        "latency_min_ms": (delta_metrics or {}).get("latency_min_ms"),
        "latency_avg_ms": (delta_metrics or {}).get("latency_avg_ms"),
        "latency_p50_ms": (delta_metrics or {}).get("latency_p50_ms"),
        "latency_p95_ms": (delta_metrics or {}).get("latency_p95_ms"),
        "latency_p99_ms": (delta_metrics or {}).get("latency_p99_ms"),
        "latency_max_ms": (delta_metrics or {}).get("latency_max_ms"),
        "latency_definition": (delta_metrics or {}).get("latency_definition"),
        "latency_percentile_method": (delta_metrics or {}).get("latency_percentile_method"),
        "spark_worker_peak_cpu_percent": worker_peaks.get("peak_cpu_percent"),
        "spark_worker_peak_memory_mb": worker_peaks.get("peak_memory_mb"),
        "broker_peak_cpu_percent": broker_peaks.get("peak_cpu_percent"),
        "broker_peak_memory_mb": broker_peaks.get("peak_memory_mb"),
        "resource_sample_count": len(resource_samples),
        "kafka_lag_sample_count": len(lag_samples),
        "fixed_infrastructure": manifest["throughput_baseline"],
        "observed_spark_runtime": runtime_observation,
        "stream_process_return_codes": process_return_codes,
        "errors": {
            "run": failure_reason,
            "metrics": metrics_error,
            "resource_sampler": resource_sampler.last_error if resource_sampler else "sampler did not start",
            "kafka_lag_sampler": lag_sampler.last_error if lag_sampler else "sampler did not start",
            "spark_log_tails": log_tails,
        },
        "null_metric_reasons": {
            "latency": metrics_error or (
                f"Latency was measured for {latency_count or 0} of {produced} produced messages."
                if not latency_complete
                else None
            ),
            "kafka_lag": (
                "No comparable Kafka latest offsets and Spark source end offsets were captured."
                if maximum_lag is None or final_lag is None
                else None
            ),
            "resources": (
                "Docker stats did not provide numeric CPU and memory peaks for both containers."
                if resource_failed
                else None
            ),
        },
    }
    write_json(artifacts["result"], result)
    manifest["status"] = status
    manifest["finished_at"] = utc_now()
    manifest["result_metrics"] = result
    if failure_reason:
        manifest["error"] = {"type": failure_kind, "message": failure_reason}
    write_json(artifacts["manifest"], manifest)
    print(
        f"[THROUGHPUT] run={config.run_id} status={status} "
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
    if args.warmup_batches < 0:
        raise ValueError("--warmup-batches must be zero or greater.")
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
            warmup_batches=args.warmup_batches,
            drain_timeout_seconds=args.drain_timeout,
        )
        run_results.append(result)
        if result.get("status") in {"FAILED", "INVALID_GENERATOR"}:
            break
        if result.get("status") == "SATURATED":
            print("[THROUGHPUT] clear saturation; stopping further repetitions at this rate")
            break

    aggregate = aggregate_repetitions(run_results)
    summary = {
        "schema_version": 1,
        "experiment_type": "throughput_repetitions",
        "experiment_id": experiment_id,
        "created_at": utc_now(),
        "scenario": THROUGHPUT_BASELINE,
        "git_commit": git_commit,
        "source_record_limit": source_limit,
        "requested_repetitions": args.repetitions,
        "completed_repetitions": len(run_results),
        "warmup_batches_excluded_per_query": args.warmup_batches,
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
    if config.scenario == THROUGHPUT_BASELINE:
        artifacts.update({
            "spark_progress": run_dir / "spark_progress.jsonl",
            "spark_progress_bronze_temp": run_dir / "spark_progress_bronze.tmp.jsonl",
            "spark_progress_silver_temp": run_dir / "spark_progress_silver.tmp.jsonl",
            "resource_metrics": run_dir / "resource_metrics.jsonl",
            "kafka_lag": run_dir / "kafka_lag.jsonl",
            "delta_metrics": run_dir / "delta_metrics.json",
        })
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
                    "{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}",
                    name,
                ],
                capture_output=True,
                timeout=15,
            )
            memory_bytes, nano_cpus = (int(part) for part in completed.stdout.split())
            limits[name] = {
                "configured_memory_limit_mb": memory_bytes // (1024 * 1024) if memory_bytes else None,
                "configured_cpu_limit_cores": nano_cpus / 1_000_000_000 if nano_cpus else None,
            }
        except Exception as exc:
            limits[name] = {
                "configured_memory_limit_mb": None,
                "configured_cpu_limit_cores": None,
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
    containers = _container_hard_limits(["weather-spark-worker", "weather-kafka"])
    return {
        "measurement_scope": "Kafka -> Bronze -> Silver and DLQ; Gold excluded",
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
            "worker_count": 1,
            "worker_cores_available": worker_cores,
            "worker_memory_mb_available": worker_memory,
            "streaming_applications": 2,
            "executor_cores_per_application": 1,
            "cores_max_per_application": 1,
            "executor_memory_mb_per_application": 1024,
            "total_requested_executor_cores": 2,
            "shuffle_partitions": 1,
            "trigger_interval_seconds": 1,
            "deployment_mode": "client",
            "configured_container_limits": containers.get("weather-spark-worker"),
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
    return manifest


def run_benchmark(args) -> int:
    scenario = normalize_scenario(args.scenario)
    if args.max_source_events is not None and args.max_source_events <= 0:
        raise ValueError("--max-source-events must be a positive integer.")
    if scenario == THROUGHPUT_BASELINE:
        return run_throughput_benchmark(args)
    if args.calibrate_load_generator:
        raise ValueError("--calibrate-load-generator requires --scenario throughput.")
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
        description="Run an isolated correctness or fixed-configuration throughput benchmark."
    )
    parser.add_argument(
        "--scenario",
        required=True,
        choices=("b0", "wm10m", "throughput"),
        help="b0 correctness, wm10m correctness, or the fixed throughput baseline.",
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
        default=1_000,
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
        "--warmup-batches",
        type=int,
        default=2,
        help="Per-query micro-batches retained in raw progress and excluded from summaries.",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=300.0,
        help="Maximum seconds to wait after replay ends for Bronze/Silver and Kafka lag to drain.",
    )
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
