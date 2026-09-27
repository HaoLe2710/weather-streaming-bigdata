"""Run one isolated correctness benchmark from Windows, PowerShell, or CI."""

import argparse
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid


REPO_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = REPO_ROOT / "spark" / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from benchmark_config import (  # noqa: E402
    B0_CORRECTNESS,
    BenchmarkConfig,
    normalize_scenario,
    read_json,
    utc_now,
    write_json,
)


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
        ["git", "status", "--porcelain"],
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
) -> None:
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
        _container_artifact_path(script),
    ])
    print(f"[SPARK] Running {script.name}")
    _run(command)


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
    return {
        "manifest": run_dir / "manifest.json",
        "result": run_dir / "result.json",
        "simulator": run_dir / "simulator.json",
    }


def _make_manifest(
    config: BenchmarkConfig,
    *,
    git_commit: str,
    source: Path,
    artifact_paths: dict[str, Path],
    versions: dict[str, str | None],
) -> dict:
    return {
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


def run_benchmark(args) -> int:
    scenario = normalize_scenario(args.scenario)
    run_id = args.run_id or _new_run_id()
    config = BenchmarkConfig.for_scenario(
        scenario,
        run_id,
        source_record_limit=args.max_source_events,
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
        description="Run an isolated B0 or Kafka-direct watermark benchmark."
    )
    parser.add_argument(
        "--scenario",
        required=True,
        choices=("b0", "wm10m"),
        help="b0 correctness or the final 10-minute Kafka watermark scenario.",
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
        default=10_000,
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
