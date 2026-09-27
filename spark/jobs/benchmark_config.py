"""Shared run identity, scenario defaults, and isolated benchmark paths.

This module has no Spark dependency so the host-side runner and Spark jobs can
use the same path and scenario contract.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import PurePosixPath
import re
from typing import Any


B0_CORRECTNESS = "B0_CORRECTNESS"
KAFKA_WATERMARK_WM10M = "KAFKA_WATERMARK_WM10M"
THROUGHPUT_BASELINE = "THROUGHPUT_BASELINE"
SCALABILITY_BENCHMARK = "SCALABILITY_BENCHMARK"

DEFAULT_DATA_ROOT = "/opt/project/data/benchmark/runs"
DEFAULT_CHECKPOINT_ROOT = "/opt/project/data/checkpoints/benchmark/runs"
DEFAULT_RESULTS_ROOT = "/opt/project/results/benchmarks"

SCENARIO_SLUGS = {
    B0_CORRECTNESS: "b0",
    KAFKA_WATERMARK_WM10M: "wm10m",
    THROUGHPUT_BASELINE: "throughput",
    SCALABILITY_BENCHMARK: "scalability",
}


def normalize_scenario(value: str) -> str:
    normalized = value.strip().upper().replace("-", "_")
    aliases = {
        "B0": B0_CORRECTNESS,
        "B0_CORRECTNESS": B0_CORRECTNESS,
        "WM10M": KAFKA_WATERMARK_WM10M,
        "KAFKA_WATERMARK_WM10M": KAFKA_WATERMARK_WM10M,
        "THROUGHPUT": THROUGHPUT_BASELINE,
        "THROUGHPUT_BASELINE": THROUGHPUT_BASELINE,
        "SCALABILITY": SCALABILITY_BENCHMARK,
        "SCALABILITY_BENCHMARK": SCALABILITY_BENCHMARK,
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported benchmark scenario: {value!r}. "
            "Choose B0_CORRECTNESS, KAFKA_WATERMARK_WM10M, THROUGHPUT_BASELINE, or SCALABILITY_BENCHMARK."
        ) from exc


def _normalise_posix(path: str) -> str:
    return str(PurePosixPath(path))


def _run_topic(slug: str, run_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,47}", run_id):
        raise ValueError(
            "run_id must be 1-48 Kafka-safe letters, digits, '_' or '-'."
        )
    topic_run_id = run_id.lower()
    return f"weather.bench.{slug}.{topic_run_id}"


@dataclass(frozen=True)
class RunPaths:
    root: str
    bronze: str | None
    silver: str | None
    dlq: str | None
    gold: str | None
    bronze_checkpoint: str | None
    silver_checkpoint: str | None
    dlq_checkpoint: str | None
    gold_checkpoint: str | None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "root": self.root,
            "bronze": self.bronze,
            "silver": self.silver,
            "dlq": self.dlq,
            "gold": self.gold,
            "bronze_checkpoint": self.bronze_checkpoint,
            "silver_checkpoint": self.silver_checkpoint,
            "dlq_checkpoint": self.dlq_checkpoint,
            "gold_checkpoint": self.gold_checkpoint,
        }


@dataclass(frozen=True)
class BenchmarkConfig:
    scenario: str
    run_id: str
    topic: str
    topic_partitions: int
    paths: RunPaths
    source_record_limit: int
    requested_replay_rate: int
    seed: int
    duplicate_rate: float
    invalid_rate: float
    late_rate: float
    out_of_order_rate: float
    late_delay_events: int
    out_of_order_max_delay: int
    watermark: str | None
    window_duration: str | None
    max_offsets_per_trigger: int | None
    config_id: str | None = None
    experiment_id: str | None = None
    requested_worker_count: int = 1
    worker_cores_each: int = 4
    worker_memory_mb_each: int = 4096
    bronze_cores_max: int = 1
    silver_cores_max: int = 1
    executor_cores: int = 1
    executor_memory_mb: int = 1024
    shuffle_partitions: int = 1
    trigger_interval: str = "1 second"

    @property
    def slug(self) -> str:
        return SCENARIO_SLUGS[self.scenario]

    @classmethod
    def for_scenario(
        cls,
        scenario: str,
        run_id: str,
        *,
        topic: str | None = None,
        data_root: str = DEFAULT_DATA_ROOT,
        checkpoint_root: str = DEFAULT_CHECKPOINT_ROOT,
        source_record_limit: int = 10_000,
        requested_replay_rate: int = 1_000,
        seed: int = 42,
        duplicate_rate: float | None = None,
        invalid_rate: float | None = None,
        late_rate: float | None = None,
        out_of_order_rate: float | None = None,
        late_delay_events: int | None = None,
        out_of_order_max_delay: int | None = None,
        watermark: str | None = None,
        window_duration: str | None = None,
        max_offsets_per_trigger: int | None = None,
        topic_partitions: int = 1,
        config_id: str | None = None,
        experiment_id: str | None = None,
        workers: int = 1,
        worker_cores_each: int = 4,
        worker_memory_mb_each: int = 4096,
        bronze_cores_max: int = 1,
        silver_cores_max: int = 1,
        executor_cores: int = 1,
        executor_memory_mb: int = 1024,
        shuffle_partitions: int = 1,
        trigger_interval: str = "1 second",
    ) -> "BenchmarkConfig":
        scenario = normalize_scenario(scenario)
        if source_record_limit <= 0:
            raise ValueError("source_record_limit must be a positive bounded value.")
        if requested_replay_rate < 0:
            raise ValueError("requested_replay_rate must be zero or greater.")
        if max_offsets_per_trigger is not None and max_offsets_per_trigger <= 0:
            raise ValueError("max_offsets_per_trigger must be positive.")
        if late_delay_events is not None and late_delay_events < 0:
            raise ValueError("late_delay_events must be zero or greater.")
        if out_of_order_max_delay is not None and out_of_order_max_delay <= 0:
            raise ValueError("out_of_order_max_delay must be positive.")
        requested_resources = {
            "topic_partitions": topic_partitions,
            "workers": workers,
            "worker_cores_each": worker_cores_each,
            "worker_memory_mb_each": worker_memory_mb_each,
            "bronze_cores_max": bronze_cores_max,
            "silver_cores_max": silver_cores_max,
            "executor_cores": executor_cores,
            "executor_memory_mb": executor_memory_mb,
            "shuffle_partitions": shuffle_partitions,
        }
        invalid_resources = [
            name for name, value in requested_resources.items()
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0
        ]
        if invalid_resources:
            raise ValueError(
                "Scalability resource settings must be positive integers: "
                + ", ".join(invalid_resources)
            )
        if not trigger_interval.strip():
            raise ValueError("trigger_interval must not be empty.")
        if scenario == SCALABILITY_BENCHMARK:
            if bronze_cores_max + silver_cores_max > workers * worker_cores_each:
                raise ValueError("Combined application core caps exceed available Spark worker cores.")
            expected_config_id = (
                f"p{topic_partitions}-b{bronze_cores_max}-s{silver_cores_max}-w{workers}"
            )
            config_id = config_id or expected_config_id
            if config_id != expected_config_id:
                raise ValueError(
                    f"config_id must match requested resources: expected {expected_config_id!r}."
                )
        elif any((
            topic_partitions != 1,
            workers != 1,
            worker_cores_each != 4,
            worker_memory_mb_each != 4096,
            bronze_cores_max != 1,
            silver_cores_max != 1,
            executor_cores != 1,
            executor_memory_mb != 1024,
            shuffle_partitions != 1,
            trigger_interval != "1 second",
            config_id is not None,
            experiment_id is not None,
        )):
            raise ValueError("Resource overrides are only supported for the scalability scenario.")
        slug = SCENARIO_SLUGS[scenario]
        topic = topic or _run_topic(slug, run_id)
        expected_topic = _run_topic(slug, run_id)
        if topic != expected_topic:
            raise ValueError(
                "Benchmark topics must be run-scoped. "
                f"Expected {expected_topic!r}, received {topic!r}."
            )

        data_root = _normalise_posix(data_root)
        checkpoint_root = _normalise_posix(checkpoint_root)
        run_root = _normalise_posix(
            str(PurePosixPath(data_root) / slug / run_id)
        )
        checkpoint_run_root = _normalise_posix(
            str(PurePosixPath(checkpoint_root) / slug / run_id)
        )

        def data_path(*parts: str) -> str:
            return _normalise_posix(str(PurePosixPath(run_root).joinpath(*parts)))

        def checkpoint_path(name: str) -> str:
            return _normalise_posix(
                str(PurePosixPath(checkpoint_run_root) / name)
            )

        if scenario == B0_CORRECTNESS:
            defaults = {
                "duplicate_rate": 0.02,
                "invalid_rate": 0.01,
                "late_rate": 0.05,
                "out_of_order_rate": 0.05,
                "late_delay_events": 240,
                "out_of_order_max_delay": 40,
            }
            paths = RunPaths(
                root=run_root,
                bronze=data_path("bronze", "weather_raw"),
                silver=data_path("silver", "weather_clean"),
                dlq=data_path("silver", "weather_invalid"),
                gold=None,
                bronze_checkpoint=checkpoint_path("bronze"),
                silver_checkpoint=checkpoint_path("silver"),
                dlq_checkpoint=checkpoint_path("dlq"),
                gold_checkpoint=None,
            )
            watermark = watermark or "10 minutes"
            window_duration = None
            max_offsets_per_trigger = None
        elif scenario == KAFKA_WATERMARK_WM10M:
            defaults = {
                "duplicate_rate": 0.0,
                "invalid_rate": 0.0,
                "late_rate": 0.10,
                "out_of_order_rate": 0.0,
                "late_delay_events": 4_000,
                "out_of_order_max_delay": 40,
            }
            paths = RunPaths(
                root=run_root,
                bronze=None,
                silver=None,
                dlq=None,
                gold=data_path("gold", "weather_window_1h"),
                bronze_checkpoint=None,
                silver_checkpoint=None,
                dlq_checkpoint=None,
                gold_checkpoint=checkpoint_path("gold"),
            )
            watermark = watermark or "10 minutes"
            window_duration = window_duration or "1 hour"
            max_offsets_per_trigger = max_offsets_per_trigger or 500
        else:
            defaults = {
                "duplicate_rate": 0.0,
                "invalid_rate": 0.0,
                "late_rate": 0.0,
                "out_of_order_rate": 0.0,
                "late_delay_events": 240,
                "out_of_order_max_delay": 40,
            }
            paths = RunPaths(
                root=run_root,
                bronze=data_path("bronze", "weather_raw"),
                silver=data_path("silver", "weather_clean"),
                dlq=data_path("silver", "weather_invalid"),
                gold=None,
                bronze_checkpoint=checkpoint_path("bronze"),
                silver_checkpoint=checkpoint_path("silver"),
                dlq_checkpoint=checkpoint_path("dlq"),
                gold_checkpoint=None,
            )
            watermark = watermark or "10 minutes"
            window_duration = None
            max_offsets_per_trigger = None

        rates = (
            defaults["duplicate_rate"]
            if duplicate_rate is None else duplicate_rate,
            defaults["invalid_rate"]
            if invalid_rate is None else invalid_rate,
            defaults["late_rate"]
            if late_rate is None else late_rate,
            defaults["out_of_order_rate"]
            if out_of_order_rate is None else out_of_order_rate,
        )
        if any(rate < 0.0 or rate > 1.0 for rate in rates):
            raise ValueError("Fault rates must be between 0.0 and 1.0.")
        if scenario in {THROUGHPUT_BASELINE, SCALABILITY_BENCHMARK} and any(rate != 0.0 for rate in rates):
            raise ValueError("Performance benchmarks require every injected fault rate to be zero.")

        return cls(
            scenario=scenario,
            run_id=run_id,
            topic=topic,
            topic_partitions=topic_partitions,
            paths=paths,
            source_record_limit=source_record_limit,
            requested_replay_rate=requested_replay_rate,
            seed=seed,
            duplicate_rate=(
                defaults["duplicate_rate"]
                if duplicate_rate is None else duplicate_rate
            ),
            invalid_rate=(
                defaults["invalid_rate"]
                if invalid_rate is None else invalid_rate
            ),
            late_rate=(
                defaults["late_rate"]
                if late_rate is None else late_rate
            ),
            out_of_order_rate=(
                defaults["out_of_order_rate"]
                if out_of_order_rate is None else out_of_order_rate
            ),
            late_delay_events=(
                defaults["late_delay_events"]
                if late_delay_events is None else late_delay_events
            ),
            out_of_order_max_delay=(
                defaults["out_of_order_max_delay"]
                if out_of_order_max_delay is None
                else out_of_order_max_delay
            ),
            watermark=watermark,
            window_duration=window_duration,
            max_offsets_per_trigger=max_offsets_per_trigger,
            config_id=config_id,
            experiment_id=experiment_id,
            requested_worker_count=workers,
            worker_cores_each=worker_cores_each,
            worker_memory_mb_each=worker_memory_mb_each,
            bronze_cores_max=bronze_cores_max,
            silver_cores_max=silver_cores_max,
            executor_cores=executor_cores,
            executor_memory_mb=executor_memory_mb,
            shuffle_partitions=shuffle_partitions,
            trigger_interval=trigger_interval,
        )

    @classmethod
    def from_environment(
        cls,
        *,
        required: bool = False,
        expected_scenario: str | None = None,
    ) -> "BenchmarkConfig | None":
        scenario = os.getenv("BENCHMARK_SCENARIO") or os.getenv("SCENARIO")
        run_id = os.getenv("RUN_ID")
        if not scenario and not run_id and not required:
            return None
        if not scenario or not run_id:
            raise ValueError(
                "Benchmark jobs require both BENCHMARK_SCENARIO and RUN_ID."
            )

        scenario = normalize_scenario(scenario)
        if expected_scenario and scenario != normalize_scenario(expected_scenario):
            raise ValueError(
                f"This job requires scenario {normalize_scenario(expected_scenario)}, "
                f"received {scenario}."
            )

        config = cls.for_scenario(
            scenario,
            run_id,
            topic=os.getenv("KAFKA_TOPIC") or None,
            data_root=os.getenv("BENCHMARK_DATA_ROOT", DEFAULT_DATA_ROOT),
            checkpoint_root=os.getenv(
                "BENCHMARK_CHECKPOINT_ROOT",
                DEFAULT_CHECKPOINT_ROOT,
            ),
            source_record_limit=int(
                os.getenv("SOURCE_RECORD_LIMIT", "10000")
            ),
            requested_replay_rate=int(
                os.getenv("REQUESTED_REPLAY_RATE", "1000")
            ),
            seed=int(os.getenv("SEED", "42")),
            duplicate_rate=_optional_float("DUPLICATE_RATE"),
            invalid_rate=_optional_float("INVALID_RATE"),
            late_rate=_optional_float("LATE_RATE"),
            out_of_order_rate=_optional_float("OUT_OF_ORDER_RATE"),
            late_delay_events=_optional_int("LATE_DELAY_EVENTS"),
            out_of_order_max_delay=_optional_int("OUT_OF_ORDER_MAX_DELAY"),
            watermark=os.getenv("WATERMARK_DELAY") or None,
            window_duration=os.getenv("WINDOW_DURATION") or None,
            max_offsets_per_trigger=_optional_int(
                "MAX_OFFSETS_PER_TRIGGER"
            ),
            topic_partitions=int(os.getenv("TOPIC_PARTITIONS", "1")),
            config_id=os.getenv("CONFIG_ID") or None,
            experiment_id=os.getenv("EXPERIMENT_ID") or None,
            workers=int(os.getenv("SPARK_WORKER_COUNT", "1")),
            worker_cores_each=int(os.getenv("SPARK_WORKER_CORES_EACH", "4")),
            worker_memory_mb_each=int(os.getenv("SPARK_WORKER_MEMORY_MB_EACH", "4096")),
            bronze_cores_max=int(os.getenv("BRONZE_CORES_MAX", "1")),
            silver_cores_max=int(os.getenv("SILVER_CORES_MAX", "1")),
            executor_cores=int(os.getenv("SPARK_EXECUTOR_CORES", "1")),
            executor_memory_mb=int(os.getenv("SPARK_EXECUTOR_MEMORY_MB", "1024")),
            shuffle_partitions=int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "1")),
            trigger_interval=os.getenv("TRIGGER_INTERVAL", "1 second"),
        )

        expected_paths = {
            "BRONZE_PATH": config.paths.bronze,
            "SILVER_PATH": config.paths.silver,
            "DLQ_PATH": config.paths.dlq,
            "GOLD_PATH": config.paths.gold,
        }
        for variable, expected in expected_paths.items():
            actual = os.getenv(variable)
            if actual and _normalise_posix(actual) != expected:
                raise ValueError(
                    f"{variable} must resolve from scenario/run_id. "
                    f"Expected {expected!r}, received {actual!r}."
                )

        expected_checkpoints = {
            "BRONZE_CHECKPOINT": config.paths.bronze_checkpoint,
            "SILVER_CHECKPOINT": config.paths.silver_checkpoint,
            "DLQ_CHECKPOINT": config.paths.dlq_checkpoint,
            "GOLD_CHECKPOINT": config.paths.gold_checkpoint,
            "CHECKPOINT_PATH": (
                config.paths.bronze_checkpoint
                if scenario in {B0_CORRECTNESS, THROUGHPUT_BASELINE, SCALABILITY_BENCHMARK}
                else config.paths.gold_checkpoint
            ),
        }
        for variable, expected in expected_checkpoints.items():
            actual = os.getenv(variable)
            if actual and _normalise_posix(actual) != expected:
                raise ValueError(
                    f"{variable} must resolve from scenario/run_id. "
                    f"Expected {expected!r}, received {actual!r}."
                )
        return config

    def simulator_arguments(self) -> list[str]:
        return [
            "--topic", self.topic,
            "--max-source-events", str(self.source_record_limit),
            "--rate", str(self.requested_replay_rate),
            "--seed", str(self.seed),
            "--duplicate-rate", str(self.duplicate_rate),
            "--invalid-rate", str(self.invalid_rate),
            "--late-rate", str(self.late_rate),
            "--out-of-order-rate", str(self.out_of_order_rate),
            "--late-delay-events", str(self.late_delay_events),
            "--out-of-order-max-delay", str(self.out_of_order_max_delay),
            "--run-id", self.run_id,
        ]

    def spark_environment(self, stage: str) -> dict[str, str]:
        environment = {
            "BENCHMARK_SCENARIO": self.scenario,
            "RUN_ID": self.run_id,
            "KAFKA_TOPIC": self.topic,
            "KAFKA_BOOTSTRAP_SERVERS": "broker:19092",
            "BENCHMARK_DATA_ROOT": self.paths.root.rsplit(
                f"/{self.slug}/{self.run_id}", 1
            )[0],
            "BENCHMARK_CHECKPOINT_ROOT": self._checkpoint_root(),
            "SOURCE_RECORD_LIMIT": str(self.source_record_limit),
            "REQUESTED_REPLAY_RATE": str(self.requested_replay_rate),
            "SEED": str(self.seed),
            "DUPLICATE_RATE": str(self.duplicate_rate),
            "INVALID_RATE": str(self.invalid_rate),
            "LATE_RATE": str(self.late_rate),
            "OUT_OF_ORDER_RATE": str(self.out_of_order_rate),
            "LATE_DELAY_EVENTS": str(self.late_delay_events),
            "OUT_OF_ORDER_MAX_DELAY": str(self.out_of_order_max_delay),
        }
        if self.scenario == SCALABILITY_BENCHMARK:
            stage_cores = (
                self.bronze_cores_max if stage == "bronze"
                else self.silver_cores_max if stage == "silver"
                else 1
            )
            environment.update({
                "CONFIG_ID": self.config_id or "",
                "EXPERIMENT_ID": self.experiment_id or "",
                "TOPIC_PARTITIONS": str(self.topic_partitions),
                "SPARK_WORKER_COUNT": str(self.requested_worker_count),
                "SPARK_WORKER_CORES_EACH": str(self.worker_cores_each),
                "SPARK_WORKER_MEMORY_MB_EACH": str(self.worker_memory_mb_each),
                "BRONZE_CORES_MAX": str(self.bronze_cores_max),
                "SILVER_CORES_MAX": str(self.silver_cores_max),
                "SPARK_CORES_MAX": str(stage_cores),
                "SPARK_EXECUTOR_CORES": str(self.executor_cores),
                "SPARK_EXECUTOR_MEMORY_MB": str(self.executor_memory_mb),
                "SPARK_SQL_SHUFFLE_PARTITIONS": str(self.shuffle_partitions),
                "TRIGGER_INTERVAL": self.trigger_interval,
            })
        if self.watermark:
            environment["WATERMARK_DELAY"] = self.watermark
        if self.window_duration:
            environment["WINDOW_DURATION"] = self.window_duration
        if self.max_offsets_per_trigger is not None:
            environment["MAX_OFFSETS_PER_TRIGGER"] = str(
                self.max_offsets_per_trigger
            )

        if stage == "bronze":
            self._require_paths("bronze", "bronze_checkpoint")
            environment.update({
                "BRONZE_PATH": self.paths.bronze,
                "CHECKPOINT_PATH": self.paths.bronze_checkpoint,
                "BRONZE_CHECKPOINT": self.paths.bronze_checkpoint,
                "STARTING_OFFSETS": "earliest",
                "AVAILABLE_NOW": "false" if self.scenario in {THROUGHPUT_BASELINE, SCALABILITY_BENCHMARK} else "true",
            })
            if self.scenario in {THROUGHPUT_BASELINE, SCALABILITY_BENCHMARK}:
                results_run_root = PurePosixPath(DEFAULT_RESULTS_ROOT) / self.slug / self.run_id
                environment.update({
                    "APP_NAME": f"WeatherBronzeStreaming-{self.run_id}",
                    "TRIGGER_INTERVAL": self.trigger_interval,
                    "SPARK_PROGRESS_PATH": str(results_run_root / "spark_progress_bronze.tmp.jsonl"),
                    "BENCHMARK_STOP_SIGNAL": str(results_run_root / "stop.signal"),
                    "QUERY_NAME": f"WeatherBronzeStreaming-{self.run_id}",
                })
        elif stage == "silver":
            self._require_paths(
                "bronze", "silver", "dlq", "silver_checkpoint", "dlq_checkpoint"
            )
            environment.update({
                "BRONZE_PATH": self.paths.bronze,
                "SILVER_PATH": self.paths.silver,
                "DLQ_PATH": self.paths.dlq,
                "SILVER_CHECKPOINT": self.paths.silver_checkpoint,
                "DLQ_CHECKPOINT": self.paths.dlq_checkpoint,
                "AVAILABLE_NOW": "false" if self.scenario in {THROUGHPUT_BASELINE, SCALABILITY_BENCHMARK} else "true",
            })
            if self.scenario in {THROUGHPUT_BASELINE, SCALABILITY_BENCHMARK}:
                results_run_root = PurePosixPath(DEFAULT_RESULTS_ROOT) / self.slug / self.run_id
                environment.update({
                    "APP_NAME": f"WeatherSilverStreaming-{self.run_id}",
                    "TRIGGER_INTERVAL": self.trigger_interval,
                    "SPARK_PROGRESS_PATH": str(results_run_root / "spark_progress_silver.tmp.jsonl"),
                    "BENCHMARK_STOP_SIGNAL": str(results_run_root / "stop.signal"),
                    "SILVER_QUERY_NAME": f"WeatherSilverStreaming-{self.run_id}-silver",
                    "DLQ_QUERY_NAME": f"WeatherSilverStreaming-{self.run_id}-dlq",
                })
        elif stage == "gold":
            self._require_paths("gold", "gold_checkpoint")
            environment.update({
                "GOLD_PATH": self.paths.gold,
                "CHECKPOINT_PATH": self.paths.gold_checkpoint,
                "GOLD_CHECKPOINT": self.paths.gold_checkpoint,
            })
        elif stage in {"check_b0", "check_watermark"}:
            if stage == "check_b0":
                if self.scenario != B0_CORRECTNESS:
                    raise ValueError("B0 checker requires B0_CORRECTNESS.")
                self._require_paths("bronze", "silver", "dlq")
                environment.update({
                    "BRONZE_PATH": self.paths.bronze,
                    "SILVER_PATH": self.paths.silver,
                    "DLQ_PATH": self.paths.dlq,
                })
            else:
                if self.scenario != KAFKA_WATERMARK_WM10M:
                    raise ValueError(
                        "Watermark checker requires KAFKA_WATERMARK_WM10M."
                    )
                self._require_paths("gold")
                environment["GOLD_PATH"] = self.paths.gold
        elif stage == "check_throughput":
            if self.scenario not in {THROUGHPUT_BASELINE, SCALABILITY_BENCHMARK}:
                raise ValueError("Streaming throughput metrics require a performance scenario.")
            self._require_paths("bronze", "silver", "dlq")
            environment.update({
                "BRONZE_PATH": self.paths.bronze,
                "SILVER_PATH": self.paths.silver,
                "DLQ_PATH": self.paths.dlq,
            })
        else:
            raise ValueError(f"Unknown Spark benchmark stage: {stage}")

        return environment

    def _checkpoint_root(self) -> str:
        candidate = self.paths.bronze_checkpoint or self.paths.gold_checkpoint
        if not candidate:
            raise ValueError("Run has no checkpoint path.")
        suffix = "/bronze" if self.paths.bronze_checkpoint else "/gold"
        return candidate.removesuffix(suffix).rsplit(
            f"/{self.slug}/{self.run_id}", 1
        )[0]

    def _require_paths(self, *names: str) -> None:
        for name in names:
            if getattr(self.paths, name) is None:
                raise ValueError(
                    f"Scenario {self.scenario} does not define required path {name}."
                )

    def simulator_defaults(self) -> dict[str, Any]:
        defaults = {
            "scenario": self.scenario,
            "run_id": self.run_id,
            "topic": self.topic,
            "topic_partitions": self.topic_partitions,
            "source_record_limit": self.source_record_limit,
            "requested_replay_rate": self.requested_replay_rate,
            "seed": self.seed,
            "duplicate_rate": self.duplicate_rate,
            "invalid_rate": self.invalid_rate,
            "late_rate": self.late_rate,
            "out_of_order_rate": self.out_of_order_rate,
            "late_delay_events": self.late_delay_events,
            "out_of_order_max_delay": self.out_of_order_max_delay,
            "watermark": self.watermark,
            "window_duration": self.window_duration,
            "max_offsets_per_trigger": self.max_offsets_per_trigger,
        }
        if self.scenario == SCALABILITY_BENCHMARK:
            defaults["scalability"] = {
                "config_id": self.config_id,
                "experiment_id": self.experiment_id,
                "topic_partitions": self.topic_partitions,
                "workers": self.requested_worker_count,
                "worker_cores_each": self.worker_cores_each,
                "worker_memory_mb_each": self.worker_memory_mb_each,
                "bronze_cores_max": self.bronze_cores_max,
                "silver_cores_max": self.silver_cores_max,
                "executor_cores": self.executor_cores,
                "executor_memory_mb": self.executor_memory_mb,
                "shuffle_partitions": self.shuffle_partitions,
                "trigger_interval": self.trigger_interval,
            }
        return defaults


def _optional_float(name: str) -> float | None:
    value = os.getenv(name)
    return float(value) if value is not None else None


def _optional_int(name: str) -> int | None:
    value = os.getenv(name)
    return int(value) if value is not None else None


def write_json(path: str | os.PathLike[str], payload: dict[str, Any]) -> None:
    destination = os.fspath(path)
    parent = os.path.dirname(destination)
    if parent:
        os.makedirs(parent, exist_ok=True)
    temporary = destination + ".tmp"
    with open(temporary, "w", encoding="utf-8", newline="\n") as output:
        json.dump(payload, output, indent=2, sort_keys=True)
        output.write("\n")
    os.replace(temporary, destination)


def read_json(path: str | os.PathLike[str]) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}.")
    return value


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
