"""Host-side Docker resource and Kafka offset samplers for throughput runs."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import subprocess
import threading
import time
from typing import Any

from performance_metrics import latest_spark_end_offsets, read_jsonl


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        stream.flush()


def _memory_mb(value: str) -> float | None:
    match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)\s*", value)
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2).lower()
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
    factor = factors.get(unit)
    return amount * factor if factor is not None else None


def _parse_memory_usage(value: str) -> tuple[float | None, float | None]:
    parts = value.split("/", maxsplit=1)
    if len(parts) != 2:
        return None, None
    return _memory_mb(parts[0]), _memory_mb(parts[1])


class DockerResourceSampler:
    def __init__(
        self,
        output_path: str | Path,
        *,
        interval_seconds: float = 1.0,
        configured_memory_limits_mb: dict[str, float | None] | None = None,
    ):
        self.path = Path(output_path)
        self.interval_seconds = interval_seconds
        self.configured_memory_limits_mb = configured_memory_limits_mb or {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self.sample_count = 0
        self.last_error: str | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="docker-resource-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)

    def _run(self) -> None:
        containers = {
            "weather-spark-worker": "spark_worker",
            "weather-kafka": "kafka_broker",
        }
        while not self._stop.is_set():
            captured = _utc_now()
            try:
                completed = subprocess.run(
                    [
                        "docker", "stats", "--no-stream", "--format", "{{json .}}",
                        *containers.keys(),
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=8,
                )
                seen: set[str] = set()
                for line in completed.stdout.splitlines():
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    name = payload.get("Name") or payload.get("Container")
                    service = containers.get(name)
                    if not service:
                        continue
                    usage_mb, reported_limit_mb = _parse_memory_usage(payload.get("MemUsage", ""))
                    cpu_text = str(payload.get("CPUPerc", "")).rstrip("%")
                    memory_text = str(payload.get("MemPerc", "")).rstrip("%")
                    try:
                        cpu_percent = float(cpu_text)
                    except ValueError:
                        cpu_percent = None
                    try:
                        memory_percent = float(memory_text)
                    except ValueError:
                        memory_percent = None
                    _append_jsonl(self.path, {
                        "timestamp_utc": captured,
                        "container": name,
                        "service": service,
                        "cpu_percent": cpu_percent,
                        "memory_usage_mb": usage_mb,
                        "memory_limit_mb": reported_limit_mb,
                        "configured_memory_limit_mb": self.configured_memory_limits_mb.get(name),
                        "memory_percent": memory_percent,
                        "source": "docker stats --no-stream",
                    })
                    seen.add(name)
                missing = sorted(set(containers) - seen)
                if missing:
                    raise RuntimeError("docker stats omitted container(s): " + ", ".join(missing))
                with self._lock:
                    self.sample_count += len(seen)
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
                _append_jsonl(self.path, {
                    "timestamp_utc": captured,
                    "container": None,
                    "error": self.last_error,
                    "source": "docker stats --no-stream",
                })
            self._stop.wait(self.interval_seconds)


class KafkaLagSampler:
    """Compare broker high offsets with Spark Kafka-source end offsets."""

    def __init__(
        self,
        output_path: str | Path,
        progress_path: str | Path,
        *,
        bootstrap_servers: str,
        topic: str,
        partitions: int = 1,
        run_id: str,
        interval_seconds: float = 1.0,
    ):
        self.path = Path(output_path)
        self.progress_path = Path(progress_path)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.partitions = partitions
        self.run_id = run_id
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._latest: dict[str, Any] | None = None
        self._sample_sequence = 0
        self.last_error: str | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="kafka-lag-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=10)

    def sample_now(self, timeout_seconds: float = 10.0) -> dict[str, Any] | None:
        with self._lock:
            target_sequence = self._sample_sequence + 1
        self._wake.set()
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            with self._lock:
                if self._sample_sequence >= target_sequence:
                    return dict(self._latest) if self._latest else None
            time.sleep(0.05)
        return self.latest_sample()

    def latest_sample(self) -> dict[str, Any] | None:
        with self._lock:
            return dict(self._latest) if self._latest else None

    def _run(self) -> None:
        consumer = None
        try:
            from confluent_kafka import Consumer, TopicPartition

            consumer = Consumer({
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": f"throughput-lag-{self.run_id}",
                "enable.auto.commit": False,
                "allow.auto.create.topics": False,
            })
            while not self._stop.is_set():
                self._capture(consumer, TopicPartition)
                self._wake.wait(self.interval_seconds)
                self._wake.clear()
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self._capture_error(self.last_error)
        finally:
            if consumer is not None:
                consumer.close()

    def _capture(self, consumer, TopicPartition) -> None:
        captured = _utc_now()
        spark_end_offsets = latest_spark_end_offsets(
            read_jsonl(self.progress_path), self.topic
        )
        latest_offsets: dict[str, int] = {}
        low_offsets: dict[str, int] = {}
        error = None
        try:
            for partition in range(self.partitions):
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(self.topic, partition),
                    timeout=4.0,
                    cached=False,
                )
                key = f"{self.topic}:{partition}"
                low_offsets[key] = int(low)
                latest_offsets[key] = int(high)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.last_error = error

        lag = None
        if not error and spark_end_offsets:
            comparable = [
                key for key in latest_offsets
                if key in spark_end_offsets
            ]
            if len(comparable) == self.partitions:
                lag = sum(
                    max(0, latest_offsets[key] - spark_end_offsets[key])
                    for key in comparable
                )
        row = {
            "timestamp_utc": captured,
            "topic": self.topic,
            "partition_count": self.partitions,
            "kafka_low_offsets": low_offsets,
            "kafka_latest_offsets": latest_offsets,
            "spark_end_offsets": spark_end_offsets,
            "lag_records": lag,
            "definition": "sum(max(0, Kafka high offset - Spark Kafka source end offset))",
            "error": error,
        }
        _append_jsonl(self.path, row)
        with self._lock:
            self._latest = row
            self._sample_sequence += 1

    def _capture_error(self, message: str) -> None:
        row = {
            "timestamp_utc": _utc_now(),
            "topic": self.topic,
            "partition_count": self.partitions,
            "kafka_latest_offsets": {},
            "spark_end_offsets": {},
            "lag_records": None,
            "definition": "sum(max(0, Kafka high offset - Spark Kafka source end offset))",
            "error": message,
        }
        _append_jsonl(self.path, row)
        with self._lock:
            self._latest = row
            self._sample_sequence += 1
