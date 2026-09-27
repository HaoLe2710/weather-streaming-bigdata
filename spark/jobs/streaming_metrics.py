"""Shared Spark Streaming progress persistence and benchmark stop handling."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import time

from pyspark.sql.streaming import StreamingQueryListener


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class JsonlStreamingListener(StreamingQueryListener):
    """Append query lifecycle and unmodified progress objects to JSONL."""

    def __init__(self, output_path: str, stage: str):
        super().__init__()
        self.path = Path(output_path)
        self.stage = stage
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _append(self, event_type: str, **fields) -> None:
        row = {
            "stage": self.stage,
            "event_type": event_type,
            "captured_at_utc": utc_now(),
            **fields,
        }
        encoded = json.dumps(row, sort_keys=True, separators=(",", ":"))
        with self._lock:
            with self.path.open("a", encoding="utf-8", newline="\n") as output:
                output.write(encoded + "\n")
                output.flush()

    def onQueryStarted(self, event) -> None:
        self._append(
            "query_started",
            query_id=str(event.id),
            query_run_id=str(event.runId),
            query_name=event.name,
        )

    def onQueryProgress(self, event) -> None:
        progress = json.loads(event.progress.json)
        self._append(
            "progress",
            query_id=str(event.progress.id),
            query_run_id=str(event.progress.runId),
            query_name=event.progress.name,
            progress=progress,
        )

    def onQueryTerminated(self, event) -> None:
        self._append(
            "query_terminated",
            query_id=str(event.id),
            query_run_id=str(event.runId) if event.runId is not None else None,
            exception=getattr(event, "exception", None),
        )


def wait_for_stop_signal(queries, stop_signal_path: str) -> None:
    """Keep queries live until the host confirms that Kafka and Silver drained."""
    signal_path = Path(stop_signal_path)
    while True:
        active = [query for query in queries if query.isActive]
        if not active:
            failures = [query.exception() for query in queries if query.exception()]
            if failures:
                raise RuntimeError(f"Streaming query failed: {failures[0]}")
            raise RuntimeError("Streaming query stopped before the benchmark stop signal.")
        if signal_path.exists():
            for query in active:
                query.stop()
            break
        time.sleep(0.2)

    for query in queries:
        if not query.awaitTermination(30_000):
            raise TimeoutError(f"Streaming query did not stop cleanly: {query.name}")
        failure = query.exception()
        if failure:
            raise RuntimeError(f"Streaming query failed: {failure}")


def make_listener_from_environment(stage: str):
    output_path = os.getenv("SPARK_PROGRESS_PATH")
    if not output_path:
        raise ValueError("SPARK_PROGRESS_PATH is required for throughput instrumentation.")
    return JsonlStreamingListener(output_path, stage)
