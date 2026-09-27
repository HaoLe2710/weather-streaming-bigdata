import sys
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "benchmark"))

from performance_metrics import (  # noqa: E402
    add_warmup_markers,
    classify_capacity,
    parse_kafka_offsets,
    percentile,
    summarize_progress,
)
from telemetry import _memory_mb, _parse_memory_usage  # noqa: E402


class PerformanceMetricTests(unittest.TestCase):
    def test_percentile_uses_linear_interpolation(self):
        self.assertEqual(percentile([0, 10], 0.5), 5)
        self.assertEqual(percentile([], 0.95), None)

    def test_parse_kafka_offsets_preserves_topic_and_partition(self):
        self.assertEqual(
            parse_kafka_offsets('{"weather.test":{"0":12,"1":8}}', "weather.test"),
            {"weather.test:0": 12, "weather.test:1": 8},
        )

    def test_warmup_batches_are_marked_per_streaming_query(self):
        rows = [
            {
                "stage": "bronze",
                "event_type": "progress",
                "progress": {"name": "bronze", "batchId": batch},
            }
            for batch in range(3)
        ]
        marked = add_warmup_markers(rows, 2)
        self.assertEqual([row["warmup_excluded"] for row in marked], [True, True, False])

    def test_progress_summary_excludes_warmup_and_dlq_query(self):
        rows = []
        for name, stage, num_rows, warmup in (
            ("weather-bronze", "bronze", 10, True),
            ("weather-bronze", "bronze", 20, False),
            ("weather-silver", "silver", 20, False),
            ("weather-dlq", "silver", 20, False),
        ):
            rows.append({
                "stage": stage,
                "event_type": "progress",
                "warmup_excluded": warmup,
                "progress": {
                    "name": name,
                    "numInputRows": num_rows,
                    "inputRowsPerSecond": float(num_rows),
                    "processedRowsPerSecond": float(num_rows) * 2,
                    "durationMs": {"triggerExecution": 50},
                },
            })
        summary = summarize_progress(rows)
        self.assertEqual(summary["bronze_input_records"], 30)
        self.assertEqual(summary["silver_input_records"], 20)
        self.assertEqual(summary["bronze"]["included_batches"], 1)
        self.assertEqual(summary["silver"]["included_batches"], 1)
        self.assertEqual(summary["bronze"]["avg_processed_rows_per_sec"], 40.0)

    def test_capacity_threshold_is_two_trigger_intervals(self):
        self.assertEqual(
            classify_capacity(
                failed=False,
                all_records_processed=True,
                final_lag=0,
                production_peak_lag=200,
                requested_rate=100,
                trigger_interval_seconds=1,
            )[0],
            "UNDER_CAPACITY",
        )
        self.assertEqual(
            classify_capacity(
                failed=False,
                all_records_processed=False,
                final_lag=5,
                production_peak_lag=500,
                requested_rate=100,
                trigger_interval_seconds=1,
            )[0],
            "SATURATED",
        )

    def test_docker_memory_units_parse_to_mib(self):
        self.assertEqual(_memory_mb("4g"), 4096)
        self.assertEqual(_parse_memory_usage("259.2MiB / 6.698GiB"), (259.2, 6858.752))


if __name__ == "__main__":
    unittest.main()
