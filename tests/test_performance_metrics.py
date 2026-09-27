import sys
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "benchmark"))

from performance_metrics import (  # noqa: E402
    aggregate_repetitions,
    classify_capacity,
    linear_regression_slope,
    parse_kafka_offsets,
    percentile,
    replay_to_bronze_latency_metrics,
    resource_statistics,
    select_steady_state_window,
    summarize_kafka_to_bronze_lag,
    summarize_progress,
)
from telemetry import _memory_mb, _parse_memory_usage  # noqa: E402


def _progress(stage, name, batch_id, timestamp, *, rate=100.0, rows=10, duration=50):
    return {
        "stage": stage,
        "event_type": "progress",
        "captured_at_utc": timestamp,
        "progress": {
            "name": name,
            "batchId": batch_id,
            "timestamp": timestamp,
            "numInputRows": rows,
            "inputRowsPerSecond": rate,
            "processedRowsPerSecond": rate,
            "durationMs": {"triggerExecution": duration},
        },
    }


class PerformanceMetricTests(unittest.TestCase):
    def test_percentile_handles_empty_single_and_interpolated_samples(self):
        self.assertIsNone(percentile([], 0.95))
        self.assertEqual(percentile([7], 0.50), 7)
        self.assertEqual(percentile([0, 10], 0.5), 5)

    def test_parse_kafka_offsets_preserves_topic_and_partition(self):
        self.assertEqual(
            parse_kafka_offsets('{"weather.test":{"0":12,"1":8}}', "weather.test"),
            {"weather.test:0": 12, "weather.test:1": 8},
        )

    def test_steady_state_waits_for_elapsed_warmup_and_both_query_batch_cutoffs(self):
        rows = [
            {"stage": "bronze", "event_type": "query_started", "captured_at_utc": "2026-01-01T00:00:00Z"},
            {"stage": "silver", "event_type": "query_started", "captured_at_utc": "2026-01-01T00:00:01Z"},
        ]
        for stage, query, third_batch in (
            ("bronze", "run-bronze", 18),
            ("silver", "run-silver", 22),
        ):
            for batch_id, second in enumerate((5, 10, third_batch, 30)):
                rows.append(_progress(
                    stage, query, batch_id,
                    f"2026-01-01T00:00:{second:02d}Z",
                ))
        rows.append(_progress(
            "silver", "run-dlq", 2, "2026-01-01T00:00:35Z"
        ))

        window = select_steady_state_window(
            rows,
            generation_start_time="2026-01-01T00:00:04Z",
            generation_end_time="2026-01-01T00:00:40Z",
            minimum_warmup_seconds=15,
            minimum_completed_batches=3,
        )
        self.assertEqual(window["steady_state_start"], "2026-01-01T00:00:22.000+00:00")
        self.assertEqual(window["steady_state_progress_sample_counts"], {"bronze": 1, "silver": 2})
        self.assertEqual(window["warmup_policy"]["minimum_completed_batches_per_query"], 3)

    def test_progress_aggregates_bronze_and_main_silver_separately(self):
        rows = [
            _progress("bronze", "run-bronze", 1, "2026-01-01T00:00:01Z", rate=50, rows=5),
            _progress("bronze", "run-bronze", 2, "2026-01-01T00:00:02Z", rate=100, rows=10),
            _progress("silver", "run-silver", 1, "2026-01-01T00:00:02Z", rate=80, rows=8),
            _progress("silver", "run-dlq", 1, "2026-01-01T00:00:02Z", rate=900, rows=900),
        ]
        summary = summarize_progress(
            rows,
            steady_state_start="2026-01-01T00:00:02Z",
            generation_end_time="2026-01-01T00:00:05Z",
        )
        self.assertEqual(summary["bronze"]["avg_processed_rows_per_sec"], 100)
        self.assertEqual(summary["silver"]["avg_processed_rows_per_sec"], 80)
        self.assertEqual(summary["bronze"]["steady_state_sample_count"], 1)
        self.assertEqual(summary["silver"]["steady_state_sample_count"], 1)
        self.assertEqual(summary["silver_input_records"], 8)

    def test_lag_slope_reports_increase_decline_and_insufficient_samples(self):
        self.assertEqual(linear_regression_slope([(0, 0), (1, 10), (2, 20)]), 10)
        self.assertEqual(linear_regression_slope([(0, 20), (1, 10), (2, 0)]), -10)
        self.assertIsNone(linear_regression_slope([(0, 1)]))

    def test_lag_separates_startup_steady_and_final_source_lag(self):
        samples = [
            {"timestamp_utc": "2026-01-01T00:00:05Z", "kafka_to_bronze_lag_records": 30},
            {"timestamp_utc": "2026-01-01T00:00:10Z", "kafka_to_bronze_lag_records": 20},
            {"timestamp_utc": "2026-01-01T00:00:20Z", "kafka_to_bronze_lag_records": 10},
            {"timestamp_utc": "2026-01-01T00:00:30Z", "kafka_to_bronze_lag_records": 5},
            {"timestamp_utc": "2026-01-01T00:00:45Z", "kafka_to_bronze_lag_records": 0},
        ]
        summary = summarize_kafka_to_bronze_lag(
            samples,
            generation_start_time="2026-01-01T00:00:04Z",
            generation_end_time="2026-01-01T00:00:35Z",
            steady_state_start="2026-01-01T00:00:20Z",
        )
        self.assertEqual(summary["startup_peak_kafka_to_bronze_lag"], 30)
        self.assertEqual(summary["steady_state_peak_kafka_to_bronze_lag"], 10)
        self.assertEqual(summary["steady_state_p95_kafka_to_bronze_lag"], 9.75)
        self.assertEqual(summary["steady_state_lag_slope_records_per_sec"], -0.5)
        self.assertEqual(summary["final_source_lag"], 0)

    def test_resource_statistics_include_average_percentile_peak_and_unlimited_fields(self):
        rows = [
            {"container": "weather-spark-worker", "cpu_percent": 50, "memory_usage_mb": 10},
            {"container": "weather-spark-worker", "cpu_percent": 100, "memory_usage_mb": 30},
        ]
        worker = resource_statistics(rows)["weather-spark-worker"]
        self.assertEqual(worker["cpu_avg_percent"], 75)
        self.assertEqual(worker["cpu_p95_percent"], 97.5)
        self.assertEqual(worker["cpu_peak_percent"], 100)
        self.assertEqual(worker["memory_avg_mb"], 20)
        self.assertEqual(worker["memory_p95_mb"], 29)
        self.assertIsNone(worker["container_cpu_quota_cores"])
        self.assertIsNone(worker["container_memory_limit_mb"])

    def test_capacity_classification_uses_sustained_evidence_not_startup_peak(self):
        common = {
            "failed": False,
            "pipeline_completed": True,
            "final_source_lag": 0,
            "actual_rate": 100,
            "pipeline_sustainable_rate": 100,
            "steady_state_lag_slope_records_per_sec": 0,
            "pipeline_drain_seconds": 5,
            "generation_duration_seconds": 50,
        }
        self.assertEqual(classify_capacity(**common)[0], "UNDER_CAPACITY")
        self.assertEqual(classify_capacity(**{
            **common,
            "pipeline_sustainable_rate": 92,
            "steady_state_lag_slope_records_per_sec": 5,
            "pipeline_drain_seconds": 20,
        })[0], "NEAR_CAPACITY")
        self.assertEqual(classify_capacity(**{
            **common,
            "pipeline_sustainable_rate": 80,
            "steady_state_lag_slope_records_per_sec": 25,
        })[0], "SATURATED")
        self.assertEqual(classify_capacity(**{**common, "failed": True})[0], "FAILED")
        self.assertEqual(classify_capacity(**{**common, "pipeline_completed": False})[0], "SATURATED")

    def test_legacy_latency_fields_and_repetition_results_remain_readable(self):
        latency = replay_to_bronze_latency_metrics({
            "latency_count": 10,
            "latency_p95_ms": 125.0,
            "latency_p99_ms": 150.0,
        })
        self.assertEqual(latency["count"], 10)
        self.assertEqual(latency["p95_ms"], 125.0)
        aggregate = aggregate_repetitions([{
            "run_id": "legacy",
            "status": "NEAR_CAPACITY",
            "requested_rate_msgs_sec": 100,
            "actual_generated_msgs_sec": 99,
            "avg_processed_rows_per_sec": 98,
            "silver_avg_processed_rows_per_sec": 97,
            "latency_p95_ms": 125,
        }])
        self.assertEqual(aggregate["bronze_processed_rate_avg"], 98)
        self.assertEqual(aggregate["silver_processed_rate_avg"], 97)
        self.assertEqual(aggregate["replay_to_bronze_latency_p95_ms"], 125)
        self.assertEqual(aggregate["capacity_classification"], "NEAR_CAPACITY")

    def test_docker_memory_units_parse_to_mib(self):
        self.assertEqual(_memory_mb("4g"), 4096)
        self.assertEqual(_parse_memory_usage("259.2MiB / 6.698GiB"), (259.2, 6858.752))


if __name__ == "__main__":
    unittest.main()
