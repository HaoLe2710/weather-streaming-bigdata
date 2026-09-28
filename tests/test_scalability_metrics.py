import sys
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "benchmark"))

from scalability_metrics import (  # noqa: E402
    ScalabilityConfig,
    aggregate_scalability_runs,
    build_config_id,
    compute_scalability_metrics,
    parse_config_id,
    partition_distribution,
    scalability_resource_metrics,
    select_best_core_config,
    select_best_partition_config,
    summarize_progress_percentiles,
    validate_runtime_allocation,
)


def _valid_comparison_run(
    run_id="run",
    bronze_rate=100,
    silver_rate=100,
    **overrides,
):
    run = {
        "run_id": run_id,
        "valid_for_comparison": True,
        "status": "UNDER_CAPACITY",
        "correctness_passed": True,
        "correctness_checks": {
            "bronze_matches_expected": True,
            "silver_matches_expected": True,
            "dlq_is_empty": True,
            "duplicate_event_groups_are_empty": True,
            "quality_violations_are_empty": True,
        },
        "final_source_lag": 0,
        "stream_process_return_codes": {"bronze": 0, "silver": 0},
        "scalability_runtime_validation": {
            "passed": True,
            "actual_allocated_cores_total": 2,
        },
        "avg_processed_rows_per_sec": bronze_rate,
        "silver_avg_processed_rows_per_sec": silver_rate,
    }
    run.update(overrides)
    return run


class ScalabilityMetricsTests(unittest.TestCase):
    def test_config_id_round_trips_and_rejects_noncanonical_values(self):
        config_id = build_config_id(6, 2, 2, 1)
        self.assertEqual(config_id, "p6-b2-s2-w1")
        self.assertEqual(
            parse_config_id(config_id),
            {"partitions": 6, "bronze": 2, "silver": 2, "workers": 1},
        )
        with self.assertRaises(ValueError):
            parse_config_id("pBest-b2-s2-w1")

    def test_scalability_config_checks_worker_capacity(self):
        ScalabilityConfig(partitions=6, bronze_cores=4, silver_cores=4, workers=2).validate()
        with self.assertRaises(ValueError):
            ScalabilityConfig(partitions=3, bronze_cores=3, silver_cores=2, workers=1).validate()

    def test_speedup_efficiency_and_zero_baseline_edges(self):
        values = compute_scalability_metrics(
            baseline_rate=100,
            candidate_rate=180,
            baseline_cores=2,
            candidate_cores=4,
        )
        self.assertAlmostEqual(values["speedup"], 1.8)
        self.assertAlmostEqual(values["throughput_gain_percent"], 80)
        self.assertEqual(values["compute_multiplier"], 2)
        self.assertAlmostEqual(values["scaling_efficiency"], 0.9)
        self.assertEqual(values["candidate_rows_per_core"], 45)
        zero = compute_scalability_metrics(
            baseline_rate=0,
            candidate_rate=10,
            baseline_cores=2,
            candidate_cores=2,
        )
        self.assertIsNone(zero["speedup"])
        self.assertIsNone(zero["throughput_gain_percent"])
        self.assertIsNone(zero["scaling_efficiency"])

    def test_partition_imbalance_counts_empty_partitions(self):
        result = partition_distribution({"weather.topic:0": 80, "1": 20}, 3)
        self.assertEqual(result["records_per_partition"], {"0": 80, "1": 20, "2": 0})
        self.assertEqual(result["min_partition_records"], 0)
        self.assertEqual(result["max_partition_records"], 80)
        self.assertAlmostEqual(result["mean_partition_records"], 100 / 3)
        self.assertAlmostEqual(result["partition_imbalance_ratio"], 2.4)

    def test_aggregation_handles_one_run_missing_metrics_and_median(self):
        aggregate = aggregate_scalability_runs([
            _valid_comparison_run(
                run_id="one",
                bronze_rate=120,
                silver_rate=130,
                status="UNDER_CAPACITY",
                capacity_classification="UNDER_CAPACITY",
                pipeline_sustainable_rate=999,
                actual_generated_msgs_sec=100,
                replay_to_bronze_latency_p95_ms=50,
                steady_state_p95_kafka_to_bronze_lag=0,
                drain_seconds=5,
                scalability_resource_metrics={
                    "cluster_aggregate": {
                        "cpu_avg_percent": 60,
                        "cpu_p95_percent": 80,
                        "cpu_peak_percent": 95,
                        "memory_avg_mb": 900,
                        "memory_p95_mb": 1000,
                        "memory_peak_mb": 1200,
                    },
                    "kafka_broker": {
                        "cpu_avg_percent": 10,
                        "cpu_p95_percent": 12,
                        "cpu_peak_percent": 14,
                        "memory_avg_mb": 500,
                        "memory_p95_mb": 550,
                        "memory_peak_mb": 600,
                    },
                },
            ),
            _valid_comparison_run(
                run_id="two",
                bronze_rate=100,
                silver_rate=110,
                status="NEAR_CAPACITY",
                capacity_classification="NEAR_CAPACITY",
                pipeline_sustainable_rate=1,
                actual_generated_msgs_sec=100,
                replay_to_bronze_latency_p95_ms=None,
                steady_state_p95_kafka_to_bronze_lag=20,
                drain_seconds=7,
            ),
        ])
        self.assertEqual(aggregate["run_count"], 2)
        self.assertEqual(aggregate["valid_run_count"], 2)
        self.assertEqual(aggregate["pipeline_rate_median"], 110)
        self.assertEqual(aggregate["pipeline_rate_min"], 100)
        self.assertEqual(aggregate["pipeline_rate_max"], 120)
        self.assertEqual(aggregate["latency_p95_mean"], 50)
        self.assertEqual(aggregate["worker_cpu_p95_mean"], 80)
        self.assertEqual(aggregate["worker_cpu_avg_percent_mean"], 60)
        self.assertEqual(aggregate["worker_memory_peak_mb_mean"], 1200)
        self.assertEqual(aggregate["kafka_cpu_peak_percent_mean"], 14)

    def test_aggregation_excludes_load_generator_limited_runs_from_comparison_metrics(self):
        aggregate = aggregate_scalability_runs([
            _valid_comparison_run(
                run_id="valid",
                bronze_rate=5100,
                silver_rate=4900,
                actual_generated_msgs_sec=5000,
                pipeline_sustainable_rate=4900,
            ),
            _valid_comparison_run(
                run_id="limited",
                bronze_rate=3000,
                silver_rate=3100,
                valid_for_comparison=False,
                status="LOAD_GENERATOR_LIMITED",
                actual_generated_msgs_sec=3000,
                pipeline_sustainable_rate=9000,
            ),
        ])
        self.assertEqual(aggregate["run_count"], 2)
        self.assertEqual(aggregate["valid_run_count"], 1)
        self.assertEqual(aggregate["metrics_scope"], "valid_runs")
        self.assertEqual(aggregate["pipeline_rate_median"], 4900)

    def test_pipeline_aggregates_per_run_bottlenecks_for_same_stage(self):
        aggregate = aggregate_scalability_runs([
            _valid_comparison_run("one", bronze_rate=9000, silver_rate=8000),
            _valid_comparison_run("two", bronze_rate=9500, silver_rate=8200),
        ])
        self.assertEqual(
            [row["pipeline_rate"] for row in aggregate["per_run_pipeline_rate"]],
            [8000, 8200],
        )
        self.assertEqual(aggregate["pipeline_rate_mean"], 8100)
        self.assertEqual(aggregate["pipeline_rate_median"], 8100)
        self.assertEqual(aggregate["pipeline_rate_min"], 8000)
        self.assertEqual(aggregate["pipeline_rate_max"], 8200)

    def test_pipeline_rate_remains_correct_when_bottleneck_switches(self):
        aggregate = aggregate_scalability_runs([
            _valid_comparison_run("bronze-fast", bronze_rate=10000, silver_rate=7000),
            _valid_comparison_run("silver-fast", bronze_rate=7000, silver_rate=10000),
        ])
        self.assertEqual(
            [row["pipeline_rate"] for row in aggregate["per_run_pipeline_rate"]],
            [7000, 7000],
        )
        self.assertEqual(aggregate["metrics"]["bronze_processed_rate_avg"]["median"], 8500)
        self.assertEqual(aggregate["metrics"]["silver_processed_rate_avg"]["median"], 8500)
        self.assertEqual(aggregate["pipeline_rate_median"], 7000)
        self.assertNotEqual(aggregate["pipeline_rate_median"], 8500)

    def test_missing_stage_excludes_run_and_marks_legacy_pipeline(self):
        run = _valid_comparison_run("legacy", bronze_rate=9000, silver_rate=8500)
        run.pop("silver_avg_processed_rows_per_sec")
        run["pipeline_sustainable_rate"] = 8000
        aggregate = aggregate_scalability_runs([run])
        self.assertEqual(aggregate["valid_run_count"], 0)
        self.assertIsNone(aggregate["per_run_pipeline_rate"][0]["pipeline_rate"])
        self.assertIsNone(aggregate["pipeline_rate_median"])
        self.assertEqual(aggregate["pipeline_rate_status"], "LEGACY_PIPELINE_NOT_RECOMPUTABLE")
        self.assertEqual(aggregate["legacy_pipeline_status"], "LEGACY_PIPELINE_NOT_RECOMPUTABLE")

    def test_failed_correctness_excludes_run_with_both_stage_rates(self):
        run = _valid_comparison_run("incorrect", bronze_rate=9000, silver_rate=8500)
        run["correctness_passed"] = False
        run["correctness_checks"]["silver_matches_expected"] = False
        aggregate = aggregate_scalability_runs([run])
        self.assertEqual(aggregate["valid_run_count"], 0)
        self.assertIsNone(aggregate["pipeline_rate_median"])
        self.assertIsNone(aggregate["per_run_pipeline_rate"][0]["pipeline_rate"])

    def test_incomplete_correctness_lag_query_or_allocation_excludes_run(self):
        variants = [
            _valid_comparison_run("no-checks", bronze_rate=9000, silver_rate=8500, correctness_checks=None),
            _valid_comparison_run("lagging", bronze_rate=9000, silver_rate=8500, final_source_lag=1),
            _valid_comparison_run(
                "query-failed",
                bronze_rate=9000,
                silver_rate=8500,
                stream_process_return_codes={"bronze": 0, "silver": 1},
            ),
            _valid_comparison_run(
                "allocation-mismatch",
                bronze_rate=9000,
                silver_rate=8500,
                scalability_runtime_validation={"passed": False},
            ),
        ]
        for run in variants:
            with self.subTest(run_id=run["run_id"]):
                aggregate = aggregate_scalability_runs([run])
                self.assertEqual(aggregate["valid_run_count"], 0)
                self.assertIsNone(aggregate["pipeline_rate_median"])
                self.assertIsNone(aggregate["per_run_pipeline_rate"][0]["pipeline_rate"])

    def test_speedup_uses_aggregate_of_run_level_pipeline_medians(self):
        baseline = aggregate_scalability_runs([
            _valid_comparison_run("base-one", bronze_rate=9000, silver_rate=8000),
            _valid_comparison_run("base-two", bronze_rate=8500, silver_rate=8000),
        ])
        candidate = aggregate_scalability_runs([
            _valid_comparison_run("candidate-one", bronze_rate=10000, silver_rate=9000),
            _valid_comparison_run("candidate-two", bronze_rate=6000, silver_rate=10000),
        ])

        metrics = compute_scalability_metrics(
            baseline_rate=baseline["pipeline_rate_median"],
            candidate_rate=candidate["pipeline_rate_median"],
            baseline_cores=2,
            candidate_cores=2,
        )

        self.assertEqual(baseline["pipeline_rate_median"], 8000)
        self.assertEqual(candidate["pipeline_rate_median"], 7500)
        self.assertEqual(candidate["metrics"]["bronze_processed_rate_avg"]["median"], 8000)
        self.assertEqual(candidate["metrics"]["silver_processed_rate_avg"]["median"], 9500)
        self.assertAlmostEqual(metrics["speedup"], 7500 / 8000)

    def test_best_partition_uses_median_then_latency_and_partition_tiebreaks(self):
        summaries = {
            "p1-b1-s1-w1": {
                "valid_run_count": 2,
                "pipeline_rate_median": 100,
                "latency_p95_mean": 90,
                "steady_lag_p95_mean": 5,
                "capacity_classifications": ["UNDER_CAPACITY", "UNDER_CAPACITY"],
            },
            "p3-b1-s1-w1": {
                "valid_run_count": 2,
                "pipeline_rate_median": 101,
                "latency_p95_mean": 40,
                "steady_lag_p95_mean": 2,
                "capacity_classifications": ["UNDER_CAPACITY", "UNDER_CAPACITY"],
            },
            "p6-b1-s1-w1": {
                "valid_run_count": 2,
                "pipeline_rate_median": 100.5,
                "latency_p95_mean": 50,
                "steady_lag_p95_mean": 1,
                "capacity_classifications": ["UNDER_CAPACITY", "UNDER_CAPACITY"],
            },
        }
        self.assertEqual(select_best_partition_config(summaries), "p3-b1-s1-w1")

    def test_best_core_ignores_failed_or_generator_limited_configs(self):
        summaries = {
            "p3-b1-s1-w1": {
                "valid_run_count": 2,
                "pipeline_rate_median": 100,
                "capacity_classifications": ["UNDER_CAPACITY", "UNDER_CAPACITY"],
                "latency_p95_mean": 60,
            },
            "p3-b2-s1-w1": {
                "valid_run_count": 2,
                "pipeline_rate_median": 101,
                "capacity_classifications": ["UNDER_CAPACITY", "UNDER_CAPACITY"],
                "latency_p95_mean": 40,
            },
            "p3-b2-s2-w1": {
                "valid_run_count": 0,
                "pipeline_rate_median": 500,
                "capacity_classifications": ["LOAD_GENERATOR_LIMITED", "FAILED"],
            },
        }
        self.assertEqual(select_best_core_config(summaries), "p3-b2-s1-w1")

    def test_allocation_mismatch_is_explicitly_invalid(self):
        config = ScalabilityConfig(partitions=3, bronze_cores=2, silver_cores=1, workers=1)
        observation = {
            "apps": [
                {
                    "name": "WeatherBronzeStreaming-r1",
                    "executors": [{"cores": 1, "memory_mb": 1024, "worker_id": "w1"}],
                },
                {
                    "name": "WeatherSilverStreaming-r1",
                    "executors": [{"cores": 1, "memory_mb": 1024, "worker_id": "w1"}],
                },
            ],
            "workers": [
                {"id": "w1", "state": "ALIVE", "cores_available": 4, "memory_available_mb": 4096},
                {"id": "old-worker", "state": "DEAD", "cores_available": 4, "memory_available_mb": 4096},
            ],
            "other_active_apps": [],
        }
        result = validate_runtime_allocation(observation, run_id="r1", config=config)
        self.assertFalse(result["passed"])
        self.assertEqual(result["worker_count"], 1)
        self.assertEqual(result["dead_worker_ids"], ["old-worker"])
        self.assertEqual(result["actual_allocated_cores_by_app"]["WeatherBronzeStreaming-r1"], 1)
        self.assertTrue(any("requested 2 cores but has 1 allocated" in error for error in result["errors"]))

    def test_allocation_uses_master_core_totals_when_worker_ui_omits_executor_cores(self):
        config = ScalabilityConfig(partitions=3, bronze_cores=2, silver_cores=1, workers=1)
        observation = {
            "apps": [
                {
                    "name": "WeatherBronzeStreaming-r1",
                    "cores": 2,
                    "actual_allocated_cores": 2,
                    "executors": [
                        {"cores": None, "memory_mb": 1024, "worker_id": "w1"},
                        {"cores": None, "memory_mb": 1024, "worker_id": "w1"},
                    ],
                },
                {
                    "name": "WeatherSilverStreaming-r1",
                    "cores": 1,
                    "actual_allocated_cores": 1,
                    "executors": [
                        {"cores": None, "memory_mb": 1024, "worker_id": "w1"},
                    ],
                },
            ],
            "workers": [
                {"id": "w1", "state": "ALIVE", "cores_available": 4, "memory_available_mb": 4096},
            ],
            "other_active_apps": [],
        }

        result = validate_runtime_allocation(observation, run_id="r1", config=config)

        self.assertTrue(result["passed"], result["errors"])
        self.assertEqual(result["actual_allocated_cores_by_app"]["WeatherBronzeStreaming-r1"], 2)
        self.assertEqual(result["actual_allocated_cores_total"], 3)
        self.assertFalse(result["executor_core_width_observed_by_app"]["WeatherBronzeStreaming-r1"])

    def test_progress_percentiles_use_main_silver_query_and_window(self):
        rows = [
            {"stage": "bronze", "event_type": "progress", "captured_at_utc": "2026-01-01T00:00:10Z", "progress": {"processedRowsPerSecond": 100, "durationMs": {"triggerExecution": 20}}},
            {"stage": "silver", "event_type": "progress", "captured_at_utc": "2026-01-01T00:00:10Z", "progress": {"name": "run-silver", "processedRowsPerSecond": 80, "durationMs": {"triggerExecution": 30}}},
            {"stage": "silver", "event_type": "progress", "captured_at_utc": "2026-01-01T00:00:11Z", "progress": {"name": "run-dlq", "processedRowsPerSecond": 900, "durationMs": {"triggerExecution": 1}}},
            {"stage": "bronze", "event_type": "progress", "captured_at_utc": "2026-01-01T00:00:30Z", "progress": {"processedRowsPerSecond": 1000, "durationMs": {"triggerExecution": 1}}},
        ]
        result = summarize_progress_percentiles(
            rows,
            steady_state_start="2026-01-01T00:00:05Z",
            generation_end_time="2026-01-01T00:00:20Z",
        )
        self.assertEqual(result["bronze"]["sample_count"], 1)
        self.assertEqual(result["bronze"]["processed_rate_p50"], 100)
        self.assertEqual(result["silver"]["sample_count"], 1)
        self.assertEqual(result["silver"]["processed_rate_p50"], 80)

    def test_worker_metrics_report_each_worker_and_cluster_sum(self):
        rows = [
            {"timestamp_utc": "t1", "container": "weather-spark-worker", "container_name": "worker-1", "service": "spark_worker", "cpu_percent": 40, "memory_usage_mb": 1000},
            {"timestamp_utc": "t1", "container": "spark-worker-2", "container_name": "worker-2", "service": "spark_worker", "cpu_percent": 50, "memory_usage_mb": 1100},
            {"timestamp_utc": "t1", "container": "weather-kafka", "container_name": "weather-kafka", "service": "kafka_broker", "cpu_percent": 10, "memory_usage_mb": 500},
            {"timestamp_utc": "t2", "container": "weather-spark-worker", "container_name": "worker-1", "service": "spark_worker", "cpu_percent": 60, "memory_usage_mb": 1200},
            {"timestamp_utc": "t2", "container": "spark-worker-2", "container_name": "worker-2", "service": "spark_worker", "cpu_percent": 70, "memory_usage_mb": 1300},
        ]
        result = scalability_resource_metrics(rows)
        self.assertEqual(result["cluster_worker_count"], 2)
        self.assertEqual(len(result["per_worker"]), 2)
        self.assertEqual(result["cluster_aggregate"]["cpu_avg_percent"], 110)
        self.assertEqual(result["cluster_aggregate"]["memory_avg_mb"], 2300)
        self.assertEqual(result["kafka_broker"]["cpu_peak_percent"], 10)


if __name__ == "__main__":
    unittest.main()
