import sys
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "spark" / "jobs"))

from benchmark_config import (  # noqa: E402
    B0_CORRECTNESS,
    KAFKA_WATERMARK_WM10M,
    THROUGHPUT_BASELINE,
    BenchmarkConfig,
    normalize_scenario,
)


class BenchmarkConfigScenarioTests(unittest.TestCase):
    def test_existing_b0_defaults_are_unchanged(self):
        config = BenchmarkConfig.for_scenario(B0_CORRECTNESS, "b0-test")
        self.assertEqual(config.duplicate_rate, 0.02)
        self.assertEqual(config.invalid_rate, 0.01)
        self.assertEqual(config.late_rate, 0.05)
        self.assertEqual(config.out_of_order_rate, 0.05)
        self.assertIsNone(config.paths.gold)
        self.assertEqual(config.spark_environment("bronze")["AVAILABLE_NOW"], "true")

    def test_existing_wm10m_defaults_are_unchanged(self):
        config = BenchmarkConfig.for_scenario(KAFKA_WATERMARK_WM10M, "wm-test")
        self.assertEqual(config.late_rate, 0.10)
        self.assertEqual(config.late_delay_events, 4_000)
        self.assertEqual(config.max_offsets_per_trigger, 500)
        self.assertIsNone(config.paths.bronze)
        self.assertIsNotNone(config.paths.gold)

    def test_throughput_is_fault_free_and_run_scoped(self):
        config = BenchmarkConfig.for_scenario(
            "throughput",
            "rate-test",
            source_record_limit=50_000,
            requested_replay_rate=500,
        )
        self.assertEqual(config.scenario, THROUGHPUT_BASELINE)
        self.assertEqual(normalize_scenario("throughput"), THROUGHPUT_BASELINE)
        self.assertEqual(config.topic, "weather.bench.throughput.rate-test")
        self.assertEqual(config.topic_partitions, 1)
        self.assertEqual(
            (config.duplicate_rate, config.invalid_rate, config.late_rate, config.out_of_order_rate),
            (0.0, 0.0, 0.0, 0.0),
        )
        self.assertIsNotNone(config.paths.bronze)
        self.assertIsNotNone(config.paths.silver)
        self.assertIsNone(config.paths.gold)
        self.assertEqual(config.spark_environment("bronze")["TRIGGER_INTERVAL"], "1 second")

    def test_throughput_rejects_fault_injection(self):
        with self.assertRaises(ValueError):
            BenchmarkConfig.for_scenario(
                THROUGHPUT_BASELINE,
                "fault-test",
                invalid_rate=0.01,
            )


if __name__ == "__main__":
    unittest.main()
