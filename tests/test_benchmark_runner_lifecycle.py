import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "benchmark"))

import run_benchmark  # noqa: E402


class BenchmarkRunnerLifecycleTests(unittest.TestCase):
    def test_simulator_command_explicitly_pins_the_20_location_benchmark(self):
        config = run_benchmark.BenchmarkConfig.for_scenario(
            run_benchmark.B0_CORRECTNESS,
            "b0-dataset-pin",
        )
        source = REPO_ROOT / "data" / "historical" / "raw"
        command = run_benchmark._simulator_command(
            config,
            source,
            REPO_ROOT / "results" / "simulator.json",
        )
        dataset_index = command.index("--dataset")
        self.assertEqual(command[dataset_index + 1], "benchmark-20")
        source_index = command.index("--source")
        self.assertEqual(Path(command[source_index + 1]), source)

    def test_scalability_cli_can_aggregate_run_results(self):
        aggregate = run_benchmark.aggregate_scalability_runs(
            [
                {
                    "run_id": "run-1",
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
                    "scalability_runtime_validation": {"passed": True},
                    "avg_processed_rows_per_sec": 123.0,
                    "silver_avg_processed_rows_per_sec": 150.0,
                    "pipeline_sustainable_rate": 123.0,
                }
            ]
        )

        self.assertEqual(aggregate["run_count"], 1)
        self.assertEqual(aggregate["pipeline_rate_mean"], 123.0)

    def test_compose_exec_uses_direct_binary_when_available(self):
        with patch.object(run_benchmark.shutil, "which", return_value="docker-compose"):
            self.assertEqual(run_benchmark._compose_exec_prefix(), ["docker-compose"])

    def test_compose_exec_falls_back_to_docker_plugin(self):
        with patch.object(run_benchmark.shutil, "which", return_value=None):
            self.assertEqual(run_benchmark._compose_exec_prefix(), ["docker", "compose"])

    def test_incomplete_producer_delivery_is_generator_limited(self):
        reason = run_benchmark._scalability_generator_limit_reason(
            {
                "producer_delivery_complete": False,
                "producer_flush_remaining": 150_201,
            },
            generator_rate_valid=True,
        )
        self.assertEqual(
            reason,
            "Simulator could not flush every queued Kafka message; queued messages remaining=150201.",
        )

    def test_calibrated_delivery_has_no_generator_limit_reason(self):
        self.assertIsNone(
            run_benchmark._scalability_generator_limit_reason(
                {"producer_delivery_complete": True},
                generator_rate_valid=True,
            )
        )

    def test_generator_rate_outside_calibration_is_limited(self):
        reason = run_benchmark._scalability_generator_limit_reason(
            {"producer_delivery_complete": True},
            generator_rate_valid=False,
        )
        self.assertIn("outside its calibrated", reason)

    def test_temporary_directory_cleanup_retries_transient_windows_lock(self):
        with tempfile.TemporaryDirectory() as parent:
            target = Path(parent) / "run-logs"
            target.mkdir()
            (target / "spark.log").write_text("complete", encoding="utf-8")
            remove = run_benchmark.shutil.rmtree
            outcomes = [PermissionError("file is temporarily held"), None]

            def flaky_remove(path):
                outcome = outcomes.pop(0)
                if outcome is not None:
                    raise outcome
                remove(path)

            with patch.object(run_benchmark.shutil, "rmtree", side_effect=flaky_remove), patch.object(
                run_benchmark.time, "sleep"
            ):
                self.assertIsNone(run_benchmark._cleanup_temporary_directory(target))
            self.assertFalse(target.exists())

    @unittest.skipUnless(run_benchmark.os.name == "nt", "Windows process tree handling")
    def test_windows_shutdown_kills_the_compose_process_tree(self):
        class FakeProcess:
            pid = 4321
            return_code = None

            def poll(self):
                return self.return_code

            def wait(self, timeout=None):
                if self.return_code is None:
                    raise run_benchmark.subprocess.TimeoutExpired("docker-compose", timeout)
                return self.return_code

        process = FakeProcess()

        def taskkill(command, **kwargs):
            process.return_code = 1
            return run_benchmark.subprocess.CompletedProcess(command, 1)

        with tempfile.TemporaryDirectory() as temp_dir:
            stop_signal = Path(temp_dir) / "stop.signal"
            with patch.object(
                run_benchmark.subprocess, "run", side_effect=taskkill
            ) as run, patch.object(
                run_benchmark, "_spark_active_apps", return_value={"apps": []}
            ):
                result = run_benchmark._stop_stream_processes(
                    {"silver": process},
                    stop_signal,
                    run_id="test-run",
                    timeout_seconds=0,
                )

        self.assertEqual(result, {"silver": 1})
        self.assertEqual(run.call_args.args[0], ["taskkill", "/PID", "4321", "/T", "/F"])

    def test_shutdown_waits_for_spark_master_to_release_the_run(self):
        class FinishedProcess:
            pid = 1234

            def poll(self):
                return 0

            def wait(self, timeout=None):
                return 0

        observation = {"apps": [{"id": "app-test", "name": "WeatherSilver-test"}]}
        with tempfile.TemporaryDirectory() as temp_dir:
            stop_signal = Path(temp_dir) / "stop.signal"
            with patch.object(
                run_benchmark,
                "_spark_active_apps",
                side_effect=[observation, {"apps": []}, {"apps": []}],
            ) as inspect_master:
                result = run_benchmark._stop_stream_processes(
                    {"silver": FinishedProcess()},
                    stop_signal,
                    run_id="test",
                    timeout_seconds=3,
                )

        self.assertEqual(result, {"silver": 0})
        self.assertEqual(inspect_master.call_count, 3)


if __name__ == "__main__":
    unittest.main()
