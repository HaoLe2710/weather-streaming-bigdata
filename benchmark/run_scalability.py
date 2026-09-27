"""Run the bounded OFAT Kafka/Spark scalability experiment matrix."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_benchmark import (  # noqa: E402
    REPO_ROOT as RUNNER_ROOT,
    RESULTS_ROOT,
    SCALABILITY_BENCHMARK,
    BenchmarkConfig,
    _compose_service_containers,
    _docker_compose,
    _find_calibration,
    _new_run_id,
    _require_clean_worktree,
    _require_services,
    _run_throughput_iteration,
    _runtime_versions,
    _spark_active_apps,
    read_json,
    run_load_generator_calibration,
    utc_now,
    write_json,
)
from scalability_metrics import (  # noqa: E402
    aggregate_scalability_runs,
    compute_scalability_metrics,
    select_best_core_config,
    select_best_partition_config,
)


DEFAULT_SOURCE = REPO_ROOT / "data" / "historical" / "raw"


def _set_worker_count(count: int, *, timeout_seconds: float = 120.0) -> dict:
    _docker_compose(
        "up", "-d", "--scale", f"spark-worker={count}", "spark-worker",
        timeout=timeout_seconds,
    )
    deadline = time.monotonic() + timeout_seconds
    last_workers = []
    while time.monotonic() < deadline:
        observation = _spark_active_apps("__scalability_preflight__")
        last_workers = observation.get("workers", [])
        alive = [worker for worker in last_workers if worker.get("state") == "ALIVE"]
        if len(alive) == count:
            config_values = _compose_service_containers("spark-worker")
            if len(config_values) == count:
                return {
                    "requested_count": count,
                    "workers": alive,
                    "compose_containers": config_values,
                }
        time.sleep(2)
    raise TimeoutError(
        f"Expected {count} ALIVE Spark workers; observed "
        f"{len(last_workers)}: {json.dumps(last_workers, sort_keys=True)}"
    )


def _calibrate(rate: int, *, source: Path, seed: int) -> tuple[dict, Path]:
    args = argparse.Namespace(
        scenario="scalability",
        max_source_events=10_000,
        source=str(source),
        seed=seed,
        calibration_rates=[rate],
    )
    run_load_generator_calibration(args)
    try:
        return _find_calibration(rate, _current_commit())
    except RuntimeError:
        root = REPO_ROOT / RESULTS_ROOT / "scalability" / "experiments"
        candidates = sorted(root.glob("*/summary.json"), key=lambda item: item.parent.name, reverse=True)
        for path in candidates:
            try:
                summary = read_json(path)
            except (OSError, ValueError):
                continue
            if summary.get("experiment_type") != "load_generator_calibration":
                continue
            for item in summary.get("rates", []):
                if int(item.get("requested_rate_msgs_sec", -1)) == rate:
                    selected = dict(item)
                    selected["calibration_git_commit"] = summary.get("git_commit")
                    selected["calibration_reused_from_previous_commit"] = False
                    selected["calibration_compatibility_reason"] = "Calibration executed for this scalability experiment."
                    return selected, path
        raise


def _current_commit() -> str:
    import subprocess

    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _config_id(partitions: int, bronze: int, silver: int, workers: int) -> str:
    return f"p{partitions}-b{bronze}-s{silver}-w{workers}"


def _summarize_config(results: list[dict]) -> dict:
    return aggregate_scalability_runs(results)


def _persist_experiment(
    path: Path,
    experiment: dict,
    config_results: dict[str, list[dict]],
    *,
    status: str,
    stress_results: dict[str, dict[str, list[dict]]] | None = None,
) -> None:
    config_summaries = {
        config_id: _summarize_config(results)
        for config_id, results in config_results.items()
        if results
    }
    run_rows = [run for results in config_results.values() for run in results]
    run_rows.extend(
        run
        for rate_results in (stress_results or {}).values()
        for results in rate_results.values()
        for run in results
    )
    baseline = config_summaries.get("p1-b1-s1-w1", {})
    base_rate = baseline.get("pipeline_rate_median")
    baseline_cores = 2
    for config_id, summary in config_summaries.items():
        # Dimensions are canonical in config_id and retained with each result.
        sample = next((row for row in config_results[config_id] if row.get("scalability_infrastructure")), {})
        requested = ((sample.get("scalability_infrastructure") or {}).get("scalability_config") or {})
        app_cores = (requested.get("bronze_cores_max") or 0) + (requested.get("silver_cores_max") or 0)
        summary["scalability_metrics_vs_control"] = compute_scalability_metrics(
            baseline_rate=base_rate,
            candidate_rate=summary.get("pipeline_rate_median"),
            baseline_cores=baseline_cores,
            candidate_cores=app_cores or None,
        )
        if config_id != "p1-b1-s1-w1" and app_cores == baseline_cores:
            summary["scalability_metrics_vs_control"]["scaling_efficiency"] = None
            summary["scalability_metrics_vs_control"]["compute_multiplier"] = None
        summary["configuration"] = requested
        summary["worker_core_capacity_multiplier"] = (
            (requested.get("workers") or 0) * (requested.get("worker_cores_each") or 0) / 4
            if requested else None
        )
        summary["throughput_per_allocated_core"] = (
            summary.get("pipeline_rate_median") / summary.get("actual_allocated_cores_median")
            if summary.get("pipeline_rate_median") is not None
            and summary.get("actual_allocated_cores_median")
            else None
        )
    stress_summaries: dict[str, dict[str, dict]] = {}
    for rate, rate_results in (stress_results or {}).items():
        rate_summaries = {
            config_id: _summarize_config(results)
            for config_id, results in rate_results.items()
            if results
        }
        rate_baseline = rate_summaries.get("p1-b1-s1-w1", {})
        for config_id, summary in rate_summaries.items():
            sample = next(
                (row for row in rate_results[config_id] if row.get("scalability_infrastructure")),
                {},
            )
            requested = ((sample.get("scalability_infrastructure") or {}).get("scalability_config") or {})
            app_cores = (requested.get("bronze_cores_max") or 0) + (requested.get("silver_cores_max") or 0)
            summary["scalability_metrics_vs_control"] = compute_scalability_metrics(
                baseline_rate=rate_baseline.get("pipeline_rate_median"),
                candidate_rate=summary.get("pipeline_rate_median"),
                baseline_cores=2,
                candidate_cores=app_cores or None,
            )
            if config_id != "p1-b1-s1-w1" and app_cores == 2:
                summary["scalability_metrics_vs_control"]["scaling_efficiency"] = None
                summary["scalability_metrics_vs_control"]["compute_multiplier"] = None
            summary["configuration"] = requested
        stress_summaries[rate] = rate_summaries
    experiment.update({
        "status": status,
        "updated_at": utc_now(),
        "config_summaries": config_summaries,
        "high_stress_config_summaries": stress_summaries,
        "runs": [
            {
                "run_id": row.get("run_id"),
                "config_id": row.get("config_id"),
                "status": row.get("status"),
                "valid_for_comparison": row.get("valid_for_comparison"),
                "result": f"results/benchmarks/scalability/{row.get('run_id')}/result.json",
            }
            for row in run_rows
        ],
        "run_count": len(run_rows),
    })
    write_json(path, experiment)


def run_matrix(args) -> int:
    if args.repetitions < 2:
        raise ValueError("The OFAT matrix requires at least two independent repetitions per configuration.")
    if args.final_repetitions < args.repetitions:
        raise ValueError("--final-repetitions must be at least --repetitions.")
    if args.rate <= 0 or args.source_records <= 0:
        raise ValueError("Rate and source record count must be positive.")
    source = Path(args.source).resolve()
    if not source.is_dir():
        raise FileNotFoundError(f"Historical source directory not found: {source}")

    _require_clean_worktree()
    _require_services()
    versions = _runtime_versions()
    commit = _current_commit()
    experiment_id = args.experiment_id or _new_run_id()
    experiment_dir = REPO_ROOT / RESULTS_ROOT / "scalability" / "experiments" / experiment_id
    experiment_dir.mkdir(parents=True, exist_ok=False)
    summary_path = experiment_dir / "summary.json"
    experiment = {
        "schema_version": 1,
        "experiment_type": "kafka_spark_scalability_ofat",
        "experiment_id": experiment_id,
        "created_at": utc_now(),
        "git_commit": commit,
        "scenario": SCALABILITY_BENCHMARK,
        "source": str(source),
        "source_records": args.source_records,
        "primary_rate_msgs_sec": args.rate,
        "repetitions_per_configuration": args.repetitions,
        "final_repetitions": args.final_repetitions,
        "software_versions": versions,
        "dataset_policy": "Existing 20-location historical source retained; Gold excluded.",
        "physical_topology_limitation": "All Docker workers share one physical host; this measures logical containerized scale-out.",
        "stage_status": {"A_partitions": "PENDING", "B_cores": "PENDING", "C_workers": "PENDING", "D_final_validation": "PENDING", "higher_stress": "NOT_REQUIRED_YET"},
        "calibrations": [],
        "selected_partition_config": None,
        "selected_single_worker_config": None,
        "selected_two_worker_config": None,
    }
    config_results: dict[str, list[dict]] = {}
    stress_results: dict[str, dict[str, list[dict]]] = {}
    write_json(experiment_dir / "experiment_manifest.json", experiment)
    _persist_experiment(summary_path, experiment, config_results, status="RUNNING", stress_results=stress_results)

    def ensure_calibrated(rate: int):
        try:
            calibration, path = _find_calibration(rate, commit)
        except RuntimeError:
            calibration, path = _calibrate(rate, source=source, seed=args.seed)
        experiment["calibrations"].append({
            "rate_msgs_sec": rate,
            "summary": path.relative_to(REPO_ROOT).as_posix(),
            "actual_rate_msgs_sec": calibration.get("actual_generated_msgs_sec"),
            "deviation_percent": calibration.get("deviation_percent"),
            "calibrated": calibration.get("calibrated"),
        })
        write_json(experiment_dir / "experiment_manifest.json", experiment)
        return calibration, path

    calibration, calibration_path = ensure_calibrated(args.rate)
    if not calibration.get("calibrated"):
        experiment["error"] = {
            "type": "load_generator_calibration_failed",
            "message": f"The load generator did not calibrate within +/-10% at {args.rate} msg/s.",
        }
        experiment["stage_status"].update({
            "A_partitions": "SKIPPED_UNCALIBRATED",
            "B_cores": "NOT_RUN",
            "C_workers": "NOT_RUN",
            "D_final_validation": "NOT_RUN",
            "higher_stress": "NOT_RUN",
        })
        _persist_experiment(summary_path, experiment, config_results, status="FAILED", stress_results=stress_results)
        write_json(experiment_dir / "experiment_manifest.json", experiment)
        return 2

    def run_config(
        partitions: int,
        bronze: int,
        silver: int,
        workers: int,
        *,
        repetitions: int,
        label: str,
        rate: int | None = None,
        calibration_for_run: dict | None = None,
        calibration_path_for_run: Path | None = None,
        result_store: dict[str, list[dict]] | None = None,
        source_records: int | None = None,
    ):
        run_rate = rate or args.rate
        run_source_records = source_records or args.source_records
        active_calibration = calibration_for_run or calibration
        active_calibration_path = calibration_path_for_run or calibration_path
        result_store = config_results if result_store is None else result_store
        config_id = _config_id(partitions, bronze, silver, workers)
        _set_worker_count(workers)
        existing = len(result_store.get(config_id, []))
        for repetition in range(existing + 1, repetitions + 1):
            run_id = _new_run_id()
            config = BenchmarkConfig.for_scenario(
                SCALABILITY_BENCHMARK,
                run_id,
                source_record_limit=run_source_records,
                requested_replay_rate=run_rate,
                seed=args.seed,
                experiment_id=experiment_id,
                topic_partitions=partitions,
                workers=workers,
                bronze_cores_max=bronze,
                silver_cores_max=silver,
                worker_cores_each=4,
                worker_memory_mb_each=4096,
                executor_cores=1,
                executor_memory_mb=1024,
                shuffle_partitions=1,
                trigger_interval="1 second",
            )
            print(f"[SCALABILITY] stage={label} config={config_id} repetition={repetition}/{repetitions}")
            result = _run_throughput_iteration(
                config=config,
                source=source,
                versions=versions,
                git_commit=commit,
                calibration=active_calibration,
                calibration_path=active_calibration_path,
                warmup_seconds=args.warmup_seconds,
                warmup_min_batches=args.warmup_min_batches,
                drain_timeout_seconds=args.drain_timeout,
            )
            result_store.setdefault(config_id, []).append(result)
            _persist_experiment(summary_path, experiment, config_results, status="RUNNING", stress_results=stress_results)
            if result.get("status") in {"FAILED", "INVALID_FOR_COMPARISON", "INVALID_CORRECTNESS", "LOAD_GENERATOR_LIMITED"}:
                print(f"[SCALABILITY] stopping {config_id}: {result.get('status')}")
                break
        return _summarize_config(result_store.get(config_id, []))

    def summaries_for_workers(worker_count: int | None = None):
        return {
            config_id: _summarize_config(rows)
            for config_id, rows in config_results.items()
            if rows and (worker_count is None or config_id.endswith(f"-w{worker_count}"))
        }

    try:
        _set_worker_count(1)
        for partitions in (1, 3, 6):
            run_config(partitions, 1, 1, 1, repetitions=args.repetitions, label="A_partition_scaling")
        experiment["stage_status"]["A_partitions"] = "COMPLETE"
        stage_a = summaries_for_workers(1)
        selected_partition = select_best_partition_config({
            config_id: summary
            for config_id, summary in stage_a.items()
            if config_id.endswith("-b1-s1-w1")
        })
        p_best = int(selected_partition.split("-")[0][1:])
        experiment["selected_partition_config"] = {
            "config_id": selected_partition,
            "rule": "Highest median sustainable pipeline throughput among correctness-passing, calibrated, valid runs; within 2% ties prefer lower p95 latency, lower lag p95, then fewer partitions.",
            "summary": stage_a[selected_partition],
        }
        if args.stage == "partitions":
            experiment["stage_status"].update({
                "B_cores": "NOT_REQUESTED",
                "C_workers": "NOT_REQUESTED",
                "D_final_validation": "NOT_REQUESTED",
                "higher_stress": "NOT_REQUESTED",
            })
            _persist_experiment(summary_path, experiment, config_results, status="PARTIAL", stress_results=stress_results)
            return 0
        _persist_experiment(summary_path, experiment, config_results, status="RUNNING", stress_results=stress_results)

        run_config(p_best, 2, 1, 1, repetitions=args.repetitions, label="B_bronze_core_scaling")
        run_config(p_best, 2, 2, 1, repetitions=args.repetitions, label="B_silver_core_scaling")
        experiment["stage_status"]["B_cores"] = "COMPLETE"
        stage_b = {
            config_id: summary
            for config_id, summary in summaries_for_workers(1).items()
            if parse_config(config_id)[0] == p_best
        }
        c_best = select_best_core_config(stage_b)
        best_dimensions = [int(value[1:]) for value in c_best.split("-")]
        best_partitions, best_bronze, best_silver, _ = best_dimensions
        experiment["selected_single_worker_config"] = {
            "config_id": c_best,
            "rule": "Highest median sustainable pipeline rate; within 2% ties prefer fewer application cores, then lower latency, lag, and drain.",
            "summary": stage_b[c_best],
        }
        if args.stage == "cores":
            experiment["stage_status"].update({
                "C_workers": "NOT_REQUESTED",
                "D_final_validation": "NOT_REQUESTED",
                "higher_stress": "NOT_REQUESTED",
            })
            _persist_experiment(summary_path, experiment, config_results, status="PARTIAL", stress_results=stress_results)
            return 0
        _persist_experiment(summary_path, experiment, config_results, status="RUNNING", stress_results=stress_results)

        run_config(best_partitions, best_bronze, best_silver, 2, repetitions=args.repetitions, label="C1_worker_count_only")
        run_config(best_partitions, 4, 4, 2, repetitions=args.repetitions, label="C2_combined_scale_out")
        experiment["stage_status"]["C_workers"] = "COMPLETE"
        stage_c = summaries_for_workers(2)
        selected_two_worker = select_best_core_config(stage_c)
        experiment["selected_two_worker_config"] = {
            "config_id": selected_two_worker,
            "classification": "WORKER_COUNT_ONLY" if selected_two_worker.endswith("-w2") and parse_cores(selected_two_worker) == parse_cores(c_best) else "COMBINED_SCALE_OUT",
            "summary": stage_c[selected_two_worker],
        }

        if args.final_repetitions > args.repetitions:
            final_configs = {
                "p1-b1-s1-w1": (1, 1, 1, 1),
                c_best: (best_partitions, best_bronze, best_silver, 1),
                selected_two_worker: tuple(parse_config(selected_two_worker)),
            }
            for _, (partitions, bronze, silver, workers) in final_configs.items():
                run_config(partitions, bronze, silver, workers, repetitions=args.final_repetitions, label="D_final_validation")
        experiment["stage_status"]["D_final_validation"] = "COMPLETE"

        valid_primary_runs = [
            row for rows in config_results.values() for row in rows
            if row.get("valid_for_comparison") is True
            and row.get("requested_rate_msgs_sec") == args.rate
        ]
        input_limited = bool(valid_primary_runs) and len(valid_primary_runs) == sum(map(len, config_results.values())) and all(
            row.get("capacity_classification") == "UNDER_CAPACITY"
            and row.get("pipeline_sustainable_rate") is not None
            and row.get("actual_generated_msgs_sec") is not None
            and row["pipeline_sustainable_rate"] >= 0.95 * row["actual_generated_msgs_sec"]
            for row in valid_primary_runs
        )
        experiment["five_thousand_input_limited"] = input_limited if args.rate == 5_000 else None
        if args.stage == "all" and args.adaptive_stress and args.rate == 5_000 and input_limited:
            experiment["stage_status"]["higher_stress"] = "RUNNING"
            write_json(experiment_dir / "experiment_manifest.json", experiment)
            high_calibration, high_path = ensure_calibrated(8_000)
            if high_calibration.get("calibrated"):
                high_config_list = [
                    (1, 1, 1, 1, "baseline"),
                    (best_partitions, best_bronze, best_silver, 1, "best_single"),
                    (*parse_config(selected_two_worker), "best_two_worker"),
                ]
                high_8k_results = stress_results.setdefault("8000", {})
                for partitions, bronze, silver, workers, label in high_config_list:
                    run_config(
                        partitions, bronze, silver, workers,
                        repetitions=args.repetitions,
                        label=f"high_8000_{label}",
                        rate=8_000,
                        calibration_for_run=high_calibration,
                        calibration_path_for_run=high_path,
                        result_store=high_8k_results,
                                source_records=max(args.source_records, 480_000),
                    )
                high_8k_rows = [row for rows in high_8k_results.values() for row in rows]
                input_limited_8k = bool(high_8k_rows) and all(
                    row.get("valid_for_comparison") is True
                    and row.get("capacity_classification") == "UNDER_CAPACITY"
                    and row.get("pipeline_sustainable_rate") is not None
                    and row.get("actual_generated_msgs_sec") is not None
                    and row["pipeline_sustainable_rate"] >= 0.95 * row["actual_generated_msgs_sec"]
                    for row in high_8k_rows
                )
                if input_limited_8k:
                    high_10k_calibration, high_10k_path = ensure_calibrated(10_000)
                    if high_10k_calibration.get("calibrated"):
                        high_10k_results = stress_results.setdefault("10000", {})
                        for partitions, bronze, silver, workers, label in high_config_list:
                            run_config(
                                partitions, bronze, silver, workers,
                                repetitions=args.repetitions,
                                label=f"high_10000_{label}",
                                rate=10_000,
                                calibration_for_run=high_10k_calibration,
                                calibration_path_for_run=high_10k_path,
                                result_store=high_10k_results,
                                source_records=max(args.source_records, 600_000),
                            )
                        experiment["stage_status"]["higher_stress"] = "COMPLETE_8000_AND_10000"
                    else:
                        experiment["stage_status"]["higher_stress"] = "COMPLETE_8000_10K_UNCALIBRATED"
                else:
                    experiment["stage_status"]["higher_stress"] = "COMPLETE_8000"
            else:
                experiment["stage_status"]["higher_stress"] = "SKIPPED_UNCALIBRATED"
        elif args.rate == 5_000:
            experiment["stage_status"]["higher_stress"] = "NOT_REQUIRED_INPUT_NOT_LIMITED" if not input_limited else "NOT_REQUESTED"
        else:
            experiment["stage_status"]["higher_stress"] = "NOT_REQUESTED"
        _persist_experiment(summary_path, experiment, config_results, status="COMPLETE", stress_results=stress_results)
        print(f"[SCALABILITY] experiment summary={summary_path.relative_to(REPO_ROOT)}")
        return 0
    except Exception as exc:
        experiment["error"] = {"type": type(exc).__name__, "message": str(exc)}
        _persist_experiment(summary_path, experiment, config_results, status="FAILED", stress_results=stress_results)
        raise
    finally:
        try:
            worker_summary = _set_worker_count(1)
            experiment["final_worker_state"] = worker_summary
        except Exception as exc:
            experiment["worker_restore_error"] = f"{type(exc).__name__}: {exc}"
        write_json(experiment_dir / "experiment_manifest.json", experiment)
        _persist_experiment(
            summary_path,
            experiment,
            config_results,
            status=experiment.get("status", "FAILED"),
            stress_results=stress_results,
        )


def parse_config(config_id: str) -> tuple[int, int, int, int]:
    pieces = config_id.split("-")
    return tuple(int(piece[1:]) for piece in pieces)  # type: ignore[return-value]


def parse_cores(config_id: str) -> tuple[int, int]:
    _, bronze, silver, _ = parse_config(config_id)
    return bronze, silver


def parse_args():
    parser = argparse.ArgumentParser(description="Coordinate the Kafka/Spark scalability OFAT benchmark.")
    parser.add_argument("--stage", choices=("all", "partitions", "cores", "workers"), default="all", help="Run through the selected OFAT stage; dependencies are run first.")
    parser.add_argument("--experiment-id", default=None)
    parser.add_argument("--source", default=str(DEFAULT_SOURCE))
    parser.add_argument("--source-records", type=int, default=500_000)
    parser.add_argument("--rate", type=int, default=5_000)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--final-repetitions", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--warmup-seconds", type=float, default=30.0)
    parser.add_argument("--warmup-min-batches", type=int, default=3)
    parser.add_argument("--drain-timeout", type=float, default=300.0)
    parser.add_argument("--adaptive-stress", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run_matrix(parse_args()))
