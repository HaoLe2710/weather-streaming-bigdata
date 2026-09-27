"""Recompute throughput summaries without modifying original run artifacts."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import uuid


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "benchmark"))
sys.path.insert(0, str(REPO_ROOT / "spark" / "jobs"))

from benchmark_config import write_json  # noqa: E402
from performance_metrics import (  # noqa: E402
    DEFAULT_WARMUP_MIN_BATCHES,
    DEFAULT_WARMUP_SECONDS,
    aggregate_repetitions,
    analyze_run_artifacts,
    read_jsonl,
)


THROUGHPUT_ROOT = REPO_ROOT / "results" / "benchmarks" / "throughput"
RAW_FILENAMES = (
    "spark_progress.jsonl",
    "kafka_lag.jsonl",
    "resource_metrics.jsonl",
    "simulator.json",
    "delta_metrics.json",
    "result.json",
    "manifest.json",
)


def read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object in {path}.")
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_rates(value: str) -> set[int]:
    try:
        rates = {int(part.strip()) for part in value.split(",") if part.strip()}
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Rates must be comma-separated integers.") from exc
    if not rates or any(rate <= 0 for rate in rates):
        raise argparse.ArgumentTypeError("At least one positive rate is required.")
    return rates


def analyze_run(run_dir: Path, warmup_seconds: float, minimum_batches: int) -> tuple[dict, dict]:
    source_files = {name: run_dir / name for name in RAW_FILENAMES}
    missing = [name for name, path in source_files.items() if not path.is_file()]
    if missing:
        raise ValueError("Missing raw artifact(s): " + ", ".join(missing))

    original_result = read_json(source_files["result.json"])
    manifest = read_json(source_files["manifest.json"])
    simulator = read_json(source_files["simulator.json"])
    delta = read_json(source_files["delta_metrics.json"])
    before = {name: sha256(path) for name, path in source_files.items()}
    analysis = analyze_run_artifacts(
        progress_rows=read_jsonl(source_files["spark_progress.jsonl"]),
        lag_samples=read_jsonl(source_files["kafka_lag.jsonl"]),
        resource_samples=read_jsonl(source_files["resource_metrics.jsonl"]),
        simulator_summary=simulator,
        delta_metrics=delta,
        manifest=manifest,
        legacy_result=original_result,
        minimum_warmup_seconds=warmup_seconds,
        minimum_completed_batches=minimum_batches,
        drain_complete=original_result.get("drain_complete"),
        stream_process_return_codes=original_result.get("stream_process_return_codes"),
        failure_reason=(original_result.get("errors") or {}).get("run"),
        metrics_error=(original_result.get("errors") or {}).get("metrics"),
    )
    after = {name: sha256(path) for name, path in source_files.items()}
    if before != after:
        raise RuntimeError(f"An input artifact changed during reanalysis: {run_dir.name}")

    entry = {
        "run_id": run_dir.name,
        "source_run_directory": run_dir.relative_to(REPO_ROOT).as_posix(),
        "previous_capacity_classification": original_result.get(
            "capacity_classification", original_result.get("status")
        ),
        "raw_artifact_sha256": before,
        "analysis": analysis,
    }
    return entry, analysis


def reanalyze(args) -> Path:
    experiment_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    experiment_dir = THROUGHPUT_ROOT / "experiments" / experiment_id
    experiment_dir.mkdir(parents=True, exist_ok=False)
    reanalysis_dir = experiment_dir / "runs"
    reanalysis_dir.mkdir()

    selected: dict[int, list[dict]] = {rate: [] for rate in sorted(args.rates)}
    skipped: list[dict] = []
    for run_dir in sorted(path for path in THROUGHPUT_ROOT.iterdir() if path.is_dir()):
        if run_dir.name == "experiments":
            continue
        result_path = run_dir / "result.json"
        simulator_path = run_dir / "simulator.json"
        if not result_path.is_file() or not simulator_path.is_file():
            continue
        try:
            legacy_result = read_json(result_path)
            simulator = read_json(simulator_path)
            rate = int(legacy_result.get(
                "requested_rate", legacy_result.get("requested_rate_msgs_sec", -1)
            ))
            produced = int(simulator.get("kafka_messages") or 0)
        except (ValueError, TypeError, OSError, json.JSONDecodeError) as exc:
            skipped.append({"run_id": run_dir.name, "reason": f"metadata unreadable: {exc}"})
            continue
        if rate not in selected:
            continue
        if produced != 50_000:
            skipped.append({
                "run_id": run_dir.name,
                "requested_rate": rate,
                "reason": f"expected the existing 50,000-message run; found {produced}",
            })
            continue
        if legacy_result.get("status") in {"FAILED", "INVALID_GENERATOR"}:
            skipped.append({"run_id": run_dir.name, "requested_rate": rate, "reason": "run was not a valid completed throughput run"})
            continue
        try:
            entry, analysis = analyze_run(
                run_dir, args.warmup_seconds, args.minimum_batches
            )
        except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
            skipped.append({"run_id": run_dir.name, "requested_rate": rate, "reason": str(exc)})
            continue
        target = reanalysis_dir / f"{run_dir.name}.json"
        write_json(target, entry)
        selected[rate].append({"analysis": analysis, "run_id": run_dir.name, "path": target})

    missing_rates = [rate for rate, runs in selected.items() if not runs]
    if missing_rates:
        raise RuntimeError("No valid existing runs found for rate(s): " + ", ".join(map(str, missing_rates)))

    rate_rows = []
    for rate, runs in selected.items():
        aggregate = aggregate_repetitions([run["analysis"] for run in runs])
        rate_entry = {
            "requested_rate": rate,
            "run_count": len(runs),
            "run_ids": [run["run_id"] for run in runs],
            "run_analyses": [run["path"].relative_to(REPO_ROOT).as_posix() for run in runs],
            "aggregate": aggregate,
        }
        rate_rows.append(rate_entry)

    summary = {
        "schema_version": 2,
        "experiment_type": "throughput_raw_artifact_reanalysis",
        "experiment_id": experiment_id,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "methodology": {
            "raw_inputs_modified": False,
            "minimum_warmup_seconds": args.warmup_seconds,
            "minimum_completed_batches_per_query": args.minimum_batches,
            "steady_state_start": "later of query-start plus elapsed warm-up, the selected completed-batch count for both main queries, and replay start",
            "steady_state_end": "simulator generation_end_time; post-replay drain excluded",
            "rates": sorted(selected),
        },
        "rates": rate_rows,
        "skipped_runs": skipped,
    }
    summary_path = experiment_dir / "summary.json"
    write_json(summary_path, summary)
    return summary_path


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rates", type=parse_rates, default=parse_rates("100,500,1000"))
    parser.add_argument("--warmup-seconds", type=float, default=DEFAULT_WARMUP_SECONDS)
    parser.add_argument("--minimum-batches", type=int, default=DEFAULT_WARMUP_MIN_BATCHES)
    args = parser.parse_args()
    if args.warmup_seconds < 0:
        parser.error("--warmup-seconds must be zero or greater")
    if args.minimum_batches < 1:
        parser.error("--minimum-batches must be at least one")
    return args


if __name__ == "__main__":
    try:
        output = reanalyze(parse_args())
        print(output.relative_to(REPO_ROOT).as_posix())
    except Exception as exc:
        print(f"Reanalysis failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)
