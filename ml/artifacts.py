"""Small, atomic, and resumable modeling-result artifact helpers."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from typing import Any, Iterable, Mapping


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _clean_json(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(key): _clean_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean_json(item) for item in value]
    if hasattr(value, "item"):
        return _clean_json(value.item())
    return value


def atomic_write_json(path: str | Path, value: Any) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        _clean_json(value),
        indent=2,
        ensure_ascii=False,
        default=_json_default,
        allow_nan=False,
    ) + "\n"
    _atomic_write_text(destination, payload)


def atomic_write_text(path: str | Path, value: str) -> None:
    _atomic_write_text(Path(path), value)


def _atomic_write_text(destination: Path, value: str) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def read_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_csv(path: str | Path, records: Iterable[Mapping[str, Any]], fieldnames: list[str]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for record in records:
                writer.writerow(
                    {
                        key: json.dumps(_clean_json(record.get(key)), ensure_ascii=False)
                        if isinstance(record.get(key), (dict, list, tuple))
                        else record.get(key)
                        for key in fieldnames
                    }
                )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


CANDIDATE_CSV_FIELDS = [
    "candidate_id",
    "params",
    "train_rows",
    "validation_rows",
    "best_iteration",
    "validation_mae",
    "validation_rmse",
    "validation_r2",
    "validation_bias",
    "training_time_seconds",
    "device",
    "status",
    "error",
]


def persist_candidate_results(directory: str | Path, records: list[dict[str, Any]]) -> None:
    root = Path(directory)
    atomic_write_json(root / "candidate_results.json", records)
    rows = []
    for record in records:
        metrics = record.get("validation_metrics") or {}
        rows.append(
            {
                **record,
                "validation_mae": metrics.get("mae"),
                "validation_rmse": metrics.get("rmse"),
                "validation_r2": metrics.get("r2"),
                "validation_bias": metrics.get("mean_error"),
            }
        )
    write_csv(root / "candidate_results.csv", rows, CANDIDATE_CSV_FIELDS)
