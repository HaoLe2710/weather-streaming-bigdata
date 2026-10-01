"""Deterministic synthetic Kafka inputs for state-machine edge-case smokes only."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

from historical.location_catalog import load_catalog  # noqa: E402


def _observation(index: int, *, location_id: str, city: str, latitude: float, longitude: float) -> dict[str, object]:
    instant = datetime(2024, 6, 1, tzinfo=timezone.utc) + timedelta(hours=index)
    wind_speed = float(index % 9)
    return {
        "event_id": f"{location_id}_fixture_{instant:%Y%m%dT%H%M%SZ}",
        "location_id": location_id,
        "city": city,
        "latitude": latitude,
        "longitude": longitude,
        "event_time": instant.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "temperature_c": 19.0 + index * 0.1,
        "humidity_pct": float(55 + index % 20),
        "precipitation_mm": float(index % 4) * 0.25,
        "pressure_hpa": 1008.0 + index * 0.02,
        "wind_speed_kmh": wind_speed,
        "wind_gust_kmh": wind_speed + float(index % 3),
        "weather_code": 1,
        "source": "controlled_test_fixture_only",
    }


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


def create_streaming_test_fixtures(output_dir: Path) -> dict[str, int]:
    canonical_hcm = next(item for item in load_catalog() if item["location_id"] == "VN_HCM")
    common = {
        "location_id": str(canonical_hcm["location_id"]),
        "city": str(canonical_hcm["name"]),
        "latitude": float(canonical_hcm["latitude"]),
        "longitude": float(canonical_hcm["longitude"]),
    }
    warmup = [_observation(index, **common) for index in range(25)]

    # One exact-hour gap at t=12; t=37 arrives first, then older hours, then a
    # byte-identical duplicate of t=37 after multiple ten-offset micro-batches.
    gap_arrival_indices = [37, *[index for index in range(37) if index != 12], 37]
    gap_out_of_order = [_observation(index, **common) for index in gap_arrival_indices]
    _write_jsonl(output_dir / "warmup_fixture.jsonl", warmup)
    _write_jsonl(output_dir / "gap_out_of_order_duplicate_fixture.jsonl", gap_out_of_order)
    return {
        "warmup_source_rows": len(warmup),
        "warmup_unique_location_hours": len({item["event_time"] for item in warmup}),
        "gap_source_rows_including_duplicate": len(gap_out_of_order),
        "gap_unique_location_hours": len({item["event_time"] for item in gap_out_of_order}),
        "gap_missing_event_hour_index": 12,
        "out_of_order_first_event_hour_index": 37,
        "duplicate_event_hour_index": 37,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Write non-parity Kafka input fixtures for streaming edge-case tests")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(create_streaming_test_fixtures(args.output_dir), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
