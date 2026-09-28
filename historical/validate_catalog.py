"""Write a machine-readable validation report for the canonical catalog."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import uuid

try:
    from .location_catalog import CATALOG_PATH, load_catalog, validate_catalog
except ImportError:
    from location_catalog import CATALOG_PATH, load_catalog, validate_catalog


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=CATALOG_PATH)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    locations = load_catalog(args.catalog)
    catalog_bytes = args.catalog.read_bytes()
    report = validate_catalog(locations)
    report.update({
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "catalog_path": str(args.catalog),
        "catalog_sha256": hashlib.sha256(catalog_bytes).hexdigest(),
    })
    if args.output:
        output_path = args.output
    else:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        output_path = Path("results/data-expansion") / run_id / "catalog_validation.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={output_path}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
