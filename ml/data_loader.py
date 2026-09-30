"""Artifact-driven feature contracts and memory-bounded Parquet split loading."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path, PurePosixPath
from typing import Any

import numpy as np

from ml.artifacts import canonical_json_sha256, sha256_file
from ml.config import (
    EXPECTED_LOCATION_COUNT,
    EXPECTED_MODEL_FEATURES,
    EXPECTED_PARQUET_BYTES,
    EXPECTED_PARQUET_FILES,
    EXPECTED_ROWS_PER_TEST_LOCATION,
    EXPECTED_SPLIT_ROWS,
    EXPECTED_TOTAL_ROWS,
    FEATURE_SET_ID,
    FORBIDDEN_FEATURES,
    TARGET_COLUMN,
)


@dataclass(frozen=True)
class FeatureContract:
    feature_set_id: str
    target: str
    model_features: tuple[str, ...]
    feature_list_sha256: str
    feature_manifest_sha256: str
    feature_spec_sha256: str
    ml_schema_sha256: str
    feature_validation_sha256: str
    parquet_inventory_sha256: str
    output_columns: tuple[str, ...]
    feature_manifest_path: str
    feature_spec_path: str
    ml_schema_path: str
    feature_validation_path: str
    parquet_inventory_path: str
    artifacts: dict[str, Any]


@dataclass
class SplitArrays:
    features: np.ndarray
    target: np.ndarray
    location_id: np.ndarray | None = None
    event_time: np.ndarray | None = None
    target_time: np.ndarray | None = None

    @property
    def row_count(self) -> int:
        return int(self.target.shape[0])


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return value


def load_feature_contract(artifact_dir: str | Path) -> FeatureContract:
    """Resolve the exact ordered model inputs from the frozen FE V1 artifacts."""

    root = Path(artifact_dir)
    required_files = {
        "feature_manifest.json",
        "feature_spec.json",
        "ml_schema.json",
        "feature_validation.json",
        "parquet_inventory.json",
        "checksums.json",
    }
    missing = sorted(name for name in required_files if not (root / name).is_file())
    if missing:
        raise FileNotFoundError(f"Feature-contract artifacts are missing: {missing}")

    manifest_path = root / "feature_manifest.json"
    spec_path = root / "feature_spec.json"
    schema_path = root / "ml_schema.json"
    validation_path = root / "feature_validation.json"
    inventory_path = root / "parquet_inventory.json"
    manifest = _read_json(manifest_path)
    spec = _read_json(spec_path)
    schema = _read_json(schema_path)
    validation = _read_json(validation_path)
    inventory = _read_json(inventory_path)
    checksums = _read_json(root / "checksums.json")

    declared_checksums = checksums.get("files", {})
    checked_paths = {
        "feature_manifest.json": manifest_path,
        "feature_spec.json": spec_path,
        "feature_validation.json": validation_path,
        "ml_schema.json": schema_path,
        "parquet_inventory.json": inventory_path,
    }
    for name, path in checked_paths.items():
        expected = declared_checksums.get(name)
        actual = sha256_file(path)
        if expected != actual:
            raise ValueError(f"Frozen feature artifact checksum mismatch for {name}")

    if manifest.get("status") != "PASS" or validation.get("status") != "PASS":
        raise ValueError("Frozen feature-engineering evidence is not PASS")
    if manifest.get("feature_set_id") != FEATURE_SET_ID or spec.get("feature_set_id") != FEATURE_SET_ID:
        raise ValueError("Feature-set ID does not match the V1 modeling contract")
    if schema.get("feature_set_id") != FEATURE_SET_ID:
        raise ValueError("ML schema feature-set ID does not match the V1 modeling contract")

    # The frozen artifacts encode MODEL_FEATURE membership as manifest.model_features;
    # the corresponding feature_spec entries use role="feature" inside model_features.
    # Preserve manifest order and require exact agreement from both machine-readable files.
    model_features = manifest.get("model_features")
    spec_features = spec.get("model_features")
    if not isinstance(model_features, list) or not all(isinstance(name, str) for name in model_features):
        raise ValueError("feature_manifest.model_features must be an ordered list of names")
    if not isinstance(spec_features, list):
        raise ValueError("feature_spec.model_features must be a list")
    spec_names = [item.get("name") for item in spec_features if item.get("role") == "feature"]
    if len(spec_names) != len(spec_features) or spec_names != model_features:
        raise ValueError("Manifest feature order differs from feature_spec MODEL_FEATURE membership")
    if len(model_features) != EXPECTED_MODEL_FEATURES or len(set(model_features)) != len(model_features):
        raise ValueError(f"Expected {EXPECTED_MODEL_FEATURES} unique ordered model features")

    target = manifest.get("label")
    spec_label = spec.get("label")
    if (
        target != TARGET_COLUMN
        or not isinstance(spec_label, dict)
        or spec_label.get("name") != TARGET_COLUMN
        or spec_label.get("role") != "target"
    ):
        raise ValueError(f"Unexpected target contract: {target!r}")
    forbidden_inputs = sorted(FORBIDDEN_FEATURES.intersection(model_features))
    if forbidden_inputs:
        raise ValueError(f"Forbidden metadata/label fields entered the model list: {forbidden_inputs}")

    output_columns = spec.get("output_columns")
    schema_columns = schema.get("columns")
    if not isinstance(output_columns, list) or not all(isinstance(name, str) for name in output_columns):
        raise ValueError("feature_spec.output_columns must be an ordered list of names")
    if not isinstance(schema_columns, list):
        raise ValueError("ml_schema.columns must be a list")
    schema_names = [column.get("name") for column in schema_columns]
    if output_columns != schema_names:
        raise ValueError("feature_spec output column order differs from ml_schema")
    if set(model_features) - set(schema_names) or target not in schema_names:
        raise ValueError("Model features or target are absent from the ML schema")
    numeric_types = {"double", "float", "int", "integer", "bigint", "long", "smallint"}
    schema_by_name = {column["name"]: column for column in schema_columns}
    invalid_types = {
        name: schema_by_name[name].get("type")
        for name in model_features
        if str(schema_by_name[name].get("type", "")).lower() not in numeric_types
    }
    if invalid_types:
        raise ValueError(f"Model features must be numeric: {invalid_types}")

    quality = validation.get("numeric_quality", {})
    if quality.get("null_required_feature_values") != 0:
        raise ValueError("Frozen feature validation reports null required model inputs")
    if quality.get("nan_feature_values") != 0 or quality.get("infinite_feature_values") != 0:
        raise ValueError("Frozen feature validation reports non-finite model inputs")
    per_split_quality = quality.get("per_split", {})
    for split in EXPECTED_SPLIT_ROWS:
        stats = per_split_quality.get(split)
        if not isinstance(stats, dict) or not (set(model_features) | {target}).issubset(stats):
            raise ValueError(f"Feature validation lacks complete {split} per-feature null evidence")
        if any(
            int(values.get("null", -1)) != 0
            or int(values.get("nan", -1)) != 0
            or int(values.get("infinite", -1)) != 0
            for name, values in stats.items()
            if name in set(model_features) | {target}
        ):
            raise ValueError(f"Feature validation reports invalid required values in {split}")

    if manifest.get("output_rows") != EXPECTED_TOTAL_ROWS or validation.get("output_rows") != EXPECTED_TOTAL_ROWS:
        raise ValueError("Frozen feature row count differs from the expected total")
    if manifest.get("source_location_count") != EXPECTED_LOCATION_COUNT:
        raise ValueError("Frozen feature artifact does not represent all 63 locations")
    if schema.get("feature_count") != EXPECTED_MODEL_FEATURES:
        raise ValueError("Frozen ML schema feature count differs from the V1 contract")
    if inventory.get("parquet_file_count") != EXPECTED_PARQUET_FILES:
        raise ValueError("Frozen Parquet inventory file count differs from the V1 contract")
    if inventory.get("total_bytes") != EXPECTED_PARQUET_BYTES:
        raise ValueError("Frozen Parquet inventory size differs from the V1 contract")

    return FeatureContract(
        feature_set_id=FEATURE_SET_ID,
        target=target,
        model_features=tuple(model_features),
        feature_list_sha256=canonical_json_sha256(model_features),
        feature_manifest_sha256=sha256_file(manifest_path),
        feature_spec_sha256=sha256_file(spec_path),
        ml_schema_sha256=sha256_file(schema_path),
        feature_validation_sha256=sha256_file(validation_path),
        parquet_inventory_sha256=sha256_file(inventory_path),
        output_columns=tuple(output_columns),
        feature_manifest_path=manifest_path.as_posix(),
        feature_spec_path=spec_path.as_posix(),
        ml_schema_path=schema_path.as_posix(),
        feature_validation_path=validation_path.as_posix(),
        parquet_inventory_path=inventory_path.as_posix(),
        artifacts={
            "feature_manifest": manifest,
            "feature_spec": spec,
            "ml_schema": schema,
            "feature_validation": validation,
            "parquet_inventory": inventory,
            "checksums": checksums,
        },
    )


def feature_list_artifact(contract: FeatureContract) -> dict[str, Any]:
    return {
        "feature_set_id": contract.feature_set_id,
        "target": contract.target,
        "feature_list_sha256": contract.feature_list_sha256,
        "feature_role_source": "feature_manifest.model_features, cross-checked in order against feature_spec.model_features entries with role=feature",
        "features": list(contract.model_features),
    }


def _expected_file_sizes(contract: FeatureContract) -> dict[str, int]:
    entries = contract.artifacts["parquet_inventory"].get("files", [])
    expected: dict[str, int] = {}
    for item in entries:
        relative = PurePosixPath(item["path"]).as_posix()
        if relative in expected:
            raise ValueError(f"Duplicate file in frozen Parquet inventory: {relative}")
        expected[relative] = int(item["size_bytes"])
    if len(expected) != EXPECTED_PARQUET_FILES:
        raise ValueError("Frozen Parquet inventory has an unexpected file entry count")
    return expected


def verify_dataset(dataset_dir: str | Path, contract: FeatureContract) -> dict[str, Any]:
    """Validate inventory, footer rows, split schema, and frozen null evidence.

    This routine reads Parquet footers and the already checksummed FE validation report;
    it does not materialize any TEST values or labels.
    """

    try:
        import pyarrow.dataset as ds
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - exercised in Colab or with optional deps
        raise RuntimeError("PyArrow is required to inspect the frozen Parquet dataset") from exc

    root = Path(dataset_dir)
    if not root.is_dir():
        raise FileNotFoundError(f"Feature dataset directory does not exist: {root}")
    actual_files = sorted(path for path in root.rglob("*.parquet") if path.is_file())
    expected_sizes = _expected_file_sizes(contract)
    actual_sizes = {path.relative_to(root).as_posix(): path.stat().st_size for path in actual_files}
    if actual_sizes != expected_sizes:
        missing = sorted(set(expected_sizes) - set(actual_sizes))
        extra = sorted(set(actual_sizes) - set(expected_sizes))
        changed = sorted(
            name for name in set(expected_sizes) & set(actual_sizes) if expected_sizes[name] != actual_sizes[name]
        )
        raise ValueError(f"Parquet inventory mismatch; missing={missing}, extra={extra}, changed_size={changed}")
    total_bytes = sum(actual_sizes.values())
    if len(actual_files) != EXPECTED_PARQUET_FILES or total_bytes != EXPECTED_PARQUET_BYTES:
        raise ValueError("Dataset Parquet file count or byte total differs from the frozen inventory")

    rows_by_split = {split: 0 for split in EXPECTED_SPLIT_ROWS}
    footer_schemas: list[list[str]] = []
    footer_null_counts: dict[str, int] = {}
    required_footer_columns = set(contract.model_features) | {contract.target}  # model inputs/label
    required_footer_columns.update({"location_id", "event_time", "target_time"})
    footer_null_complete = set(required_footer_columns)

    for path in actual_files:
        relative = path.relative_to(root).as_posix()
        split_part = PurePosixPath(relative).parts[0]
        if not split_part.startswith("split="):
            raise ValueError(f"Parquet file is not partitioned by split: {relative}")
        split = split_part.split("=", 1)[1]
        if split not in rows_by_split:
            raise ValueError(f"Unexpected split partition: {split}")
        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.metadata
        rows_by_split[split] += metadata.num_rows
        names = list(parquet_file.schema_arrow.names)
        footer_schemas.append(names)
        for name in required_footer_columns:
            if name not in names:
                footer_null_complete.discard(name)
                continue
            column_index = names.index(name)
            has_all_stats = True
            null_total = 0
            for row_group_index in range(metadata.num_row_groups):
                column = metadata.row_group(row_group_index).column(column_index)
                stats = column.statistics
                if stats is None or not getattr(stats, "has_null_count", False):
                    has_all_stats = False
                    break
                null_total += int(stats.null_count)
            if has_all_stats:
                footer_null_counts[name] = footer_null_counts.get(name, 0) + null_total
            else:
                footer_null_complete.discard(name)
    if len({tuple(names) for names in footer_schemas}) != 1:
        raise ValueError("Parquet files do not share one consistent physical schema")
    if rows_by_split != EXPECTED_SPLIT_ROWS:
        raise ValueError(f"Split row counts differ from the frozen contract: {rows_by_split}")
    if sum(rows_by_split.values()) != EXPECTED_TOTAL_ROWS:
        raise ValueError("Total footer row count differs from the frozen contract")

    dataset = ds.dataset(root, format="parquet", partitioning="hive", exclude_invalid_files=True)
    dataset_schema_names = set(dataset.schema.names)
    required_schema_names = set(contract.output_columns)
    if dataset_schema_names != required_schema_names:
        raise ValueError(
            "Dataset schema differs from ml_schema; "
            f"missing={sorted(required_schema_names - dataset_schema_names)}, "
            f"extra={sorted(dataset_schema_names - required_schema_names)}"
        )

    nonzero_footer_nulls = {name: count for name, count in footer_null_counts.items() if count != 0}
    if nonzero_footer_nulls:
        raise ValueError(f"Parquet footer reports null values in required columns: {nonzero_footer_nulls}")
    if not required_footer_columns.issubset(dataset_schema_names):
        raise ValueError("Parquet schema is missing required model, target, or evaluation columns")

    return {
        "status": "PASS",
        "feature_set_id": contract.feature_set_id,
        "dataset_path": root.as_posix(),
        "parquet_file_count": len(actual_files),
        "parquet_bytes": total_bytes,
        "rows_by_split": rows_by_split,
        "total_rows": sum(rows_by_split.values()),
        "dataset_schema_columns": len(dataset_schema_names),
        "model_feature_count": len(contract.model_features),
        "target": contract.target,
        "test_values_materialized": False,
        "footer_null_counts": footer_null_counts,
        "footer_null_count_fields_complete": sorted(footer_null_complete),
        "upstream_validation": {
            "path": contract.feature_validation_path,
            "sha256": contract.feature_validation_sha256,
            "null_required_feature_values": contract.artifacts["feature_validation"]["numeric_quality"][
                "null_required_feature_values"
            ],
            "nan_feature_values": contract.artifacts["feature_validation"]["numeric_quality"][
                "nan_feature_values"
            ],
            "infinite_feature_values": contract.artifacts["feature_validation"]["numeric_quality"][
                "infinite_feature_values"
            ],
        },
    }


def _as_datetime64_ns(values: Any) -> np.ndarray:
    result = np.asarray(values)
    if result.dtype.kind != "M":
        raise ValueError(f"Expected timestamp data, received dtype {result.dtype}")
    return result.astype("datetime64[ns]", copy=False)


def _timestamp_filter_scalar(pa: Any, value: str, arrow_type: Any) -> Any:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
        if arrow_type.tz is None:
            parsed = parsed.replace(tzinfo=None)
    return pa.scalar(parsed, type=arrow_type)


def load_split_arrays(
    dataset_dir: str | Path,
    split: str,
    contract: FeatureContract,
    *,
    include_metadata: bool = False,
    expected_rows: int | None = None,
    batch_size: int = 65_536,
    location_ids: list[str] | tuple[str, ...] | None = None,
    event_time_start: str | None = None,
    event_time_end: str | None = None,
) -> SplitArrays:
    """Read one partition in batches as float32; optional filters support bounded TRAIN/VALIDATION smokes.

    Event-time bounds are UTC and use the half-open interval [start, end). Full production
    runs omit filters and validate against the frozen split row count.
    """

    if split not in EXPECTED_SPLIT_ROWS:
        raise ValueError(f"Unknown split {split!r}")
    try:
        import pyarrow as pa
        import pyarrow.dataset as ds
    except ImportError as exc:  # pragma: no cover - exercised in Colab or with optional deps
        raise RuntimeError("PyArrow is required to load the frozen Parquet dataset") from exc

    root = Path(dataset_dir)
    dataset = ds.dataset(root, format="parquet", partitioning="hive", exclude_invalid_files=True)
    split_filter = ds.field("split") == split
    if location_ids is not None:
        if not location_ids:
            raise ValueError("location_ids filter must not be empty")
        split_filter = split_filter & ds.field("location_id").isin(list(location_ids))
    event_time_type = dataset.schema.field("event_time").type
    if event_time_start is not None:
        start_scalar = _timestamp_filter_scalar(pa, event_time_start, event_time_type)
        split_filter = split_filter & (ds.field("event_time") >= start_scalar)
    if event_time_end is not None:
        end_scalar = _timestamp_filter_scalar(pa, event_time_end, event_time_type)
        split_filter = split_filter & (ds.field("event_time") < end_scalar)
    actual_rows = dataset.count_rows(filter=split_filter)
    if expected_rows is None:
        filtered = location_ids is not None or event_time_start is not None or event_time_end is not None
        expected_rows = actual_rows if filtered else EXPECTED_SPLIT_ROWS[split]
    if actual_rows != expected_rows:
        raise ValueError(f"Filtered {split} row count is {actual_rows}; expected {expected_rows}")
    if expected_rows <= 0:
        raise ValueError(f"Filtered {split} selection is empty")

    columns = [*contract.model_features, contract.target]
    if include_metadata:
        columns.extend(name for name in ("location_id", "event_time", "target_time") if name not in columns)
    absent = sorted(set(columns) - set(dataset.schema.names))
    if absent:
        raise ValueError(f"Required columns are absent from the Parquet dataset: {absent}")

    x = np.empty((expected_rows, len(contract.model_features)), dtype=np.float32)
    y = np.empty(expected_rows, dtype=np.float32)
    locations: list[str] | None = [] if include_metadata else None
    event_times = np.empty(expected_rows, dtype="datetime64[ns]") if include_metadata else None
    target_times = np.empty(expected_rows, dtype="datetime64[ns]") if include_metadata else None

    scanner = dataset.scanner(
        columns=columns,
        filter=split_filter,
        batch_size=batch_size,
        use_threads=True,
    )
    offset = 0
    for batch in scanner.to_batches():
        end = offset + batch.num_rows
        if end > expected_rows:
            raise ValueError(f"{split} contains more rows than expected ({end} > {expected_rows})")
        for feature_index, name in enumerate(contract.model_features):
            values = batch.column(batch.schema.get_field_index(name))
            if values.null_count:
                raise ValueError(f"{split} has {values.null_count} null values in model feature {name}")
            numeric = values.to_numpy(zero_copy_only=False)
            if not np.isfinite(numeric).all():
                raise ValueError(f"{split} has NaN or infinite values in model feature {name}")
            x[offset:end, feature_index] = numeric

        labels = batch.column(batch.schema.get_field_index(contract.target))
        if labels.null_count:
            raise ValueError(f"{split} has {labels.null_count} null target values")
        y_values = labels.to_numpy(zero_copy_only=False)
        if not np.isfinite(y_values).all():
            raise ValueError(f"{split} has NaN or infinite target values")
        y[offset:end] = y_values

        if include_metadata:
            location_values = batch.column(batch.schema.get_field_index("location_id"))
            event_values = batch.column(batch.schema.get_field_index("event_time"))
            target_values = batch.column(batch.schema.get_field_index("target_time"))
            if location_values.null_count or event_values.null_count or target_values.null_count:
                raise ValueError(f"{split} has null evaluation metadata")
            assert locations is not None and event_times is not None and target_times is not None
            locations.extend(str(value) for value in location_values.to_pylist())
            event_times[offset:end] = _as_datetime64_ns(event_values.to_numpy(zero_copy_only=False))
            target_times[offset:end] = _as_datetime64_ns(target_values.to_numpy(zero_copy_only=False))
        offset = end

    if offset != expected_rows:
        raise ValueError(f"{split} contains {offset} rows; expected {expected_rows}")
    return SplitArrays(
        features=x,
        target=y,
        location_id=np.asarray(locations, dtype=object) if locations is not None else None,
        event_time=event_times,
        target_time=target_times,
    )
