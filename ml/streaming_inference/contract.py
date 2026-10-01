from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from ml.artifacts import canonical_json_sha256


MODEL_ID = "WEATHER_XGBOOST_GLOBAL_T1H_V1"
MODEL_SHA256 = "07fffbaed3e4934017a03135eddba8fc200fbcd22b799d246ea816695f98704a"
MODEL_BYTES = 51_662_443
FEATURE_SET_ID = "WEATHER_FORECAST_FE_V1"
FEATURE_LIST_SHA256 = "ec716f47ac4eca99a945a2ba1c50ba1297c1509a2aa5f80cff063042e40295bf"
FEATURE_COUNT = 73
FORECAST_HORIZON_HOURS = 1
XGBOOST_VERSION = "3.4.1"

MODEL_RUN_ID = "20261001T171122Z-xgb-v1"
FEATURE_RUN_ID = "20260930T1545Z-feature-v1"


@dataclass(frozen=True)
class FeatureContract:
    feature_names: tuple[str, ...]
    feature_list_sha256: str
    feature_set_id: str
    source_dataset_id: str
    model_id: str
    model_sha256: str
    model_bytes: int
    training_code_commit: str
    feature_code_commit: str
    timezone: str


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read required inference contract file {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Inference contract file must contain a JSON object: {path}")
    return value


def load_feature_contract(root: str | Path | None = None) -> FeatureContract:
    """Load and cross-check all committed V1 feature/model contract records."""
    base = Path(root) if root is not None else repository_root()
    feature_dir = base / "results" / "feature-engineering" / FEATURE_RUN_ID
    model_dir = base / "results" / "modeling" / MODEL_RUN_ID
    feature_spec = _read_json(feature_dir / "feature_spec.json")
    feature_manifest = _read_json(feature_dir / "feature_manifest.json")
    model_feature_list = _read_json(model_dir / "model_feature_list.json")
    training_manifest = _read_json(model_dir / "training_manifest.json")
    artifact_manifest = _read_json(model_dir / "model_artifact_manifest.json")

    names = model_feature_list.get("features")
    if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
        raise ValueError("Canonical model_feature_list.json has no ordered string feature list")
    ordered_names = tuple(names)
    feature_hash = canonical_json_sha256(list(ordered_names))
    expected_from_spec = tuple(
        item.get("name")
        for item in feature_spec.get("model_features", [])
        if isinstance(item, dict) and item.get("role") == "feature"
    )
    expected_from_manifest = tuple(feature_manifest.get("model_features", []))

    checks = {
        "feature set": (
            model_feature_list.get("feature_set_id") == FEATURE_SET_ID
            and feature_spec.get("feature_set_id") == FEATURE_SET_ID
            and feature_manifest.get("feature_set_id") == FEATURE_SET_ID
            and training_manifest.get("feature_set_id") == FEATURE_SET_ID
        ),
        "feature count": len(ordered_names) == FEATURE_COUNT,
        "feature-list digest": (
            feature_hash == FEATURE_LIST_SHA256
            and model_feature_list.get("feature_list_sha256") == FEATURE_LIST_SHA256
            and training_manifest.get("feature_list_sha256") == FEATURE_LIST_SHA256
        ),
        "feature-spec order": expected_from_spec == ordered_names,
        "feature-manifest order": expected_from_manifest == ordered_names,
        "training feature count": training_manifest.get("model_feature_count") == FEATURE_COUNT,
        "model identity": (
            training_manifest.get("model_id") == MODEL_ID
            and artifact_manifest.get("model_id") == MODEL_ID
        ),
        "model digest": (
            training_manifest.get("model_sha256") == MODEL_SHA256
            and artifact_manifest.get("sha256") == MODEL_SHA256
        ),
        "model bytes": (
            training_manifest.get("model_bytes") == MODEL_BYTES
            and artifact_manifest.get("size_bytes") == MODEL_BYTES
        ),
        "xgboost runtime": training_manifest.get("xgboost_version") == XGBOOST_VERSION,
        "timezone": feature_spec.get("timezone") == "Asia/Ho_Chi_Minh",
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise ValueError(f"Frozen inference contract validation failed: {failed}")
    if len(set(ordered_names)) != FEATURE_COUNT:
        raise ValueError("Canonical model feature list contains duplicate feature names")

    forbidden = {
        "target_temperature_1h",
        "target_time",
        "event_time",
        "split",
        "event_id",
        "location_id",
        "city",
        "province_name",
        "weather_code",
    }
    if forbidden.intersection(ordered_names):
        raise ValueError(f"Forbidden non-feature columns in model input: {sorted(forbidden.intersection(ordered_names))}")

    return FeatureContract(
        feature_names=ordered_names,
        feature_list_sha256=feature_hash,
        feature_set_id=FEATURE_SET_ID,
        source_dataset_id=str(feature_manifest["source_dataset_id"]),
        model_id=MODEL_ID,
        model_sha256=MODEL_SHA256,
        model_bytes=MODEL_BYTES,
        training_code_commit=str(training_manifest["training_code_commit"]),
        feature_code_commit=str(feature_manifest["feature_git_commit"]),
        timezone="Asia/Ho_Chi_Minh",
    )


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
