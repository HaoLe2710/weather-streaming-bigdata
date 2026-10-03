from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from .contract import FeatureContract, load_feature_contract, repository_root


MODEL_ID = "WEATHER_XGBOOST_GLOBAL_T2H_V1_1"
MODEL_SHA256 = "bd5ee153b2709ac661557bdd11f8322b80de1264c65a27d1d6c79fbcf63ee66a"
MODEL_BYTES = 27_726_327
FEATURE_SET_ID = "WEATHER_FORECAST_FE_T2H_V1_1"
FEATURE_LIST_SHA256 = "20a5d2fb56d9b7231f4c43b39ad7a833298d76b1bfd0f127b2b251c57e5d7fd2"
FEATURE_COUNT = 73
FORECAST_HORIZON_HOURS = 2
XGBOOST_VERSION = "3.4.1"
CANONICAL_COLAB_RUN_ID = "20261003T122405Z-xgb-t2h-v1-1-colab"
CANONICAL_MODEL_DIR = "20261003T122405Z-xgb-t2h-v1-1-colab"
FEATURE_LIST_FILENAME = "feature_list.json"
MODEL_FILENAME = "weather_forecast_xgboost_t2h_v1_1.json"
FEATURE_HASH_ALGORITHM = "sha256 of this canonical JSON artifact, including identity and order"
PROVIDER_MODEL = "ecmwf_ifs"
PROVIDER_NAME = "Open-Meteo"
LIVE_SOURCE = "OPEN_METEO_LIVE_HOURLY"
LIVE_ENDPOINT = "https://api.open-meteo.com/v1/forecast"
REPLAY_SOURCE = "OPEN_METEO_HISTORICAL_FORECAST"
HISTORICAL_FORECAST_ENDPOINT = "https://historical-forecast-api.open-meteo.com/v1/forecast"


def default_feature_list_path() -> Path:
    configured = os.getenv("WEATHER_T2H_FEATURE_LIST_PATH")
    if configured:
        return Path(configured)
    return (
        repository_root()
        / "results"
        / "modeling-t2h"
        / CANONICAL_MODEL_DIR
        / FEATURE_LIST_FILENAME
    )


def default_model_path() -> Path:
    configured = os.getenv("WEATHER_T2H_MODEL_PATH")
    if configured:
        return Path(configured)
    return (
        repository_root()
        / "results"
        / "modeling-t2h"
        / CANONICAL_MODEL_DIR
        / MODEL_FILENAME
    )


def _identity_bound_feature_hash(feature_names: tuple[str, ...]) -> str:
    # This object and serialization must match
    # ml.forecast_t2h_v1_1._canonical_json_bytes(_feature_order_artifact()).
    artifact = {
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "feature_count": len(feature_names),
        "ordered_model_features": list(feature_names),
        "hash_algorithm": FEATURE_HASH_ALGORITHM,
    }
    payload = (json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def validate_t2h_configuration() -> None:
    expected = {
        "WEATHER_T2H_MODEL_ID": MODEL_ID,
        "WEATHER_T2H_MODEL_SHA256": MODEL_SHA256,
        "WEATHER_T2H_FEATURE_SET_ID": FEATURE_SET_ID,
        "WEATHER_T2H_FEATURE_LIST_SHA256": FEATURE_LIST_SHA256,
        "WEATHER_T2H_FORECAST_HORIZON_HOURS": str(FORECAST_HORIZON_HOURS),
        "WEATHER_T2H_PROVIDER_MODEL": PROVIDER_MODEL,
    }
    mismatches = {
        name: {"expected": value, "configured": os.environ[name]}
        for name, value in expected.items()
        if name in os.environ and os.environ[name] != value
    }
    if mismatches:
        raise ValueError(f"T2H runtime configuration differs from the frozen contract: {mismatches}")


def load_t2h_feature_contract(path: str | Path | None = None) -> FeatureContract:
    validate_t2h_configuration()
    feature_path = Path(path) if path is not None else default_feature_list_path()
    if not feature_path.is_file():
        raise FileNotFoundError(f"canonical T2H feature-list artifact is missing: {feature_path}")
    try:
        artifact: Any = json.loads(feature_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read canonical T2H feature-list artifact {feature_path}: {exc}") from exc
    if not isinstance(artifact, dict):
        raise ValueError("canonical T2H feature-list artifact must be a JSON object")
    names = artifact.get("ordered_model_features")
    if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
        raise ValueError("canonical T2H feature-list artifact has no ordered string feature list")
    feature_names = tuple(names)
    if artifact.get("model_id") != MODEL_ID:
        raise ValueError(f"T2H feature-list model id mismatch: {artifact.get('model_id')!r}")
    if artifact.get("feature_set_id") != FEATURE_SET_ID:
        raise ValueError(f"T2H feature-set id mismatch: {artifact.get('feature_set_id')!r}")
    if artifact.get("feature_count") != FEATURE_COUNT or len(feature_names) != FEATURE_COUNT:
        raise ValueError(f"T2H feature count must be {FEATURE_COUNT}")
    if artifact.get("hash_algorithm") != FEATURE_HASH_ALGORITHM:
        raise ValueError("T2H feature-list hash algorithm does not match the frozen contract")
    if len(set(feature_names)) != FEATURE_COUNT:
        raise ValueError("canonical T2H feature list contains duplicate names")
    actual_hash = _identity_bound_feature_hash(feature_names)
    if actual_hash != FEATURE_LIST_SHA256:
        raise ValueError(f"T2H feature-list SHA-256 mismatch: expected {FEATURE_LIST_SHA256}, found {actual_hash}")

    # Modeling T2H V1.1 freezes the same ordered numerical feature columns as
    # V1 while binding them to a distinct model/feature-set identity. Compare
    # against the shared runtime list without importing the offline pipeline's
    # data-acquisition dependencies into Spark executors.
    if feature_names != load_feature_contract().feature_names:
        raise ValueError("T2H feature-list names/order differ from the canonical offline feature implementation")

    return FeatureContract(
        feature_names=feature_names,
        feature_list_sha256=actual_hash,
        feature_set_id=FEATURE_SET_ID,
        source_dataset_id="WEATHER_FORECAST_SOURCE_T2H_V1_1/ecmwf_ifs",
        model_id=MODEL_ID,
        model_sha256=MODEL_SHA256,
        model_bytes=MODEL_BYTES,
        training_code_commit="28d6b569f7ba9a883ce294edde67ce746dc4ee67",
        feature_code_commit="28d6b569f7ba9a883ce294edde67ce746dc4ee67",
        timezone="Asia/Ho_Chi_Minh",
    )
