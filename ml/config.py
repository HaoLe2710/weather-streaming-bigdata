"""Frozen inputs and constants for Weather Forecast Modeling V1."""

from __future__ import annotations

MODEL_ID = "WEATHER_XGBOOST_GLOBAL_T1H_V1"
FEATURE_SET_ID = "WEATHER_FORECAST_FE_V1"
TARGET_COLUMN = "target_temperature_1h"
PERSISTENCE_FEATURE = "temperature_c"
RANDOM_SEED = 42

EXPECTED_TOTAL_ROWS = 3_312_729
EXPECTED_LOCATION_COUNT = 63
EXPECTED_ROWS_PER_TEST_LOCATION = 8_760
EXPECTED_SPLIT_ROWS = {
    "TRAIN": 2_207_457,
    "VALIDATION": 553_392,
    "TEST": 551_880,
}
EXPECTED_FINAL_TRAINING_ROWS = EXPECTED_SPLIT_ROWS["TRAIN"] + EXPECTED_SPLIT_ROWS["VALIDATION"]
EXPECTED_MODEL_FEATURES = 73
EXPECTED_PARQUET_FILES = 15
EXPECTED_PARQUET_BYTES = 367_979_619

FORBIDDEN_FEATURES = frozenset(
    {
        "target_temperature_1h",
        "split",
        "event_time",
        "target_time",
        "location_id",
        "province_name",
        "city",
        "event_id",
    }
)
METADATA_COLUMNS = ("location_id", "event_time", "target_time")

MAX_BOOST_ROUNDS = 2_500
EARLY_STOPPING_ROUNDS = 100
XGBOOST_TREE_METHOD = "hist"

FEATURE_ARTIFACT_RELATIVE_PATH = "results/feature-engineering/20260930T1545Z-feature-v1"
LOCAL_DATASET_RELATIVE_PATH = "data/ml/weather_forecast_fe_v1"
DRIVE_DATASET_RELATIVE_PATH = "weather-streaming-bigdata/datasets/weather_forecast_fe_v1"
DRIVE_ARTIFACT_RELATIVE_PATH = "weather-streaming-bigdata/artifacts/weather_forecast_xgboost_v1"

GPU_CANDIDATES = (
    {
        "candidate_id": "gpu_d6_lr005",
        "max_depth": 6,
        "learning_rate": 0.05,
        "min_child_weight": 1.0,
        "subsample": 0.90,
        "colsample_bytree": 0.90,
        "reg_lambda": 1.0,
        "reg_alpha": 0.0,
    },
    {
        "candidate_id": "gpu_d4_regularized",
        "max_depth": 4,
        "learning_rate": 0.05,
        "min_child_weight": 3.0,
        "subsample": 0.90,
        "colsample_bytree": 0.90,
        "reg_lambda": 2.0,
        "reg_alpha": 0.0,
    },
    {
        "candidate_id": "gpu_d8_regularized",
        "max_depth": 8,
        "learning_rate": 0.05,
        "min_child_weight": 3.0,
        "subsample": 0.85,
        "colsample_bytree": 0.90,
        "reg_lambda": 3.0,
        "reg_alpha": 0.1,
    },
    {
        "candidate_id": "gpu_d6_lr003",
        "max_depth": 6,
        "learning_rate": 0.03,
        "min_child_weight": 1.0,
        "subsample": 0.90,
        "colsample_bytree": 0.80,
        "reg_lambda": 3.0,
        "reg_alpha": 0.1,
    },
    {
        "candidate_id": "gpu_d5_lr008",
        "max_depth": 5,
        "learning_rate": 0.08,
        "min_child_weight": 2.0,
        "subsample": 0.90,
        "colsample_bytree": 0.95,
        "reg_lambda": 1.0,
        "reg_alpha": 0.1,
    },
)

CPU_CANDIDATES = (
    GPU_CANDIDATES[0],
    GPU_CANDIDATES[1],
    GPU_CANDIDATES[4],
)
