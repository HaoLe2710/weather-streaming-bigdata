from __future__ import annotations

from pathlib import Path
import tempfile
import shutil
import unittest

import numpy as np
import xgboost as xgb

from ml.config import (
    EXPECTED_PARQUET_BYTES,
    EXPECTED_SPLIT_ROWS,
    EXPECTED_TOTAL_ROWS,
    FEATURE_ARTIFACT_RELATIVE_PATH,
    FORBIDDEN_FEATURES,
    LOCAL_DATASET_RELATIVE_PATH,
)
from ml.baseline import persistence_predictions
from ml.data_loader import load_feature_contract, load_split_arrays, verify_dataset
from ml.metrics import regression_metrics


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class ModelingFeatureContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifact_dir = REPOSITORY_ROOT / FEATURE_ARTIFACT_RELATIVE_PATH
        cls.contract = load_feature_contract(cls.artifact_dir)

    def test_contract_uses_the_frozen_order_and_forbids_metadata(self):
        self.assertEqual(len(self.contract.model_features), 73)
        self.assertEqual(self.contract.model_features[0], "temperature_c")
        self.assertEqual(self.contract.target, "target_temperature_1h")
        self.assertFalse(FORBIDDEN_FEATURES.intersection(self.contract.model_features))
        self.assertEqual(len(self.contract.feature_list_sha256), 64)

    def test_feature_evidence_checksums_are_enforced(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            copied = Path(temporary_directory) / "feature-contract"
            shutil.copytree(self.artifact_dir, copied)
            with (copied / "ml_schema.json").open("a", encoding="utf-8") as handle:
                handle.write(" ")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                load_feature_contract(copied)

    def test_dataset_inventory_and_footers_match_frozen_splits_without_reading_test_values(self):
        dataset_dir = REPOSITORY_ROOT / LOCAL_DATASET_RELATIVE_PATH
        if not dataset_dir.is_dir():
            self.skipTest("Git-ignored feature dataset is not present in this checkout")
        report = verify_dataset(dataset_dir, self.contract)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["total_rows"], EXPECTED_TOTAL_ROWS)
        self.assertEqual(report["rows_by_split"], EXPECTED_SPLIT_ROWS)
        self.assertEqual(report["parquet_bytes"], EXPECTED_PARQUET_BYTES)
        self.assertFalse(report["test_values_materialized"])

    def test_bounded_arrow_loader_reads_only_train_and_validation_locations(self):
        dataset_dir = REPOSITORY_ROOT / LOCAL_DATASET_RELATIVE_PATH
        if not dataset_dir.is_dir():
            self.skipTest("Git-ignored feature dataset is not present in this checkout")
        locations = ["VN_HANOI", "VN_HCM"]
        train = load_split_arrays(
            dataset_dir,
            "TRAIN",
            self.contract,
            include_metadata=True,
            location_ids=locations,
            event_time_start="2020-01-02T00:00:00Z",
            event_time_end="2020-01-08T00:00:00Z",
        )
        validation = load_split_arrays(
            dataset_dir,
            "VALIDATION",
            self.contract,
            include_metadata=True,
            location_ids=locations,
            event_time_start="2024-01-02T00:00:00Z",
            event_time_end="2024-01-08T00:00:00Z",
        )
        self.assertEqual(train.features.dtype.name, "float32")
        self.assertEqual(validation.features.dtype.name, "float32")
        self.assertEqual(set(train.location_id), set(locations))
        self.assertEqual(set(validation.location_id), set(locations))
        self.assertLess(train.row_count, EXPECTED_SPLIT_ROWS["TRAIN"])
        self.assertLess(validation.row_count, EXPECTED_SPLIT_ROWS["VALIDATION"])
        np.testing.assert_array_equal(
            train.target_time,
            train.event_time + np.timedelta64(1, "h"),
        )
        baseline = persistence_predictions(validation.features, self.contract.model_features)
        baseline_metrics = regression_metrics(validation.target, baseline)
        self.assertEqual(baseline_metrics["n"], validation.row_count)

        training_matrix = xgb.DMatrix(
            train.features,
            label=train.target,
            feature_names=list(self.contract.model_features),
        )
        validation_matrix = xgb.DMatrix(
            validation.features,
            label=validation.target,
            feature_names=list(self.contract.model_features),
        )
        smoke_model = xgb.train(
            {
                "objective": "reg:squarederror",
                "eval_metric": "mae",
                "tree_method": "hist",
                "device": "cpu",
                "seed": 42,
                "max_depth": 3,
                "eta": 0.1,
                "nthread": 1,
                "verbosity": 0,
            },
            training_matrix,
            num_boost_round=5,
            verbose_eval=False,
        )
        predictions = smoke_model.predict(validation_matrix)
        model_metrics = regression_metrics(validation.target, predictions)
        self.assertTrue(np.isfinite(model_metrics["mae"]))
        with tempfile.TemporaryDirectory() as temporary_directory:
            model_path = Path(temporary_directory) / "local_smoke.json"
            smoke_model.save_model(model_path)
            fresh_model = xgb.Booster()
            fresh_model.load_model(model_path)
            np.testing.assert_allclose(
                predictions,
                fresh_model.predict(validation_matrix),
                rtol=1e-6,
                atol=1e-6,
            )


if __name__ == "__main__":
    unittest.main()
