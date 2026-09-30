from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import numpy as np

from ml.data_loader import SplitArrays
from ml.xgboost_model import fit_final_model, run_candidate_search, select_candidate


class XGBoostPipelineSmokeTests(unittest.TestCase):
    def test_candidate_resume_final_fit_and_json_reload(self):
        import xgboost as xgb

        rng = np.random.default_rng(42)
        train_x = rng.normal(size=(96, 2)).astype(np.float32)
        valid_x = rng.normal(size=(32, 2)).astype(np.float32)
        train_y = (0.7 * train_x[:, 0] - 0.2 * train_x[:, 1]).astype(np.float32)
        valid_y = (0.7 * valid_x[:, 0] - 0.2 * valid_x[:, 1]).astype(np.float32)
        train_data = SplitArrays(train_x, train_y)
        validation_data = SplitArrays(valid_x, valid_y)
        feature_names = ["temperature_c", "humidity_pct"]
        candidate = {
            "candidate_id": "cpu_smoke",
            "max_depth": 2,
            "learning_rate": 0.15,
            "min_child_weight": 1.0,
            "subsample": 0.9,
            "colsample_bytree": 1.0,
            "reg_lambda": 1.0,
            "reg_alpha": 0.0,
        }

        with tempfile.TemporaryDirectory() as temporary_directory:
            first = run_candidate_search(
                train_data,
                validation_data,
                feature_names,
                device="cpu",
                checkpoint_directory=temporary_directory,
                nthread=1,
                candidate_configs=[candidate],
                max_boost_rounds=12,
                early_stopping_rounds=3,
            )
            self.assertEqual(len(first), 1)
            self.assertEqual(first[0]["status"], "SUCCESS")
            resumed = run_candidate_search(
                train_data,
                validation_data,
                feature_names,
                device="cpu",
                checkpoint_directory=temporary_directory,
                nthread=1,
                candidate_configs=[candidate],
                max_boost_rounds=12,
                early_stopping_rounds=3,
            )
            self.assertEqual(len(resumed), 1)
            selected = select_candidate(resumed)
            trainval_data = SplitArrays(
                np.concatenate([train_data.features, validation_data.features]),
                np.concatenate([train_data.target, validation_data.target]),
            )
            model, final_device, fallback, duration = fit_final_model(
                trainval_data,
                feature_names,
                selected,
                device="cpu",
                nthread=1,
            )
            self.assertEqual(final_device, "cpu")
            self.assertIsNone(fallback)
            self.assertGreaterEqual(duration, 0)

            model_path = Path(temporary_directory) / "smoke_model.json"
            model.save_model(model_path)
            fresh_model = xgb.Booster()
            fresh_model.load_model(model_path)
            matrix = xgb.DMatrix(valid_x[:8], feature_names=feature_names)
            np.testing.assert_allclose(
                model.predict(matrix, iteration_range=(0, selected["best_iteration"] + 1)),
                fresh_model.predict(matrix),
                rtol=1e-6,
                atol=1e-6,
            )


if __name__ == "__main__":
    unittest.main()
