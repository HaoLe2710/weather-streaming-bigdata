from __future__ import annotations

import unittest

import numpy as np

from ml.baseline import persistence_predictions
from ml.evaluation import evaluate_per_location, summarize_location_results, validate_test_coverage
from ml.metrics import compare_metrics, regression_metrics


class ModelingMetricsTests(unittest.TestCase):
    def test_shared_metrics_and_bias_direction(self):
        metrics = regression_metrics([1, 2, 3], [2, 2, 1])
        self.assertAlmostEqual(metrics["mae"], 1.0)
        self.assertAlmostEqual(metrics["rmse"], (5 / 3) ** 0.5)
        self.assertAlmostEqual(metrics["r2"], -1.5)
        self.assertAlmostEqual(metrics["mean_error"], -1 / 3)
        self.assertEqual(metrics["n"], 3)

    def test_metric_input_validation(self):
        with self.assertRaisesRegex(ValueError, "shapes differ"):
            regression_metrics([1, 2], [1])
        with self.assertRaisesRegex(ValueError, "finite"):
            regression_metrics([1, float("nan")], [1, 2])
        with self.assertRaisesRegex(ValueError, "empty"):
            regression_metrics([], [])

    def test_gain_sign_is_positive_when_model_improves(self):
        baseline = regression_metrics([1, 2, 3], [0, 3, 4])
        model = regression_metrics([1, 2, 3], [1, 2, 2])
        comparison = compare_metrics(baseline, model)
        self.assertGreater(comparison["mae_absolute_improvement"], 0)
        self.assertGreater(comparison["mae_percentage_improvement"], 0)

    def test_persistence_uses_temperature_c_in_stored_order(self):
        matrix = np.asarray([[10, 4], [11, 9]], dtype=np.float32)
        predictions = persistence_predictions(matrix, ["humidity_pct", "temperature_c"])
        np.testing.assert_array_equal(predictions, [4, 9])
        with self.assertRaisesRegex(ValueError, "missing"):
            persistence_predictions(matrix, ["humidity_pct", "pressure_hpa"])

    def test_per_location_summary_and_coverage(self):
        actual = np.asarray([2.0, 2.0, 5.0, 5.0, 7.0, 7.0])
        baseline = np.asarray([2.5, 2.5, 5.0, 5.0, 7.5, 7.5])
        model = np.asarray([2.1, 1.9, 6.0, 4.0, 7.5, 7.5])
        locations = np.asarray(["A", "A", "B", "B", "C", "C"], dtype=object)
        rows = evaluate_per_location(actual, baseline, model, locations)
        summary = summarize_location_results(rows)
        self.assertEqual((summary["locations_improved"], summary["locations_worse"]), (1, 1))
        self.assertEqual(summary["locations_tied"], 1)

        event = np.asarray(["2025-01-01T00", "2025-01-01T00"], dtype="datetime64[h]")
        target = event + np.timedelta64(1, "h")
        bad_locations = np.asarray(["A", "B"], dtype=object)
        with self.assertRaisesRegex(ValueError, "locations"):
            validate_test_coverage(bad_locations, event, target)


if __name__ == "__main__":
    unittest.main()
