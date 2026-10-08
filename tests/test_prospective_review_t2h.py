from __future__ import annotations

import unittest

from analysis.prospective_review_t2h import (
    classify_skill,
    primary_metric_rows,
    recompute_metrics,
    target_hour_block_bootstrap,
    validate_slot_grid,
)


def _row(target_time: str, location: str, model_error: float, persistence_error: float, status: str = "EVALUATED"):
    truth = 20.0
    return {
        "target_time": target_time,
        "location_id": location,
        "status": status,
        "reference_temperature_c": truth,
        "model_prediction_temperature_c": truth + model_error,
        "persistence_prediction_temperature_c": truth + persistence_error,
        "model_error_c": model_error,
        "persistence_error_c": persistence_error,
    }


class ProspectiveReviewT2HTests(unittest.TestCase):
    def test_primary_metrics_exclude_missing_and_conflicts(self):
        evaluated = [
            _row("2026-10-03T17:00:00Z", "A", 1.0, 0.5),
            _row("2026-10-03T17:00:00Z", "B", -1.0, -0.5),
        ]
        rows = evaluated + [
            _row("2026-10-03T18:00:00Z", "A", 100.0, 100.0, "FORECAST_MISSING"),
            _row("2026-10-03T18:00:00Z", "B", 100.0, 100.0, "REFERENCE_CONFLICT"),
        ]
        primary = primary_metric_rows(rows)
        self.assertEqual(len(primary), 2)
        metrics = recompute_metrics(primary)
        self.assertAlmostEqual(metrics["xgboost"]["mae_c"], 1.0)
        self.assertAlmostEqual(metrics["persistence"]["mae_c"], 0.5)

    def test_classification_rules(self):
        def metrics(xgb_mae, xgb_rmse, persistence_mae=1.0, persistence_rmse=1.0):
            return {"sample_count": 4, "xgboost": {"mae_c": xgb_mae, "rmse_c": xgb_rmse}, "persistence": {"mae_c": persistence_mae, "rmse_c": persistence_rmse}}

        self.assertEqual(classify_skill(metrics(0.8, 0.9)), "POSITIVE_PROSPECTIVE_SKILL")
        self.assertEqual(classify_skill(metrics(0.8, 1.1)), "MIXED_PROSPECTIVE_SKILL")
        self.assertEqual(classify_skill(metrics(1.0, 1.0)), "NO_PROSPECTIVE_SKILL_VS_PERSISTENCE")
        self.assertEqual(classify_skill({"sample_count": 0}), "INSUFFICIENT_VALID_EVALUATIONS")

    def test_slot_grid_detects_duplicates_and_missing_slots(self):
        expected_hours = ["2026-10-03T17:00:00Z", "2026-10-03T18:00:00Z"]
        locations = ["A", "B"]
        rows = [
            _row(expected_hours[0], "A", 0.0, 0.0),
            _row(expected_hours[0], "B", 0.0, 0.0),
            _row(expected_hours[1], "A", 0.0, 0.0),
            _row(expected_hours[1], "A", 0.0, 0.0),
        ]
        result = validate_slot_grid(rows, expected_hours, locations)
        self.assertFalse(result["passed"])
        self.assertEqual(result["missing_slot_count"], 1)
        self.assertEqual(result["duplicate_logical_slot_count"], 1)

    def test_target_hour_block_bootstrap_is_deterministic(self):
        rows = [
            _row("2026-10-03T17:00:00Z", "A", 1.0, 0.5),
            _row("2026-10-03T17:00:00Z", "B", 2.0, 0.5),
            _row("2026-10-03T18:00:00Z", "A", -0.5, 1.0),
            _row("2026-10-03T18:00:00Z", "B", 0.25, 1.5),
        ]
        first = target_hour_block_bootstrap(rows, resamples=500, seed=17)
        second = target_hour_block_bootstrap(rows, resamples=500, seed=17)
        self.assertEqual(first, second)
        self.assertEqual(first["unique_target_hour_blocks"], 2)
        self.assertEqual(first["resampling_unit"], "unique target_time; all location rows within the hour are kept together")


if __name__ == "__main__":
    unittest.main()
