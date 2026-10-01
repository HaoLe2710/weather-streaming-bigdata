from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
from pathlib import Path
import unittest

from ml.streaming_inference.contract import FEATURE_COUNT, FEATURE_LIST_SHA256, MODEL_BYTES, MODEL_SHA256, load_feature_contract
from ml.streaming_inference.model_loader import load_verified_booster, model_load_count, predict_feature_matrix
from ml.streaming_inference.online_features import (
    build_online_feature_series,
    build_online_feature_vector,
    deterministic_forecast_id,
    make_forecast_record,
)


ROOT = Path(__file__).resolve().parents[1]


def make_observations(count: int = 40, *, location_id: str = "VN_HANOI") -> list[dict[str, object]]:
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "event_id": f"{location_id}_{index}",
            "location_id": location_id,
            "event_time": start + timedelta(hours=index),
            "latitude": 21.03,
            "longitude": 105.85,
            "temperature_c": 10.0 + index,
            "humidity_pct": float(40 + index % 10),
            "precipitation_mm": float(index % 4),
            "pressure_hpa": 1000.0 + index / 10,
            "wind_speed_kmh": float(index % 7),
            "wind_gust_kmh": float(index % 9),
        }
        for index in range(count)
    ]


class FeatureContractTests(unittest.TestCase):
    def test_frozen_order_count_and_hash_are_loaded_from_committed_contracts(self):
        contract = load_feature_contract(ROOT)
        self.assertEqual(len(contract.feature_names), FEATURE_COUNT)
        self.assertEqual(contract.feature_list_sha256, FEATURE_LIST_SHA256)
        self.assertEqual(contract.feature_set_id, "WEATHER_FORECAST_FE_V1")
        self.assertEqual(contract.model_id, "WEATHER_XGBOOST_GLOBAL_T1H_V1")

    def test_feature_order_mismatch_fails_closed(self):
        contract = load_feature_contract(ROOT)
        rows = make_observations(25)
        with self.assertRaisesRegex(ValueError, "feature order"):
            build_online_feature_series(rows, feature_names=tuple(reversed(contract.feature_names)), contract=contract)


class OnlineFeatureTests(unittest.TestCase):
    def test_empty_warmup_and_25th_observation(self):
        rows = make_observations(25)
        results = build_online_feature_series(rows)
        self.assertEqual([row.status for row in results[:24]], ["INSUFFICIENT_HISTORY"] * 24)
        self.assertTrue(results[-1].ready)
        self.assertEqual(len(results[-1].values), 73)

    def test_exact_lags_rolling_population_std_and_deltas(self):
        result = build_online_feature_series(make_observations(25))[-1]
        values = dict(zip(result.feature_names, result.values, strict=True))
        self.assertEqual(values["temp_lag_1h"], 33.0)
        self.assertEqual(values["temp_lag_24h"], 10.0)
        self.assertAlmostEqual(values["temp_roll_mean_3h"], 33.0)
        self.assertAlmostEqual(values["temp_roll_std_3h"], math.sqrt(2.0 / 3.0))
        self.assertAlmostEqual(values["temp_roll_mean_24h"], 22.5)
        self.assertEqual(values["temp_delta_1h"], 1.0)
        self.assertEqual(values["temp_delta_3h"], 3.0)
        self.assertEqual(values["precipitation_sum_3h"], 5.0)

    def test_vietnam_timezone_and_monday_zero_calendar(self):
        rows = make_observations(25)
        # 2023-01-02 00:00 UTC is Monday 07:00 in Ho Chi Minh City.
        values = dict(zip(*[build_online_feature_series(rows)[-1].feature_names, build_online_feature_series(rows)[-1].values]))
        self.assertEqual(values["local_hour"], 7.0)
        self.assertEqual(values["local_day_of_week"], 0.0)
        self.assertEqual(values["local_day_of_year"], 2.0)

    def test_future_observations_do_not_change_features_at_t(self):
        rows = make_observations(26)
        feature_time = rows[24]["event_time"]
        before = build_online_feature_vector(rows[:25], feature_time)
        after = build_online_feature_vector(rows, feature_time)
        self.assertTrue(before.ready and after.ready)
        self.assertEqual(before.values, after.values)

    def test_out_of_order_arrival_is_sorted_by_event_time(self):
        rows = make_observations(40)
        expected = build_online_feature_series(rows)[-1]
        out_of_order = rows[::2][::-1] + rows[1::2][::-1]
        actual = build_online_feature_series(out_of_order)[-1]
        self.assertEqual(actual.feature_time, expected.feature_time)
        self.assertEqual(actual.status, "READY")
        self.assertEqual(actual.feature_names, expected.feature_names)
        self.assertEqual(actual.values, expected.values)

    def test_missing_hour_is_reported_until_25_contiguous_hours_resume(self):
        rows = make_observations(40)
        rows.pop(10)
        results = {item.feature_time: item.status for item in build_online_feature_series(rows)}
        self.assertEqual(results[datetime(2023, 1, 2, 1, tzinfo=timezone.utc)], "HISTORY_GAP")
        self.assertEqual(results[datetime(2023, 1, 2, 11, tzinfo=timezone.utc)], "READY")

    def test_identical_duplicate_is_idempotent_and_conflicting_duplicate_is_rejected(self):
        rows = make_observations(27)
        duplicated = rows[:25] + [dict(rows[24])]
        self.assertEqual(len(build_online_feature_series(duplicated)), 25)
        conflict = dict(rows[10])
        conflict["temperature_c"] = 999.0
        results = build_online_feature_series(rows[:25] + [conflict, *rows[25:]])
        self.assertIn("DUPLICATE_CONFLICT", [item.status for item in results])
        self.assertEqual(results[-1].status, "DUPLICATE_CONFLICT")

    def test_cross_location_history_is_rejected(self):
        rows = make_observations(25)
        rows[0] = {**rows[0], "location_id": "VN_HCM"}
        with self.assertRaisesRegex(ValueError, "single location_id"):
            build_online_feature_series(rows)

    def test_nan_and_missing_values_do_not_produce_features(self):
        rows = make_observations(25)
        rows[-1]["temperature_c"] = float("nan")
        self.assertEqual(build_online_feature_series(rows)[-1].status, "INVALID_FEATURES")
        rows[-1]["temperature_c"] = None
        self.assertEqual(build_online_feature_series(rows)[-1].status, "INVALID_FEATURES")

    def test_forecast_id_and_target_time_are_deterministic(self):
        stamp = datetime(2023, 1, 2, tzinfo=timezone.utc)
        first = deterministic_forecast_id("VN_HANOI", stamp)
        second = deterministic_forecast_id("VN_HANOI", stamp.isoformat())
        self.assertEqual(first, second)
        record = make_forecast_record(
            location_id="VN_HANOI",
            feature_time=stamp,
            prediction_temperature_c=23.5,
            source_event_id="hanoi-1",
            inference_time=datetime(2023, 1, 3, tzinfo=timezone.utc),
        )
        self.assertEqual(record["forecast_id"], first)
        self.assertEqual(record["target_time"], stamp + timedelta(hours=1))


class FrozenModelTests(unittest.TestCase):
    def test_model_hash_and_cpu_prediction_are_cached_per_process(self):
        path = ROOT / "data" / "models" / "weather_forecast_xgboost_v1" / "weather_forecast_xgboost_v1.json"
        booster = load_verified_booster(path)
        self.assertEqual(booster.num_features(), 73)
        self.assertEqual(path.stat().st_size, MODEL_BYTES)
        values = build_online_feature_series(make_observations(25))[-1]
        before = model_load_count(path)
        first = predict_feature_matrix([values.values], values.feature_names, model_path=path)
        second = predict_feature_matrix([values.values], values.feature_names, model_path=path)
        self.assertEqual(model_load_count(path), before)
        self.assertAlmostEqual(float(first[0]), float(second[0]), places=7)

    def test_invalid_model_digest_is_rejected(self):
        path = ROOT / "data" / "models" / "weather_forecast_xgboost_v1" / "weather_forecast_xgboost_v1.json"
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            load_verified_booster(path, expected_sha256="0" * 64)


if __name__ == "__main__":
    unittest.main()
