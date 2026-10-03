from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest

from ml.streaming_inference.online_features import build_online_feature_series
from ml.streaming_inference.t2h_contract import (
    FEATURE_COUNT,
    FEATURE_LIST_SHA256,
    FEATURE_SET_ID,
    HISTORICAL_FORECAST_ENDPOINT,
    LIVE_ENDPOINT,
    LIVE_SOURCE,
    MODEL_BYTES,
    MODEL_ID,
    MODEL_SHA256,
    PROVIDER_MODEL,
    PROVIDER_NAME,
    REPLAY_SOURCE,
    default_model_path,
    load_t2h_feature_contract,
)
from ml.streaming_inference.t2h_model_loader import validate_t2h_model
from ml.streaming_inference.t2h_runtime import (
    build_t2h_forecast_record,
    deterministic_t2h_forecast_id,
)
from monitoring.evaluation_contract import forecast_validation_errors
from monitoring.forecast_evaluator import _evaluation_mode
from producer import live_hourly_weather_producer as live


ROOT = Path(__file__).resolve().parents[1]
LIVE_FEATURE_TIME = datetime(2026, 10, 3, 13, tzinfo=timezone.utc)


def make_observations(count: int = 40) -> list[dict[str, object]]:
    first = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "event_id": f"VN_HANOI_{index}",
            "location_id": "VN_HANOI",
            "event_time": first + timedelta(hours=index),
            "latitude": 21.03,
            "longitude": 105.85,
            "temperature_c": 10.0 + index / 10,
            "humidity_pct": float(60 + index % 7),
            "precipitation_mm": float(index % 3),
            "pressure_hpa": 1000.0 + index / 20,
            "wind_speed_kmh": float(index % 5),
            "wind_gust_kmh": float(index % 8),
        }
        for index in range(count)
    ]


def build_forecast(
    *,
    execution_origin: str,
    feature_time: datetime = LIVE_FEATURE_TIME,
    inference_time: datetime = datetime(2026, 10, 3, 14, 5, tzinfo=timezone.utc),
    source: str = LIVE_SOURCE,
    endpoint: str = LIVE_ENDPOINT,
):
    return build_t2h_forecast_record(
        location_id="VN_HANOI",
        feature_time=feature_time,
        prediction_temperature_c=24.25,
        source_event_id="event-vn-hanoi",
        inference_time=inference_time,
        provider=PROVIDER_NAME,
        provider_endpoint=endpoint,
        provider_model=PROVIDER_MODEL,
        source_retrieved_at=inference_time - timedelta(minutes=2),
        source=source,
        execution_origin=execution_origin,
    )


class T2HContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = load_t2h_feature_contract()

    def test_canonical_feature_count_hash_and_order(self):
        self.assertEqual(len(self.contract.feature_names), FEATURE_COUNT)
        self.assertEqual(self.contract.feature_list_sha256, FEATURE_LIST_SHA256)
        self.assertEqual(self.contract.feature_set_id, FEATURE_SET_ID)
        self.assertEqual(self.contract.model_id, MODEL_ID)
        self.assertEqual(self.contract.feature_names[0], "temperature_c")
        self.assertEqual(self.contract.feature_names[-1], "wind_speed_delta_1h")

    @unittest.skipUnless(default_model_path().is_file(), "external canonical T2H model artifact is not installed")
    def test_canonical_model_sha_size_and_cpu_load_probe(self):
        result = validate_t2h_model()
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["model_sha256"], MODEL_SHA256)
        self.assertEqual(result["model_bytes"], MODEL_BYTES)
        self.assertEqual(result["feature_count"], FEATURE_COUNT)
        self.assertTrue(result["feature_names_match"])
        self.assertEqual(result["device"], "cpu")

    def test_provider_request_explicitly_pins_ecmwf_ifs(self):
        locations = [{"location_id": "VN_HANOI", "latitude": 21.03, "longitude": 105.85}]
        params = live.hourly_request_params(locations, 24, provider_model=PROVIDER_MODEL)
        self.assertEqual(params["models"], "ecmwf_ifs")
        self.assertEqual(params["hourly"].split(","), list(live.HOURLY_VARIABLES))
        self.assertEqual(
            live.safe_hour_cutoff(datetime(2026, 10, 3, 17, 5, tzinfo=timezone.utc)),
            datetime(2026, 10, 3, 16, 0, tzinfo=timezone.utc),
        )

    def test_live_event_contains_pinned_provider_provenance(self):
        location = live.load_live_locations(ROOT / "historical" / "locations.json")[0]
        first = LIVE_FEATURE_TIME - timedelta(hours=24)
        times = [live.format_utc_hour(first + timedelta(hours=index))[:-1] for index in range(26)]
        values = {
            "temperature_2m": [20.0] * len(times),
            "relative_humidity_2m": [60.0] * len(times),
            "precipitation": [0.0] * len(times),
            "pressure_msl": [1010.0] * len(times),
            "wind_speed_10m": [5.0] * len(times),
            "wind_gusts_10m": [8.0] * len(times),
            "weather_code": [1] * len(times),
        }
        response = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "timezone": "UTC",
            "utc_offset_seconds": 0,
            "hourly_units": {"time": "iso8601", **live.EXPECTED_UNITS},
            "hourly": {"time": times, **values},
        }
        event = live.build_hourly_events(
            [location],
            {location["location_id"]: response},
            now=datetime(2026, 10, 3, 14, 5, tzinfo=timezone.utc),
            history_hours=24,
            bootstrap=False,
            ingestion_time=datetime(2026, 10, 3, 14, 2, tzinfo=timezone.utc),
            provider_model=PROVIDER_MODEL,
            provider_endpoint=LIVE_ENDPOINT,
        )["events"][0]
        self.assertEqual(event["event_time"], "2026-10-03T13:00:00Z")
        self.assertEqual(event["provider"], PROVIDER_NAME)
        self.assertEqual(event["provider_model"], PROVIDER_MODEL)
        self.assertEqual(event["provider_endpoint"], LIVE_ENDPOINT)
        self.assertEqual(event["source"], LIVE_SOURCE)
        self.assertEqual(event["source_retrieved_at"], "2026-10-03T14:02:00Z")

    def test_online_warmup_gap_and_duplicate_semantics(self):
        contract = self.contract
        rows = make_observations(25)
        built = build_online_feature_series(rows, contract=contract)
        self.assertEqual([row.status for row in built[:24]], ["INSUFFICIENT_HISTORY"] * 24)
        self.assertTrue(built[24].ready)
        self.assertEqual(len(built[24].values), FEATURE_COUNT)
        duplicate = rows[:25] + [dict(rows[24])]
        self.assertEqual(len(build_online_feature_series(duplicate, contract=contract)), 25)
        gap_rows = make_observations(40)
        gap_rows.pop(10)
        gap = build_online_feature_series(gap_rows, contract=contract)
        self.assertIn("HISTORY_GAP", [row.status for row in gap])
        self.assertEqual(gap[-1].status, "READY")


class T2HForecastContractTests(unittest.TestCase):
    def test_live_target_is_plus_two_hours_with_positive_forecast_lead(self):
        result = build_forecast(execution_origin="LIVE_PROSPECTIVE")
        self.assertEqual(result.status, "READY")
        record = result.forecast
        self.assertEqual(record["target_time"], LIVE_FEATURE_TIME + timedelta(hours=2))
        self.assertEqual((record["target_time"] - record["feature_time"]).total_seconds(), 7200)
        self.assertEqual(record["forecast_lead_seconds"], 55 * 60)
        self.assertEqual(record["execution_origin"], "LIVE_PROSPECTIVE")
        self.assertEqual(record["forecast_horizon_hours"], 2)
        self.assertEqual(record["feature_count"], FEATURE_COUNT)
        self.assertEqual(record["model_id"], MODEL_ID)
        self.assertEqual(record["model_sha256"], MODEL_SHA256)

    def test_live_non_prospective_target_is_skipped(self):
        result = build_forecast(
            execution_origin="LIVE_PROSPECTIVE",
            inference_time=datetime(2026, 10, 3, 15, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(result.status, "NON_PROSPECTIVE_SKIPPED")
        self.assertIsNone(result.forecast)
        self.assertEqual(result.forecast_lead_seconds, 0)

    def test_live_profile_rejects_a_newer_than_safe_hour_feature(self):
        with self.assertRaisesRegex(ValueError, "latest completed safe hour"):
            build_forecast(
                execution_origin="LIVE_PROSPECTIVE",
                feature_time=datetime(2026, 10, 3, 14, 0, tzinfo=timezone.utc),
            )

    def test_replay_is_allowed_negative_lead_but_never_live_source(self):
        result = build_forecast(
            execution_origin="REPLAY_VALIDATION",
            feature_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            inference_time=datetime(2026, 10, 3, 14, 5, tzinfo=timezone.utc),
            source=REPLAY_SOURCE,
            endpoint=HISTORICAL_FORECAST_ENDPOINT,
        )
        self.assertEqual(result.status, "READY")
        self.assertLess(result.forecast["forecast_lead_seconds"], 0)
        self.assertEqual(result.forecast["execution_origin"], "REPLAY_VALIDATION")
        with self.assertRaisesRegex(ValueError, "Historical Forecast source"):
            build_forecast(
                execution_origin="REPLAY_VALIDATION",
                feature_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
                inference_time=datetime(2026, 10, 3, 14, 5, tzinfo=timezone.utc),
                source=LIVE_SOURCE,
            )

    def test_forecast_id_and_monitoring_validate_t2h_contract(self):
        result = build_forecast(execution_origin="LIVE_PROSPECTIVE")
        record = result.forecast
        same_id = deterministic_t2h_forecast_id("VN_HANOI", LIVE_FEATURE_TIME, LIVE_FEATURE_TIME + timedelta(hours=2))
        self.assertEqual(record["forecast_id"], same_id)
        self.assertEqual(forecast_validation_errors(record, {"VN_HANOI"}), [])
        invalid = {**record, "forecast_lead_seconds": -1}
        self.assertIn("INVALID_FORECAST_LEAD", forecast_validation_errors(invalid, {"VN_HANOI"}))

    def test_monitoring_keeps_replay_out_of_live_prospective_mode(self):
        self.assertEqual(_evaluation_mode(None, None, "REPLAY_VALIDATION"), "REPLAY")
        self.assertEqual(_evaluation_mode(LIVE_SOURCE, None, "BACKFILL"), "REPLAY")
        self.assertEqual(_evaluation_mode(LIVE_SOURCE, None, "LIVE_PROSPECTIVE"), "LIVE_PROSPECTIVE")
        self.assertEqual(_evaluation_mode(LIVE_SOURCE, "LIVE_SOURCE_BACKFILL"), "LIVE_SOURCE_BACKFILL")


if __name__ == "__main__":
    unittest.main()
