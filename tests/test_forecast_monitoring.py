from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import unittest
from unittest.mock import patch

from ml.streaming_inference.online_features import make_forecast_record
from monitoring.evaluation_contract import LIVE_REFERENCE_SOURCE, evaluation_id, observation_key
from monitoring.forecast_evaluator import evaluate_forecasts
from monitoring.hourly_archive import canonicalize_batch
from monitoring.live_reference_backfill import fetch_live_reference_backfill
from monitoring.metrics import calculate_coverage, calculate_metrics
from monitoring.rolling_metrics import build_hourly_metrics, build_rolling_metrics
from spark.jobs.weather_forecast_monitoring import _parse_batch_records


UTC = timezone.utc
FEATURE_TIME = datetime(2026, 10, 2, 16, 0, tzinfo=UTC)
TARGET_TIME = FEATURE_TIME + timedelta(hours=1)
LIVE_MODE = LIVE_REFERENCE_SOURCE
REPLAY_SOURCE = "NATIONWIDE_63_DELTA_REPLAY"
KNOWN_LOCATIONS = {"LOC_A", "LOC_B"}


def event_id(source: str, location_id: str, event_time: datetime) -> str:
    return f"{source}|{location_id}|{event_time.strftime('%Y-%m-%dT%H:00:00Z')}"


def observation(
    source: str,
    location_id: str,
    when: datetime,
    temperature: float,
    *,
    retrieval_mode: str | None = None,
    ingestion_time: datetime | None = None,
) -> dict:
    return {
        "event_id": event_id(source, location_id, when),
        "event_type": "WEATHER_HOURLY",
        "source": source,
        "location_id": location_id,
        "city": location_id,
        "latitude": 10.0,
        "longitude": 106.0,
        "event_time": when.isoformat().replace("+00:00", "Z"),
        "ingestion_time": (ingestion_time or (when + timedelta(minutes=3))).isoformat().replace("+00:00", "Z"),
        "temperature_c": temperature,
        "humidity_pct": 65.0,
        "precipitation_mm": 0.2,
        "pressure_hpa": 1008.5,
        "wind_speed_kmh": 8.0,
        "wind_gust_kmh": 14.0,
        "weather_code": 2,
        "reference_retrieval_mode": retrieval_mode,
    }


def forecast(location_id: str = "LOC_A", *, source: str = LIVE_MODE, prediction: float = 30.0) -> dict:
    return make_forecast_record(
        location_id=location_id,
        feature_time=FEATURE_TIME,
        prediction_temperature_c=prediction,
        source_event_id=event_id(source, location_id, FEATURE_TIME),
        inference_time=FEATURE_TIME + timedelta(minutes=1),
    )


class FakeResponse:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.value).encode("utf-8")


class KafkaMonitoringPayloadTests(unittest.TestCase):
    def test_spark_binary_values_decode_as_json(self):
        payload = {"event_id": "event-1", "temperature_c": 21.5}

        class Batch:
            values = [
                json.dumps(payload).encode("utf-8"),
                bytearray(json.dumps(payload).encode("utf-8")),
                memoryview(json.dumps(payload).encode("utf-8")),
                b"not-json",
            ]

            def select(self, _column):
                return self

            def collect(self):
                return [{"value": value} for value in self.values]

        records, malformed = _parse_batch_records(Batch())

        self.assertEqual(records, [payload, payload, payload])
        self.assertEqual(malformed, 1)


class ForecastMonitoringContractTests(unittest.TestCase):
    def setUp(self):
        self.feature = observation(LIVE_MODE, "LOC_A", FEATURE_TIME, 29.0)
        self.target = observation(LIVE_MODE, "LOC_A", TARGET_TIME, 28.0)

    def test_utc_hours_reject_naive_timestamps(self):
        f = forecast()
        f["feature_time"] = FEATURE_TIME.replace(tzinfo=None)
        result = evaluate_forecasts(
            [f],
            [self.feature, self.target],
            evaluation_time=TARGET_TIME + timedelta(hours=1),
            known_location_ids=KNOWN_LOCATIONS,
        )
        self.assertEqual(result["evaluations"][0]["status"], "INVALID_PROVENANCE")
        self.assertIn("INVALID_FORECAST_TIME", result["evaluations"][0]["invalid_reason"])

    def test_forecast_joins_exact_source_event_location_and_hour(self):
        row = forecast()
        wrong_source = observation(REPLAY_SOURCE, "LOC_A", FEATURE_TIME, 29.0)
        target = observation(REPLAY_SOURCE, "LOC_A", TARGET_TIME, 28.0)
        result = evaluate_forecasts(
            [row],
            [wrong_source, target],
            evaluation_time=TARGET_TIME + timedelta(minutes=10),
            known_location_ids=KNOWN_LOCATIONS,
        )
        self.assertEqual(result["evaluations"][0]["status"], "PENDING_BASELINE")
        self.assertEqual(result["evaluations"][0]["invalid_reason"], "FEATURE_TIME_REFERENCE_MISSING_OR_AMBIGUOUS")

    def test_historical_location_cannot_score_another_locations_reference(self):
        row = forecast(location_id="LOC_A")
        observations = [
            self.feature,
            observation(LIVE_MODE, "LOC_B", TARGET_TIME, 28.0),
        ]
        result = evaluate_forecasts(
            [row],
            observations,
            evaluation_time=TARGET_TIME + timedelta(minutes=15),
            known_location_ids=KNOWN_LOCATIONS,
        )
        self.assertEqual(result["evaluations"][0]["status"], "PENDING_REFERENCE")

    def test_future_target_reference_is_not_used_before_target_time(self):
        result = evaluate_forecasts(
            [forecast()],
            [self.feature, self.target],
            evaluation_time=TARGET_TIME - timedelta(minutes=1),
            known_location_ids=KNOWN_LOCATIONS,
        )
        row = result["evaluations"][0]
        self.assertEqual(row["status"], "PENDING_TARGET_TIME")
        self.assertFalse(row["target_time_passed"])
        self.assertFalse(row["target_reference_received"])
        self.assertIsNone(row["reference_temperature_c"])

    def test_missing_late_reference_is_pending_then_evaluates_once_arrived(self):
        row = forecast()
        pending = evaluate_forecasts(
            [row],
            [self.feature],
            evaluation_time=TARGET_TIME + timedelta(minutes=10),
            known_location_ids=KNOWN_LOCATIONS,
        )["evaluations"][0]
        self.assertEqual(pending["status"], "PENDING_REFERENCE")

        evaluated = evaluate_forecasts(
            [row],
            [self.feature, self.target],
            evaluation_time=TARGET_TIME + timedelta(minutes=20),
            known_location_ids=KNOWN_LOCATIONS,
            existing_evaluations={row["forecast_id"]: pending},
        )["evaluations"][0]
        self.assertEqual(evaluated["status"], "EVALUATED")
        self.assertEqual(evaluated["persistence_prediction_temperature_c"], 29.0)
        self.assertEqual(evaluated["reference_temperature_c"], 28.0)
        self.assertEqual(evaluated["model_error_c"], 2.0)
        self.assertEqual(evaluated["persistence_error_c"], 1.0)
        self.assertEqual(evaluated["evaluation_mode"], "LIVE_PROSPECTIVE")
        self.assertEqual(evaluated["humidity_pct"], 65.0)
        self.assertTrue(evaluated["target_reference_received"])

    def test_live_backfill_mode_is_carried_from_target_reference(self):
        backfilled_target = observation(
            LIVE_MODE,
            "LOC_A",
            TARGET_TIME,
            28.0,
            retrieval_mode="LIVE_SOURCE_BACKFILL",
        )
        row = evaluate_forecasts(
            [forecast()],
            [self.feature, backfilled_target],
            evaluation_time=TARGET_TIME + timedelta(minutes=10),
            known_location_ids=KNOWN_LOCATIONS,
            cohort_id="live-cohort-1",
        )["evaluations"][0]
        self.assertEqual(row["evaluation_mode"], "LIVE_SOURCE_BACKFILL")
        self.assertEqual(row["cohort_id"], "live-cohort-1")
        self.assertIn("reference_temperature_c", row)
        self.assertNotIn("ground_truth", row)

    def test_conflicting_archive_duplicate_is_not_selected_silently(self):
        conflict = observation(REPLAY_SOURCE, "LOC_A", FEATURE_TIME, 25.0)
        original = observation(REPLAY_SOURCE, "LOC_A", FEATURE_TIME, 26.0)
        result = evaluate_forecasts(
            [forecast(source=REPLAY_SOURCE)],
            [original, conflict],
            evaluation_time=TARGET_TIME + timedelta(minutes=10),
            known_location_ids=KNOWN_LOCATIONS,
        )
        self.assertEqual(result["evaluations"][0]["status"], "REFERENCE_CONFLICT")
        self.assertEqual(result["duplicate_observation_count"], 0)
        self.assertEqual(result["invalid_observation_count"], 2)

    def test_evaluated_evidence_is_immutable_across_revisions_and_restarts(self):
        row = forecast()
        first = evaluate_forecasts(
            [row],
            [self.feature, self.target],
            evaluation_time=TARGET_TIME + timedelta(hours=1),
            known_location_ids=KNOWN_LOCATIONS,
        )["evaluations"][0]
        changed_target = observation(LIVE_MODE, "LOC_A", TARGET_TIME, 31.0)
        key = observation_key(LIVE_MODE, "LOC_A", TARGET_TIME)
        rerun = evaluate_forecasts(
            [row],
            [self.feature, changed_target],
            evaluation_time=TARGET_TIME + timedelta(hours=2),
            known_location_ids=KNOWN_LOCATIONS,
            conflicted_reference_keys={key},
            existing_evaluations={row["forecast_id"]: first},
        )["evaluations"][0]
        self.assertEqual(rerun, first)
        self.assertEqual(evaluation_id(row["forecast_id"], LIVE_MODE), first["evaluation_id"])

    def test_duplicate_forecasts_collapse_and_conflicting_id_is_invalid(self):
        one = forecast()
        duplicate = dict(one)
        result = evaluate_forecasts(
            [one, duplicate],
            [self.feature, self.target],
            evaluation_time=TARGET_TIME + timedelta(minutes=5),
            known_location_ids=KNOWN_LOCATIONS,
        )
        self.assertEqual(len(result["evaluations"]), 1)
        self.assertEqual(result["duplicate_forecast_count"], 1)

        conflicting = dict(one, prediction_temperature_c=31.0)
        result = evaluate_forecasts(
            [one, conflicting],
            [self.feature, self.target],
            evaluation_time=TARGET_TIME + timedelta(minutes=5),
            known_location_ids=KNOWN_LOCATIONS,
        )
        self.assertEqual(len(result["evaluations"]), 1)
        self.assertEqual(result["evaluations"][0]["status"], "INVALID_PROVENANCE")
        self.assertIn("CONFLICTING_FORECAST_ID", result["evaluations"][0]["invalid_reason"])


class ForecastMonitoringArchiveTests(unittest.TestCase):
    def test_identical_duplicate_is_ignored_and_conflict_preserves_first_reference(self):
        first = observation(REPLAY_SOURCE, "LOC_A", FEATURE_TIME, 26.0)
        identical = dict(first)
        conflicting = dict(first, event_id="later-event", temperature_c=31.0)
        result = canonicalize_batch(
            [first, identical, conflicting],
            KNOWN_LOCATIONS,
            archived_at=TARGET_TIME + timedelta(minutes=1),
        )
        self.assertEqual(len(result["observations"]), 1)
        self.assertEqual(result["observations"][0]["temperature_c"], 26.0)
        self.assertEqual(result["duplicate_count"], 1)
        self.assertEqual(len(result["conflicts"]), 1)
        revision = result["conflicts"][0]
        self.assertEqual(revision["status"], "REFERENCE_REVISION_DETECTED")
        self.assertNotEqual(revision["old_payload_sha256"], revision["new_payload_sha256"])


class ForecastMonitoringMetricsTests(unittest.TestCase):
    def test_error_convention_mae_rmse_bias_and_skill(self):
        metrics = calculate_metrics(
            [
                {
                    "status": "EVALUATED",
                    "location_id": "LOC_A",
                    "model_prediction_temperature_c": 30.0,
                    "persistence_prediction_temperature_c": 29.0,
                    "reference_temperature_c": 28.0,
                    "model_error_c": 2.0,
                    "persistence_error_c": 1.0,
                }
            ]
        )
        self.assertEqual(metrics["model_mae"], 2.0)
        self.assertEqual(metrics["model_rmse"], 2.0)
        self.assertEqual(metrics["model_bias"], 2.0)
        self.assertEqual(metrics["persistence_mae"], 1.0)
        self.assertEqual(metrics["persistence_rmse"], 1.0)
        self.assertEqual(metrics["persistence_bias"], 1.0)
        self.assertEqual(metrics["mae_skill"], -1.0)
        self.assertEqual(metrics["mae_improvement_c"], -1.0)

    def test_zero_persistence_denominator_and_undefined_r2_are_null(self):
        metrics = calculate_metrics(
            [
                {
                    "status": "EVALUATED",
                    "location_id": "LOC_A",
                    "model_prediction_temperature_c": 21.0,
                    "persistence_prediction_temperature_c": 20.0,
                    "reference_temperature_c": 20.0,
                    "model_error_c": 1.0,
                    "persistence_error_c": 0.0,
                }
            ]
        )
        self.assertIsNone(metrics["mae_skill"])
        self.assertIsNone(metrics["mae_improvement_pct"])
        self.assertIsNone(metrics["persistence_r2"])
        self.assertTrue(all(value is None or math.isfinite(value) for value in metrics.values() if isinstance(value, float)))

    def test_macro_and_micro_are_reported_separately(self):
        rows = [
            {
                "status": "EVALUATED",
                "location_id": location,
                "model_prediction_temperature_c": error,
                "persistence_prediction_temperature_c": 0.0,
                "reference_temperature_c": 0.0,
                "model_error_c": error,
                "persistence_error_c": 0.0,
            }
            for location, error in (("LOC_A", 1.0), ("LOC_A", 1.0), ("LOC_B", 5.0))
        ]
        metrics = calculate_metrics(rows)
        self.assertAlmostEqual(metrics["micro_global_mae"], 7.0 / 3.0)
        self.assertAlmostEqual(metrics["macro_location_mae"], 3.0)

    def test_hourly_coverage_counts_missing_and_received_references(self):
        rows = [
            {
                "target_time": TARGET_TIME,
                "location_id": "LOC_A",
                "reference_source": REPLAY_SOURCE,
                "evaluation_mode": "REPLAY",
                "status": "EVALUATED",
                "target_time_passed": True,
                "target_reference_received": True,
                "model_prediction_temperature_c": 30.0,
                "persistence_prediction_temperature_c": 29.0,
                "reference_temperature_c": 28.0,
                "model_error_c": 2.0,
                "persistence_error_c": 1.0,
            },
            {
                "target_time": TARGET_TIME,
                "location_id": "LOC_B",
                "reference_source": REPLAY_SOURCE,
                "evaluation_mode": "REPLAY",
                "status": "PENDING_REFERENCE",
                "target_time_passed": True,
                "target_reference_received": False,
            },
        ]
        record = build_hourly_metrics(rows, expected_locations=2)[0]
        self.assertEqual(record["expected_references"], 2)
        self.assertEqual(record["received_references"], 1)
        self.assertEqual(record["missing_references"], 1)
        self.assertEqual(record["reference_coverage_pct"], 50.0)

    def test_coverage_distinguishes_target_pending_and_reference_pending(self):
        coverage = calculate_coverage(
            [
                {"status": "PENDING_TARGET_TIME", "target_time_passed": False},
                {"status": "PENDING_REFERENCE", "target_time_passed": True},
                {"status": "EVALUATED", "target_time_passed": True, "target_reference_received": True},
            ]
        )
        self.assertEqual(coverage["forecasts_total"], 3)
        self.assertEqual(coverage["forecasts_target_time_passed"], 2)
        self.assertEqual(coverage["pending_target_count"], 1)
        self.assertEqual(coverage["pending_reference_count"], 1)
        self.assertEqual(coverage["evaluation_coverage_pct"], 50.0)

    def test_rolling_24h_and_7d_use_target_time_and_report_completeness(self):
        locations = [f"LOC_{index:02d}" for index in range(63)]
        start = datetime(2026, 1, 1, tzinfo=UTC)
        rows = []
        for hour in range(24):
            target = start + timedelta(hours=hour)
            for location in locations:
                rows.append(
                    {
                        "target_time": target,
                        "location_id": location,
                        "reference_source": REPLAY_SOURCE,
                        "evaluation_mode": "REPLAY",
                        "status": "EVALUATED",
                        "target_time_passed": True,
                        "target_reference_received": True,
                        "model_prediction_temperature_c": 20.0,
                        "persistence_prediction_temperature_c": 21.0,
                        "reference_temperature_c": float(hour),
                        "model_error_c": 1.0,
                        "persistence_error_c": 2.0,
                    }
                )
        result = build_rolling_metrics(
            rows,
            expected_locations=63,
            minimum_samples={24: 24, 168: 168},
            location_ids=locations,
        )
        global_24 = [row for row in result["rolling_global_metrics"] if row["window_hours"] == 24][-1]
        global_7d = [row for row in result["rolling_global_metrics"] if row["window_hours"] == 168][-1]
        self.assertEqual(global_24["sample_count"], 24 * 63)
        self.assertEqual(global_24["expected_sample_count"], 24 * 63)
        self.assertTrue(global_24["window_complete"])
        self.assertEqual(global_7d["sample_count"], 24 * 63)
        self.assertEqual(global_7d["expected_sample_count"], 168 * 63)
        self.assertFalse(global_7d["window_complete"])
        location_24 = [row for row in result["rolling_location_metrics"] if row["window_hours"] == 24][-1]
        self.assertEqual(location_24["sample_count"], 24)
        self.assertTrue(location_24["window_complete"])

    def test_two_hour_window_is_not_labeled_complete_24h(self):
        rows = []
        for hour in range(2):
            for location in ("LOC_A", "LOC_B"):
                rows.append(
                    {
                        "target_time": TARGET_TIME + timedelta(hours=hour),
                        "location_id": location,
                        "reference_source": REPLAY_SOURCE,
                        "evaluation_mode": "REPLAY",
                        "status": "EVALUATED",
                        "target_reference_received": True,
                        "model_prediction_temperature_c": 20.0,
                        "persistence_prediction_temperature_c": 21.0,
                        "reference_temperature_c": 19.0,
                        "model_error_c": 1.0,
                        "persistence_error_c": 2.0,
                    }
                )
        metrics = build_rolling_metrics(rows, expected_locations=2)
        latest_24 = [row for row in metrics["rolling_global_metrics"] if row["window_hours"] == 24][-1]
        self.assertEqual(latest_24["sample_count"], 4)
        self.assertEqual(latest_24["expected_sample_count"], 48)
        self.assertFalse(latest_24["window_complete"])
        self.assertEqual(latest_24["quality_status"], "INSUFFICIENT_SAMPLE")


class ForecastMonitoringBackfillTests(unittest.TestCase):
    def setUp(self):
        self.locations = [
            {"location_id": f"LOC_{index:02d}", "latitude": 8.0 + index * 0.1, "longitude": 102.0 + index * 0.1}
            for index in range(63)
        ]

    def test_backfill_uses_safe_target_and_correct_hour_values(self):
        target = datetime(2026, 10, 2, 17, tzinfo=UTC)
        now = datetime(2026, 10, 3, 4, 12, tzinfo=UTC)
        times = ["2026-10-02T16:00", "2026-10-02T17:00", "2026-10-02T18:00"]
        payload = [
            {
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "timezone": "UTC",
                "utc_offset_seconds": 0,
                "hourly": {
                    "time": times,
                    "temperature_2m": [20.0, 28.0, 29.0],
                    "relative_humidity_2m": [60, 61, 62],
                    "precipitation": [0.0, 0.5, 1.0],
                    "pressure_msl": [1000.0, 1001.0, 1002.0],
                    "wind_speed_10m": [5.0, 6.0, 7.0],
                    "wind_gusts_10m": [10.0, 11.0, 12.0],
                    "weather_code": [1, 2, 3],
                },
            }
            for location in self.locations
        ]
        with patch("monitoring.live_reference_backfill.urlopen", return_value=FakeResponse(payload)) as open_url:
            rows = fetch_live_reference_backfill(target, self.locations, now=now)
        self.assertEqual(len(rows), 63)
        self.assertEqual(rows[0]["temperature_c"], 28.0)
        self.assertEqual(rows[0]["reference_retrieval_mode"], "LIVE_SOURCE_BACKFILL")
        self.assertEqual(rows[0]["source"], LIVE_MODE)
        self.assertEqual(rows[0]["event_time"], "2026-10-02T17:00:00Z")
        open_url.assert_called_once()

    def test_future_or_unsafe_target_is_rejected_before_http_request(self):
        with patch("monitoring.live_reference_backfill.urlopen") as open_url:
            with self.assertRaisesRegex(ValueError, "safe completed hour"):
                fetch_live_reference_backfill(
                    datetime(2026, 10, 3, 4, tzinfo=UTC),
                    self.locations,
                    now=datetime(2026, 10, 3, 4, 12, tzinfo=UTC),
                )
        open_url.assert_not_called()


if __name__ == "__main__":
    unittest.main()
