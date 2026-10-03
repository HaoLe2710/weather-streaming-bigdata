from datetime import datetime, timedelta, timezone
import unittest

from validation.forecast_timing import (
    current_utc_hour,
    forecast_lead_seconds,
    forecast_target_time,
    is_prospective_forecast,
    last_completed_utc_hour,
)


UTC = timezone.utc


class LiveForecastTimingTests(unittest.TestCase):
    def test_current_contract_is_retrospective_at_1705_utc(self):
        feature = datetime(2026, 10, 3, 16, tzinfo=UTC)
        created = datetime(2026, 10, 3, 17, 5, tzinfo=UTC)
        target = forecast_target_time(feature, 1)

        self.assertEqual(target, datetime(2026, 10, 3, 17, tzinfo=UTC))
        self.assertEqual(forecast_lead_seconds(target, created), -300)
        self.assertFalse(is_prospective_forecast(target, created))

    def test_current_hour_t1h_has_55_minute_lead_at_1705(self):
        feature = datetime(2026, 10, 3, 17, tzinfo=UTC)
        created = datetime(2026, 10, 3, 17, 5, tzinfo=UTC)
        target = forecast_target_time(feature, 1)

        self.assertEqual(target, datetime(2026, 10, 3, 18, tzinfo=UTC))
        self.assertEqual(forecast_lead_seconds(target, created), 3300)
        self.assertTrue(is_prospective_forecast(target, created))

    def test_completed_hour_t2h_has_same_future_target_without_relabeling(self):
        feature = datetime(2026, 10, 3, 16, tzinfo=UTC)
        created = datetime(2026, 10, 3, 17, 5, tzinfo=UTC)
        target = forecast_target_time(feature, 2)

        self.assertEqual(target, datetime(2026, 10, 3, 18, tzinfo=UTC))
        self.assertEqual(forecast_lead_seconds(target, created), 3300)
        self.assertTrue(is_prospective_forecast(target, created))

    def test_target_equal_to_creation_is_not_prospective(self):
        instant = datetime(2026, 10, 3, 17, tzinfo=UTC)

        self.assertEqual(forecast_lead_seconds(instant, instant), 0)
        self.assertFalse(is_prospective_forecast(instant, instant))

    def test_offset_aware_datetimes_are_normalized_to_utc(self):
        target = datetime(2026, 10, 3, 18, tzinfo=UTC)
        created = datetime(2026, 10, 4, 0, 5, tzinfo=timezone(timedelta(hours=7)))

        self.assertEqual(forecast_lead_seconds(target, created), 3300)

    def test_hour_boundaries_and_midnight_rollover(self):
        just_after_hour = datetime(2026, 10, 3, 17, 5, tzinfo=UTC)
        midnight = datetime(2026, 10, 3, 0, 5, tzinfo=UTC)

        self.assertEqual(current_utc_hour(just_after_hour), datetime(2026, 10, 3, 17, tzinfo=UTC))
        self.assertEqual(last_completed_utc_hour(just_after_hour), datetime(2026, 10, 3, 16, tzinfo=UTC))
        self.assertEqual(current_utc_hour(midnight), datetime(2026, 10, 3, 0, tzinfo=UTC))
        self.assertEqual(last_completed_utc_hour(midnight), datetime(2026, 10, 2, 23, tzinfo=UTC))

    def test_hour_exactly_at_boundary_is_not_considered_completed(self):
        boundary = datetime(2026, 10, 3, 17, tzinfo=UTC)

        self.assertEqual(last_completed_utc_hour(boundary), datetime(2026, 10, 3, 16, tzinfo=UTC))

    def test_naive_or_non_hourly_model_timestamps_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            current_utc_hour(datetime(2026, 10, 3, 17, 5))
        with self.assertRaisesRegex(ValueError, "aligned to a UTC hour"):
            forecast_target_time(datetime(2026, 10, 3, 16, 1, tzinfo=UTC), 1)
        with self.assertRaisesRegex(ValueError, "positive integer"):
            forecast_target_time(datetime(2026, 10, 3, 16, tzinfo=UTC), 0)


if __name__ == "__main__":
    unittest.main()
