from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import httpx

from ml.streaming_inference.contract import load_feature_contract
from producer import live_hourly_weather_producer as live
from producer import weather_producer


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 10, 2, 16, 36, tzinfo=timezone.utc)
SAFE_HOUR = datetime(2026, 10, 2, 15, 0, tzinfo=timezone.utc)


def provider_response(location: dict, *, start: datetime | None = None, count: int = 26, offset: int = 0) -> dict:
    first = start or datetime(2026, 10, 1, 15, 0, tzinfo=timezone.utc)
    times = [live.format_utc_hour(first + timedelta(hours=index))[:-1] for index in range(count)]
    values = {
        "temperature_2m": [25.0 + offset + index / 10 for index in range(count)],
        "relative_humidity_2m": [60 + (index % 10) for index in range(count)],
        "precipitation": [float(index % 3) for index in range(count)],
        "pressure_msl": [1010.0 + index / 10 for index in range(count)],
        "wind_speed_10m": [7.0 + index / 10 for index in range(count)],
        "wind_gusts_10m": [12.0 + index / 10 for index in range(count)],
        "weather_code": [index % 6 for index in range(count)],
    }
    return {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "timezone": "GMT",
        "utc_offset_seconds": 0,
        "hourly_units": {"time": "iso8601", **live.EXPECTED_UNITS},
        "hourly": {"time": times, **values},
    }


class FakeProducer:
    def __init__(self):
        self.messages: list[dict] = []

    def produce(self, **kwargs):
        self.messages.append(kwargs)
        if kwargs.get("callback"):
            kwargs["callback"](None, type("Message", (), {"key": lambda self: kwargs["key"]})())

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


class SequenceClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, url, *, params, timeout):
        self.calls += 1
        return self.responses[min(self.calls - 1, len(self.responses) - 1)]


class LiveHourlyContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.locations = live.load_live_locations(ROOT / "historical" / "locations.json")
        cls.contract = load_feature_contract(ROOT)
        cls.payloads = {
            location["location_id"]: provider_response(location, offset=index / 10)
            for index, location in enumerate(cls.locations)
        }

    def run_live_cycle(self, client, producer, *, cache, now=NOW, max_retries=3, sleep=lambda _: None, locations=None):
        return live.run_cycle(
            client,
            producer,
            locations=locations or self.locations,
            topic=live.DEFAULT_TOPIC,
            endpoint=live.OPEN_METEO_URL,
            history_hours=24,
            bootstrap=False,
            partition_count=6,
            batch_size=63,
            max_retries=max_retries,
            provider_model="ecmwf_ifs",
            cache=cache,
            now=now,
            sleep=sleep,
        )

    def test_catalog_uses_the_canonical_nationwide_63(self):
        self.assertEqual(len(self.locations), 63)
        self.assertEqual(len({item["location_id"] for item in self.locations}), 63)
        self.assertEqual(len({item.get("name") or item["province_name"] for item in self.locations}), 63)
        self.assertTrue(all(item["location_id"].startswith("VN_") for item in self.locations))

    def test_bootstrap_history_is_derived_from_frozen_features(self):
        self.assertEqual(live.required_history_hours(self.contract.feature_names), 24)
        self.assertEqual(self.contract.feature_set_id, "WEATHER_FORECAST_FE_V1")
        self.assertEqual(len(self.contract.feature_names), 73)

    def test_safe_hour_is_last_completed_common_provider_hour(self):
        result = live.select_latest_safe_hour(
            [
                {
                    SAFE_HOUR - timedelta(hours=1),
                    SAFE_HOUR,
                    SAFE_HOUR + timedelta(hours=1),
                },
                {SAFE_HOUR - timedelta(hours=1), SAFE_HOUR, SAFE_HOUR + timedelta(hours=1)},
            ],
            NOW,
        )
        self.assertEqual(result, SAFE_HOUR)

    def test_safe_hour_advances_only_after_next_utc_hour_starts(self):
        now = datetime(2026, 10, 2, 17, 1, tzinfo=timezone.utc)
        result = live.select_latest_safe_hour(
            [{SAFE_HOUR, SAFE_HOUR + timedelta(hours=1), SAFE_HOUR + timedelta(hours=2)}],
            now,
        )
        self.assertEqual(result, SAFE_HOUR + timedelta(hours=1))

    def test_non_hourly_timestamps_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "not aligned"):
            live.parse_utc_hour("2026-10-02T15:15:00Z")

    def test_hourly_params_are_explicit_batched_and_bounded(self):
        params = live.hourly_request_params(self.locations, 24)
        self.assertEqual(params["timezone"], "UTC")
        self.assertEqual(params["temperature_unit"], "celsius")
        self.assertEqual(params["wind_speed_unit"], "kmh")
        self.assertEqual(params["precipitation_unit"], "mm")
        self.assertEqual(params["past_hours"], 25)
        self.assertEqual(params["forecast_hours"], 1)
        self.assertNotIn("models", params)  # Open-Meteo's documented default is auto/Best Match.
        self.assertEqual(params["hourly"].split(","), list(live.HOURLY_VARIABLES))

    def test_bootstrap_builds_25_contiguous_rows_for_63_locations_and_filters_current_hour(self):
        result = live.build_hourly_events(
            self.locations,
            self.payloads,
            now=NOW,
            history_hours=24,
            bootstrap=True,
            ingestion_time=NOW,
        )
        self.assertEqual(result["safe_hour"], "2026-10-02T15:00:00Z")
        self.assertEqual(len(result["events"]), 63 * 25)
        self.assertFalse(result["failed_locations"])
        self.assertEqual(result["future_provider_rows_filtered"], 63)
        by_location: dict[str, list[datetime]] = {}
        for event in result["events"]:
            by_location.setdefault(event["location_id"], []).append(live.parse_utc_hour(event["event_time"]))
            self.assertLessEqual(live.parse_utc_hour(event["event_time"]), SAFE_HOUR)
        self.assertEqual(len(by_location), 63)
        self.assertEqual(len(result["provider_coordinates_by_location"]), 63)
        for event in result["events"]:
            expected = result["provider_coordinates_by_location"][event["location_id"]]
            self.assertEqual([event["latitude"], event["longitude"]], expected)
        for times in by_location.values():
            self.assertEqual(times, sorted(times))
            self.assertEqual(len(times), 25)
            self.assertTrue(all(right - left == timedelta(hours=1) for left, right in zip(times, times[1:])))

    def test_bootstrap_gap_fails_closed_without_publishing_partial_history(self):
        locations = self.locations[:2]
        payloads = {item["location_id"]: provider_response(item) for item in locations}
        broken = payloads[locations[0]["location_id"]]
        missing_index = broken["hourly"]["time"].index("2026-10-02T08:00:00")
        for values in broken["hourly"].values():
            values.pop(missing_index)
        result = live.build_hourly_events(responses_by_id=payloads, locations=locations, now=NOW, history_hours=24, bootstrap=True)
        self.assertEqual(result["events"], [])
        self.assertEqual(result["history_gap_locations"], [locations[0]["location_id"]])
        self.assertIn("BOOTSTRAP_HISTORY_GAP", result["failed_locations"][locations[0]["location_id"]])

    def test_once_mode_emits_only_one_safe_hour_per_successful_location(self):
        result = live.build_hourly_events(
            self.locations,
            self.payloads,
            now=NOW,
            history_hours=24,
            bootstrap=False,
            ingestion_time=NOW,
        )
        self.assertEqual(len(result["events"]), 63)
        self.assertEqual({item["event_time"] for item in result["events"]}, {"2026-10-02T15:00:00Z"})

    def test_live_schema_units_and_provenance_match_canonical_contract(self):
        result = live.build_hourly_events(self.locations, self.payloads, now=NOW, history_hours=24, bootstrap=False)
        event = result["events"][0]
        self.assertEqual(set(event), set(live.EVENT_FIELDS))
        self.assertEqual(event["event_type"], "WEATHER_HOURLY")
        self.assertEqual(event["source"], "OPEN_METEO_LIVE_HOURLY")
        self.assertTrue(event["event_time"].endswith(":00:00Z"))
        self.assertTrue(event["ingestion_time"].endswith("Z"))
        self.assertIsInstance(event["temperature_c"], float)
        self.assertIsInstance(event["weather_code"], int)

    def test_event_id_and_kafka_key_are_deterministic_by_location_hour(self):
        first = live.build_hourly_events(self.locations, self.payloads, now=NOW, history_hours=24, bootstrap=False, ingestion_time=NOW)
        second = live.build_hourly_events(self.locations, self.payloads, now=NOW, history_hours=24, bootstrap=False, ingestion_time=NOW + timedelta(seconds=20))
        first_by_key = {(row["location_id"], row["event_time"]): row for row in first["events"]}
        second_by_key = {(row["location_id"], row["event_time"]): row for row in second["events"]}
        self.assertEqual(set(first_by_key), set(second_by_key))
        for key in first_by_key:
            self.assertEqual(first_by_key[key]["event_id"], second_by_key[key]["event_id"])
            self.assertEqual(live.kafka_key(first_by_key[key]), live.kafka_key(second_by_key[key]))
            self.assertEqual(first_by_key[key]["event_id"], f"{live.LIVE_SOURCE}|{key[0]}|{key[1]}")

    def test_restart_reemits_same_logical_ids_without_fragile_cache(self):
        before_restart = live.build_hourly_events(self.locations, self.payloads, now=NOW, history_hours=24, bootstrap=False)
        after_restart = live.build_hourly_events(self.locations, self.payloads, now=NOW, history_hours=24, bootstrap=False)
        self.assertEqual(
            [row["event_id"] for row in before_restart["events"]],
            [row["event_id"] for row in after_restart["events"]],
        )

    def test_daemon_cache_skips_same_hour_then_accepts_next_hour(self):
        cache = live.PublishedHourCache()
        events = live.build_hourly_events(self.locations, self.payloads, now=NOW, history_hours=24, bootstrap=False)["events"]
        first, duplicates = cache.filter_new(events)
        self.assertEqual((len(first), duplicates), (63, 0))
        cache.mark_published(first)
        second, duplicates = cache.filter_new(events)
        self.assertEqual((len(second), duplicates), (0, 63))
        next_events = live.build_hourly_events(
            self.locations,
            self.payloads,
            now=datetime(2026, 10, 2, 17, 1, tzinfo=timezone.utc),
            history_hours=24,
            bootstrap=False,
        )["events"]
        third, duplicates = cache.filter_new(next_events)
        self.assertEqual((len(third), duplicates), (63, 0))

    def test_same_safe_hour_preflight_skips_provider_and_kafka(self):
        cache = live.PublishedHourCache()
        completed = live.build_hourly_events(self.locations, self.payloads, now=NOW, history_hours=24, bootstrap=False)["events"]
        cache.mark_published(completed)

        class NeverCalledClient:
            calls = 0

            def get(self, *args, **kwargs):
                self.calls += 1
                raise AssertionError("provider must not be called for an already-published safe hour")

        client = NeverCalledClient()
        producer = FakeProducer()
        summary = self.run_live_cycle(client, producer, cache=cache)
        self.assertEqual(client.calls, 0)
        self.assertEqual(summary["safe_hour"], live.format_utc_hour(SAFE_HOUR))
        self.assertEqual(summary["api_request_count"], 0)
        self.assertEqual(summary["same_hour_cache_skips"], 63)
        self.assertEqual(summary["events_enqueued"], 0)
        self.assertEqual(summary["events_delivered"], 0)
        self.assertEqual(producer.messages, [])

    def test_new_safe_hour_fetches_once_and_delivers_63_unique_events(self):
        cache = live.PublishedHourCache()
        completed = live.build_hourly_events(self.locations, self.payloads, now=NOW, history_hours=24, bootstrap=False)["events"]
        cache.mark_published(completed)
        payload = [self.payloads[location["location_id"]] for location in self.locations]
        request = httpx.Request("GET", live.OPEN_METEO_URL)
        client = SequenceClient([httpx.Response(200, json=payload, request=request)])
        producer = FakeProducer()

        summary = self.run_live_cycle(client, producer, cache=cache, now=NOW + timedelta(hours=1))
        keys = [message["key"] for message in producer.messages]
        self.assertEqual(client.calls, 1)
        self.assertEqual(summary["api_request_count"], 1)
        self.assertEqual(summary["safe_hour"], live.format_utc_hour(SAFE_HOUR + timedelta(hours=1)))
        self.assertEqual(summary["successful_locations"], 63)
        self.assertEqual(summary["events_built"], 63)
        self.assertEqual(summary["events_delivered"], 63)
        self.assertEqual(summary["unique_location_hour_keys"], 63)
        self.assertEqual(len(keys), 63)
        self.assertEqual(len(set(keys)), 63)

    def test_429_honors_retry_after_and_does_not_mark_hour_complete(self):
        location = self.locations[0]
        request = httpx.Request("GET", live.OPEN_METEO_URL)
        client = SequenceClient([
            httpx.Response(429, headers={"Retry-After": "17"}, request=request),
            httpx.Response(429, headers={"Retry-After": "17"}, request=request),
        ])
        producer = FakeProducer()
        cache = live.PublishedHourCache()
        waits = []

        summary = self.run_live_cycle(
            client,
            producer,
            cache=cache,
            max_retries=2,
            sleep=waits.append,
            locations=[location],
        )
        batch_key = location["location_id"]
        self.assertEqual(client.calls, 2)
        self.assertEqual(waits, [17.0])
        self.assertEqual(summary["retry_counts"], {batch_key: 1})
        self.assertEqual(summary["retry_delays_seconds"], {batch_key: [17.0]})
        self.assertEqual(producer.messages, [])
        self.assertFalse(cache.has_completed(SAFE_HOUR, [batch_key]))

    def test_429_fallback_uses_jittered_backoff_and_stops_at_attempt_limit(self):
        location = self.locations[0]
        request = httpx.Request("GET", live.OPEN_METEO_URL)
        client = SequenceClient([
            httpx.Response(429, request=request),
            httpx.Response(429, request=request),
        ])
        producer = FakeProducer()
        waits = []

        with patch.object(live.random, "uniform", return_value=55.0):
            summary = self.run_live_cycle(
                client,
                producer,
                cache=live.PublishedHourCache(),
                max_retries=2,
                sleep=waits.append,
                locations=[location],
            )
        self.assertEqual(client.calls, 2)
        self.assertEqual(waits, [55.0])
        self.assertEqual(summary["retry_delays_seconds"], {location["location_id"]: [55.0]})
        self.assertEqual(producer.messages, [])

    def test_429_does_not_retry_after_the_safe_hour_expires(self):
        location = self.locations[0]
        request = httpx.Request("GET", live.OPEN_METEO_URL)
        client = SequenceClient([httpx.Response(429, headers={"Retry-After": "30"}, request=request)])
        producer = FakeProducer()
        waits = []
        just_before_boundary = datetime(2026, 10, 2, 16, 59, 50, tzinfo=timezone.utc)

        summary = self.run_live_cycle(
            client,
            producer,
            cache=live.PublishedHourCache(),
            now=just_before_boundary,
            sleep=waits.append,
            locations=[location],
        )
        self.assertEqual(client.calls, 1)
        self.assertEqual(len(waits), 1)
        self.assertAlmostEqual(waits[0], 10.0, delta=0.1)
        self.assertAlmostEqual(summary["retry_delays_seconds"][location["location_id"]][0], 10.0, delta=0.1)
        self.assertEqual(producer.messages, [])

    def test_429_then_recovery_publishes_one_canonical_event_per_location(self):
        request = httpx.Request("GET", live.OPEN_METEO_URL)
        payload = [self.payloads[location["location_id"]] for location in self.locations]
        client = SequenceClient([
            httpx.Response(429, headers={"Retry-After": "10"}, request=request),
            httpx.Response(200, json=payload, request=request),
        ])
        producer = FakeProducer()
        cache = live.PublishedHourCache()
        waits = []

        summary = self.run_live_cycle(client, producer, cache=cache, max_retries=2, sleep=waits.append)
        keys = [message["key"] for message in producer.messages]
        event_ids = [json.loads(message["value"])["event_id"] for message in producer.messages]
        self.assertEqual(client.calls, 2)
        self.assertEqual(waits, [10.0])
        self.assertEqual(len(producer.messages), 63)
        self.assertEqual(len(set(keys)), 63)
        self.assertEqual(len(set(event_ids)), 63)
        self.assertEqual(summary["events_delivered"], 63)
        self.assertTrue(cache.has_completed(SAFE_HOUR, [location["location_id"] for location in self.locations]))

    def test_restart_loads_completed_hour_and_skips_refetch(self):
        completed = live.build_hourly_events(self.locations, self.payloads, now=NOW, history_hours=24, bootstrap=False)["events"]
        with TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "published_hours.json"
            first_process_cache = live.PublishedHourCache(state_path)
            first_process_cache.mark_published(completed)
            restarted_cache = live.PublishedHourCache(state_path)
            producer = FakeProducer()

            class NeverCalledClient:
                calls = 0

                def get(self, *args, **kwargs):
                    self.calls += 1
                    raise AssertionError("restarted producer must trust its completed-hour cache")

            client = NeverCalledClient()
            summary = self.run_live_cycle(client, producer, cache=restarted_cache)
            self.assertEqual(client.calls, 0)
            self.assertEqual(summary["api_request_count"], 0)
            self.assertEqual(summary["same_hour_cache_skips"], 63)
            self.assertEqual(producer.messages, [])

    def test_partial_provider_failure_is_reported_by_location_and_successes_remain_publishable(self):
        locations = self.locations[:3]
        payloads = {item["location_id"]: provider_response(item) for item in locations[:2]}
        result = live.build_hourly_events(
            locations,
            payloads,
            now=NOW,
            history_hours=24,
            bootstrap=False,
            prior_failures={locations[2]["location_id"]: "HTTP 503 after bounded retries"},
        )
        self.assertEqual(len(result["events"]), 2)
        self.assertEqual(set(result["failed_locations"]), {locations[2]["location_id"]})

    def test_bootstrap_does_not_use_an_hour_with_missing_values(self):
        locations = self.locations[:1]
        payload = provider_response(locations[0])
        payload["hourly"]["temperature_2m"][24] = None
        result = live.build_hourly_events(
            locations,
            {locations[0]["location_id"]: payload},
            now=NOW,
            history_hours=24,
            bootstrap=True,
        )
        self.assertEqual(result["events"], [])
        self.assertIn("INVALID_LIVE_OBSERVATION", result["failed_locations"][locations[0]["location_id"]])

    def test_batch_fetch_uses_one_multi_coordinate_http_request(self):
        locations = self.locations
        payload = [provider_response(item, offset=index / 10) for index, item in enumerate(locations)]

        class Client:
            calls = 0

            def get(self, url, *, params, timeout):
                self.calls += 1
                self.params = params
                self.timeout = timeout
                return httpx.Response(200, json=payload, request=httpx.Request("GET", url))

        client = Client()
        result = live.fetch_hourly_responses(client, locations, history_hours=24, sleep=lambda _: None)
        self.assertEqual(client.calls, 1)
        self.assertEqual(result["api_request_count"], 1)
        self.assertEqual(len(result["responses_by_id"]), 63)
        self.assertEqual(result["responses_by_id"][locations[2]["location_id"]]["hourly"]["temperature_2m"][0], 25.2)

    def test_retry_helper_retries_transient_http_status_with_bounded_attempts(self):
        location = self.locations[0]
        good = provider_response(location)

        class Client:
            calls = 0

            def get(self, url, *, params, timeout):
                self.calls += 1
                if self.calls == 1:
                    return httpx.Response(503, request=httpx.Request("GET", url))
                return httpx.Response(200, json=good, request=httpx.Request("GET", url))

        client = Client()
        result = live.fetch_hourly_responses(client, [location], history_hours=24, batch_size=1, max_retries=2, sleep=lambda _: None)
        self.assertEqual(client.calls, 2)
        self.assertEqual(result["retry_counts"], {location["location_id"]: 1})
        self.assertEqual(set(result["responses_by_id"]), {location["location_id"]})

    def test_old_current_conditions_event_schema_remains_unchanged(self):
        location = self.locations[0]
        value = weather_producer._event_for_location(location, {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "utc_offset_seconds": 0,
            "current": {
                "time": "2026-10-02T15:00",
                "temperature_2m": 25.0,
                "relative_humidity_2m": 60,
                "precipitation": 0.0,
                "pressure_msl": 1010.0,
                "wind_speed_10m": 7.0,
                "wind_gusts_10m": 12.0,
                "weather_code": 1,
            },
        })
        self.assertEqual(value["event_type"], "WEATHER_OBSERVATION")
        self.assertEqual(value["source"], "OPEN_METEO")

    def test_old_current_conditions_request_keeps_its_bounded_retry_behavior(self):
        location = self.locations[0]
        current = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "utc_offset_seconds": 0,
            "current": {
                "time": "2026-10-02T15:00",
                "temperature_2m": 25.0,
                "relative_humidity_2m": 60,
                "precipitation": 0.0,
                "pressure_msl": 1010.0,
                "wind_speed_10m": 7.0,
                "wind_gusts_10m": 12.0,
                "weather_code": 1,
            },
        }

        class Client:
            calls = 0

            def get(self, url, *, params, timeout):
                self.calls += 1
                if self.calls == 1:
                    return httpx.Response(503, request=httpx.Request("GET", url))
                return httpx.Response(200, json=current, request=httpx.Request("GET", url))

        client = Client()
        retries = {}
        events = weather_producer.fetch_weather(
            client,
            [location],
            max_retries=2,
            sleep=lambda _: None,
            retry_counts=retries,
        )
        self.assertEqual(client.calls, 2)
        self.assertEqual(retries, {location["location_id"]: 1})
        self.assertEqual(events[0]["source"], "OPEN_METEO")

    def test_coordinate_order_guard_rejects_a_wrong_location_response(self):
        location = self.locations[0]
        response = provider_response(location)
        response["latitude"] = location["latitude"] + 1.0
        with self.assertRaisesRegex(ValueError, "from its canonical request"):
            live._parse_location_timeline(location, response)

    def test_unit_mismatch_and_non_wmo_weather_code_fail_closed(self):
        location = self.locations[0]
        wrong_units = provider_response(location)
        wrong_units["hourly_units"]["pressure_msl"] = "Pa"
        with self.assertRaisesRegex(ValueError, "unit mismatch"):
            live._parse_location_timeline(location, wrong_units)
        with self.assertRaisesRegex(ValueError, "WMO integer"):
            live._normalize_weather_values(location["location_id"], {
                "temperature_2m": 20.0,
                "relative_humidity_2m": 60.0,
                "precipitation": 0.0,
                "pressure_msl": 1010.0,
                "wind_speed_10m": 5.0,
                "wind_gusts_10m": 8.0,
                "weather_code": 100,
            })

    def test_publish_uses_composite_key_and_stable_location_partition(self):
        event = live.build_hourly_events(self.locations[:1], {self.locations[0]["location_id"]: self.payloads[self.locations[0]["location_id"]]}, now=NOW, history_hours=24, bootstrap=False)["events"][0]
        producer = FakeProducer()
        result = live.publish_events(producer, live.DEFAULT_TOPIC, [event], partition_count=4)
        message = producer.messages[0]
        self.assertEqual(result["events_delivered"], 1)
        self.assertEqual(message["key"], f"{event['location_id']}|{event['event_time']}")
        self.assertEqual(message["partition"], live.kafka_partition(event["location_id"], 4))
        self.assertEqual(live.kafka_partition(event["location_id"], 4), live.kafka_partition(event["location_id"], 4))
        self.assertEqual(json.loads(message["value"])["event_id"], event["event_id"])


if __name__ == "__main__":
    unittest.main()
