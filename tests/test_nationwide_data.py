import gzip
import json
from pathlib import Path
import tempfile
import unittest

import httpx

from historical import download_historical as downloader
from historical import validate_historical
from historical.location_catalog import (
    DATASET_BENCHMARK_20,
    DATASET_NATIONWIDE_63,
    ORIGINAL_20_IDS,
    expected_hours,
    expected_records,
    load_catalog,
    select_dataset_locations,
    validate_catalog,
)
from producer import weather_producer
from simulator import historical_stream_simulator
from spark.jobs import historical_to_delta


class LocationCatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.locations = load_catalog()

    def test_catalog_has_exact_frozen_snapshot(self):
        report = validate_catalog(self.locations)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["catalog_count"], 63)
        self.assertEqual(report["unique_location_ids"], 63)
        self.assertEqual(report["unique_canonical_names"], 63)
        self.assertEqual(report["central_city_count"], 6)
        self.assertEqual(report["province_count"], 57)
        self.assertEqual(report["original_20_preserved"], 20)
        self.assertEqual(report["new_location_count"], 43)

    def test_dataset_membership_keeps_benchmark_source_at_twenty(self):
        benchmark = select_dataset_locations(self.locations, DATASET_BENCHMARK_20)
        nationwide = select_dataset_locations(self.locations, DATASET_NATIONWIDE_63)
        self.assertEqual(len(benchmark), 20)
        self.assertEqual({item["location_id"] for item in benchmark}, set(ORIGINAL_20_IDS))
        self.assertEqual(len(nationwide), 63)

    def test_original_ids_coordinates_and_event_names_are_preserved(self):
        expected = {
            "VN_HCM": (10.8231, 106.6297, "Ho Chi Minh City"),
            "VN_HANOI": (21.0285, 105.8542, "Hanoi"),
            "VN_DANANG": (16.0544, 108.2022, "Da Nang"),
            "VN_CANTHO": (10.0452, 105.7469, "Can Tho"),
            "VN_HUE": (16.4637, 107.5909, "Hue"),
            "VN_HAIPHONG": (20.8449, 106.6881, "Hai Phong"),
            "VN_NHATRANG": (12.2388, 109.1967, "Nha Trang"),
            "VN_DALAT": (11.9404, 108.4583, "Da Lat"),
            "VN_VUNGTAU": (10.4114, 107.1362, "Vung Tau"),
            "VN_QUYNHON": (13.7820, 109.2190, "Quy Nhon"),
            "VN_BMT": (12.6667, 108.0500, "Buon Ma Thuot"),
            "VN_PLEIKU": (13.9833, 108.0000, "Pleiku"),
            "VN_PHANTHIET": (10.9289, 108.1021, "Phan Thiet"),
            "VN_VINH": (18.6796, 105.6813, "Vinh"),
            "VN_THANHHOA": (19.8067, 105.7852, "Thanh Hoa"),
            "VN_HALONG": (20.9712, 107.0448, "Ha Long"),
            "VN_BIENHOA": (10.9574, 106.8426, "Bien Hoa"),
            "VN_LONGXUYEN": (10.3864, 105.4352, "Long Xuyen"),
            "VN_RACHGIA": (10.0125, 105.0809, "Rach Gia"),
            "VN_CAMAU": (9.1768, 105.1524, "Ca Mau"),
        }
        actual = {item["location_id"]: item for item in self.locations}
        self.assertEqual(set(expected), set(ORIGINAL_20_IDS))
        for location_id, (latitude, longitude, name) in expected.items():
            with self.subTest(location_id=location_id):
                self.assertEqual(actual[location_id]["latitude"], latitude)
                self.assertEqual(actual[location_id]["longitude"], longitude)
                self.assertEqual(actual[location_id]["name"], name)

    def test_hue_is_canonical_and_historical_name_is_only_an_alias(self):
        names = {item["province_name"] for item in self.locations}
        hue = next(item for item in self.locations if item["province_name"] == "Huế")
        self.assertNotIn("Thừa Thiên Huế", names)
        self.assertIn("Thừa Thiên Huế", hue["aliases"])


class HistoricalCountTests(unittest.TestCase):
    def test_expected_hours_for_leap_and_regular_years(self):
        self.assertEqual(expected_hours(2020), 8784)
        self.assertEqual(expected_hours(2021), 8760)
        self.assertEqual(expected_hours(2024), 8784)
        self.assertEqual(expected_hours(2025), 8760)

    def test_expected_record_totals_for_each_dataset(self):
        self.assertEqual(expected_records(1), 52608)
        self.assertEqual(expected_records(20), 1052160)
        self.assertEqual(expected_records(43), 2262144)
        self.assertEqual(expected_records(63), 3314304)


class HistoricalDownloaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.locations = select_dataset_locations(load_catalog(), DATASET_NATIONWIDE_63)

    def test_batch_construction_is_deterministic_and_bounded(self):
        locations = self.locations[:23]
        batches = list(downloader.chunks(locations, 10))
        self.assertEqual([len(batch) for batch in batches], [10, 10, 3])
        self.assertEqual(
            [item["location_id"] for batch in batches for item in batch],
            [item["location_id"] for item in locations],
        )
        with self.assertRaises(ValueError):
            list(downloader.chunks(locations, 0))

    def test_batch_request_uses_utc_coordinates_and_variable_contract(self):
        locations = self.locations[:2]
        provider_response = httpx.Response(
            200,
            json=[{"hourly": {}}, {"hourly": {}}],
            request=httpx.Request("GET", downloader.BASE_URL),
        )

        class Client:
            params = None

            def get(self, url, *, params, timeout):
                self.params = params
                self.timeout = timeout
                return provider_response

        client = Client()
        payload = downloader.fetch_batch(client, locations, 2020)
        self.assertEqual(len(payload), 2)
        self.assertEqual(client.params["timezone"], "UTC")
        self.assertEqual(client.params["start_date"], "2020-01-01")
        self.assertEqual(client.params["end_date"], "2020-12-31")
        self.assertEqual(len(client.params["latitude"].split(",")), 2)
        self.assertEqual(len(client.params["longitude"].split(",")), 2)
        self.assertEqual(client.params["hourly"].split(","), downloader.HOURLY_VARIABLES)

    def test_coordinate_response_mapping_requires_exact_cardinality(self):
        locations = self.locations[:2]
        mapped = downloader.map_batch_responses(locations, [{"n": 1}, {"n": 2}])
        self.assertEqual(mapped[0][0]["location_id"], locations[0]["location_id"])
        self.assertEqual(mapped[1][1]["n"], 2)
        with self.assertRaisesRegex(ValueError, "returned 1 locations for 2"):
            downloader.map_batch_responses(locations, [{"n": 1}])

    def test_convert_location_keeps_weather_schema_and_deterministic_event_id(self):
        location = self.locations[0]
        response = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "utc_offset_seconds": 0,
            "hourly": {
                "time": ["2020-01-01T00:00"],
                "temperature_2m": [20.0],
                "relative_humidity_2m": [50],
                "precipitation": [0.0],
                "pressure_msl": [1010.0],
                "wind_speed_10m": [4.0],
                "wind_gusts_10m": [8.0],
                "weather_code": [1],
            },
        }
        record = downloader.convert_location(location, response)[0]
        self.assertEqual(record["event_id"], f"{location['location_id']}_2020-01-01T00:00:00Z")
        self.assertEqual(record["location_id"], location["location_id"])
        self.assertEqual(record["city"], location["name"])
        self.assertEqual(record["temperature_c"], 20.0)
        self.assertIsNone(record["ingestion_time"])
        self.assertEqual(record["source"], "OPEN_METEO_HISTORICAL")

    def test_retry_exhaustion_is_bounded_and_counted(self):
        request = httpx.Request("GET", downloader.BASE_URL)
        response = httpx.Response(503, request=request)

        class Client:
            calls = 0

            def get(self, *args, **kwargs):
                self.calls += 1
                return response

        client = Client()
        waits = []
        retries = {}
        with self.assertRaisesRegex(RuntimeError, "attempts=3"):
            downloader.fetch_batch(
                client,
                self.locations[:1],
                2020,
                max_retries=3,
                sleep=waits.append,
                retry_counts=retries,
            )
        self.assertEqual(client.calls, 3)
        self.assertEqual(waits, [1.0, 2.0])
        self.assertEqual(sum(retries.values()), 2)

    def test_unit_promotion_is_atomic_and_resume_detection_checks_content(self):
        location = self.locations[0]
        records = [
            {
                "event_id": f"{location['location_id']}_{stamp}",
                "location_id": location["location_id"],
                "event_time": stamp,
            }
            for stamp in ("2020-01-01T01:00:00Z", "2020-01-01T00:00:00Z")
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / location["location_id"] / "2020.jsonl.gz"
            result = downloader.write_unit(path, location, 2020, records)
            self.assertEqual(result["actual_records"], 2)
            self.assertEqual(result["missing_records"], 8782)
            self.assertTrue(downloader.unit_file_valid(path, location, 2020))
            self.assertFalse(path.with_name(path.name + ".partial").exists())
            with gzip.open(path, "rt", encoding="utf-8") as source:
                times = [json.loads(line)["event_time"] for line in source]
            self.assertEqual(times, sorted(times))

    def test_download_year_resumes_valid_units_without_refetching(self):
        locations = self.locations[:2]
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            unit_dir = root / "staging" / "units"
            raw_dir = root / "raw"
            for index, location in enumerate(locations):
                stamp = f"2020-01-01T0{index}:00:00Z"
                downloader.write_unit(
                    unit_dir / location["location_id"] / "2020.jsonl.gz",
                    location,
                    2020,
                    [{
                        "event_id": f"{location['location_id']}_{stamp}",
                        "location_id": location["location_id"],
                        "event_time": stamp,
                    }],
                )

            class NeverCalledClient:
                def get(self, *args, **kwargs):
                    raise AssertionError("resume should not call the provider")

            request_counter = {}
            result = downloader.download_year(
                NeverCalledClient(),
                locations,
                2020,
                unit_dir,
                raw_dir,
                batch_size=10,
                request_delay_seconds=0,
                request_counter=request_counter,
            )
            self.assertEqual(request_counter.get("batch_requests", 0), 0)
            self.assertEqual(result["actual_records"], 2)
            self.assertEqual(result["expected_records"], 17568)
            self.assertEqual(result["missing_records"], 17566)
            self.assertTrue((raw_dir / "weather_2020.jsonl.gz").exists())


class HistoricalRawValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.location = load_catalog()[0]

    def make_record(self, event_time):
        location = self.location
        return {
            "event_id": f"{location['location_id']}_{event_time}",
            "location_id": location["location_id"],
            "event_time": event_time,
            "temperature_c": 20.0,
            "humidity_pct": 50.0,
            "precipitation_mm": 0.0,
            "latitude": location["latitude"],
            "longitude": location["longitude"],
        }

    def test_raw_validation_enumerates_missing_hours_without_filling_them(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "weather_2020.jsonl.gz"
            records = [
                self.make_record("2020-01-01T00:00:00Z"),
                self.make_record("2020-01-01T01:00:00Z"),
            ]
            with gzip.open(path, "wt", encoding="utf-8") as output:
                for record in records:
                    output.write(json.dumps(record) + "\n")
            report = validate_historical.validate_raw_dataset(temp_dir, [self.location])
            output_json = Path(temp_dir) / "validation.json"
            validate_historical._write_reports(report, output_json)
            location_header = output_json.with_name("per_location_validation.csv").read_text(
                encoding="utf-8-sig"
            ).splitlines()[0]
            year_header = output_json.with_name("year_validation.csv").read_text(
                encoding="utf-8"
            ).splitlines()[0]
        stats = report["per_location"][0]
        self.assertEqual(report["total_records"], 2)
        self.assertEqual(report["unique_event_ids"], 2)
        self.assertEqual(report["missing_records"], 52606)
        self.assertEqual(len(stats["missing_timestamps"]), 52606)
        self.assertEqual(stats["missing_timestamps"][0], "2020-01-01T02:00:00Z")
        self.assertEqual(stats["status"], "MISSING_HOURS")
        self.assertIn("quality_violation_count", location_header)
        self.assertIn("missing_timestamps", location_header)
        self.assertIn("affected_locations", year_header)
        self.assertIn("missing_records", year_header)

    def test_raw_validation_reports_duplicate_ids_and_timestamp_keys(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "weather_2020.jsonl.gz"
            record = self.make_record("2020-01-01T00:00:00Z")
            with gzip.open(path, "wt", encoding="utf-8") as output:
                output.write(json.dumps(record) + "\n")
                output.write(json.dumps(record) + "\n")
            report = validate_historical.validate_raw_dataset(temp_dir, [self.location])
        self.assertEqual(report["unique_event_ids"], 1)
        self.assertEqual(report["duplicate_event_ids"], 1)
        self.assertEqual(report["duplicate_observation_keys"], 1)
        self.assertEqual(report["per_location"][0]["duplicate_count"], 1)
        self.assertEqual(report["per_location"][0]["status"], "DUPLICATE")


class LiveProducerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.locations = weather_producer.load_cities()

    @staticmethod
    def response_for(location):
        return {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "utc_offset_seconds": 0,
            "current": {
                "time": "2026-09-28T10:00",
                "temperature_2m": 25.0,
                "relative_humidity_2m": 60,
                "precipitation": 0.0,
                "pressure_msl": 1010.0,
                "wind_speed_10m": 7.0,
                "wind_gusts_10m": 12.0,
                "weather_code": 1,
            },
        }

    def test_catalog_loading_and_live_batches_use_the_canonical_63(self):
        self.assertEqual(len(self.locations), 63)
        batches = list(weather_producer.chunks(self.locations, 63))
        self.assertEqual([len(batch) for batch in batches], [63])
        self.assertEqual(
            [item["location_id"] for item in batches[0]],
            [item["location_id"] for item in self.locations],
        )

    def test_batch_mapping_creates_63_unique_events_with_original_schema(self):
        payload = [self.response_for(location) for location in self.locations]
        events = weather_producer.map_weather_batch(
            self.locations,
            payload,
            ingestion_time="2026-09-28T10:01:00+00:00",
        )
        ids = {location["location_id"] for location in self.locations}
        self.assertEqual(len(events), 63)
        self.assertEqual({event["location_id"] for event in events}, ids)
        self.assertEqual(len({event["event_id"] for event in events}), 63)
        self.assertEqual({event["event_time"] for event in events}, {"2026-09-28T10:00:00Z"})
        self.assertTrue(all(event["source"] == "OPEN_METEO" for event in events))
        self.assertTrue(all(event["latitude"] is not None and event["longitude"] is not None for event in events))

    def test_mapping_fails_closed_when_provider_location_count_differs(self):
        with self.assertRaisesRegex(ValueError, "returned 1 locations for 2"):
            weather_producer.map_weather_batch(
                self.locations[:2],
                [self.response_for(self.locations[0])],
            )

    def test_publish_poll_emits_all_ids_with_location_id_keys(self):
        locations = self.locations
        payload = [self.response_for(location) for location in locations]
        provider_response = httpx.Response(
            200,
            json=payload,
            request=httpx.Request("GET", weather_producer.OPEN_METEO_URL),
        )

        class Client:
            def get(self, url, *, params, timeout):
                self.params = params
                return provider_response

        class FakeProducer:
            def __init__(self):
                self.messages = []

            def produce(self, *, topic, key, value, callback):
                self.messages.append({
                    "topic": topic,
                    "key": key,
                    "event": json.loads(value),
                    "callback": callback,
                })

            def poll(self, timeout):
                return 0

            def flush(self, timeout):
                return 0

        producer = FakeProducer()
        result = weather_producer.publish_poll(
            Client(),
            producer,
            "weather.test.vn63.unit",
            locations,
        )
        expected_ids = {location["location_id"] for location in locations}
        self.assertEqual(result["enqueued_messages"], 63)
        self.assertEqual(set(result["unique_location_ids"]), expected_ids)
        self.assertEqual(result["unique_event_ids"], 63)
        self.assertEqual({message["key"] for message in producer.messages}, expected_ids)
        self.assertEqual({message["topic"] for message in producer.messages}, {"weather.test.vn63.unit"})

    def test_partial_batch_failure_publishes_only_successful_locations(self):
        locations = self.locations[:3]
        successful_payload = [self.response_for(location) for location in locations[:2]]
        success_response = httpx.Response(
            200,
            json=successful_payload,
            request=httpx.Request("GET", weather_producer.OPEN_METEO_URL),
        )
        failed_response = httpx.Response(
            503,
            request=httpx.Request("GET", weather_producer.OPEN_METEO_URL),
        )

        class Client:
            calls = 0

            def get(self, url, *, params, timeout):
                self.calls += 1
                return success_response if self.calls == 1 else failed_response

        class FakeProducer:
            def __init__(self):
                self.messages = []

            def produce(self, *, topic, key, value, callback):
                self.messages.append(json.loads(value))

            def poll(self, timeout):
                return 0

            def flush(self, timeout):
                return 0

        producer = FakeProducer()
        result = weather_producer.publish_poll(
            Client(),
            producer,
            "weather.test.vn63.partial",
            locations,
            batch_size=2,
            sleep=lambda _: None,
        )
        self.assertEqual(result["enqueued_messages"], 2)
        self.assertEqual(len(result["failed_batches"]), 1)
        self.assertEqual(result["failed_batches"][0]["location_ids"], [locations[2]["location_id"]])
        self.assertEqual(
            {event["location_id"] for event in producer.messages},
            {item["location_id"] for item in locations[:2]},
        )


class SimulatorDatasetSelectionTests(unittest.TestCase):
    def test_named_sources_and_explicit_source_override(self):
        self.assertEqual(
            historical_stream_simulator.resolve_dataset_source("benchmark-20"),
            Path("/opt/project/history-data/historical/raw"),
        )
        self.assertEqual(
            historical_stream_simulator.resolve_dataset_source("nationwide-63"),
            Path("/opt/project/history-data/historical/nationwide_63/raw"),
        )
        self.assertEqual(
            historical_stream_simulator.resolve_dataset_source("benchmark-20", "C:/custom/raw"),
            Path("C:/custom/raw"),
        )


class HistoricalDeltaDatasetSelectionTests(unittest.TestCase):
    def test_benchmark_and_nationwide_use_distinct_raw_and_delta_paths(self):
        benchmark_source, benchmark_target = historical_to_delta.dataset_paths("benchmark-20")
        nationwide_source, nationwide_target = historical_to_delta.dataset_paths("nationwide-63")
        self.assertEqual(benchmark_source, Path("/opt/project/history-data/historical/raw"))
        self.assertEqual(benchmark_target, Path("/opt/project/data/historical/weather_hourly"))
        self.assertEqual(
            nationwide_source,
            Path("/opt/project/history-data/historical/nationwide_63/raw"),
        )
        self.assertEqual(
            nationwide_target,
            Path("/opt/project/data/historical/weather_hourly_vn63"),
        )
        self.assertNotEqual(benchmark_target, nationwide_target)


if __name__ == "__main__":
    unittest.main()
