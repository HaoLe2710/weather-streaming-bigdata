from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import math
from pathlib import Path
import sys
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "spark" / "jobs"))
import weather_feature_engineering as feature_engineering


PYSPARK_AVAILABLE = importlib.util.find_spec("pyspark") is not None


class FeatureContractTests(unittest.TestCase):
    def test_expected_counts_and_feature_schema(self):
        counts = feature_engineering.expected_counts()
        self.assertEqual(counts["source_rows_per_location"], 52_608)
        self.assertEqual(counts["output_rows_per_location"], 52_583)
        self.assertEqual(counts["source_rows"], 3_314_304)
        self.assertEqual(counts["output_rows"], 3_312_729)
        self.assertEqual(counts["history_rows_dropped"], 1_512)
        self.assertEqual(counts["target_rows_dropped"], 63)
        self.assertEqual(
            counts["split_rows"],
            {"TRAIN": 2_207_457, "VALIDATION": 553_392, "TEST": 551_880},
        )
        self.assertEqual(
            counts["split_rows_per_location"],
            {"TRAIN": 35_039, "VALIDATION": 8_784, "TEST": 8_760},
        )
        self.assertEqual(len(feature_engineering.MODEL_FEATURE_COLUMNS), 73)
        self.assertEqual(len(feature_engineering.OUTPUT_COLUMNS), 79)
        self.assertEqual(len(set(feature_engineering.OUTPUT_COLUMNS)), 79)
        self.assertTrue(
            {"precipitation_sum_3h", "precipitation_sum_6h", "precipitation_sum_24h"}
            .issubset(feature_engineering.MODEL_FEATURE_COLUMNS)
        )
        self.assertNotIn("precipitation_roll_sum_3h", feature_engineering.MODEL_FEATURE_COLUMNS)

    def test_cyclical_values(self):
        self.assertAlmostEqual(feature_engineering.cyclic_pair(0, 24)[0], 0.0, places=12)
        self.assertAlmostEqual(feature_engineering.cyclic_pair(0, 24)[1], 1.0, places=12)
        self.assertAlmostEqual(feature_engineering.cyclic_pair(6, 24)[0], 1.0, places=12)
        self.assertAlmostEqual(feature_engineering.cyclic_pair(6, 24)[1], 0.0, places=12)
        self.assertAlmostEqual(feature_engineering.cyclic_pair(12, 24)[0], 0.0, places=12)
        self.assertAlmostEqual(feature_engineering.cyclic_pair(12, 24)[1], -1.0, places=12)

    def test_target_time_split_boundaries_are_utc(self):
        cases = (
            (datetime(2024, 1, 1, 0, tzinfo=timezone.utc), "VALIDATION"),
            (datetime(2024, 12, 31, 23, tzinfo=timezone.utc), "VALIDATION"),
            (datetime(2025, 1, 1, 0, tzinfo=timezone.utc), "TEST"),
            (datetime(2023, 12, 31, 23, tzinfo=timezone.utc), "TRAIN"),
            (datetime(2023, 12, 31, 22, tzinfo=timezone.utc), "TRAIN"),
        )
        for target_time, expected in cases:
            with self.subTest(target_time=target_time):
                self.assertEqual(feature_engineering.classify_split(target_time), expected)
        with self.assertRaises(ValueError):
            feature_engineering.classify_split(datetime(2026, 1, 1, tzinfo=timezone.utc))
        with self.assertRaises(ValueError):
            feature_engineering.classify_split(datetime(2024, 1, 1))

    def test_weather_code_is_metadata_not_a_numeric_feature(self):
        spec = feature_engineering.feature_specification()
        self.assertNotIn("weather_code", feature_engineering.MODEL_FEATURE_COLUMNS)
        self.assertEqual(
            next(item for item in spec["metadata"] if item["name"] == "weather_code")["role"],
            "context",
        )
        self.assertEqual(spec["feature_count"], 73)
        self.assertEqual(
            {item["name"] for item in spec["model_features"]},
            set(feature_engineering.MODEL_FEATURE_COLUMNS),
        )

    def test_only_target_construction_uses_lead(self):
        source = Path(feature_engineering.__file__).read_text(encoding="utf-8")
        self.assertEqual(source.count("F.lead("), 2)
        self.assertIn("F.lead(F.col(\"temperature_c\"), 1)", source)
        self.assertIn("F.lead(F.col(\"event_time\"), 1)", source)


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark semantic tests run inside the Spark image")
class SparkFeatureSemanticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("weather-feature-engineering-unit-tests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _frame(self, locations: list[tuple[str, datetime, int, int | None]]):
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("location_id", StringType(), False),
                StructField("event_time", TimestampType(), False),
                StructField("temperature_c", DoubleType(), False),
                StructField("humidity_pct", LongType(), False),
                StructField("precipitation_mm", DoubleType(), False),
                StructField("pressure_hpa", DoubleType(), False),
                StructField("wind_speed_kmh", DoubleType(), False),
                StructField("wind_gust_kmh", DoubleType(), False),
                StructField("latitude", DoubleType(), False),
                StructField("longitude", DoubleType(), False),
                StructField("weather_code", IntegerType(), True),
            ]
        )
        rows = []
        for location_id, start, hours, missing_hour in locations:
            temperature_offset = 100.0 if location_id == "VN_B" else 0.0
            for index in range(hours):
                if index == missing_hour:
                    continue
                rows.append(
                    (
                        location_id,
                        start + timedelta(hours=index),
                        float(index) + temperature_offset,
                        50 + index,
                        float(index),
                        1000.0 + index,
                        5.0 + index,
                        6.0 + index,
                        21.0,
                        105.0,
                        61,
                    )
                )
        return self.spark.createDataFrame(rows, schema=schema)

    def _features(self, frame):
        output, diagnostics = feature_engineering.build_feature_frame(frame)
        return output, diagnostics

    def test_lags_rolling_population_std_precipitation_and_deltas(self):
        start = datetime(2020, 1, 1, 23, tzinfo=timezone.utc).replace(tzinfo=None)
        output, _ = self._features(self._frame([("VN_A", start, 30, None)]))
        row = (
            output.where("event_time = timestamp'2020-01-02 23:00:00'")
            .select(
                "temperature_c",
                "temp_lag_1h",
                "temp_lag_3h",
                "temp_lag_6h",
                "temp_lag_12h",
                "temp_lag_24h",
                "temp_roll_mean_3h",
                "temp_roll_mean_6h",
                "temp_roll_mean_24h",
                "humidity_roll_mean_3h",
                "humidity_roll_mean_6h",
                "humidity_roll_mean_24h",
                "humidity_roll_std_3h",
                "humidity_roll_std_6h",
                "humidity_roll_std_24h",
                "pressure_roll_mean_3h",
                "pressure_roll_mean_6h",
                "pressure_roll_mean_24h",
                "pressure_roll_std_3h",
                "pressure_roll_std_6h",
                "pressure_roll_std_24h",
                "wind_speed_roll_mean_3h",
                "wind_speed_roll_mean_6h",
                "wind_speed_roll_mean_24h",
                "temp_roll_std_3h",
                "temp_roll_std_6h",
                "temp_roll_std_24h",
                "precipitation_sum_3h",
                "precipitation_sum_6h",
                "precipitation_sum_24h",
                "temp_delta_1h",
                "temp_delta_3h",
                "humidity_delta_3h",
                "humidity_delta_1h",
                "pressure_delta_1h",
                "pressure_delta_3h",
                "pressure_delta_6h",
                "wind_speed_delta_1h",
            )
            .first()
        )
        self.assertIsNotNone(row)
        expected = {
            "temperature_c": 24.0,
            "temp_lag_1h": 23.0,
            "temp_lag_3h": 21.0,
            "temp_lag_6h": 18.0,
            "temp_lag_12h": 12.0,
            "temp_lag_24h": 0.0,
            "temp_roll_mean_3h": 23.0,
            "temp_roll_mean_6h": 21.5,
            "temp_roll_mean_24h": 12.5,
            "humidity_roll_mean_3h": 73.0,
            "humidity_roll_mean_6h": 71.5,
            "humidity_roll_mean_24h": 62.5,
            "humidity_roll_std_3h": math.sqrt(2.0 / 3.0),
            "humidity_roll_std_6h": math.sqrt(35.0 / 12.0),
            "humidity_roll_std_24h": math.sqrt(575.0 / 12.0),
            "pressure_roll_mean_3h": 1023.0,
            "pressure_roll_mean_6h": 1021.5,
            "pressure_roll_mean_24h": 1012.5,
            "pressure_roll_std_3h": math.sqrt(2.0 / 3.0),
            "pressure_roll_std_6h": math.sqrt(35.0 / 12.0),
            "pressure_roll_std_24h": math.sqrt(575.0 / 12.0),
            "wind_speed_roll_mean_3h": 28.0,
            "wind_speed_roll_mean_6h": 26.5,
            "wind_speed_roll_mean_24h": 17.5,
            "temp_roll_std_3h": math.sqrt(2.0 / 3.0),
            "temp_roll_std_6h": math.sqrt(35.0 / 12.0),
            "temp_roll_std_24h": math.sqrt(575.0 / 12.0),
            "precipitation_sum_3h": 69.0,
            "precipitation_sum_6h": 129.0,
            "precipitation_sum_24h": 300.0,
            "temp_delta_1h": 1.0,
            "temp_delta_3h": 3.0,
            "humidity_delta_3h": 3.0,
            "humidity_delta_1h": 1.0,
            "pressure_delta_1h": 1.0,
            "pressure_delta_3h": 3.0,
            "pressure_delta_6h": 6.0,
            "wind_speed_delta_1h": 1.0,
        }
        for name, value in expected.items():
            with self.subTest(feature=name):
                self.assertAlmostEqual(row[name], value, places=8)

    def test_target_uses_next_hour_in_same_location(self):
        start = datetime(2020, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)
        frame = self._frame(
            [("VN_A", start, 30, None), ("VN_B", start, 30, None)]
        )
        output, _ = self._features(frame)
        rows = {
            row["location_id"]: row
            for row in output.where(
                "event_time = timestamp'2020-01-02 00:00:00'"
            ).select("location_id", "target_time", "target_temperature_1h").collect()
        }
        self.assertEqual(set(rows), {"VN_A", "VN_B"})
        self.assertEqual(rows["VN_A"]["target_temperature_1h"], 25.0)
        self.assertEqual(rows["VN_B"]["target_temperature_1h"], 125.0)
        self.assertEqual(
            rows["VN_A"]["target_time"],
            datetime(2020, 1, 2, 1),
        )

    def test_gap_rejects_non_hourly_target_and_history(self):
        start = datetime(2020, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)
        output, diagnostics = self._features(
            self._frame([("VN_A", start, 32, 26)])
        )
        rejected = output.where(
            "event_time = timestamp'2020-01-02 01:00:00'"
        ).count()
        self.assertEqual(rejected, 0)
        self.assertEqual(int(diagnostics["source_hourly_gap_edges"]), 1)
        self.assertEqual(int(diagnostics["target_time_gap_edges"]), 1)

    def test_null_weather_value_does_not_produce_partial_rolling_window(self):
        from pyspark.sql import functions as F

        start = datetime(2020, 1, 1)
        frame = self._frame([("VN_A", start, 32, None)]).withColumn(
            "precipitation_mm",
            F.when(
                F.col("event_time") == F.lit(start + timedelta(hours=20)),
                F.lit(None).cast("double"),
            ).otherwise(F.col("precipitation_mm")),
        )
        output, diagnostics = self._features(frame)
        self.assertEqual(
            output.where("event_time = timestamp'2020-01-02 00:00:00'").count(),
            0,
        )
        self.assertGreater(int(diagnostics["invalid_numeric_feature_rows"]), 0)

    def test_target_time_split_boundary_rows(self):
        before_validation = datetime(2023, 12, 30, 23)
        before_test = datetime(2024, 12, 30, 23)
        frame = self._frame(
            [
                ("VN_A", before_validation, 27, None),
                ("VN_B", before_test, 27, None),
            ]
        )
        output, _ = self._features(frame)
        rows = {
            row["location_id"]: row["split"]
            for row in output.where("event_time in (timestamp'2023-12-31 23:00:00', timestamp'2024-12-31 23:00:00')")
            .select("location_id", "split")
            .collect()
        }
        self.assertEqual(rows, {"VN_A": "VALIDATION", "VN_B": "TEST"})

    def test_local_cycle_features_use_vietnam_time(self):
        start = datetime(2020, 1, 1, 23)
        output, _ = self._features(self._frame([("VN_A", start, 27, None)]))
        row = output.where("local_hour = 6").select(
            "local_day_of_week",
            "hour_sin",
            "hour_cos",
        ).first()
        self.assertIsNotNone(row)
        self.assertEqual(row["local_day_of_week"], 4)  # Friday, Monday=0
        self.assertAlmostEqual(row["hour_sin"], 1.0, places=8)
        self.assertAlmostEqual(row["hour_cos"], 0.0, places=8)


if __name__ == "__main__":
    unittest.main()
