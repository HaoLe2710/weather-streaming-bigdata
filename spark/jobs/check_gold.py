from pyspark.sql import SparkSession
from pyspark.sql import functions as F


GOLD_PATH = (
    "/opt/project/data/gold/"
    "weather_aggregates"
)


spark = (
    SparkSession.builder
    .appName("CheckGoldWeather")
    .config(
        "spark.sql.session.timeZone",
        "UTC"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


gold = (
    spark.read
    .format("delta")
    .load(GOLD_PATH)
)


print(
    "\n========== GOLD SCHEMA =========="
)

gold.printSchema()


print(
    "\n========== GOLD DATA =========="
)

(
    gold
    .select(
        "location_id",
        "city",

        "window_start",
        "window_end",

        "observation_count",

        "avg_temperature_c",
        "min_temperature_c",
        "max_temperature_c",

        "avg_humidity_pct",

        "total_precipitation_mm",

        "avg_pressure_hpa",

        "avg_wind_speed_kmh",
        "max_wind_gust_kmh",

        "first_event_time",
        "last_event_time",
    )

    .orderBy(
        F.col(
            "window_start"
        ).desc(),

        F.col(
            "location_id"
        )
    )

    .show(
        100,
        truncate=False
    )
)


print(
    "\nTotal Gold Records:",
    gold.count()
)


# =====================================
# CHECK UNIQUE WINDOW KEY
# =====================================

duplicates = (
    gold

    .groupBy(
        "location_id",
        "window_start",
        "window_end",
    )

    .count()

    .filter(
        F.col("count") > 1
    )
)


duplicate_count = (
    duplicates.count()
)


print(
    "Duplicate Gold Window Keys:",
    duplicate_count
)


if duplicate_count > 0:

    duplicates.show(
        truncate=False
    )


# =====================================
# SUMMARY BY LOCATION
# =====================================

print(
    "\n========== LOCATION SUMMARY =========="
)

(
    gold
    .groupBy(
        "location_id",
        "city"
    )
    .agg(
        F.count("*")
        .alias(
            "window_count"
        ),

        F.max(
            "observation_count"
        )
        .alias(
            "max_observations_per_window"
        ),

        F.min(
            "window_start"
        )
        .alias(
            "earliest_window"
        ),

        F.max(
            "window_end"
        )
        .alias(
            "latest_window"
        ),
    )
    .orderBy(
        "location_id"
    )
    .show(
        truncate=False
    )
)


spark.stop()