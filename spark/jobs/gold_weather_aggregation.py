import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable


SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "/opt/project/data/silver/weather_clean",
)

GOLD_PATH = os.getenv(
    "GOLD_PATH",
    "/opt/project/data/gold/weather_aggregates",
)

GOLD_CHECKPOINT = os.getenv(
    "GOLD_CHECKPOINT",
    "/opt/project/data/checkpoints/gold_weather_aggregates",
)

AVAILABLE_NOW = (
    os.getenv("AVAILABLE_NOW", "false").strip().lower()
    in {"1", "true", "yes"}
)


spark = (
    SparkSession.builder
    .appName("WeatherGoldAggregation")
    .config(
        "spark.sql.session.timeZone",
        "UTC"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "4"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =====================================
# READ SILVER STREAM
# =====================================

silver = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
)


# =====================================
# EVENT TIME + WATERMARK
# =====================================

watermarked = (
    silver
    .withWatermark(
        "event_time",
        "10 minutes"
    )
)


# =====================================
# SLIDING WINDOW AGGREGATION
#
# Window: 30 minutes
# Slide : 5 minutes
# =====================================

aggregated = (
    watermarked
    .groupBy(
        F.window(
            F.col("event_time"),
            "30 minutes",
            "5 minutes"
        ).alias("window"),

        F.col("location_id"),
        F.col("city"),
        F.col("latitude"),
        F.col("longitude"),
    )
    .agg(

        # Number of observations
        F.count("*")
        .alias("observation_count"),

        # Temperature
        F.avg("temperature_c")
        .alias("avg_temperature_c"),

        F.min("temperature_c")
        .alias("min_temperature_c"),

        F.max("temperature_c")
        .alias("max_temperature_c"),

        F.stddev_pop("temperature_c")
        .alias("stddev_temperature_c"),

        # Humidity
        F.avg("humidity_pct")
        .alias("avg_humidity_pct"),

        F.min("humidity_pct")
        .alias("min_humidity_pct"),

        F.max("humidity_pct")
        .alias("max_humidity_pct"),

        # Rainfall
        F.sum("precipitation_mm")
        .alias("total_precipitation_mm"),

        # Pressure
        F.avg("pressure_hpa")
        .alias("avg_pressure_hpa"),

        F.min("pressure_hpa")
        .alias("min_pressure_hpa"),

        F.max("pressure_hpa")
        .alias("max_pressure_hpa"),

        # Wind
        F.avg("wind_speed_kmh")
        .alias("avg_wind_speed_kmh"),

        F.max("wind_gust_kmh")
        .alias("max_wind_gust_kmh"),

        # Event range
        F.min("event_time")
        .alias("first_event_time"),

        F.max("event_time")
        .alias("last_event_time"),
    )
)


# =====================================
# FLATTEN WINDOW STRUCTURE
# =====================================

gold = (
    aggregated
    .select(

        F.col("window.start")
        .alias("window_start"),

        F.col("window.end")
        .alias("window_end"),

        "location_id",
        "city",
        "latitude",
        "longitude",

        "observation_count",

        F.round(
            "avg_temperature_c",
            2
        ).alias(
            "avg_temperature_c"
        ),

        F.round(
            "min_temperature_c",
            2
        ).alias(
            "min_temperature_c"
        ),

        F.round(
            "max_temperature_c",
            2
        ).alias(
            "max_temperature_c"
        ),

        F.round(
            "stddev_temperature_c",
            2
        ).alias(
            "stddev_temperature_c"
        ),

        F.round(
            "avg_humidity_pct",
            2
        ).alias(
            "avg_humidity_pct"
        ),

        F.round(
            "min_humidity_pct",
            2
        ).alias(
            "min_humidity_pct"
        ),

        F.round(
            "max_humidity_pct",
            2
        ).alias(
            "max_humidity_pct"
        ),

        F.round(
            "total_precipitation_mm",
            2
        ).alias(
            "total_precipitation_mm"
        ),

        F.round(
            "avg_pressure_hpa",
            2
        ).alias(
            "avg_pressure_hpa"
        ),

        F.round(
            "min_pressure_hpa",
            2
        ).alias(
            "min_pressure_hpa"
        ),

        F.round(
            "max_pressure_hpa",
            2
        ).alias(
            "max_pressure_hpa"
        ),

        F.round(
            "avg_wind_speed_kmh",
            2
        ).alias(
            "avg_wind_speed_kmh"
        ),

        F.round(
            "max_wind_gust_kmh",
            2
        ).alias(
            "max_wind_gust_kmh"
        ),

        "first_event_time",
        "last_event_time",
    )

    .withColumn(
        "gold_updated_at",
        F.current_timestamp()
    )
)


# =====================================
# UPSERT GOLD DELTA TABLE
# =====================================

def upsert_gold(
    batch_df,
    batch_id
):

    if batch_df.isEmpty():
        print(
            f"[GOLD] batch={batch_id} "
            "has no rows"
        )
        return

    batch_df.persist()

    row_count = batch_df.count()

    print(
        f"[GOLD] batch={batch_id} "
        f"rows={row_count}"
    )

    if DeltaTable.isDeltaTable(
        spark,
        GOLD_PATH
    ):

        target = (
            DeltaTable
            .forPath(
                spark,
                GOLD_PATH
            )
        )

        (
            target.alias("target")
            .merge(
                batch_df.alias("source"),

                """
                target.location_id
                    = source.location_id
                AND target.window_start
                    = source.window_start
                AND target.window_end
                    = source.window_end
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:

        (
            batch_df.write
            .format("delta")
            .mode("overwrite")
            .save(GOLD_PATH)
        )

    batch_df.unpersist()


# =====================================
# START STREAM
# =====================================

gold_writer = (
    gold.writeStream
    .foreachBatch(upsert_gold)
    .outputMode("update")
    .option("checkpointLocation", GOLD_CHECKPOINT)
)

if AVAILABLE_NOW:
    gold_writer = gold_writer.trigger(availableNow=True)
else:
    gold_writer = gold_writer.trigger(processingTime="10 seconds")

query = gold_writer.start()

print(
    "Weather Gold Aggregation started"
)

print(
    "Query ID:",
    query.id
)


query.awaitTermination()

if AVAILABLE_NOW:
    spark.stop()
