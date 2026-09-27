"""DEPRECATED diagnostic: invalidated physical-Delta-order experiment.

This job reads Silver Delta files and does not preserve the original Kafka
arrival order. Use gold_kafka_watermark_benchmark.py for the current
Kafka-direct watermark benchmark.
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


APP_NAME = os.getenv(
    "APP_NAME",
    "WeatherGoldWatermarkBenchmark"
)

SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "/opt/project/data/benchmark/wm10m/silver/weather_clean"
)

GOLD_PATH = os.getenv(
    "GOLD_PATH",
    "/opt/project/data/benchmark/wm10m/gold/weather_window_1h"
)

CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "/opt/project/data/checkpoints/benchmark_wm10m_gold"
)

RUN_ID = os.getenv("RUN_ID")

WATERMARK_DELAY = os.getenv(
    "WATERMARK_DELAY",
    "10 minutes"
)

WINDOW_DURATION = os.getenv(
    "WINDOW_DURATION",
    "1 hour"
)

MAX_FILES_PER_TRIGGER = int(
    os.getenv(
        "MAX_FILES_PER_TRIGGER",
        "1"
    )
)

STARTING_VERSION = os.getenv(
    "STARTING_VERSION",
    "0"
)


spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .config(
        "spark.sql.session.timeZone",
        "UTC"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("========== GOLD WATERMARK CONFIG ==========")
print(f"Silver Path          : {SILVER_PATH}")
print(f"Gold Path            : {GOLD_PATH}")
print(f"Checkpoint           : {CHECKPOINT_PATH}")
print(f"Run ID               : {RUN_ID}")
print(f"Watermark            : {WATERMARK_DELAY}")
print(f"Window               : {WINDOW_DURATION}")
print(f"Max Files / Trigger  : {MAX_FILES_PER_TRIGGER}")
print(f"Starting Version     : {STARTING_VERSION}")
print("===========================================")


silver_reader = (
    spark.readStream
    .format("delta")
    .option(
        "startingVersion",
        STARTING_VERSION
    )
    .option(
        "maxFilesPerTrigger",
        MAX_FILES_PER_TRIGGER
    )
)


silver = silver_reader.load(
    SILVER_PATH
)


if RUN_ID:
    silver = silver.filter(
        F.col("simulation_run_id") == RUN_ID
    )


watermarked = (
    silver
    .withWatermark(
        "event_time",
        WATERMARK_DELAY
    )
)


gold = (
    watermarked
    .groupBy(
        F.window(
            F.col("event_time"),
            WINDOW_DURATION
        ).alias("event_window"),

        F.col("location_id"),
        F.col("simulation_run_id"),
        F.col("simulation_fault")
    )
    .agg(
        F.count("*").alias(
            "observation_count"
        ),

        F.min(
            "event_time"
        ).alias("first_event_time"),

        F.max(
            "event_time"
        ).alias("last_event_time"),

        F.avg(
            "temperature_c"
        ).alias("avg_temperature_c"),

        F.min(
            "temperature_c"
        ).alias("min_temperature_c"),

        F.max(
            "temperature_c"
        ).alias("max_temperature_c"),

        F.avg(
            "humidity_pct"
        ).alias("avg_humidity_pct"),

        F.sum(
            "precipitation_mm"
        ).alias("total_precipitation_mm"),

        F.avg(
            "pressure_hpa"
        ).alias("avg_pressure_hpa"),

        F.avg(
            "wind_speed_kmh"
        ).alias("avg_wind_speed_kmh"),

        F.max(
            "wind_gust_kmh"
        ).alias("max_wind_gust_kmh")
    )
    .select(
        F.col(
            "event_window.start"
        ).alias("window_start"),

        F.col(
            "event_window.end"
        ).alias("window_end"),

        "location_id",
        "simulation_run_id",
        "simulation_fault",
        "observation_count",
        "first_event_time",
        "last_event_time",
        "avg_temperature_c",
        "min_temperature_c",
        "max_temperature_c",
        "avg_humidity_pct",
        "total_precipitation_mm",
        "avg_pressure_hpa",
        "avg_wind_speed_kmh",
        "max_wind_gust_kmh"
    )
)


def upsert_gold(batch_df, batch_id):
    if batch_df.isEmpty():
        print(
            f"[GOLD] batch={batch_id} empty"
        )
        return

    output = (
        batch_df
        .withColumn(
            "gold_batch_id",
            F.lit(batch_id)
        )
        .withColumn(
            "gold_updated_at",
            F.current_timestamp()
        )
    )

    output.persist()

    try:
        row_count = output.count()

        print(
            f"[GOLD] batch={batch_id} "
            f"rows={row_count}"
        )

        if not DeltaTable.isDeltaTable(
            spark,
            GOLD_PATH
        ):
            (
                output.write
                .format("delta")
                .mode("append")
                .save(GOLD_PATH)
            )

            print(
                "[GOLD] Delta table created"
            )

        else:
            target = DeltaTable.forPath(
                spark,
                GOLD_PATH
            )

            (
                target.alias("t")
                .merge(
                    output.alias("s"),
                    """
                    t.window_start = s.window_start
                    AND t.window_end = s.window_end
                    AND t.location_id = s.location_id
                    AND t.simulation_run_id = s.simulation_run_id
                    AND t.simulation_fault = s.simulation_fault
                    """
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

    finally:
        output.unpersist()


query = (
    gold.writeStream
    .outputMode("update")
    .foreachBatch(upsert_gold)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .trigger(
        availableNow=True
    )
    .start()
)


print(
    "[GOLD] Watermark benchmark started"
)

query.awaitTermination()

print(
    "[GOLD] Watermark benchmark completed"
)

spark.stop()
