from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

from benchmark_config import BenchmarkConfig
from weather_schema import weather_schema, weather_valid_condition


BENCHMARK_CONFIG = BenchmarkConfig.from_environment()


APP_NAME = os.getenv(
    "APP_NAME",
    "WeatherSilverStreaming"
)

BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "/opt/project/data/bronze/weather_raw"
)

SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "/opt/project/data/silver/weather_clean"
)

SILVER_CHECKPOINT = os.getenv(
    "SILVER_CHECKPOINT",
    "/opt/project/data/checkpoints/silver_weather_clean"
)

DLQ_PATH = os.getenv(
    "DLQ_PATH",
    "/opt/project/data/silver/weather_invalid"
)

DLQ_CHECKPOINT = os.getenv(
    "DLQ_CHECKPOINT",
    "/opt/project/data/checkpoints/weather_invalid"
)

WATERMARK_DELAY = os.getenv(
    "WATERMARK_DELAY",
    "10 minutes"
)

TRIGGER_INTERVAL = os.getenv(
    "TRIGGER_INTERVAL"
)

AVAILABLE_NOW = (
    os.getenv("AVAILABLE_NOW", "false").strip().lower()
    in {"1", "true", "yes"}
)

if BENCHMARK_CONFIG is not None:
    BRONZE_PATH = BENCHMARK_CONFIG.paths.bronze
    SILVER_PATH = BENCHMARK_CONFIG.paths.silver
    SILVER_CHECKPOINT = BENCHMARK_CONFIG.paths.silver_checkpoint
    DLQ_PATH = BENCHMARK_CONFIG.paths.dlq
    DLQ_CHECKPOINT = BENCHMARK_CONFIG.paths.dlq_checkpoint

def apply_trigger(writer):

    if AVAILABLE_NOW:
        return writer.trigger(availableNow=True)

    if TRIGGER_INTERVAL:
        return writer.trigger(
            processingTime=TRIGGER_INTERVAL
        )

    return writer

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


# ==============================
# READ BRONZE STREAM
# ==============================

bronze = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)


# ==============================
# PARSE JSON
# ==============================

parsed = (
    bronze
    .withColumn(
        "data",
        F.from_json(
            F.col("payload"),
            weather_schema
        )
    )
)


weather = (
    parsed
    .select(
        "message_key",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "spark_processing_time",

        F.col("data.*")
    )
)


# ==============================
# EVENT TIME
# ==============================

weather = (
    weather
    .withColumn(
        "event_time",
        F.coalesce(
            F.try_to_timestamp(
                F.col("event_time"),
                F.lit("yyyy-MM-dd'T'HH:mm:ssX")
            ),
            F.try_to_timestamp(
                F.col("event_time"),
                F.lit("yyyy-MM-dd'T'HH:mmX")
            )
        )
    )
    .withColumn(
        "ingestion_time",
        F.try_to_timestamp(
            F.col("ingestion_time")
        )
    )
)


# ==============================
# DATA QUALITY FLAGS
# ==============================

validated = (
    weather
    .withColumn(
        "is_valid",
        weather_valid_condition()
    )
)


valid_events = (
    validated
    .filter(F.col("is_valid") == True)
)


invalid_events = (
    validated
    .filter(
        F.col("is_valid").isNull()
        |
        (F.col("is_valid") == False)
    )
)


# ==============================
# EVENT TIME + WATERMARK
# ==============================

silver = (
    valid_events

    .withWatermark(
        "event_time",
        WATERMARK_DELAY
    )

    .dropDuplicatesWithinWatermark([
        "event_id"
    ])

    .drop(
        "is_valid"
    )
)


# ==============================
# WRITE SILVER
# ==============================

silver_writer = (
    silver.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        SILVER_CHECKPOINT
    )
)

silver_query = (
    apply_trigger(
        silver_writer
    )
    .start(
        SILVER_PATH
    )
)


# ==============================
# WRITE INVALID / DLQ
# ==============================

invalid_writer = (
    invalid_events.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        DLQ_CHECKPOINT
    )
)

invalid_query = (
    apply_trigger(
        invalid_writer
    )
    .start(
        DLQ_PATH
    )
)


import time


print("Weather Silver Streaming started")

print(
    "Silver Query:",
    silver_query.id,
    silver_query.name,
    silver_query.isActive
)

print(
    "Invalid Query:",
    invalid_query.id,
    invalid_query.name,
    invalid_query.isActive
)


if AVAILABLE_NOW:
    silver_query.awaitTermination()
    invalid_query.awaitTermination()
    print("Weather Silver availableNow run completed")
    spark.stop()
else:
    while True:
        if not silver_query.isActive:
            print("ERROR: Silver query stopped")
            print(
                "Silver exception:",
                silver_query.exception()
            )
            break

        if not invalid_query.isActive:
            print("ERROR: Invalid/DLQ query stopped")
            print(
                "Invalid exception:",
                invalid_query.exception()
            )
            break

        print(
            "STATUS | "
            f"silver={silver_query.isActive} | "
            f"invalid={invalid_query.isActive}"
        )

        time.sleep(5)
