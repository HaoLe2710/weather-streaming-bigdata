import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from weather_schema import weather_schema


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker:19092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "weather.replay.wm10m"
)

GOLD_PATH = os.getenv(
    "GOLD_PATH",
    "/opt/project/data/benchmark/wm10m/gold_kafka/weather_window_1h"
)

CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "/opt/project/data/checkpoints/benchmark_wm10m_gold_kafka"
)

RUN_ID = os.getenv(
    "RUN_ID"
)

WATERMARK_DELAY = os.getenv(
    "WATERMARK_DELAY",
    "10 minutes"
)

WINDOW_DURATION = os.getenv(
    "WINDOW_DURATION",
    "1 hour"
)

MAX_OFFSETS_PER_TRIGGER = os.getenv(
    "MAX_OFFSETS_PER_TRIGGER",
    "500"
)


spark = (
    SparkSession.builder
    .appName("WeatherGoldKafkaWatermarkBenchmark")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .option(
        "maxOffsetsPerTrigger",
        MAX_OFFSETS_PER_TRIGGER
    )
    .load()
)


parsed = (
    raw
    .select(
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("payload")
    )
    .withColumn(
        "data",
        F.from_json(
            F.col("payload"),
            weather_schema
        )
    )
    .select(
        "partition",
        "offset",
        "kafka_timestamp",
        "data.*"
    )
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
)


if RUN_ID:
    parsed = parsed.filter(
        F.col("simulation_run_id") == RUN_ID
    )


valid = (
    parsed
    .filter(
        F.col("event_id").isNotNull()
        & F.col("location_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("temperature_c").between(-90, 60)
        & F.col("humidity_pct").between(0, 100)
        & (
            F.col("precipitation_mm").isNotNull()
            & (F.col("precipitation_mm") >= 0)
        )
    )
)


watermarked = (
    valid
    .withWatermark(
        "event_time",
        WATERMARK_DELAY
    )
)


gold = (
    watermarked
    .groupBy(
        F.window(
            "event_time",
            WINDOW_DURATION
        ).alias("event_window"),

        "location_id",
        "simulation_run_id",
        "simulation_fault"
    )
    .agg(
        F.count("*").alias("observation_count"),

        F.min("event_time").alias(
            "first_event_time"
        ),

        F.max("event_time").alias(
            "last_event_time"
        )
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
        "last_event_time"
    )
)


def write_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(
            f"[GOLD-KAFKA] batch={batch_id} empty"
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
        print(
            f"[GOLD-KAFKA] batch={batch_id} "
            f"rows={output.count()}"
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
    .foreachBatch(write_batch)
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
    "[GOLD-KAFKA] Benchmark started"
)

query.awaitTermination()

print(
    "[GOLD-KAFKA] Benchmark completed"
)

spark.stop()