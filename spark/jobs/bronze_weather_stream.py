from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker:19092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "weather.raw"
)

BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "/opt/project/data/bronze/weather_raw"
)

CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "/opt/project/data/checkpoints/bronze_weather_raw"
)

STARTING_OFFSETS = os.getenv(
    "STARTING_OFFSETS",
    "latest"
)

APP_NAME = os.getenv(
    "APP_NAME",
    "WeatherBronzeStreaming"
)

TRIGGER_INTERVAL = os.getenv(
    "TRIGGER_INTERVAL"
)

print("========== BRONZE CONFIG ==========")
print(f"App Name        : {APP_NAME}")
print(f"Kafka Bootstrap : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Kafka Topic     : {KAFKA_TOPIC}")
print(f"Starting Offset : {STARTING_OFFSETS}")
print(f"Bronze Path     : {BRONZE_PATH}")
print(f"Checkpoint Path : {CHECKPOINT_PATH}")
print("===================================")

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


kafka_stream = (
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
        STARTING_OFFSETS
    )
    .load()
)

bronze = kafka_stream.select(
    F.col("key")
    .cast("string")
    .alias("message_key"),

    F.col("value")
    .cast("string")
    .alias("payload"),

    F.col("topic"),

    F.col("partition"),

    F.col("offset"),

    F.col("timestamp")
    .alias("kafka_timestamp"),

    F.current_timestamp()
    .alias("spark_processing_time"),
)


writer = (
    bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
)

if TRIGGER_INTERVAL:
    writer = writer.trigger(
        processingTime=TRIGGER_INTERVAL
    )

query = writer.start(
    BRONZE_PATH
)


print(
    f"Weather Bronze Streaming started "
    f"[topic={KAFKA_TOPIC}]"
)

query.awaitTermination()