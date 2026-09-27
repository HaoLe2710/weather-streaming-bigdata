from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

from benchmark_config import BenchmarkConfig, SCALABILITY_BENCHMARK, THROUGHPUT_BASELINE
from streaming_metrics import make_listener_from_environment, wait_for_stop_signal


BENCHMARK_CONFIG = BenchmarkConfig.from_environment()


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

AVAILABLE_NOW = (
    os.getenv("AVAILABLE_NOW", "false").strip().lower()
    in {"1", "true", "yes"}
)

APP_NAME = os.getenv(
    "APP_NAME",
    "WeatherBronzeStreaming"
)

TRIGGER_INTERVAL = os.getenv(
    "TRIGGER_INTERVAL"
)

BENCHMARK_STOP_SIGNAL = os.getenv("BENCHMARK_STOP_SIGNAL")
QUERY_NAME = os.getenv("QUERY_NAME")

if BENCHMARK_CONFIG is not None:
    BRONZE_PATH = BENCHMARK_CONFIG.paths.bronze
    CHECKPOINT_PATH = BENCHMARK_CONFIG.paths.bronze_checkpoint
    KAFKA_TOPIC = BENCHMARK_CONFIG.topic
    STARTING_OFFSETS = "earliest"

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

if BENCHMARK_CONFIG is not None and BENCHMARK_CONFIG.scenario in {
    THROUGHPUT_BASELINE,
    SCALABILITY_BENCHMARK,
}:
    spark.streams.addListener(make_listener_from_environment("bronze"))


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

if AVAILABLE_NOW:
    writer = writer.trigger(availableNow=True)
elif TRIGGER_INTERVAL:
    writer = writer.trigger(
        processingTime=TRIGGER_INTERVAL
    )

if QUERY_NAME:
    writer = writer.queryName(QUERY_NAME)

query = writer.start(
    BRONZE_PATH
)


print(
    f"Weather Bronze Streaming started "
    f"[topic={KAFKA_TOPIC}]"
)

if BENCHMARK_CONFIG is not None and BENCHMARK_CONFIG.scenario in {
    THROUGHPUT_BASELINE,
    SCALABILITY_BENCHMARK,
}:
    if not BENCHMARK_STOP_SIGNAL:
        raise ValueError("BENCHMARK_STOP_SIGNAL is required for throughput runs.")
    wait_for_stop_signal([query], BENCHMARK_STOP_SIGNAL)
    print("Weather Bronze throughput run drained and stopped")
else:
    query.awaitTermination()
    if AVAILABLE_NOW:
        print("Weather Bronze availableNow run completed")

spark.stop()
