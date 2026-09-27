"""Read run-scoped Delta outputs and summarize Bronze replay latency."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from benchmark_config import THROUGHPUT_BASELINE, BenchmarkConfig, write_json
from weather_schema import weather_schema


config = BenchmarkConfig.from_environment(
    required=True,
    expected_scenario=THROUGHPUT_BASELINE,
)
result_path = os.environ["RESULT_PATH"]

spark = (
    SparkSession.builder
    .appName(f"CheckThroughputMetrics-{config.run_id}")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.format("delta").load(config.paths.bronze)
silver = spark.read.format("delta").load(config.paths.silver)
dlq = spark.read.format("delta").load(config.paths.dlq)

parsed = (
    bronze
    .withColumn("data", F.from_json(F.col("payload"), weather_schema))
    .withColumn(
        "replay_ingestion_time",
        F.try_to_timestamp(F.col("data.ingestion_time")),
    )
    .filter(
        F.col("spark_processing_time").isNotNull()
        & F.col("replay_ingestion_time").isNotNull()
    )
    .withColumn(
        "replay_to_bronze_processing_ms",
        (
            F.expr("unix_micros(spark_processing_time)")
            - F.expr("unix_micros(replay_ingestion_time)")
        )
        / F.lit(1000.0),
    )
)

latency = parsed.agg(
    F.count("replay_to_bronze_processing_ms").alias("latency_count"),
    F.min("replay_to_bronze_processing_ms").alias("latency_min_ms"),
    F.avg("replay_to_bronze_processing_ms").alias("latency_avg_ms"),
    F.expr(
        "percentile_approx(replay_to_bronze_processing_ms, 0.50, 10000)"
    ).alias("latency_p50_ms"),
    F.expr(
        "percentile_approx(replay_to_bronze_processing_ms, 0.95, 10000)"
    ).alias("latency_p95_ms"),
    F.expr(
        "percentile_approx(replay_to_bronze_processing_ms, 0.99, 10000)"
    ).alias("latency_p99_ms"),
    F.max("replay_to_bronze_processing_ms").alias("latency_max_ms"),
).first()

bronze_count = bronze.count()
silver_count = silver.count()
dlq_count = dlq.count()
latency_count = int(latency["latency_count"])

result = {
    "run_id": config.run_id,
    "scenario": config.scenario,
    "bronze_records": bronze_count,
    "silver_records": silver_count,
    "dlq_records": dlq_count,
    "latency_count": latency_count,
    "latency_missing_records": bronze_count - latency_count,
    "latency_min_ms": latency["latency_min_ms"],
    "latency_avg_ms": latency["latency_avg_ms"],
    "latency_p50_ms": latency["latency_p50_ms"],
    "latency_p95_ms": latency["latency_p95_ms"],
    "latency_p99_ms": latency["latency_p99_ms"],
    "latency_max_ms": latency["latency_max_ms"],
    "latency_definition": (
        "Bronze spark_processing_time minus simulator ingestion_time, "
        "measured for every Bronze message after the stream drains. "
        "Historical event_time is not used."
    ),
    "latency_percentile_method": "Spark percentile_approx, accuracy=10000",
}

write_json(result_path, result)
print(result)
spark.stop()
