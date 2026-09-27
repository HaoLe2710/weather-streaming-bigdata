"""Create an empty run-scoped Bronze Delta table before starting Silver."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from benchmark_config import THROUGHPUT_BASELINE, BenchmarkConfig


config = BenchmarkConfig.from_environment(
    required=True,
    expected_scenario=THROUGHPUT_BASELINE,
)

spark = (
    SparkSession.builder
    .appName(os.getenv("APP_NAME", f"InitThroughputBronze-{config.run_id}"))
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bronze_schema = StructType([
    StructField("message_key", StringType(), True),
    StructField("payload", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("spark_processing_time", TimestampType(), True),
])

empty_bronze = spark.createDataFrame([], bronze_schema)
empty_bronze.write.format("delta").mode("errorifexists").save(config.paths.bronze)
print(f"Initialized empty run-scoped Bronze Delta table: {config.paths.bronze}")
spark.stop()
