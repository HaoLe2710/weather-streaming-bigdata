from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

from benchmark_config import BenchmarkConfig


BENCHMARK_CONFIG = BenchmarkConfig.from_environment()


PATH = os.getenv(
    "BRONZE_PATH",
    "/opt/project/data/benchmark/bronze/weather_raw"
)

if BENCHMARK_CONFIG is not None:
    PATH = BENCHMARK_CONFIG.paths.bronze


spark = (
    SparkSession.builder
    .appName("CheckBenchmarkBronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


df = (
    spark.read
    .format("delta")
    .load(PATH)
)


print(
    "\n========== BENCHMARK BRONZE =========="
)


total = df.count()

print(
    f"Total Bronze Records: {total:,}"
)


parsed = (
    df
    .withColumn(
        "simulation_fault",
        F.get_json_object(
            "payload",
            "$.simulation_fault"
        )
    )
    .withColumn(
        "simulation_run_id",
        F.get_json_object(
            "payload",
            "$.simulation_run_id"
        )
    )
)


print(
    "\n========== FAULT DISTRIBUTION =========="
)

(
    parsed
    .groupBy(
        "simulation_fault"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
    .show(
        truncate=False
    )
)


print(
    "\n========== RUN DISTRIBUTION =========="
)

(
    parsed
    .groupBy(
        "simulation_run_id"
    )
    .count()
    .show(
        truncate=False
    )
)


print(
    "\n========== KAFKA PARTITIONS =========="
)

(
    df
    .groupBy(
        "partition"
    )
    .count()
    .orderBy(
        "partition"
    )
    .show()
)


spark.stop()
