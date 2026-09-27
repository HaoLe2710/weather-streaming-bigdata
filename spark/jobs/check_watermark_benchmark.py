import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "/opt/project/data/benchmark/wm10m/bronze/weather_raw"
)

SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "/opt/project/data/benchmark/wm10m/silver/weather_clean"
)

DLQ_PATH = os.getenv(
    "DLQ_PATH",
    "/opt/project/data/benchmark/wm10m/silver/weather_invalid"
)


spark = (
    SparkSession.builder
    .appName(
        "CheckWatermarkBenchmark"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


bronze = (
    spark.read
    .format("delta")
    .load(BRONZE_PATH)
)


silver = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)


# =====================================
# Parse benchmark metadata from Bronze
# =====================================

bronze_parsed = (
    bronze
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


# =====================================
# TOTAL COUNTS
# =====================================

bronze_count = (
    bronze.count()
)

silver_count = (
    silver.count()
)


print(
    "\n========== WATERMARK BENCHMARK =========="
)

print(
    f"Bronze records : {bronze_count:,}"
)

print(
    f"Silver records : {silver_count:,}"
)

print(
    f"Dropped before Silver : "
    f"{bronze_count - silver_count:,}"
)


# =====================================
# BRONZE FAULT DISTRIBUTION
# =====================================

print(
    "\n========== BRONZE FAULTS =========="
)

bronze_faults = (
    bronze_parsed
    .groupBy(
        "simulation_fault"
    )
    .count()
)

bronze_faults.show(
    truncate=False
)


# =====================================
# SILVER FAULT DISTRIBUTION
# =====================================

print(
    "\n========== SILVER FAULTS =========="
)

silver_faults = (
    silver
    .groupBy(
        "simulation_fault"
    )
    .count()
)

silver_faults.show(
    truncate=False
)


# =====================================
# LATE EVENT ANALYSIS
# =====================================

bronze_late = (
    bronze_parsed
    .filter(
        F.col(
            "simulation_fault"
        ) == "LATE"
    )
    .count()
)

silver_late = (
    silver
    .filter(
        F.col(
            "simulation_fault"
        ) == "LATE"
    )
    .count()
)


late_dropped = (
    bronze_late
    - silver_late
)


late_drop_rate = (
    (
        late_dropped
        / bronze_late
    )
    * 100
    if bronze_late > 0
    else 0
)


print(
    "\n========== LATE EVENT RESULT =========="
)

print(
    f"Late generated : {bronze_late:,}"
)

print(
    f"Late accepted  : {silver_late:,}"
)

print(
    f"Late dropped   : {late_dropped:,}"
)

print(
    f"Late drop rate : "
    f"{late_drop_rate:.2f}%"
)


# =====================================
# NORMAL EVENT ANALYSIS
# =====================================

bronze_normal = (
    bronze_parsed
    .filter(
        F.col(
            "simulation_fault"
        ) == "NORMAL"
    )
    .count()
)

silver_normal = (
    silver
    .filter(
        F.col(
            "simulation_fault"
        ) == "NORMAL"
    )
    .count()
)


print(
    "\n========== NORMAL EVENT RESULT =========="
)

print(
    f"Normal generated : "
    f"{bronze_normal:,}"
)

print(
    f"Normal accepted  : "
    f"{silver_normal:,}"
)

print(
    f"Normal dropped   : "
    f"{bronze_normal - silver_normal:,}"
)


spark.stop()