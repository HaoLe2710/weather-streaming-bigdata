import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "/opt/project/data/benchmark/wm10m/silver/weather_clean"
)

GOLD_PATH = os.getenv(
    "GOLD_PATH",
    "/opt/project/data/benchmark/wm10m/gold/weather_window_1h"
)

RUN_ID = os.getenv("RUN_ID")


spark = (
    SparkSession.builder
    .appName("CheckGoldWatermarkBenchmark")
    .config(
        "spark.sql.session.timeZone",
        "UTC"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


silver = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)


gold = (
    spark.read
    .format("delta")
    .load(GOLD_PATH)
)


if RUN_ID:
    silver = silver.filter(
        F.col("simulation_run_id") == RUN_ID
    )

    gold = gold.filter(
        F.col("simulation_run_id") == RUN_ID
    )


def silver_fault_count(name):
    return (
        silver
        .filter(
            F.col("simulation_fault") == name
        )
        .count()
    )


def gold_fault_count(name):
    row = (
        gold
        .filter(
            F.col("simulation_fault") == name
        )
        .agg(
            F.coalesce(
                F.sum("observation_count"),
                F.lit(0)
            ).alias("accepted")
        )
        .first()
    )

    return int(row["accepted"])


silver_total = silver.count()

gold_total = (
    gold
    .agg(
        F.coalesce(
            F.sum("observation_count"),
            F.lit(0)
        ).alias("accepted")
    )
    .first()["accepted"]
)

gold_total = int(gold_total)


late_generated = silver_fault_count(
    "LATE"
)

late_accepted = gold_fault_count(
    "LATE"
)

late_dropped = (
    late_generated
    - late_accepted
)

late_drop_rate = (
    late_dropped
    / late_generated
    * 100
    if late_generated > 0
    else 0
)


normal_generated = silver_fault_count(
    "NORMAL"
)

normal_accepted = gold_fault_count(
    "NORMAL"
)

normal_dropped = (
    normal_generated
    - normal_accepted
)


print(
    "\n========== GOLD WATERMARK BENCHMARK =========="
)

print(
    f"Silver input observations : "
    f"{silver_total:,}"
)

print(
    f"Gold accepted observations: "
    f"{gold_total:,}"
)

print(
    f"Total dropped             : "
    f"{silver_total - gold_total:,}"
)


print(
    "\n========== LATE EVENT RESULT =========="
)

print(
    f"Late generated : "
    f"{late_generated:,}"
)

print(
    f"Late accepted  : "
    f"{late_accepted:,}"
)

print(
    f"Late dropped   : "
    f"{late_dropped:,}"
)

print(
    f"Late drop rate : "
    f"{late_drop_rate:.2f}%"
)


print(
    "\n========== NORMAL EVENT RESULT =========="
)

print(
    f"Normal generated : "
    f"{normal_generated:,}"
)

print(
    f"Normal accepted  : "
    f"{normal_accepted:,}"
)

print(
    f"Normal dropped   : "
    f"{normal_dropped:,}"
)


print(
    "\n========== GOLD FAULT DISTRIBUTION =========="
)

(
    gold
    .groupBy(
        "simulation_fault"
    )
    .agg(
        F.sum(
            "observation_count"
        ).alias(
            "accepted_observations"
        ),

        F.count("*").alias(
            "window_groups"
        )
    )
    .orderBy(
        "simulation_fault"
    )
    .show(
        truncate=False
    )
)


print(
    "\n========== DUPLICATE GOLD KEYS =========="
)

duplicate_keys = (
    gold
    .groupBy(
        "window_start",
        "window_end",
        "location_id",
        "simulation_run_id",
        "simulation_fault"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

print(
    f"Duplicate Gold keys: "
    f"{duplicate_keys}"
)


spark.stop()