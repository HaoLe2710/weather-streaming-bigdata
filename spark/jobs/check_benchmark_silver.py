from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BRONZE_PATH = (
    "/opt/project/data/benchmark/"
    "bronze/weather_raw"
)

SILVER_PATH = (
    "/opt/project/data/benchmark/"
    "silver/weather_clean"
)

DLQ_PATH = (
    "/opt/project/data/benchmark/"
    "silver/weather_invalid"
)


spark = (
    SparkSession.builder
    .appName(
        "CheckBenchmarkSilver"
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

dlq = (
    spark.read
    .format("delta")
    .load(DLQ_PATH)
)


bronze_count = bronze.count()
silver_count = silver.count()
dlq_count = dlq.count()


print(
    "\n========== BENCHMARK COUNTS =========="
)

print(
    f"Bronze Records : {bronze_count:,}"
)

print(
    f"Silver Records : {silver_count:,}"
)

print(
    f"DLQ Records    : {dlq_count:,}"
)

print(
    f"Not in Silver/DLQ : "
    f"{bronze_count - silver_count - dlq_count:,}"
)


print(
    "\n========== SILVER FAULT DISTRIBUTION =========="
)

(
    silver
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
    "\n========== DLQ FAULT DISTRIBUTION =========="
)

(
    dlq
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
    "\n========== DUPLICATE CHECK =========="
)

duplicates = (
    silver
    .groupBy(
        "event_id"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)

duplicate_groups = (
    duplicates.count()
)

print(
    f"Duplicate event_id groups: "
    f"{duplicate_groups}"
)


print(
    "\n========== DATA QUALITY =========="
)

(
    silver
    .agg(

        F.count("*")
        .alias("total"),

        F.sum(
            F.col(
                "event_time"
            ).isNull().cast("int")
        ).alias(
            "null_event_time"
        ),

        F.sum(
            (
                (
                    F.col(
                        "temperature_c"
                    ) < -90
                )
                |
                (
                    F.col(
                        "temperature_c"
                    ) > 60
                )
            ).cast("int")
        ).alias(
            "invalid_temperature"
        ),

        F.sum(
            (
                (
                    F.col(
                        "humidity_pct"
                    ) < 0
                )
                |
                (
                    F.col(
                        "humidity_pct"
                    ) > 100
                )
            ).cast("int")
        ).alias(
            "invalid_humidity"
        ),

        F.sum(
            (
                F.col(
                    "precipitation_mm"
                ) < 0
            ).cast("int")
        ).alias(
            "invalid_precipitation"
        ),
    )

    .show(
        truncate=False
    )
)


spark.stop()