import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "/opt/project/data/benchmark/wm10m/bronze/weather_raw"
)

RUN_ID = os.getenv("RUN_ID")


spark = (
    SparkSession.builder
    .appName("CheckReplayEventLag")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


bronze = (
    spark.read
    .format("delta")
    .load(BRONZE_PATH)
)


events = (
    bronze
    .select(
        "partition",
        "offset",
        "kafka_timestamp",

        F.get_json_object(
            "payload",
            "$.event_id"
        ).alias("event_id"),

        F.get_json_object(
            "payload",
            "$.location_id"
        ).alias("location_id"),

        F.get_json_object(
            "payload",
            "$.event_time"
        ).alias("event_time_raw"),

        F.get_json_object(
            "payload",
            "$.ingestion_time"
        ).alias("ingestion_time_raw"),

        F.get_json_object(
            "payload",
            "$.simulation_fault"
        ).alias("simulation_fault"),

        F.get_json_object(
            "payload",
            "$.simulation_run_id"
        ).alias("simulation_run_id")
    )
    .withColumn(
        "event_time",
        F.to_timestamp("event_time_raw")
    )
    .withColumn(
        "ingestion_time",
        F.to_timestamp("ingestion_time_raw")
    )
    .drop(
        "event_time_raw",
        "ingestion_time_raw"
    )
)


if RUN_ID:
    events = events.filter(
        F.col("simulation_run_id") == RUN_ID
    )


arrival_window = (
    Window
    .orderBy(
        F.col("ingestion_time").asc_nulls_last(),
        F.col("kafka_timestamp").asc(),
        F.col("partition").asc(),
        F.col("offset").asc()
    )
    .rowsBetween(
        Window.unboundedPreceding,
        -1
    )
)


events = (
    events
    .withColumn(
        "max_event_time_before",
        F.max("event_time").over(arrival_window)
    )
    .withColumn(
        "arrival_lag_minutes",
        (
            F.col("max_event_time_before").cast("long")
            - F.col("event_time").cast("long")
        ) / 60.0
    )
    .withColumn(
        "window_end_1h",
        F.expr(
            "date_trunc('hour', event_time) "
            "+ INTERVAL 1 HOUR"
        )
    )
    .withColumn(
        "too_late_for_1h_wm10m",
        F.when(
            (
                F.col("max_event_time_before").isNotNull()
            )
            &
            (
                F.col("max_event_time_before")
                >
                F.expr(
                    "window_end_1h "
                    "+ INTERVAL 10 MINUTES"
                )
            ),
            1
        ).otherwise(0)
    )
)


late = events.filter(
    F.col("simulation_fault") == "LATE"
)


print("\n========== REPLAY EVENT-TIME LAG ==========")

late.agg(
    F.count("*").alias("late_records"),

    F.min(
        "arrival_lag_minutes"
    ).alias("min_lag_minutes"),

    F.avg(
        "arrival_lag_minutes"
    ).alias("avg_lag_minutes"),

    F.max(
        "arrival_lag_minutes"
    ).alias("max_lag_minutes"),

    F.expr(
        """
        percentile_approx(
            arrival_lag_minutes,
            array(0.50, 0.95, 0.99),
            10000
        )
        """
    ).alias("lag_p50_p95_p99"),

    F.sum(
        F.when(
            F.col("arrival_lag_minutes") > 10,
            1
        ).otherwise(0)
    ).alias("lag_gt_10m"),

    F.sum(
        F.when(
            F.col("arrival_lag_minutes") > 70,
            1
        ).otherwise(0)
    ).alias("lag_gt_70m"),

    F.sum(
        "too_late_for_1h_wm10m"
    ).alias("predicted_too_late")
).show(
    truncate=False
)


print("\n========== MOST DELAYED LATE EVENTS ==========")

(
    late
    .select(
        "event_id",
        "location_id",
        "event_time",
        "ingestion_time",
        "max_event_time_before",
        "arrival_lag_minutes",
        "window_end_1h",
        "too_late_for_1h_wm10m"
    )
    .orderBy(
        F.desc("arrival_lag_minutes")
    )
    .show(
        20,
        truncate=False
    )
)


spark.stop()