from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SILVER_PATH = "/opt/project/data/silver/weather_clean"
INVALID_PATH = "/opt/project/data/silver/weather_invalid"


spark = (
    SparkSession.builder
    .appName("CheckSilverWeather")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==============================
# CHECK VALID SILVER DATA
# ==============================

silver = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)


print("\n========== SILVER SCHEMA ==========")
silver.printSchema()


print("\n========== SILVER DATA ==========")

silver.select(
    "event_id",
    "location_id",
    "city",
    "event_time",
    "ingestion_time",
    "temperature_c",
    "humidity_pct",
    "precipitation_mm",
    "pressure_hpa",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "source",
).orderBy(
    F.col("event_time").desc()
).show(
    50,
    truncate=False
)


silver_count = silver.count()

print(
    f"\nTotal Silver Records: {silver_count}"
)


# ==============================
# CHECK DUPLICATES
# ==============================

duplicates = (
    silver
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
)


duplicate_count = duplicates.count()

print(
    f"Duplicate event_id groups: "
    f"{duplicate_count}"
)

if duplicate_count > 0:
    print("\n========== DUPLICATES ==========")

    duplicates.show(
        50,
        truncate=False
    )


# ==============================
# BASIC DATA QUALITY CHECK
# ==============================

quality = silver.select(
    F.count("*").alias("total"),

    F.sum(
        F.col("event_time").isNull().cast("int")
    ).alias("null_event_time"),

    F.sum(
        F.col("temperature_c").isNull().cast("int")
    ).alias("null_temperature"),

    F.sum(
        F.col("humidity_pct").isNull().cast("int")
    ).alias("null_humidity"),

    F.min("temperature_c").alias("min_temperature"),

    F.max("temperature_c").alias("max_temperature"),

    F.min("humidity_pct").alias("min_humidity"),

    F.max("humidity_pct").alias("max_humidity"),
)


print("\n========== DATA QUALITY ==========")

quality.show(
    truncate=False
)


# ==============================
# CHECK INVALID / DLQ DATA
# ==============================

try:
    invalid = (
        spark.read
        .format("delta")
        .load(INVALID_PATH)
    )

    print("\n========== INVALID / DLQ ==========")

    print(
        f"Total Invalid Records: "
        f"{invalid.count()}"
    )

    invalid.select(
        "event_id",
        "location_id",
        "city",
        "event_time",
        "temperature_c",
        "humidity_pct",
        "precipitation_mm",
    ).show(
        50,
        truncate=False
    )

except Exception:
    print(
        "\nNo Invalid/DLQ Delta table found yet."
    )


spark.stop()