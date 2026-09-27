from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)
from pyspark.sql import functions as F


def weather_valid_condition():
    """Return the shared core weather-record validation predicate.

    Keep this predicate aligned with the live Silver quality contract so
    benchmark jobs do not accept records that production would reject.
    """
    return (
        F.col("event_id").isNotNull()
        & F.col("location_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("temperature_c").between(-90.0, 60.0)
        & F.col("humidity_pct").between(0.0, 100.0)
        & F.col("precipitation_mm").isNotNull()
        & (F.col("precipitation_mm") >= 0)
        & F.col("latitude").between(-90.0, 90.0)
        & F.col("longitude").between(-180.0, 180.0)
    )


weather_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), True),

    StructField("location_id", StringType(), False),
    StructField("city", StringType(), True),

    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),

    StructField("event_time", StringType(), False),
    StructField("ingestion_time", StringType(), True),

    StructField("temperature_c", DoubleType(), True),
    StructField("humidity_pct", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
    StructField("pressure_hpa", DoubleType(), True),

    StructField("wind_speed_kmh", DoubleType(), True),
    StructField("wind_gust_kmh", DoubleType(), True),

    StructField("weather_code", DoubleType(), True),

    StructField("source", StringType(), True),
    StructField(
        "simulation_run_id",
        StringType(),
        True
    ),

    StructField(
        "simulation_fault",
        StringType(),
        True
    ),
])
