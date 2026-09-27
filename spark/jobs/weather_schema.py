from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
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