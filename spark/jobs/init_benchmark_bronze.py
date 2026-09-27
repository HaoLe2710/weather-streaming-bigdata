import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)


BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "/opt/project/data/benchmark/wm10m/bronze/weather_raw"
)


spark = (
    SparkSession.builder
    .appName("InitBenchmarkBronze")
    .getOrCreate()
)


schema = StructType([
    StructField(
        "message_key",
        StringType(),
        True
    ),

    StructField(
        "payload",
        StringType(),
        True
    ),

    StructField(
        "topic",
        StringType(),
        True
    ),

    StructField(
        "partition",
        IntegerType(),
        True
    ),

    StructField(
        "offset",
        LongType(),
        True
    ),

    StructField(
        "kafka_timestamp",
        TimestampType(),
        True
    ),

    StructField(
        "spark_processing_time",
        TimestampType(),
        True
    ),
])


empty_df = (
    spark.createDataFrame(
        [],
        schema
    )
)


(
    empty_df.write
    .format("delta")
    .mode("overwrite")
    .save(BRONZE_PATH)
)


print(
    f"Initialized Bronze Delta table: "
    f"{BRONZE_PATH}"
)


spark.stop()