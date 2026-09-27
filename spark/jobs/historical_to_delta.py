from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SOURCE_PATH = (
    "/opt/project/history-data/"
    "historical/raw/*.jsonl.gz"
)

TARGET_PATH = (
    "/opt/project/data/"
    "historical/weather_hourly"
)


spark = (
    SparkSession.builder
    .appName(
        "HistoricalWeatherToDelta"
    )
    .config(
        "spark.sql.session.timeZone",
        "UTC"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "8"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print(
    "Reading historical JSONL..."
)


raw = (
    spark.read
    .json(SOURCE_PATH)
)


print(
    "Raw records:",
    raw.count()
)


weather = (
    raw

    .withColumn(
        "event_time",
        F.to_timestamp(
            "event_time"
        )
    )

    .withColumn(
        "year",
        F.year(
            "event_time"
        )
    )

    .dropDuplicates([
        "event_id"
    ])
)


valid = (
    weather.filter(

        F.col(
            "event_id"
        ).isNotNull()

        &

        F.col(
            "location_id"
        ).isNotNull()

        &

        F.col(
            "event_time"
        ).isNotNull()

        &

        F.col(
            "temperature_c"
        ).between(
            -90.0,
            60.0
        )

        &

        F.col(
            "humidity_pct"
        ).between(
            0.0,
            100.0
        )

        &

        (
            F.col(
                "precipitation_mm"
            ) >= 0
        )
    )
)


print(
    "Valid records:",
    valid.count()
)


print(
    "\nRecords by year:"
)

(
    valid
    .groupBy("year")
    .count()
    .orderBy("year")
    .show()
)


print(
    "\nRecords by location:"
)

(
    valid
    .groupBy(
        "location_id"
    )
    .count()
    .orderBy(
        F.desc("count")
    )
    .show(
        30,
        False
    )
)


(
    valid.write

    .format("delta")

    .mode("overwrite")

    .partitionBy(
        "year"
    )

    .save(
        TARGET_PATH
    )
)


print(
    "\nHistorical Delta dataset written:"
)

print(
    TARGET_PATH
)


spark.stop()