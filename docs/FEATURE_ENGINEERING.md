# Nationwide Weather Forecast Features V1

## Purpose and boundary

WEATHER_FORECAST_FE_V1 converts the validated NATIONWIDE_63 historical Delta table into a leakage-safe, Snappy-compressed Parquet dataset for later Google Colab work. Each row uses information available at observation time event_time to label temperature at exactly event_time + 1 hour for the same location_id.

This milestone ends at the feature dataset, validation evidence, and this contract. It does not train or serialize a model and does not change producer, simulator, Bronze, Silver, Gold, or benchmark behavior.

## Source and lineage

The immutable input is /opt/project/data/historical/weather_hourly_vn63. The job verifies the persisted evidence in results/data-expansion/20260928T145824Z-vn63/, then checks the live Delta count, all 63 IDs, per-location row counts, duplicate observation keys, required source columns, input values, and hourly continuity.

| Delta field | Spark type | Use |
|---|---|---|
| event_id | string | Source identity; not exported as a model feature |
| event_type | string | Source metadata |
| location_id | string | Same-location windows and output identity |
| city | string | Source context; not needed in V1 output |
| latitude | double | Numeric location feature |
| longitude | double | Numeric location feature |
| event_time | timestamp | UTC feature time |
| ingestion_time | string | Source metadata |
| temperature_c | double | Current feature, lag source, rolling source, and target source |
| humidity_pct | bigint | Current feature, lag source, and rolling source |
| precipitation_mm | double | Current feature, lag and trailing-sum source |
| pressure_hpa | double | Current feature, lag source, and rolling source |
| wind_speed_kmh | double | Current feature, lag and rolling-mean source |
| wind_gust_kmh | double | Current feature |
| weather_code | bigint | Preserved context, excluded from numeric model features |
| source | string | Source metadata |
| year | int | Existing Delta partition column |

The Delta names above are retained for current weather features. The original API variable mappings are recorded in feature_spec.json; for example, source temperature_2m maps to Delta/output temperature_c. The job records the Delta version before and after and fails if it changes. It also records the source manifest hash, Delta validation hash, source commit, and catalog checksum.

## Feature contract

The future model is one global model for all locations. location_id stays a string identifier and is never treated as an ordinal. Latitude and longitude are numeric model features. weather_code remains available for later experiments without an invented numeric distance or V1 one-hot encoding.

| Category | V1 features | Definition | Future data used? |
|---|---|---|---|
| Current weather | temperature_c, humidity_pct, precipitation_mm, pressure_hpa, wind_speed_kmh, wind_gust_kmh | Observation at event_time | No |
| Location | latitude, longitude | Canonical coordinates carried in Delta | No |
| Local calendar | local_hour, local_day_of_week, local_month, local_day_of_year | Calendar from event_time in Asia/Ho_Chi_Minh; Monday is 0 | No |
| Cyclical time | hour_sin/cos, day_of_week_sin/cos, day_of_year_sin/cos | Hour period 24, weekday period 7, year period 365.25; day-of-year uses (day-1) | No |
| Lag | temp_lag_*h, humidity_lag_*h, pressure_lag_*h, precipitation_lag_*h, wind_speed_lag_*h | Same-location value exactly the named number of hours earlier | No |
| Rolling | *_roll_mean_*h, *_roll_std_*h, precipitation_sum_*h | Trailing current-inclusive windows; standard deviation is stddev_pop | No |
| Change | temp_delta_*h, humidity_delta_*h, pressure_delta_*h, wind_speed_delta_1h | Current value minus the same-location value at the lag | No |
| Target | target_temperature_1h | Same-location temperature at target_time | Yes, label only |

There are 73 numeric model features: 6 current weather, 2 coordinates, 10 time, 23 lags, 24 rolling/accumulation, and 8 changes. The Parquet dataset has 79 columns: those 73 features, the target, location_id, event_time, target_time, weather_code, and split.

The exact lag set is:

- Temperature, humidity, and pressure: 1h, 3h, 6h, 12h, 24h.
- Precipitation and wind speed: 1h, 3h, 6h, 24h.

Rolling windows include the current time t: 3h is [t-2h, t], 6h is [t-5h, t], and 24h is [t-23h, t]. Temperature, humidity, and pressure have mean and population standard deviation; precipitation has sums; wind speed has means. There are no centered windows.

## Leakage and continuity rules

Every lag, rolling window, and target window partitions by location_id and orders by UTC event_time. For a complete unique hourly series, checking that the 24th prior timestamp is exactly event_time - 24 hours proves all rows in that 24-hour history are contiguous. Rows without that history are excluded. Target time and temperature use the only two forward operations: lead(event_time, 1) and lead(temperature_c, 1). A candidate is kept only if target_time == event_time + 1 hour.

Missing, NaN, or infinite weather inputs are never filled. They make the affected feature row ineligible and are reported. The validated full source has no invalid model inputs or hourly gaps. The last observation in each location cannot produce the +1h target and is dropped.

~~~mermaid
flowchart LR
    M24[t-24h] --> PAST[Past Observations]
    PAST --> T[Feature Time t]
    T --> TARGET[Target t+1h]
    PAST --> FEATURES[Allowed Features]
    T --> FEATURES
    TARGET -. forbidden as feature .-> FEATURES
~~~

## Time-based splits and row counts

Splits use target_time in UTC, never a random row split:

| Split | Target-time interval | Rows/location | Rows |
|---|---|---:|---:|
| TRAIN | < 2024-01-01 00:00 UTC | 35,039 | 2,207,457 |
| VALIDATION | [2024-01-01, 2025-01-01) | 8,784 | 553,392 |
| TEST | [2025-01-01, 2026-01-01) | 8,760 | 551,880 |
| Total | 2020–2025 target timeline | 52,583 | 3,312,729 |

For each of 63 locations, the source has 52,608 observations. The job excludes the first 24 observations for required history (24 × 63 = 1,512 rows) and the final targetless observation (1 × 63 = 63 rows), for 1,575 total dropped rows and 3,312,729 output rows.

Boundary examples:

- Feature time 2023-12-31 23:00 UTC targets 2024-01-01 00:00 UTC, so the row belongs to VALIDATION.
- Feature time 2024-12-31 23:00 UTC targets 2025-01-01 00:00 UTC, so the row belongs to TEST.

## Pipeline

~~~mermaid
flowchart LR
    D[NATIONWIDE_63 Delta<br/>3,314,304 rows] --> Q[Quality & continuity checks]
    Q --> T[Time + Location Features]
    T --> L[Lag Features]
    L --> R[Rolling Features]
    R --> C[Weather Change Features]
    C --> Y[Create +1h Temperature Target]
    Y --> F[Drop Boundary / Invalid Rows]
    F --> S[Split by target_time]
    S --> TR[TRAIN<br/>2020-2023]
    S --> VA[VALIDATION<br/>2024]
    S --> TE[TEST<br/>2025]
    TR --> P[Partitioned Parquet]
    VA --> P
    TE --> P
    P -. future .-> X[Google Colab Model Training]
~~~

Google Colab model training in the diagram is a later milestone. This phase stops after the validated Parquet handoff.

## Run the job

The Docker Compose Spark services read Delta from the named weather-data volume. Their /opt/project/history-data mount maps to the repository's host data/ directory.

From PowerShell at the repository root:

~~~powershell
docker compose up -d spark-master spark-worker
$featureGitCommit = git rev-parse HEAD
docker compose exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.jars.ivy=/tmp/weather-spark-ivy --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.session.timeZone=UTC --conf spark.sql.shuffle.partitions=16 --conf spark.sql.adaptive.enabled=true --conf spark.sql.parquet.compression.codec=snappy --executor-memory 3g --executor-cores 4 --conf spark.cores.max=4 --conf spark.executor.instances=1 --packages io.delta:delta-spark_2.13:4.0.0 /opt/project/spark/jobs/weather_feature_engineering.py --run-id 20260930T1600Z-feature-v1 --feature-git-commit $featureGitCommit
~~~

Use a unique run ID. The job refuses to overwrite a prior output or report directory. It writes to a sibling staging directory, reads Parquet back with Spark, validates it, verifies the Delta version is unchanged, then promotes the output directory.

For a bounded smoke run, add --smoke --smoke-hours 72 and pass separate paths, for example:

~~~powershell
--output /opt/project/history-data/ml/weather_forecast_fe_v1_smoke/<run_id> --run-id <run_id>
~~~

The smoke selects two deterministic location IDs, reads 24 hours of history plus 72 feature hours and the next target observation, and expects 144 output rows.

## Output and evidence

The full dataset is written to:

~~~text
data/ml/weather_forecast_fe_v1/
├── split=TRAIN/
├── split=VALIDATION/
└── split=TEST/
~~~

It is Snappy-compressed Parquet and is ignored by Git. No single-file coalesce is used. Spark and Python can read the entire dataset from the folder, or each split from its partition folder.

Run evidence is stored under results/feature-engineering/<run_id>/:

| File | Contents |
|---|---|
| feature_manifest.json | Feature-set ID, lineage, source version, feature commit, output location and counts |
| feature_spec.json | Feature names, roles, categories, sources, formulas and future-data policy |
| feature_validation.json | Source/output counts, splits, uniqueness, null/NaN/infinity, continuity and target checks |
| ml_schema.json | Ordered output column names, Spark types, roles, and feature counts |
| per_location_split_counts.csv | Each location and split count plus target-time bounds |
| parquet_inventory.json | Relative part-file paths, byte sizes, timestamps, file count and total bytes |
| statistics_by_split.json | Per-feature min, max, mean, and population standard deviation by split |
| runtime_summary.json | Runtime, Spark/Delta versions, master, executor settings, shuffle count and catalog checksum |
| checksums.json | SHA-256 of the small run reports; Parquet is inventoried by path and size |

## Validation and handoff

The job validates the exact 3,314,304 source rows, all 63 IDs and 52,608 rows/location; 3,312,729 output rows; 2,207,457 TRAIN, 553,392 VALIDATION, and 551,880 TEST rows; per-location split counts and boundaries; unique (location_id, event_time) keys; complete model-feature values; no NaN or infinite values; hourly continuity; target time and label equality against the next Delta observation; unchanged Delta version; exact output schema; and successful Spark Parquet read-back.

The host-side project checks are:

~~~powershell
python -m compileall benchmark producer historical simulator spark tests
python -m unittest discover -s tests
docker compose config --quiet
git diff --check
~~~

The host suite skips Spark-only semantic tests if PySpark is absent. Run those tests inside the Spark image:

~~~powershell
docker cp tests\test_weather_feature_engineering.py weather-spark-master:/tmp/test_weather_feature_engineering.py
docker compose exec -T spark-master sh -c 'PYTHONPATH=/opt/project/spark/jobs /opt/spark/bin/spark-submit --master "local[2]" /tmp/test_weather_feature_engineering.py'
~~~

V1 is ready for the later Colab modeling phase only after the full dataset and read-back validations pass. Training, persistence baseline, global XGBoost, evaluation, and model export are outside this feature-engineering milestone.
