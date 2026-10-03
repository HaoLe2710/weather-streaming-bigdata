# Live Hourly Observation Pipeline V1

## Purpose and status

This path provides one canonical, provider-model hourly input per `NATIONWIDE_63` location for the frozen `WEATHER_XGBOOST_GLOBAL_T1H_V1` inference job. It leaves the existing 10-second current-conditions producer, `weather.raw`, and the historical replay publisher in place. It does not retrain the model or change the online feature builder.

Frozen provenance remains `WEATHER_XGBOOST_GLOBAL_T1H_V1` (`07fffbaed3e4934017a03135eddba8fc200fbcd22b799d246ea816695f98704a`, 51,662,443 bytes), `WEATHER_FORECAST_FE_V1` (73 features; ordered-list SHA-256 `ec716f47ac4eca99a945a2ba1c50ba1297c1509a2aa5f80cff063042e40295bf`), and a +1-hour target. The live path only supplies records to this contract.

The provider's `/v1/forecast` product returns numerical weather-model values. In this document, “hourly observation” means a canonical hourly input record; it does not claim that the value is a station measurement. Open-Meteo documents the forecast product as a continuously updated series assembled from applicable weather-model output. Current conditions are based on 15-minute model data, which is why the existing 10-second poll is not an hourly ML input ([Forecast API](https://open-meteo.com/en/docs)).

## Source contracts

The archived training dataset was downloaded from `https://archive-api.open-meteo.com/v1/archive` for 2020-01-01 through 2025-12-31. It requested UTC hourly values for `temperature_2m`, `relative_humidity_2m`, `precipitation`, `pressure_msl`, `wind_speed_10m`, `wind_gusts_10m`, and `weather_code`. The downloader stores those as `temperature_c`, `humidity_pct`, `precipitation_mm`, `pressure_hpa`, `wind_speed_kmh`, `wind_gust_kmh`, and `weather_code`; archived events use source `OPEN_METEO_HISTORICAL`.

The live path uses `https://api.open-meteo.com/v1/forecast` and requests the same seven hourly variable names. It sets `timezone=UTC`, `temperature_unit=celsius`, `wind_speed_unit=kmh`, and `precipitation_unit=mm` explicitly. `models` is omitted so the API's documented `auto` / Best Match default applies. One comma-separated coordinate request covers all 63 locations by default. The response must contain 63 ordered location objects; each response coordinate must be within 15 km of its paired canonical request coordinate. Event latitude and longitude use the provider's resolved grid coordinates, consistent with the historical downloader's use of provider response coordinates.

| Variable | Event field | Unit | Time meaning |
| --- | --- | --- | --- |
| `temperature_2m` | `temperature_c` | °C | Instant at the indicated hour |
| `relative_humidity_2m` | `humidity_pct` | % | Instant at the indicated hour |
| `precipitation` | `precipitation_mm` | mm | Sum over the preceding hour |
| `pressure_msl` | `pressure_hpa` | hPa | Mean-sea-level pressure at the indicated hour |
| `wind_speed_10m` | `wind_speed_kmh` | km/h | Instant at 10 m |
| `wind_gusts_10m` | `wind_gust_kmh` | km/h | Maximum gust over the preceding hour at 10 m |
| `weather_code` | `weather_code` | WMO code | Weather interpretation code at the indicated hour |

These units and variable definitions match the archived API contract. The products are not distribution-identical: the archive endpoint returns historical reanalysis/archive data, while the live Forecast endpoint returns the latest applicable numerical forecast-model product. Recent values requested through `past_hours` remain model-product values, not newly measured station data. This is a documented limitation for predictive accuracy; this phase validates ingestion and inference integration only ([Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), [Forecast API](https://open-meteo.com/en/docs)).

## UTC hour selection and leakage guard

Provider timeline strings are parsed as UTC only after the response reports `utc_offset_seconds=0` and timezone `UTC` or `GMT`. The producer rejects timestamps whose minute, second, or microsecond is non-zero, sorts by parsed instant, and never relies on response-array position for chronological ordering.

The safe-hour cutoff is the start of the current UTC hour minus one hour. For example, at `2026-10-02T16:36Z`, the newest permitted input is `2026-10-02T15:00:00Z`; the in-progress `16:00` provider row is filtered. The producer chooses the latest timestamp at or before that cutoff that is common to the successful location timelines. Bootstrap additionally requires 25 exact consecutive hours ending at that timestamp. Every emitted `event_time` is formatted as `YYYY-MM-DDTHH:00:00Z` and must be no later than the cutoff.

This one-hour closed-period buffer keeps an incomplete provider hour out of model features. Forecast records still follow the frozen contract `feature_time → target_time = feature_time + 1 hour`.

## Event and Kafka contract

Events retain the existing weather field names and carry:

```text
event_id, event_type, location_id, city, latitude, longitude,
event_time, ingestion_time, temperature_c, humidity_pct,
precipitation_mm, pressure_hpa, wind_speed_kmh, wind_gust_kmh,
weather_code, source
```

`event_type` is `WEATHER_HOURLY`; `source` is `OPEN_METEO_LIVE_HOURLY`. `event_time` is the represented UTC hour. `ingestion_time` is the UTC time the publisher built the event. The deterministic ID is `OPEN_METEO_LIVE_HOURLY|<location_id>|<event_time>`. The Kafka key is `<location_id>|<event_time>`; an explicit CRC32-derived partition based only on `location_id` keeps all hours for a location on one partition, preserving per-location order while retaining a unique location-hour key. Keep the topic partition count stable while bootstrapping and serving a location's stream.

Serialized event types match the existing inference reader: identifiers, labels, event type, timestamps, and source are strings; coordinates and the six continuous weather measurements are JSON numbers; `weather_code` is an integer WMO code. `city` is the canonical display name. The Spark weather schema can cast integral provider weather values into its numeric representation without renaming fields.

The producer uses Kafka idempotence for retried sends within a producer session, but delivery across publisher restarts is still treated as at-least-once. Correctness does not depend on a local cache: inference deduplicates canonical `(location_id, event_time)` rows with equal weather payloads, rejects conflicting payloads for that key, and merges forecast rows by deterministic `forecast_id`.

## Publisher modes and bootstrap

Run `producer/live_hourly_weather_producer.py` with:

| Mode | Behavior |
| --- | --- |
| `once` | Fetch one common latest safe hour and publish it for each successful location. |
| `daemon` | Poll every five minutes by default. An in-memory cache suppresses repeated sends for an hour in that process; downstream deterministic deduplication remains authoritative after restart. |
| `bootstrap` | Fetch recent history from the same live Forecast endpoint and publish a contiguous warm-up range, oldest to newest within each location. Any incomplete location aborts the whole bootstrap without publishing a partial warm state. |

The producer reads the frozen feature list and derives the lookback instead of scattering a `24` constant. `WEATHER_FORECAST_FE_V1` requires 24 prior hours plus the feature-time observation, so the default bootstrap is 25 rows per location. The API request asks for 25 previous hourly timestamps plus only the current provider hour (`past_hours=25`, `forecast_hours=1`); every row newer than the safe-hour cutoff is filtered. The verified end-to-end window is recorded below.

If a location has a missing hour or invalid required value, bootstrap reports `BOOTSTRAP_HISTORY_GAP` or a specific validation failure and emits no bootstrap records. It never fills gaps or uses the 2020–2025 archive to pretend the live state is contiguous.

## Failure handling and configuration

HTTP timeouts, transport errors, HTTP 429, and HTTP 5xx receive at most three retries by default using bounded exponential backoff or the provider's bounded `Retry-After`. Other HTTP errors and malformed JSON/schema fail without retry. A failed batch records every affected canonical ID; a per-location provider error records only that ID. `once` and `daemon` may publish valid successful locations; the nationwide acceptance run requires all 63.

The optional `live` Docker Compose profile uses `producer/Dockerfile`; the existing default Compose services and 10-second producer are unchanged. Supported settings are `WEATHER_LIVE_HOURLY_BOOTSTRAP_SERVERS`, `WEATHER_LIVE_HOURLY_TOPIC`, `WEATHER_LIVE_HOURLY_POLL_INTERVAL_SECONDS`, `WEATHER_LIVE_HOURLY_REQUEST_TIMEOUT_SECONDS`, `WEATHER_LIVE_HOURLY_MAX_RETRIES`, `WEATHER_LIVE_HOURLY_HISTORY_HOURS`, `WEATHER_LIVE_HOURLY_BATCH_SIZE`, `WEATHER_LIVE_HOURLY_ENDPOINT`, and `WEATHER_LIVE_HOURLY_CATALOG`; CLI flags can override them.

Example commands:

```powershell
docker compose up -d broker
docker compose exec -T broker /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server broker:19092 --create --if-not-exists `
  --topic weather.hourly.observations.v1 --partitions 8 --replication-factor 1

docker compose --profile live run --rm live-hourly-producer `
  --mode bootstrap --summary-json /opt/project/results/live-hourly-observation/<RUN_ID>/publisher_bootstrap.json

docker compose --profile live up -d live-hourly-producer
```

The inference job continues to consume only `weather.hourly.observations.v1`; it does not subscribe to `weather.raw`. For isolated validation use fresh state, forecast, rejection, checkpoint, and results paths. Keep those under ignored `data/` except for small JSON evidence under `results/live-hourly-observation/<RUN_ID>/`. Never delete the shared `weather-data` volume to reset a validation run.

## Acceptance evidence and limitations

## Verified live end-to-end run

Run `20261002T173723Z-live-hourly-v1-final` is persisted under `results/live-hourly-observation/20261002T173723Z-live-hourly-v1-final/`. Across four live provider cycles, each request returned 26 aligned hourly timestamps for each of 63 locations (6,552 fetched hourly samples total). The producer accepted the 25-hour contiguous window `2026-10-01T16:00:00Z` through `2026-10-02T16:00:00Z` and filtered the 63 in-progress `2026-10-02T17:00:00Z` rows per cycle. The latest safe hour was `2026-10-02T16:00:00Z`.

The live bootstrap sent 1,575 events for 63/63 locations. Spark kept 1,575 unique location-hour state rows (25 per location), skipped 1,512 warm-up rows, and produced 63 forecasts. The daemon's first same-hour poll and a fresh publisher process each re-sent 63 deterministic IDs; Spark counted 126 identical duplicate input rows, retained no duplicate canonical state keys, and wrote no duplicate forecast IDs. The second poll by the same daemon skipped all 63 rows. There were zero history gaps, conflicts, rejected rows, invalid forecasts, or target-time violations. All forecasts have feature time `2026-10-02T16:00:00Z`, target time `2026-10-02T17:00:00Z`, the frozen model SHA, and the frozen 73-feature contract.

The replay regression published the deterministic 72-hour historical window (4,536 rows) on an isolated replay topic and produced 3,024 forecasts. Compared with the previous inference phase's forecast Delta output for the same window, all 3,024 forecast IDs matched, the maximum prediction difference was `0.0 °C`, and model/time/feature provenance had zero mismatches. This checks inference regression; it is not a forecast-accuracy evaluation.

The publisher API median/p95 latencies over the four cycles were 1.2854/1.6183 seconds; median/p95 Kafka publish time for non-empty sends was 0.0185/0.0570 seconds. Full publisher-cycle median/p95 was 1.3016/1.6999 seconds. Maximum observed publisher RSS was 44,457,984 bytes (44.5 MB, 42.4 MiB), and sampled publisher CPU totaled 0.267 seconds. Spark used two executor cores and 2 GiB configured executor memory; the run recorded batch and prediction timings but did not collect process-level Spark memory or CPU utilization. The acceptance query used a 5-second trigger for quicker validation; its longest microbatch took 30.47 seconds, so this is not a production throughput benchmark or latency SLO.

**Merge recommendation: `READY_TO_MERGE_WITH_LIMITATION`.** The real live-to-inference path, bootstrap, duplicate behavior, frozen-model provenance, and replay regression passed. The remaining source-product difference between historical archive values and live Forecast API values must remain visible; live values are modelled, not station observations. Forecast Kafka output and accuracy monitoring remain outside this phase. Human review should precede merging; this phase does not create a release tag.

The persisted JSON evidence includes provider semantics, schema and units, catalog coordinates, bootstrap continuity, safe-hour selection, future filtering, same-hour polls, publisher restart, live inference, forecast statistics, runtime/resources, replay regression, unit/regression results, and checksums. Raw provider payloads, model files, Delta tables, and checkpoints are not copied into the evidence directory.

This phase does not measure forecast accuracy because target-time observations have not yet been joined and evaluated. Forecast monitoring and per-location error metrics belong to Forecast Monitoring & Evaluation V1. The archive/live model-source distribution difference remains visible in all acceptance reports.

The follow-on [Forecast Monitoring & Evaluation V1](FORECAST_MONITORING_EVALUATION_V1.md) archives these hourly inputs durably and scores a forecast only after its same-source target hour is eligible. Open-Meteo Forecast API values remain model-derived references, not station measurements.
