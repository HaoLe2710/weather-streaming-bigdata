SELECT COUNT(*) AS state_rows,
       COUNT(DISTINCT location_id) AS locations,
       COUNT(DISTINCT STRUCT(location_id, event_time)) AS unique_location_hour_keys,
       MIN(event_time) AS min_event_time_utc,
       MAX(event_time) AS max_event_time_utc,
       SUM(CASE WHEN MINUTE(event_time) <> 0 OR SECOND(event_time) <> 0 THEN 1 ELSE 0 END) AS hour_alignment_violations,
       SUM(CASE WHEN event_time > TIMESTAMP '2026-10-02 16:00:00' THEN 1 ELSE 0 END) AS future_state_rows
FROM delta.`/opt/project/data/streaming/weather_forecast_xgboost_v1/live_20261002T170150Z-live-hourly-v1/state`;
SELECT MIN(location_rows) AS min_rows_per_location, MAX(location_rows) AS max_rows_per_location,
       COUNT(*) AS location_count
FROM (SELECT location_id, COUNT(*) AS location_rows
      FROM delta.`/opt/project/data/streaming/weather_forecast_xgboost_v1/live_20261002T170150Z-live-hourly-v1/state`
      GROUP BY location_id);
