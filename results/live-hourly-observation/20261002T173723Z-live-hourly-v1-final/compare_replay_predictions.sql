WITH old_f AS (
  SELECT forecast_id, location_id, feature_time, target_time, prediction_temperature_c, model_sha256, feature_list_sha256
  FROM delta.`/opt/project/data/streaming/weather_forecast_xgboost_v1/forecasts`
  WHERE feature_time BETWEEN TIMESTAMP '2020-01-02 00:00:00' AND TIMESTAMP '2020-01-03 23:00:00'
),
new_f AS (
  SELECT forecast_id, location_id, feature_time, target_time, prediction_temperature_c, model_sha256, feature_list_sha256
  FROM delta.`/opt/project/data/streaming/weather_forecast_xgboost_v1/replay_20261002T170150Z-live-hourly-v1/forecasts`
)
SELECT COUNT(*) AS joined_or_unmatched_rows,
       SUM(CASE WHEN o.forecast_id IS NULL OR n.forecast_id IS NULL THEN 1 ELSE 0 END) AS unmatched_forecast_ids,
       SUM(CASE WHEN o.forecast_id IS NOT NULL AND n.forecast_id IS NOT NULL AND ABS(o.prediction_temperature_c - n.prediction_temperature_c) > 0.0000001 THEN 1 ELSE 0 END) AS prediction_mismatches,
       MAX(ABS(o.prediction_temperature_c - n.prediction_temperature_c)) AS max_abs_prediction_difference_c,
       SUM(CASE WHEN o.forecast_id IS NOT NULL AND n.forecast_id IS NOT NULL AND (o.location_id <> n.location_id OR o.feature_time <> n.feature_time OR o.target_time <> n.target_time OR o.model_sha256 <> n.model_sha256 OR o.feature_list_sha256 <> n.feature_list_sha256) THEN 1 ELSE 0 END) AS provenance_or_time_mismatches
FROM old_f o FULL OUTER JOIN new_f n ON o.forecast_id = n.forecast_id;
SELECT 'baseline' AS run, COUNT(*) AS forecast_rows FROM delta.`/opt/project/data/streaming/weather_forecast_xgboost_v1/forecasts`
WHERE feature_time BETWEEN TIMESTAMP '2020-01-02 00:00:00' AND TIMESTAMP '2020-01-03 23:00:00'
UNION ALL
SELECT 'current_live_phase_replay' AS run, COUNT(*) AS forecast_rows FROM delta.`/opt/project/data/streaming/weather_forecast_xgboost_v1/replay_20261002T170150Z-live-hourly-v1/forecasts`;
