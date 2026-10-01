# Preserved failed attempt

The first nationwide stream attempt incorrectly required replay coordinates to equal the current administrative catalog centers. Historical feature rows carry source coordinates from the frozen feature dataset, and those differ for some locations. That run rejected all 4,536 source rows as `LOCATION_COORDINATE_MISMATCH`; it produced no forecast rows and is not acceptance evidence.

The original batch log remains at `inference_batches.jsonl`. The corresponding Delta rejection table was moved intact inside the preserved `weather-data` volume to:

`/opt/project/data/streaming/weather_forecast_xgboost_v1/failed_attempts/catalog_coordinate_mismatch/rejected_observations`

The corrected implementation validates canonical location IDs, finite geographic ranges, and a stable coordinate pair per location in the replay source, preserving the coordinates used for training. Full nationwide feature and prediction parity then passed.
