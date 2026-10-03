# Forecast Monitoring & Evaluation V1 — Acceptance Report

Acceptance run: 2026-10-03 UTC. Detailed persisted row checks and per-location metrics are in [the validation JSON](../results/forecast-monitoring/20261003T0515Z-forecast-monitoring-v1-acceptance/persisted_metrics_validation.json); concise run totals are in [the acceptance JSON](../results/forecast-monitoring/20261003T0515Z-forecast-monitoring-v1-acceptance/acceptance.json).

## 1–10. Phase, provenance, and reference contract

1. **Git / phase setup.** Work is on `feature/forecast-monitoring-evaluation-v1`, based on `3c518f5936af396d07ccea603865442ac30bfb9c`. The phase is not merged and creates no tag.
2. **Baseline provenance.** Model `WEATHER_XGBOOST_GLOBAL_T1H_V1`, verified 51,662,443-byte artifact SHA-256 `07fffbaed3e4934017a03135eddba8fc200fbcd22b799d246ea816695f98704a`; feature set `WEATHER_FORECAST_FE_V1`, 73 features, ordered-list SHA-256 `ec716f47ac4eca99a945a2ba1c50ba1297c1509a2aa5f80cff063042e40295bf`. Inference code and model were not changed, retrained, or tuned.
3. **Pre-change GitNexus.** The existing inference symbol returned `risk=UNKNOWN` with incomplete/stale graph coverage. New monitor symbols were not in the index; the parser’s direct call site was confirmed by text search. This is not reported as a clean graph verdict.
4. **Monitoring architecture.** The hourly Kafka source is archived into canonical Delta; that archive and frozen forecast Delta feed delayed evaluation; evaluations feed hourly and rolling global/location metrics. Inference state/checkpoint remain separate.
5. **Evaluation reference.** Same source, `location_id`, and UTC `event_time == target_time` canonical hourly record.
6. **Why this is not station ground truth.** `OPEN_METEO_LIVE_HOURLY` comes from Open-Meteo Forecast API, a numerical weather-model product. Metrics describe agreement with that reference product, not station-observed accuracy.
7. **Durable archive.** Canonical path: `/opt/project/data/streaming/weather_hourly_observations_v1/observations`; key `(source, location_id, event_time)`.
8. **Archive idempotency.** The replay archive ends with 4,662 unique rows. A repeated 63-row replay input added zero rows and was counted as 63 duplicates. The live source archive contains 6,174 unique rows; the first Kafka archive ignored 1,827 repeated messages, and the repeated 63-row backfill ignored 63 duplicates. Total duplicate inputs ignored across the live archive cycles: 1,890.
9. **Reference revision policy.** The first accepted payload remains canonical. A changed payload for the same key records old/new hashes and event IDs in `reference_revisions`; it does not replace the accepted row. No revisions occurred in these runs.
10. **Forecast input.** Existing frozen forecast Delta rows, including model/feature hashes, feature/target times, prediction, inference time, and exact `source_event_id`.

## 11–20. Joins, lifecycle, and metrics

11. **Feature-time persistence join.** Exact feature-time `source_event_id`, same location, source, and feature hour. Independent Delta checks found zero baseline/join mismatches.
12. **Target-time reference join.** Same reference source, location, exact target hour, and stored target event ID. Independent Delta checks found zero target join mismatches.
13. **Target-time gate.** Evaluation requires `evaluation_time >= target_time`; all 3,150 replay rows have passed target time, and all evaluated rows passed the persisted gate check.
14. **Lifecycle.** `PENDING_TARGET_TIME`, `PENDING_REFERENCE`, `PENDING_BASELINE`, `READY`, `EVALUATED`, `REFERENCE_CONFLICT`, and `INVALID_PROVENANCE`; pending rows advance once a valid target arrives, completed rows stay immutable.
15. **Evaluation schema.** Stores forecast/model provenance, source and feature/target event IDs and hashes, both predictions, target reference/weather fields, signed/absolute/squared errors, status/mode, target gate, ingestion/evaluation timestamps and lags.
16. **Evaluation identity / idempotency.** Deterministic versioned evaluation ID; Delta merge updates a pending row when its target arrives and inserts no second row on repeat. 3,150 unique IDs for 3,150 replay forecasts; zero duplicate evaluations.
17. **Error convention.** Both errors are `prediction - reference`; absolute error and squared error are derived from that signed value.
18. **Persistence baseline.** `temperature_c` at feature time only. It is joined from the exact source event used by the forecast and is compared against the same target-time reference as XGBoost.
19. **Primary metrics.** MAE, RMSE, bias, and R² when defined; micro/global and macro/location summaries are distinguished. Undefined values remain null.
20. **Skill metrics.** MAE skill is `1 - model_MAE / persistence_MAE`; improvements use persistence minus model. A zero persistence denominator yields null.

## 21–29. Replay, late reference, and durability acceptance

21. **Replay setup.** Isolated topic `weather.forecast-monitoring.replay.v1`, 63 locations, 73 consecutive UTC hours from `2020-01-01T00:00:00Z` through `2020-01-04T00:00:00Z`; one additional hour was appended on the same checkpoint.
22. **73-hour forecasts.** 4,599 source rows; 3,087 forecasts; zero invalid rows or reference conflicts.
23. **73-hour evaluations.** 3,024 evaluated, 63 pending target references.
24. **Pending-reference test.** The 63 forecasts for the newest available feature hour remained pending until the next target-hour events arrived.
25. **Late-reference test.** Appending 63 observations advanced 63 pending evaluations and added 63 forecasts for the new feature hour. Final 74-hour state: 4,662 unique references, 3,150 forecasts, 3,087 evaluated, 63 pending.
26. **Metric parity.** Independent plain-Python calculations over persisted evaluation rows match persisted rolling 168-hour metrics with max absolute difference `0.0` at tolerance `1e-9`.
27. **Per-location isolation.** Feature and target source/location/time joins were checked against archive rows; no cross-location or event-ID mismatches. Replay evaluated all 63 locations.
28. **Restart test.** Restart on the same checkpoint and Delta paths read zero new source rows; inserted/updated zero evaluations and preserved 3,150 unique evaluation IDs.
29. **Replay idempotency.** Replaying the same 63 events ignored all 63, added no archive rows, created no revisions, and inserted/updated zero evaluations.

## 30–41. Live cohort and monitoring windows

30. **Live cohort.** `live-20261002T173723Z-live-hourly-v1-final`: 63 forecasts at feature `2026-10-02T16:00:00Z`, target `2026-10-02T17:00:00Z`.
31. **Live evaluation mode.** The original Kafka capture ended at safe hour 16:00Z; it did not contain the target hour. The target was fetched later from Open-Meteo and is explicitly labeled `LIVE_SOURCE_BACKFILL`, not prospective capture.
32. **Live XGBoost metrics.** At 63 samples: MAE `0.5516831292 °C`, RMSE `0.6244337401 °C`, bias `-0.4748347812 °C`.
33. **Live persistence metrics.** MAE `0.3190476190 °C`, RMSE `0.4340653158 °C`, bias `-0.0238095238 °C`.
34. **Live skill vs persistence.** MAE skill `-0.7291560766`; RMSE improvement `-43.8570918472%`. On this one-hour cohort, XGBoost trails persistence.
35. **Reference coverage.** 63/63 references and evaluations, 100% for the one-hour snapshot. Zero missing references after backfill.
36. **Hourly snapshot.** Target `2026-10-02T17:00:00Z`, 63 of 63 expected locations, `MODEL_TRAILS_PERSISTENCE`.
37. **Rolling 24h.** Replay latest fully complete window ends `2020-01-04T01:00:00Z`: 1,512/1,512, complete. The newest replay snapshot at `02:00Z` is 1,449/1,512 (95.8333%), incomplete because 63 newest targets are pending. Live: 63/1,512 (4.1667%), incomplete.
38. **Rolling 7d.** Replay: 3,087/10,584 references (29.1667%), incomplete, with no complete 7-day snapshot. Live: 63/10,584 (0.5952%), incomplete.
39. **Per-location results.** Replay: model MAE beats persistence in 63/63 locations over the 49 evaluated target hours. Live one-hour backfill: 13/63 locations beat persistence; 50/63 tie or trail (zero ties in this sample). No live location meets the 24-hour or 7-day minimum sample threshold.
40. **Distribution monitoring.** Hourly Delta stores count, mean, population standard deviation, min, p05, p50, p95, and max for target weather values, predictions, and model/persistence errors. The backfilled target distribution has 63 rows; selected summaries are in the validation JSON.
41. **Reference/evaluation latency.** Live backfill reference-arrival lag median/p95: `43,610 s`; evaluation lag median/p95: `43,630.332983 s`. These reflect later retrieval of a target roughly 12 hours old, not prospective stream latency.

## 42–51. Runtime, artifacts, and decision

42. **Runtime metrics.** Across the seven accepted monitor cycles, median cycle time was `40.783 s`, p95 was `48.359 s` (nearest-rank), and max was `48.359 s`. Individual cycles: replay 73-hour `38.008 s`; 74-hour late reference `48.359 s`; restart `46.227 s`; duplicate replay `43.141 s`; live archive `32.857 s`; live backfill `40.783 s`; repeat backfill `38.577 s`. The 63-row inference extension took `49.363 s`.
43. **Resource usage.** Replay monitor peak RSS: 358,584,320 bytes (73h) and 376,991,744 bytes (74h). Live monitor peak RSS: 224,866,304 bytes (archive), 224,821,248 bytes (backfill), 223,543,296 bytes (repeat). Cycle JSONL also records CPU time and stage durations.
44. **Persisted artifacts.** Delta tables and checkpoints are in the preserved Docker data volume. The acceptance directory under `results/forecast-monitoring/` contains the required architecture, archive, contract, replay, late-reference, parity, baseline, live, rolling, revision, restart, runtime, resource, and test JSON files; it also contains `checksums.json`, detailed per-location metric validation, publisher/inference evidence, and monitor cycle JSONL.
45. **Unit/regression tests.** Full suite: 144 passed, 6 skipped, 0 failed. The six existing Spark feature-engineering semantics tests require the Spark image. Kafka binary parsing, lifecycle, idempotency, joins, metrics, backfill, and limits have monitoring tests; Kafka/Delta integration was also exercised in Spark.
46. **Documentation.** Architecture, reference and error semantics, paths/configuration, replay acceptance, and live backfill limits are documented in [Forecast Monitoring & Evaluation V1](FORECAST_MONITORING_EVALUATION_V1.md) and this report.
47. **Post-change GitNexus.** Re-indexed with `node .gitnexus/run.cjs analyze --skills --embeddings --verbose`: 2,815 nodes, 4,938 edges, 92 clusters, and 173 flows; 10 repo-specific skills were generated. `detect_changes(scope=all)` mapped four documentation symbols and zero affected processes with graph risk `low`; it did not map the added Python evaluator/aggregation symbols. Exact `context` and upstream `impact` requests for `evaluate_forecasts` and `build_rolling_metrics` returned `UNKNOWN`/not found. Direct source search confirms their definitions and calls from the monitoring Spark job and tests, but the missing graph walk is recorded as unresolved rather than an all-clear. Semantic vector indexing was unavailable; GitNexus reports exact-scan fallback.
48. **Git commit/push.** The completed feature branch is committed and pushed to `origin`; its final HEAD is included in the completion message. No merge or tag is performed in this phase.
49. **Remaining limitations.** The live comparison has one backfilled target hour, not a long-term sample. The Open-Meteo reference is model-derived, not station measurement. Live 24-hour and 7-day windows are incomplete. Replay metrics validate the implementation on historical rows; they are not live accuracy claims.
50. **Merge recommendation.** `READY_TO_MERGE_WITH_LIMITATION`: delayed evaluation, joins, baseline, metric parity, replay, restart, and backfill idempotency passed. Do not interpret the one-hour live result as a long-term performance verdict.
51. **Recommended next milestone.** Forecast API & Alerting V1, followed by Vietnam Forecast Dashboard V1.

## Direct answers

- The evaluation reference is the canonical same-source hourly Forecast API record at the forecast’s target time and location; it is not station ground truth.
- The evaluator cannot score before target time. Future target rows are not scored early.
- Persistence uses only feature-time temperature. Model and persistence use the same target reference and include location/source/time in their joins.
- Missing targets remain pending. Identical references are ignored; changed payloads create revision evidence and do not overwrite prior canonical or evaluated rows.
- A restart or repeated backfill creates no duplicate evaluation. All aggregate metrics can be rebuilt from canonical evaluation rows.
- XGBoost does not beat persistence on the available live cohort: 63 samples, 13 locations better, 50 equal-or-worse. This is too small for a long-term claim.
- Live 24h and 7d windows are incomplete. The replay’s latest fully completed 24h window is complete; its latest snapshot and 7d window remain incomplete.
- V1 is ready for merge with the stated limitation; the user’s branch has not been merged by this phase.
