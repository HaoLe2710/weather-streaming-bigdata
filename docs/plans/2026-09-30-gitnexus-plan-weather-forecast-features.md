# GitNexus Engineering Plan

> Task: Build WEATHER_FORECAST_FE_V1 from complete NATIONWIDE_63 Delta, with target-time splits, validated leakage-safe Parquet, lineage and documentation.
> Evidence verified at commit b10d9b914ad36ab862343e24c2757158a6cee58c; GitNexus index refreshed this session with --index-only --pdg.
> Evidence provenance schema 2; exact generated plan path excluded from the global dirty digest.

## 1. Objective

Materialize the 63-location nationwide hourly dataset as reproducible Snappy Parquet for later Colab training. Predict same-location temperature exactly one hour after feature_time. Stop before model training.

## 2. Current Behaviour

[verified] historical_to_delta.py selects isolated NATIONWIDE_63 source/target paths and writes a Delta table partitioned by year. [verified] Persisted delta_validation.json reports 3,314,304 rows and 63 locations. [verified] Source fields are temperature_c, humidity_pct, precipitation_mm, pressure_hpa, wind_speed_kmh, wind_gust_kmh, weather_code, latitude, longitude, event_time and location_id.

## 3. Relevant Architecture

[verified] Compose mounts the durable weather-data volume at /opt/project/data and host ./data at /opt/project/history-data. Read /opt/project/data/historical/weather_hourly_vn63; write host-visible /opt/project/history-data/ml/weather_forecast_fe_v1. Keep live-stream and benchmark code untouched.

## 4. GitNexus Findings

[graph] context main in historical_to_delta.py shows main calling parse_args and run_conversion; impact is LOW with its file and test import. [graph] impact load_catalog upstream depth 3 is CRITICAL: 9 direct callers, 7 flows, 3 modules across downloader, producer, Delta conversion and validation. Do not change it. [graph] weather_schema is UNKNOWN because module-level use is unresolved; source search confirms streaming dependencies, so do not change it. [verified] No feature/ML/forecast/lag/rolling/Parquet split implementation exists.

## 5. Statement-Level PDG Findings

[graph] PDG layer is indexed. Statement-level query did not resolve an executable block at selected multiline DataFrame anchors and reported pdg-no-block-at-line; interprocedural output is a callgraph bridge only. [verified] Source sequence reads raw input, casts event_time, validates and writes Delta. [inferred] A separate batch job can read completed Delta without changing that sequence.

## 6. Proposed Changes

- Add spark/jobs/weather_feature_engineering.py with lineage/input checks, same-location ordered windows, 24-hour continuity, trailing features, +1h target, target_time split, validation and staged Parquet read-back/promotion.
- Add tests/test_weather_feature_engineering.py for deterministic Spark series, two locations, gaps, leap/year boundaries, cyclical values, rolling stats and exact splits.
- Add docs/FEATURE_ENGINEERING.md with the feature contract and future Colab handoff.
- Commit small evidence only; actual Parquet stays under ignored data/ml/.

## 7. Implementation Sequence

1. Commit this plan behind a staged GitNexus gate.
2. Add the isolated job and feature semantics tests; validate pure helpers and Spark transformations.
3. Run a bounded two-location, 72-hour smoke against nationwide Delta and persist its report.
4. Run full feature generation, read back Parquet, validate all counts and persist manifests/inventory/stats/checksums.
5. Add documentation, run requested checks, refresh GitNexus, review affected flows and commit code plus evidence logically.

## 8. Test Strategy

Verify 52,608 source / 52,583 output per location; 3,314,304 / 3,312,729 total; TRAIN 2,207,457, VALIDATION 553,392, TEST 551,880. Spark tests cover lag1/3/6/12/24, trailing 3/6/24 mean/stddev_pop, precipitation sums, current-minus-lag deltas, same-location +1h label, rejection of 01:00-to-03:00, cyclical values and 2023/24 plus 2024/25 target-time boundaries. Full run checks all locations, counts, required null/NaN/infinity, duplicate keys, continuity, split boundaries, per-location counts and Parquet read-back.

## 9. Risk and Impact Analysis

Window sorting is the main resource risk. Use DataFrames, AQE, 16 shuffle partitions, no cache and no full collect. Record input Delta version before/after. Write to run-scoped staging and promote only after validation. Catalog and streaming schema are critical/unresolved shared boundaries and stay unchanged. B0/performance/scalability remain out of scope because runtime paths do not change.

## 10. Files Expected to Change

| File | Symbols | Reason |
| --- | --- | --- |
| spark/jobs/weather_feature_engineering.py | new standalone job | feature generation, validation, artifacts |
| tests/test_weather_feature_engineering.py | new tests | temporal feature semantics |
| docs/FEATURE_ENGINEERING.md | new document | data contract and handoff |
| results/feature-engineering/<run_id>/* | small evidence only | materialization validation |

## 11. Reusable Implementation Context

```yaml
implementation_context:
  task_summary: Build leakage-safe WEATHER_FORECAST_FE_V1 Parquet from immutable NATIONWIDE_63 Delta.
  acceptance_criteria:
    - Use all 63 locations from 3314304 source rows.
    - Output exactly 3312729 rows; drop 1512 initial-history rows and 63 final-target rows.
    - target_temperature_1h is exact same-location t+1h; no future features.
    - Split by target_time: TRAIN 2207457, VALIDATION 553392, TEST 551880.
    - Snappy Parquet read-back passes with no required null, NaN, infinity, duplicate or continuity violations.
    - Persist lineage, spec, role schema, validation, split counts, inventory, statistics, runtime and checksums.
  evidence_provenance:
    {
      "schema_version": 2,
      "head_commit": "b10d9b914ad36ab862343e24c2757158a6cee58c",
      "generated_plan_path": "docs/plans/2026-09-30-gitnexus-plan-weather-forecast-features.md",
      "global_dirty_digest": {
        "algorithm": "sha256",
        "canonicalization": "gitnexus-evidence-provenance-v2 NUL-framed UTF-8 records",
        "value": "0a9c85780067d9afcd0764f307b60891e3cee927ee11eaeb5ec7826d10fd82cd"
      },
      "cited_path_manifest": [
        {
          "path": ".gitignore",
          "object_kind": {
            "head": "regular",
            "index": "regular",
            "worktree": "regular",
            "untracked": "absent"
          },
          "state": "clean",
          "rename_from": null,
          "rename_to": null,
          "head_digest": "sha256:386f88608bf7f77c79763973aa28b1a14e5e543aeeeb0a2a88ddf32dcbcb3cc7",
          "index_digest": "sha256:386f88608bf7f77c79763973aa28b1a14e5e543aeeeb0a2a88ddf32dcbcb3cc7",
          "worktree_digest": "sha256:e7f39cd465ed49eaca8df052ff07de2cfde091e82bc4045b53ebe597fbfa3765",
          "untracked_digest": "absent"
        },
        {
          "path": "docker-compose.yml",
          "object_kind": {
            "head": "regular",
            "index": "regular",
            "worktree": "regular",
            "untracked": "absent"
          },
          "state": "clean",
          "rename_from": null,
          "rename_to": null,
          "head_digest": "sha256:9e073e6cbf74b931774e9c68302708f3d3943576daa4703cab4ba3ba05fb5a7f",
          "index_digest": "sha256:9e073e6cbf74b931774e9c68302708f3d3943576daa4703cab4ba3ba05fb5a7f",
          "worktree_digest": "sha256:aa1c3bdaad5eae841aaede9b06e77a1c0fab9f61a8a0e3afab053c172f11baf5",
          "untracked_digest": "absent"
        },
        {
          "path": "historical/location_catalog.py",
          "object_kind": {
            "head": "regular",
            "index": "regular",
            "worktree": "regular",
            "untracked": "absent"
          },
          "state": "clean",
          "rename_from": null,
          "rename_to": null,
          "head_digest": "sha256:6ca9c0c803780986f89f60c03114989bfabda3f6272fba55097720c1955234dd",
          "index_digest": "sha256:6ca9c0c803780986f89f60c03114989bfabda3f6272fba55097720c1955234dd",
          "worktree_digest": "sha256:b061573acfac513f8b626f17070a0f0ee46d18b616aff3c9e85498b2773395fb",
          "untracked_digest": "absent"
        },
        {
          "path": "historical/locations.json",
          "object_kind": {
            "head": "regular",
            "index": "regular",
            "worktree": "regular",
            "untracked": "absent"
          },
          "state": "clean",
          "rename_from": null,
          "rename_to": null,
          "head_digest": "sha256:2a5db244cfa206b61966c9df273a44ea6e80096231cf942d927616e8da9d197f",
          "index_digest": "sha256:2a5db244cfa206b61966c9df273a44ea6e80096231cf942d927616e8da9d197f",
          "worktree_digest": "sha256:e57910b3bc222784b0d1cdddf30593bfbece9065ba12d67c73f8ac9bc9fe8728",
          "untracked_digest": "absent"
        },
        {
          "path": "results/data-expansion/20260928T145824Z-vn63/dataset_manifest.json",
          "object_kind": {
            "head": "regular",
            "index": "regular",
            "worktree": "regular",
            "untracked": "absent"
          },
          "state": "clean",
          "rename_from": null,
          "rename_to": null,
          "head_digest": "sha256:429ac67c7c8f2a51f812cfddd4857959c1a3b0e2b88c8c66e6cd4a4b31e96f44",
          "index_digest": "sha256:429ac67c7c8f2a51f812cfddd4857959c1a3b0e2b88c8c66e6cd4a4b31e96f44",
          "worktree_digest": "sha256:6b1cf970aa68b9149eeda4e481ffd7f618059c94b8ba1b63e737a8d0428f5e48",
          "untracked_digest": "absent"
        },
        {
          "path": "results/data-expansion/20260928T145824Z-vn63/delta_validation.json",
          "object_kind": {
            "head": "regular",
            "index": "regular",
            "worktree": "regular",
            "untracked": "absent"
          },
          "state": "clean",
          "rename_from": null,
          "rename_to": null,
          "head_digest": "sha256:8d6adef49bfbc6546cb31e3391b86df3c29f82e315915e8c832fce7f3d33aa46",
          "index_digest": "sha256:8d6adef49bfbc6546cb31e3391b86df3c29f82e315915e8c832fce7f3d33aa46",
          "worktree_digest": "sha256:446fd053c61c9cf7544ca5415f595a85e0f4af35d5d741e923cfc11baa38f420",
          "untracked_digest": "absent"
        },
        {
          "path": "spark/jobs/historical_to_delta.py",
          "object_kind": {
            "head": "regular",
            "index": "regular",
            "worktree": "regular",
            "untracked": "absent"
          },
          "state": "clean",
          "rename_from": null,
          "rename_to": null,
          "head_digest": "sha256:e14e13cb395ef407bd071d2285e55567aea47653e985411391cbcaad8be3a0e5",
          "index_digest": "sha256:e14e13cb395ef407bd071d2285e55567aea47653e985411391cbcaad8be3a0e5",
          "worktree_digest": "sha256:0d917c8bc27acfad4a52d7750fb2712cc6fc556b209244c29033daacb2edc4b0",
          "untracked_digest": "absent"
        },
        {
          "path": "spark/jobs/weather_schema.py",
          "object_kind": {
            "head": "regular",
            "index": "regular",
            "worktree": "regular",
            "untracked": "absent"
          },
          "state": "clean",
          "rename_from": null,
          "rename_to": null,
          "head_digest": "sha256:b289f58802c05881df2c498f82833384749f8bed44b5e94dd39dd247ba4ceac7",
          "index_digest": "sha256:b289f58802c05881df2c498f82833384749f8bed44b5e94dd39dd247ba4ceac7",
          "worktree_digest": "sha256:bbd4de30b70868302afb30bf0c655770d59700076e3a070916bc7cd216269358",
          "untracked_digest": "absent"
        },
        {
          "path": "tests/test_nationwide_data.py",
          "object_kind": {
            "head": "regular",
            "index": "regular",
            "worktree": "regular",
            "untracked": "absent"
          },
          "state": "clean",
          "rename_from": null,
          "rename_to": null,
          "head_digest": "sha256:7f133900d8555f90a5ef075eeb1c9182e0e412cebac2f77def70e0cce20592d9",
          "index_digest": "sha256:7f133900d8555f90a5ef075eeb1c9182e0e412cebac2f77def70e0cce20592d9",
          "worktree_digest": "sha256:2edc09501cc03e0be16b2dfa64e65c6a2e0cea0a03db68929b65e6634c5bc99c",
          "untracked_digest": "absent"
        }
      ]
    }
  primary_symbols:
    - symbol: main
      file: spark/jobs/historical_to_delta.py
      lines: 202-205
      role: converter entry point left unchanged
    - symbol: run_conversion
      file: spark/jobs/historical_to_delta.py
      lines: 63-199
      role: immutable Delta producer
    - symbol: load_catalog
      file: historical/location_catalog.py
      lines: 261-269
      role: critical shared function left unchanged
    - symbol: weather_schema
      file: spark/jobs/weather_schema.py
      lines: 29-64
      role: streaming schema left unchanged
  related_symbols:
    - symbol: delta_validation.json
      relationship: lineage evidence
      relevance: validated IDs, count and status
    - symbol: docker-compose spark-master/spark-worker mounts
      relationship: runtime storage
      relevance: named input volume and host-visible output
  execution_path:
    - Read immutable nationwide Delta and source validation manifests.
    - Check 63 IDs, source rows, unique observation keys, coordinates and hourly continuity.
    - Build local calendar/current/past-only lag/rolling/change features.
    - Use lead only for target_time and target_temperature_1h; reject gaps and incomplete rows.
    - Split by target_time; write staging Snappy Parquet, read back and validate before promotion.
  pdg_constraints:
    - description: Keep historical Delta conversion order unchanged; consume its completed output only.
      affected_statements: [spark/jobs/historical_to_delta.py:96-127]
    - description: Do not change critical catalog or unknown streaming schema shared boundaries.
      affected_statements: [historical/location_catalog.py:261-294, spark/jobs/weather_schema.py:29-64]
  architectural_patterns:
    - Follow isolated batch Spark and run-scoped JSON reporting conventions in historical_to_delta.py.
    - Use Compose host data mount for Colab output and named volume for source Delta.
  files_to_modify:
    - spark/jobs/weather_feature_engineering.py
    - tests/test_weather_feature_engineering.py
    - docs/FEATURE_ENGINEERING.md
  tests:
    - tests/test_weather_feature_engineering.py covers count, lag, rolling, delta, target, gap, cyclical and split boundaries.
    - producer/.venv/Scripts/python.exe -m unittest discover -s tests
    - Run PySpark DataFrame tests in Spark image.
  verification_commands:
    - producer/.venv/Scripts/python.exe -m compileall benchmark producer historical simulator spark tests
    - producer/.venv/Scripts/python.exe -m unittest discover -s tests
    - docker compose config --quiet
    - git diff --check
  risks:
    - Window shuffle/sort memory pressure; use AQE, 16 partitions, no cache and no full collect.
    - Gaps/non-finite inputs must be reported/excluded without imputation.
  assumptions:
    - Existing weather-data volume contains the validated nationwide Delta path.
    - Delta fields match historical downloader naming and validation evidence.
  open_questions: []
  avoid:
    - Do not edit catalog, historical converter, streaming schema, producer, simulator, Bronze, Silver, Gold, benchmark runner or source Delta.
    - Do not train models, scale, encode categories, random-split or create additional targets.
```

## 12. Assumptions and Open Questions

[assumed] The Docker weather-data volume still contains source Delta; verify before smoke/full runs. [verified] data/ml/ is ignored. Optional PyArrow check is deferred unless already installed.

## 13. Definition of Done

All 63 locations, exact target/split/count/continuity/null and read-back invariants pass. Full Parquet exists at the host-visible versioned path. Lineage/spec/schema/stats/inventory are persisted. Static/unit tests and final GitNexus checks pass. No models or runtime pipeline changes.