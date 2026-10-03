from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json

import httpx
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from historical.forecast_t2h_v1_1 import (
    DEFAULT_ARTIFACT_ROOT,
    PREDICTOR_UNITS,
    SOURCE_CONFIG,
    TARGET_UNITS,
    build_request_parameters,
    chunks,
    download_range,
    estimated_api_call_units,
    fetch_json,
    load_nationwide_locations,
    map_batch_responses,
    MODEL_ID,
    probe_live_forecast_alignment,
    quota_wait_seconds,
    validate_hourly_response,
    validate_normalized_sources,
    SOURCE_CONTRACT_ID,
)
from ml.forecast_t2h_v1_1 import (
    FEATURE_SET_ID,
    MODEL_FEATURE_COLUMNS,
    TARGET_COLUMN,
    build_location_features,
    feature_source_max_time_is_safe,
    feature_list_sha256,
    materialize_feature_dataset,
    persistence_predictions,
    validate_dataset_manifest,
    write_feature_contract,
    write_distribution_comparison,
    write_source_alignment,
)
from ml.metrics import regression_metrics
from ml.train_forecast_t2h_v1_1 import (
    _reload_prediction_difference,
    validate_model_manifest,
    write_checksums,
)


def _source_frames(hours: int = 48, *, start: datetime | None = None):
    start = start or datetime(2020, 1, 1, tzinfo=timezone.utc)
    times = pd.date_range(start, periods=hours, freq="h", tz="UTC")
    location_id = "test-location"
    location = {
        "location_id": location_id,
        "name": "Test",
        "latitude": 10.0,
        "longitude": 105.0,
    }
    temperature = np.arange(hours, dtype=np.float64) / 10.0 + 20.0
    predictors = pd.DataFrame({
        "location_id": location_id,
        "valid_time": times,
        "temperature_c": temperature,
        "humidity_pct": 70.0 + np.arange(hours) % 3,
        "precipitation_mm": np.arange(hours) % 4,
        "pressure_hpa": 1000.0 + np.arange(hours) % 5,
        "wind_speed_kmh": 10.0 + np.arange(hours) % 6,
        "wind_gust_kmh": 20.0 + np.arange(hours) % 7,
        "weather_code": np.arange(hours) % 4,
        "model_id": "ecmwf_ifs",
        "chunk_id": "predictor-test-chunk",
        "provider_grid_latitude": 10.0,
        "provider_grid_longitude": 105.0,
    })
    targets = pd.DataFrame({
        "location_id": location_id,
        "valid_time": times,
        "temperature_target_c": temperature + 0.5,
        "model_id": "era5",
        "chunk_id": "target-test-chunk",
        "provider_grid_latitude": 10.0,
        "provider_grid_longitude": 105.0,
    })
    return location, predictors, targets


def _mock_open_meteo_handler(request: httpx.Request) -> httpx.Response:
    params = request.url.params
    latitudes = [float(item) for item in params["latitude"].split(",")]
    longitudes = [float(item) for item in params["longitude"].split(",")]
    start = date.fromisoformat(params["start_date"])
    end = date.fromisoformat(params["end_date"])
    hours = int((datetime.combine(end + timedelta(days=1), datetime.min.time()) - datetime.combine(start, datetime.min.time())).total_seconds() / 3600)
    times = [
        (datetime.combine(start, datetime.min.time()) + timedelta(hours=index)).isoformat(timespec="minutes")
        for index in range(hours)
    ]
    variables = params["hourly"].split(",")
    units = PREDICTOR_UNITS if "weather_code" in variables else TARGET_UNITS
    responses = []
    for latitude, longitude in zip(latitudes, longitudes, strict=True):
        hourly = {"time": times}
        for name in variables:
            hourly[name] = [float(index % 10) for index in range(hours)]
        responses.append({
            "latitude": latitude,
            "longitude": longitude,
            "timezone": "GMT",
            "utc_offset_seconds": 0,
            "hourly_units": {name: units[name] for name in variables},
            "hourly": hourly,
        })
    return httpx.Response(200, json=responses, request=request)


def _mock_live_forecast_handler(request: httpx.Request) -> httpx.Response:
    params = request.url.params
    latitudes = [float(item) for item in params["latitude"].split(",")]
    longitudes = [float(item) for item in params["longitude"].split(",")]
    times = [
        (datetime(2026, 10, 3, tzinfo=timezone.utc) + timedelta(hours=index)).isoformat(timespec="minutes")
        for index in range(24)
    ]
    responses = []
    for index, (latitude, longitude) in enumerate(zip(latitudes, longitudes, strict=True)):
        hourly = {"time": times}
        for variable in PREDICTOR_UNITS:
            hourly[variable] = [float(index + hour / 10.0) for hour in range(24)]
        responses.append({
            "latitude": latitude,
            "longitude": longitude,
            "timezone": "GMT",
            "utc_offset_seconds": 0,
            "hourly_units": PREDICTOR_UNITS,
            "hourly": hourly,
        })
    return httpx.Response(200, json=responses, request=request)


def test_feature_contract_is_new_identity_with_73_v1_ordered_features(tmp_path):
    artifact = write_feature_contract(tmp_path)
    saved = json.loads((tmp_path / "feature_list.json").read_text(encoding="utf-8"))
    assert FEATURE_SET_ID == "WEATHER_FORECAST_FE_T2H_V1_1"
    assert len(MODEL_FEATURE_COLUMNS) == 73
    assert artifact["feature_count"] == 73
    assert saved["ordered_model_features"] == list(MODEL_FEATURE_COLUMNS)
    assert artifact["feature_list_sha256"] != "ec716f47ac4eca99a945a2ba1c50ba1297c1509a2aa5f80cff063042e40295bf"
    import hashlib
    assert artifact["feature_list_sha256"] == hashlib.sha256((tmp_path / "feature_list.json").read_bytes()).hexdigest()
    assert TARGET_COLUMN not in MODEL_FEATURE_COLUMNS
    assert "weather_code" not in MODEL_FEATURE_COLUMNS


def test_model_manifest_validator_binds_exact_feature_list_and_t2h_horizon():
    manifest = {
        "model_id": MODEL_ID,
        "feature_set_id": FEATURE_SET_ID,
        "source_contract_id": SOURCE_CONTRACT_ID,
        "target_column": TARGET_COLUMN,
        "target_offset_seconds": 7200,
        "feature_count": len(MODEL_FEATURE_COLUMNS),
        "feature_list_sha256": feature_list_sha256(),
        "feature_names": list(MODEL_FEATURE_COLUMNS),
        "serialization_format": "XGBoost JSON",
        "model_sha256": "a" * 64,
    }
    assert validate_model_manifest(manifest)["status"] == "PASS"
    assert validate_model_manifest(manifest)["checks"]["target_offset_seconds"] is True

    wrong_horizon = {**manifest, "target_offset_seconds": 3600}
    validation = validate_model_manifest(wrong_horizon)
    assert validation["status"] == "FAIL"
    assert "target_offset_seconds" in validation["errors"]


def test_serialized_model_reload_parity_uses_the_same_predictor_device(tmp_path):
    import xgboost as xgb

    features = ["temperature_c", "humidity_pct"]
    matrix = np.asarray([
        [10.0, 40.0],
        [11.0, 45.0],
        [12.0, 50.0],
        [13.0, 55.0],
        [14.0, 60.0],
        [15.0, 65.0],
    ], dtype=np.float32)
    target = np.asarray([10.5, 11.1, 12.4, 12.8, 14.2, 14.9], dtype=np.float32)
    training = xgb.DMatrix(matrix, label=target, feature_names=features)
    booster = xgb.train(
        {"objective": "reg:squarederror", "tree_method": "hist", "device": "cpu", "seed": 4},
        training,
        num_boost_round=4,
    )
    saved_model = tmp_path / "model.json"
    booster.save_model(str(saved_model))
    reloaded = xgb.Booster()
    reloaded.load_model(str(saved_model))

    difference = _reload_prediction_difference(
        booster,
        reloaded,
        matrix,
        features,
        device="cpu",
        best_rounds=4,
    )
    assert difference == pytest.approx(0.0, abs=1e-7)


def test_t2h_persistence_baseline_arithmetic_is_explicit():
    feature_names = list(MODEL_FEATURE_COLUMNS)
    feature_matrix = np.zeros((1, len(feature_names)), dtype=np.float32)
    feature_matrix[0, feature_names.index("temperature_c")] = 30.0
    prediction = persistence_predictions(feature_matrix, feature_names)
    metrics = regression_metrics(np.asarray([27.0]), prediction)
    assert prediction.tolist() == [30.0]
    assert metrics["mae"] == pytest.approx(3.0)
    assert metrics["rmse"] == pytest.approx(3.0)
    assert metrics["mean_error"] == pytest.approx(3.0)


def test_checksum_inventory_lists_a_run_local_model_once(tmp_path):
    model = tmp_path / "run" / "frozen.json"
    model.parent.mkdir(parents=True)
    model.write_text('{"learner":{}}', encoding="utf-8")
    (model.parent / "metrics.json").write_text("{}", encoding="utf-8")
    inventory = write_checksums(model.parent, model)
    entries = [item for item in inventory["files"] if item["path"] == "frozen.json"]
    assert len(entries) == 1
    assert entries[0]["kind"] == "model_artifact"
    assert entries[0]["sha256"]


def test_t2h_label_is_exact_same_location_timestamp_and_features_end_at_feature_time():
    location, predictors, targets = _source_frames()
    rows, summary = build_location_features(predictors, targets, location)
    assert len(rows) == 22
    first = rows.iloc[0]
    assert first["feature_time"] == pd.Timestamp("2020-01-02T00:00:00Z")
    assert first["target_time"] == pd.Timestamp("2020-01-02T02:00:00Z")
    assert first["target_temperature_2h"] == pytest.approx(23.1)
    assert first["target_location_id"] == first["location_id"]
    assert first["temperature_c"] == pytest.approx(22.4)
    assert first["temp_lag_1h"] == pytest.approx(22.3)
    assert first["temp_roll_mean_3h"] == pytest.approx(22.3)
    assert first["temp_roll_std_3h"] == pytest.approx(np.sqrt(2.0 / 300.0))
    assert first["precipitation_sum_3h"] == pytest.approx(5.0)
    assert first["local_hour"] == 7
    assert first["temp_delta_1h"] == pytest.approx(0.1)
    assert (rows["target_time"] - rows["feature_time"]).dt.total_seconds().eq(7200).all()
    assert (rows["max_feature_source_time"] <= rows["feature_time"]).all()
    assert summary["rows_missing_required_feature_history"] == 24
    assert summary["feature_count"] == 73


def test_source_hour_gaps_are_not_filled_and_invalidate_trailing_history():
    location, predictors, targets = _source_frames()
    missing_time = predictors.loc[30, "valid_time"]
    predictors = predictors[predictors["valid_time"] != missing_time].reset_index(drop=True)
    rows, summary = build_location_features(predictors, targets, location)
    eligible_times = set(rows["feature_time"])
    assert missing_time not in eligible_times
    assert missing_time + pd.Timedelta(hours=10) not in eligible_times
    assert summary["predictor_missing_hour_count"] == 1
    assert summary["eligible_rows"] < 22


def test_missing_exact_t2h_target_drops_only_the_matching_feature_row():
    location, predictors, targets = _source_frames()
    missing_target_time = targets.loc[32, "valid_time"]
    targets = targets[targets["valid_time"] != missing_target_time].reset_index(drop=True)
    rows, summary = build_location_features(predictors, targets, location)
    assert pd.Timestamp("2020-01-02T06:00:00Z") not in set(rows["feature_time"])
    assert summary["target_missing_hour_count"] == 1
    # One removed interior target plus the two expected right-edge targets.
    assert summary["rows_missing_exact_t2h_target_after_features"] == 3


def test_future_source_values_do_not_change_features_at_earlier_feature_time():
    location, predictors, targets = _source_frames()
    original, _ = build_location_features(predictors, targets, location)
    changed_predictors = predictors.copy()
    future_rows = changed_predictors["valid_time"] > pd.Timestamp("2020-01-02T04:00:00Z")
    changed_predictors.loc[future_rows, "temperature_c"] += 99.0
    changed_predictors.loc[future_rows, "humidity_pct"] = 0.0
    changed, _ = build_location_features(changed_predictors, targets, location)
    timestamp = pd.Timestamp("2020-01-02T04:00:00Z")
    left = original.loc[original["feature_time"] == timestamp, list(MODEL_FEATURE_COLUMNS)].to_numpy()
    right = changed.loc[changed["feature_time"] == timestamp, list(MODEL_FEATURE_COLUMNS)].to_numpy()
    np.testing.assert_array_equal(left, right)


def test_explicit_utc_feature_source_time_guard():
    feature_time = pd.Timestamp("2024-01-01T12:00:00Z")
    assert feature_source_max_time_is_safe(feature_time, [feature_time - pd.Timedelta(hours=24), feature_time])
    assert not feature_source_max_time_is_safe(feature_time, [feature_time + pd.Timedelta(hours=1)])


def test_request_parameters_pin_model_units_and_utc():
    locations = load_nationwide_locations()[:2]
    parameters = build_request_parameters(
        locations,
        "predictors",
        date(2020, 1, 1),
        date(2020, 1, 2),
    )
    assert parameters["models"] == "ecmwf_ifs"
    assert parameters["timezone"] == "GMT"
    assert parameters["temperature_unit"] == "celsius"
    assert parameters["wind_speed_unit"] == "kmh"
    assert parameters["precipitation_unit"] == "mm"
    assert len(parameters["latitude"].split(",")) == 2
    assert SOURCE_CONFIG["targets"]["model_id"] == "era5"
    assert estimated_api_call_units("predictors", date(2024, 1, 1), date(2024, 1, 14), 10) == pytest.approx(7.0)
    assert estimated_api_call_units("targets", date(2024, 1, 1), date(2024, 1, 14), 10) == pytest.approx(1.0)


def test_batch_mapping_rejects_wrong_response_order():
    locations = [
        {"location_id": "A", "latitude": 10.0, "longitude": 100.0},
        {"location_id": "B", "latitude": 20.0, "longitude": 110.0},
    ]
    payload = [
        {"latitude": 20.0, "longitude": 110.0},
        {"latitude": 10.0, "longitude": 100.0},
    ]
    with pytest.raises(ValueError, match="response order cannot be trusted"):
        map_batch_responses(locations, payload, source_name="predictors")


def test_hourly_response_requires_units_sorted_unique_utc_and_required_fields():
    locations = load_nationwide_locations()[:1]
    parameters = build_request_parameters(locations, "predictors", date(2020, 1, 1), date(2020, 1, 1))
    response = _mock_open_meteo_handler(httpx.Request("GET", "https://example.test", params=parameters)).json()[0]
    times, arrays, units = validate_hourly_response(response, source_name="predictors")
    assert len(times) == 24
    assert arrays["temperature_2m"][0] == 0.0
    assert units["wind_gusts_10m"] == "km/h"
    response["hourly_units"]["temperature_2m"] = "F"
    with pytest.raises(ValueError, match="unexpected unit"):
        validate_hourly_response(response, source_name="predictors")


def test_fetch_json_retries_bounded_retryable_status_and_stops_on_client_error():
    attempts: list[int] = []
    sleeps: list[float] = []
    results: list[tuple[int, int | None]] = []
    call_count = 0

    def retryable_handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(503, headers={"Retry-After": "0"}, request=request)
        return httpx.Response(200, json={"ok": True}, request=request)

    with httpx.Client(transport=httpx.MockTransport(retryable_handler)) as client:
        result = fetch_json(
            client,
            "https://example.test",
            {},
            max_retries=3,
            sleep=sleeps.append,
            on_attempt=attempts.append,
            on_result=lambda attempt, status: results.append((attempt, status)),
        )
    assert result == {"ok": True}
    assert attempts == [1, 2]
    assert sleeps == [0.0]
    assert results == [(1, 503), (2, 200)]

    client_errors = 0

    def bad_request_handler(request: httpx.Request) -> httpx.Response:
        nonlocal client_errors
        client_errors += 1
        return httpx.Response(400, request=request)

    with httpx.Client(transport=httpx.MockTransport(bad_request_handler)) as client:
        with pytest.raises(RuntimeError, match="bounded attempts"):
            fetch_json(client, "https://example.test", {}, max_retries=4, sleep=lambda _: None)
    assert client_errors == 1


def test_429_without_retry_after_waits_one_minute_and_is_observable():
    calls = 0
    sleeps: list[float] = []
    statuses: list[int | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                429,
                json={"error": True, "reason": "Minutely API request limit exceeded. Please try again in one minute."},
                request=request,
            )
        return httpx.Response(200, json={"ok": True}, request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = fetch_json(
            client,
            "https://example.test",
            {},
            max_retries=3,
            sleep=sleeps.append,
            on_result=lambda _, status: statuses.append(status),
        )
    assert result == {"ok": True}
    assert sleeps == [60.0]
    assert statuses == [429, 200]


def test_full_download_is_resumable_and_raw_validation_reports_exact_coverage(tmp_path):
    data_root = tmp_path / "historical_forecast"
    artifact_root = tmp_path / "evidence"
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.host or "")
        return _mock_open_meteo_handler(request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        manifest = download_range(
            data_root=data_root,
            artifact_root=artifact_root,
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 1),
            batch_size=63,
            max_retries=2,
            request_delay_seconds=0,
            client=client,
            sleep=lambda _: None,
        )
        assert len(calls) == 2
        assert len(manifest["chunks"]) == 2
        assert all(item["status"] == "SUCCESS" and item["attempts"] == 1 for item in manifest["chunks"])
        resumed = download_range(
            data_root=data_root,
            artifact_root=artifact_root,
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 1),
            batch_size=63,
            max_retries=2,
            request_delay_seconds=0,
            client=client,
            sleep=lambda _: None,
        )
    assert len(calls) == 2
    assert resumed["completed_chunk_count"] == 2
    report = validate_normalized_sources(
        data_root=data_root,
        artifact_root=artifact_root,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 1),
    )
    assert report["status"] == "PASS"
    assert report["sources"]["predictors"]["normalized_rows"] == 63 * 24
    assert report["sources"]["targets"]["normalized_rows"] == 63 * 24
    assert report["sources"]["predictors"]["duplicate_rows"] == 0
    assert report["sources"]["predictors"]["gap_or_unexpected_hour_count"] == 0

    first_location = load_nationwide_locations()[0]
    predictor_directory = (
        data_root
        / "normalized"
        / "predictors"
        / "model_id=ecmwf_ifs"
        / "year=2020"
        / f"location_id={first_location['location_id']}"
    )
    predictor_file = next(predictor_directory.glob("*.parquet"))
    table = pq.read_table(predictor_file)
    pq.write_table(table.slice(1), predictor_file, compression="snappy")
    gap_report = validate_normalized_sources(
        data_root=data_root,
        artifact_root=artifact_root,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 1, 1),
    )
    assert gap_report["status"] == "PASS_WITH_GAPS"
    assert gap_report["sources"]["predictors"]["gap_or_unexpected_hour_count"] == 1
    assert gap_report["warnings"]


def test_daily_quota_guard_defers_before_sending_an_over_budget_request(tmp_path):
    sent_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent_requests.append(request)
        return _mock_open_meteo_handler(request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        manifest = download_range(
            data_root=tmp_path / "data",
            artifact_root=tmp_path / "evidence",
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 1),
            batch_size=63,
            max_retries=2,
            request_delay_seconds=0,
            max_daily_api_call_units=0.1,
            client=client,
            sleep=lambda _: None,
        )
    assert sent_requests == []
    assert manifest["status"] == "DEFERRED_DAILY_LIMIT"
    assert manifest["deferred_chunk_count"] == 2


def test_daily_quota_guard_counts_rejected_429_attempt_units(tmp_path):
    sent_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent_requests.append(request)
        return _mock_open_meteo_handler(request)

    data_root = tmp_path / "data"
    artifact_root = tmp_path / "evidence"
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        initial = download_range(
            data_root=data_root,
            artifact_root=artifact_root,
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 1),
            batch_size=63,
            request_delay_seconds=0,
            max_daily_api_call_units=1_000,
            client=client,
            sleep=lambda _: None,
        )
        assert initial["completed_chunk_count"] == 2

        manifest_path = artifact_root / "download_manifest.json"
        saved = json.loads(manifest_path.read_text(encoding="utf-8"))
        predictor = next(entry for entry in saved["chunks"] if entry["source_role"] == "predictors")
        predictor["status"] = "FAILED"
        predictor["failure_reason"] = "simulated interrupted resume"
        saved["daily_quota_ledger"].append({
            "at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "chunk_id": "simulated-rejected-attempt",
            "attempt": 1,
            "estimated_api_call_units": 100.0,
            "charged": False,
            "response_status": 429,
        })
        manifest_path.write_text(json.dumps(saved), encoding="utf-8")
        sent_requests.clear()

        resumed = download_range(
            data_root=data_root,
            artifact_root=artifact_root,
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 1),
            batch_size=63,
            request_delay_seconds=0,
            max_daily_api_call_units=10.0,
            client=client,
            sleep=lambda _: None,
        )

    assert sent_requests == []
    assert resumed["status"] == "DEFERRED_DAILY_LIMIT"
    assert resumed["estimated_api_call_units_used_today"] > 100.0
    assert resumed["estimated_api_call_units_charged_today"] < 10.0


def test_rolling_quota_wait_counts_429_attempts_and_waits_for_oldest_units_to_expire():
    now = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
    ledger = [
        {
            "at_utc": (now - timedelta(seconds=59)).isoformat().replace("+00:00", "Z"),
            "estimated_api_call_units": 400.0,
            "charged": False,
            "response_status": 429,
        },
        {
            "at_utc": (now - timedelta(seconds=30)).isoformat().replace("+00:00", "Z"),
            "estimated_api_call_units": 100.0,
            "charged": True,
            "response_status": 200,
        },
    ]
    wait = quota_wait_seconds(
        ledger,
        now=now,
        requested_units=200.0,
        limit=600.0,
        window_seconds=60.0,
    )
    assert wait == pytest.approx(2.0)


def test_rolling_quota_wait_is_zero_when_hour_window_has_capacity():
    now = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
    ledger = [{
        "at_utc": (now - timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
        "estimated_api_call_units": 4_000.0,
        "charged": True,
    }]
    assert quota_wait_seconds(
        ledger,
        now=now,
        requested_units=1_000.0,
        limit=5_000.0,
        window_seconds=3_600.0,
    ) == 0.0


def test_rolling_quota_wait_rejects_single_request_over_window_limit():
    with pytest.raises(ValueError, match="single request estimate"):
        quota_wait_seconds(
            [],
            now=datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc),
            requested_units=601.0,
            limit=600.0,
            window_seconds=60.0,
        )


def test_live_forecast_probe_persists_best_match_vs_pinned_ifs_sample(tmp_path):
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return _mock_live_forecast_handler(request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        report = probe_live_forecast_alignment(
            artifact_root=tmp_path,
            client=client,
            sleep=lambda _: None,
        )
    saved = json.loads((tmp_path / "live_forecast_probe.json").read_text(encoding="utf-8"))
    assert len(calls) == 2
    assert "models" not in calls[0].url.params
    assert calls[1].url.params["models"] == "ecmwf_ifs"
    assert report["locations"] == 63
    assert report["hours_per_location"] == [24]
    assert report["comparison"]["exact_match"] is True
    assert report["comparison"]["coordinate_mismatches"] == 0
    assert report["estimated_api_call_units"] == pytest.approx(6.3)
    assert len(saved["raw_responses"]["best_match"]) == 63


def test_materializer_writes_63_location_partitions_and_target_time_splits(tmp_path):
    locations = load_nationwide_locations()
    source_root = tmp_path / "source"
    start = pd.Timestamp("2023-12-29T00:00:00Z")
    end_exclusive = pd.Timestamp("2025-01-04T00:00:00Z")
    for location_index, location in enumerate(locations):
        location_id = str(location["location_id"])
        for year in range(2023, 2026):
            year_start = max(start, pd.Timestamp(f"{year}-01-01T00:00:00Z"))
            year_end = min(end_exclusive, pd.Timestamp(f"{year + 1}-01-01T00:00:00Z"))
            times = pd.date_range(year_start, year_end, freq="h", inclusive="left", tz="UTC")
            offset = np.arange(len(times), dtype=np.float64)
            temperature = 18.0 + location_index * 0.01 + offset / 10000.0
            common = {
                "provider": "open-meteo",
                "source_contract_id": SOURCE_CONTRACT_ID,
                "retrieved_at_utc": "2026-10-03T00:00:00Z",
                "location_id": location_id,
                "city": str(location.get("name") or location_id),
                "latitude": float(location["latitude"]),
                "longitude": float(location["longitude"]),
                "provider_grid_latitude": float(location["latitude"]),
                "provider_grid_longitude": float(location["longitude"]),
                "valid_time": times,
            }
            predictor = pd.DataFrame({
                **common,
                "source_role": "predictors",
                "endpoint": SOURCE_CONFIG["predictors"]["endpoint"],
                "model_id": "ecmwf_ifs",
                "chunk_id": f"predictor-fixture-{year}",
                "temperature_c": temperature,
                "humidity_pct": 65.0 + offset % 20,
                "precipitation_mm": (offset % 4) / 10.0,
                "pressure_hpa": 1005.0 + offset % 7,
                "wind_speed_kmh": 8.0 + offset % 5,
                "wind_gust_kmh": 15.0 + offset % 9,
                "weather_code": (offset % 4).astype(np.int16),
            })
            target = pd.DataFrame({
                **common,
                "source_role": "targets",
                "endpoint": SOURCE_CONFIG["targets"]["endpoint"],
                "model_id": "era5",
                "chunk_id": f"target-fixture-{year}",
                "temperature_target_c": temperature + 0.4,
            })
            for source_name, frame, model_id in (
                ("predictors", predictor, "ecmwf_ifs"),
                ("targets", target, "era5"),
            ):
                destination = (
                    source_root
                    / "normalized"
                    / source_name
                    / f"model_id={model_id}"
                    / f"year={year}"
                    / f"location_id={location_id}"
                    / f"fixture-{source_name}-{year}.parquet"
                )
                destination.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), destination, compression="snappy")

    dataset_root = tmp_path / "ml-ready"
    artifact_root = tmp_path / "evidence"
    manifest = materialize_feature_dataset(
        data_root=source_root,
        dataset_root=dataset_root,
        artifact_root=artifact_root,
        start_date=date(2023, 12, 29),
        end_date=date(2025, 1, 3),
    )
    validation = validate_dataset_manifest(dataset_root, artifact_root)
    split_validation = json.loads((artifact_root / "split_validation.json").read_text(encoding="utf-8"))
    statistics = json.loads((artifact_root / "feature_statistics_train.json").read_text(encoding="utf-8"))

    live_probe_path = artifact_root / "live_forecast_probe.json"
    probe_responses = []
    for location_index, location in enumerate(locations):
        hourly = {"time": [f"2026-10-03T{hour:02d}:00" for hour in range(24)]}
        for variable in PREDICTOR_UNITS:
            hourly[variable] = [float(location_index + hour / 10.0) for hour in range(24)]
        probe_responses.append({
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "timezone": "GMT",
            "hourly": hourly,
        })
    live_probe_path.write_text(json.dumps({
        "observed_at_utc": "2026-10-03T00:00:00Z",
        "locations": 63,
        "hours_per_location": [24],
        "comparison": {"exact_match": True, "coordinate_mismatches": 0},
        "raw_responses": {"best_match": probe_responses, "ecmwf_ifs": probe_responses},
    }), encoding="utf-8")
    alignment = write_source_alignment(artifact_root, live_probe_path)
    distributions = write_distribution_comparison(dataset_root, live_probe_path, artifact_root)

    assert manifest["feature_count"] == 73
    assert sum(manifest["rows_by_split"].values()) == manifest["total_rows"]
    assert all(count == 63 for count in manifest["locations_by_split"].values())
    assert validation["status"] == "PASS"
    assert split_validation["rows_by_split"]["TRAIN"] > 0
    assert split_validation["rows_by_split"]["VALIDATION"] > 0
    assert split_validation["rows_by_split"]["TEST"] > 0
    assert split_validation["test_partition_materialized_before_freeze"] is True
    assert split_validation["test_partition_values_loaded_by_trainer_before_freeze"] is False
    assert len(statistics) == 73
    assert {"count", "mean", "std", "min", "p01", "p05", "p50", "p95", "p99", "max"} <= set(next(iter(statistics.values())))
    assert alignment["status"] == "PASS_WITH_LIMITATIONS"
    assert alignment["sources"]["current_live_forecast"]["probe_comparison"]["exact_match"] is True
    assert distributions["status"] == "DESCRIPTIVE_ONLY"
    assert distributions["training_row_count"] == manifest["rows_by_split"]["TRAIN"]
    assert distributions["variables"]["temperature_2m"]["live_forecast_explicit_ecmwf_ifs"]["count"] == 63 * 24
    assert distributions["variables"]["weather_code"]["model_input"] is False
    assert distributions["variables"]["weather_code"]["live_forecast_best_match"]["count"] == 63 * 24
