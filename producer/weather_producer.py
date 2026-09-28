from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sys
import time
from typing import Any, Iterable, Iterator

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[1]
HISTORICAL_DIR = PROJECT_ROOT / "historical"
if str(HISTORICAL_DIR) not in sys.path:
    sys.path.insert(0, str(HISTORICAL_DIR))

from location_catalog import load_catalog


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "weather.raw"
OPEN_METEO_HOST = "api.open-meteo.com"
OPEN_METEO_URL = f"https://{OPEN_METEO_HOST}/v1/forecast"
POLL_INTERVAL_SECONDS = 10
LIVE_BATCH_SIZE = 63
MAX_RETRIES = 3
REQUEST_TIMEOUT_SECONDS = 30
CATALOG_FILE = HISTORICAL_DIR / "locations.json"
CURRENT_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "pressure_msl",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
]


def load_cities(catalog_path: str | Path = CATALOG_FILE) -> list[dict[str, Any]]:
    """Load the shared, validated catalog; retained name for producer compatibility."""
    return load_catalog(catalog_path)


def chunks(locations: list[dict[str, Any]], size: int = LIVE_BATCH_SIZE) -> Iterator[list[dict[str, Any]]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    for offset in range(0, len(locations), size):
        yield locations[offset:offset + size]


def normalize_time(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("provider current.time is missing")
    if len(value) == 16:
        value += ":00"
    return value if value.endswith("Z") else value + "Z"


def _retry_delay(response: httpx.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(max(float(retry_after), 0), 30)
            except ValueError:
                pass
    return float(min(2 ** (attempt - 1), 10))


def _normalized_responses(locations: list[dict[str, Any]], payload: Any) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    responses = [payload] if isinstance(payload, dict) else payload
    if not isinstance(responses, list):
        raise ValueError("Open-Meteo response must be an object or a list")
    if len(responses) != len(locations):
        raise ValueError(
            f"Open-Meteo returned {len(responses)} locations for {len(locations)} coordinates"
        )
    if any(not isinstance(item, dict) for item in responses):
        raise ValueError("each Open-Meteo response item must be an object")
    return list(zip(locations, responses, strict=True))


def _event_for_location(
    location: dict[str, Any],
    response: dict[str, Any],
    *,
    ingestion_time: str | None = None,
) -> dict[str, Any]:
    current = response.get("current")
    if not isinstance(current, dict):
        raise ValueError(f"missing current weather values for {location['location_id']}")
    if response.get("utc_offset_seconds", 0) != 0:
        raise ValueError(f"provider response for {location['location_id']} is not UTC")

    latitude = response.get("latitude")
    longitude = response.get("longitude")
    if (
        not isinstance(latitude, (int, float))
        or isinstance(latitude, bool)
        or not math.isfinite(latitude)
        or not -90 <= latitude <= 90
        or not isinstance(longitude, (int, float))
        or isinstance(longitude, bool)
        or not math.isfinite(longitude)
        or not -180 <= longitude <= 180
    ):
        raise ValueError(f"provider returned invalid coordinates for {location['location_id']}")

    event_time = normalize_time(current.get("time"))
    ingestion_time = ingestion_time or datetime.now(timezone.utc).isoformat()
    return {
        "event_id": f"{location['location_id']}_{event_time}",
        "event_type": "WEATHER_OBSERVATION",
        "location_id": location["location_id"],
        "city": location["name"],
        "latitude": latitude,
        "longitude": longitude,
        "event_time": event_time,
        "ingestion_time": ingestion_time,
        "temperature_c": current.get("temperature_2m"),
        "humidity_pct": current.get("relative_humidity_2m"),
        "precipitation_mm": current.get("precipitation"),
        "pressure_hpa": current.get("pressure_msl"),
        "wind_speed_kmh": current.get("wind_speed_10m"),
        "wind_gust_kmh": current.get("wind_gusts_10m"),
        "weather_code": current.get("weather_code"),
        "source": "OPEN_METEO",
    }


def map_weather_batch(
    locations: list[dict[str, Any]],
    payload: Any,
    *,
    ingestion_time: str | None = None,
) -> list[dict[str, Any]]:
    """Map the provider's ordered multi-coordinate response to requested IDs."""
    return [
        _event_for_location(location, response, ingestion_time=ingestion_time)
        for location, response in _normalized_responses(locations, payload)
    ]


def fetch_weather(
    client: httpx.Client,
    locations: list[dict[str, Any]],
    *,
    max_retries: int = MAX_RETRIES,
    sleep=time.sleep,
    retry_counts: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    if not locations:
        raise ValueError("cannot fetch an empty location batch")
    params = {
        "latitude": ",".join(str(location["latitude"]) for location in locations),
        "longitude": ",".join(str(location["longitude"]) for location in locations),
        "current": ",".join(CURRENT_VARIABLES),
        "timezone": "UTC",
    }
    batch_key = ",".join(location["location_id"] for location in locations)
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        response = None
        try:
            response = client.get(OPEN_METEO_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 429 or response.status_code >= 500:
                last_error = httpx.HTTPStatusError(
                    f"retryable Open-Meteo status {response.status_code}",
                    request=response.request,
                    response=response,
                )
                if attempt < max_retries:
                    if retry_counts is not None:
                        retry_counts[batch_key] = retry_counts.get(batch_key, 0) + 1
                    sleep(_retry_delay(response, attempt))
                    continue
                break
            response.raise_for_status()
            return map_weather_batch(locations, response.json())
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_error = exc
            retryable = not isinstance(exc, httpx.HTTPStatusError) or (
                exc.response.status_code == 429 or exc.response.status_code >= 500
            )
            if not retryable or attempt >= max_retries:
                break
            if retry_counts is not None:
                retry_counts[batch_key] = retry_counts.get(batch_key, 0) + 1
            sleep(_retry_delay(response, attempt))
        except (ValueError, KeyError) as exc:
            last_error = exc
            break
    raise RuntimeError(
        f"live request failed for IDs={[item['location_id'] for item in locations]} "
        f"after {max_retries} attempts: {last_error}"
    ) from last_error


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
        return
    print(
        f"[KAFKA] topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def publish_poll(
    client: httpx.Client,
    producer,
    topic: str,
    locations: list[dict[str, Any]],
    *,
    batch_size: int = LIVE_BATCH_SIZE,
    max_retries: int = MAX_RETRIES,
    sleep=time.sleep,
) -> dict[str, Any]:
    poll_timestamp = datetime.now(timezone.utc).isoformat()
    enqueued: list[dict[str, Any]] = []
    failed_batches: list[dict[str, Any]] = []
    retry_counts: dict[str, int] = {}

    for batch in chunks(locations, batch_size):
        try:
            events = fetch_weather(
                client,
                batch,
                max_retries=max_retries,
                sleep=sleep,
                retry_counts=retry_counts,
            )
            for event in events:
                producer.produce(
                    topic=topic,
                    key=event["location_id"],
                    value=json.dumps(event, ensure_ascii=False),
                    callback=delivery_report,
                )
                producer.poll(0)
                enqueued.append(event)
                print(
                    f"[WEATHER] {event['city']} "
                    f"temp={event['temperature_c']}°C "
                    f"humidity={event['humidity_pct']}%"
                )
        except Exception as exc:
            failed = {
                "poll_timestamp": poll_timestamp,
                "location_ids": [item["location_id"] for item in batch],
                "error": str(exc),
            }
            failed_batches.append(failed)
            print(f"[ERROR] failed_ids={failed['location_ids']} poll={poll_timestamp} error={exc}")

    remaining = producer.flush(timeout=30)
    return {
        "poll_timestamp": poll_timestamp,
        "requested_locations": len(locations),
        "enqueued_messages": len(enqueued),
        "unique_location_ids": sorted({event["location_id"] for event in enqueued}),
        "unique_event_ids": len({event["event_id"] for event in enqueued}),
        "failed_batches": failed_batches,
        "retry_counts": retry_counts,
        "producer_flush_remaining": remaining,
        "topic": topic,
    }


def _write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    temporary.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish live Open-Meteo observations for the canonical catalog.")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--catalog", type=Path, default=CATALOG_FILE)
    parser.add_argument("--polls", type=int, default=0, help="Number of polls; 0 runs until interrupted.")
    parser.add_argument("--interval-seconds", type=float, default=POLL_INTERVAL_SECONDS)
    parser.add_argument("--batch-size", type=int, default=LIVE_BATCH_SIZE)
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES)
    parser.add_argument("--summary-json", type=Path)
    args = parser.parse_args(argv)
    if args.polls < 0:
        parser.error("--polls must be zero or greater")
    if args.interval_seconds < 0:
        parser.error("--interval-seconds cannot be negative")
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    if args.max_retries <= 0:
        parser.error("--max-retries must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError("confluent-kafka is required to run the live producer") from exc

    locations = load_cities(args.catalog)
    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "weather-openmeteo-producer",
    })
    print("Weather Producer started")
    print(f"Kafka: {args.bootstrap_servers}")
    print(f"Topic: {args.topic}")
    print(f"Locations: {len(locations)}")

    poll_results = []
    completed_polls = 0
    with httpx.Client() as client:
        while args.polls == 0 or completed_polls < args.polls:
            result = publish_poll(
                client,
                producer,
                args.topic,
                locations,
                batch_size=args.batch_size,
                max_retries=args.max_retries,
            )
            poll_results.append(result)
            completed_polls += 1
            if args.summary_json:
                _write_summary(args.summary_json, {
                    "catalog_location_count": len(locations),
                    "polls_completed": completed_polls,
                    "poll_results": poll_results,
                })
            if result["failed_batches"] or result["producer_flush_remaining"]:
                if args.polls and completed_polls >= args.polls:
                    return 1
            if args.polls and completed_polls >= args.polls:
                break
            time.sleep(args.interval_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
