import json
import time
import uuid
from datetime import datetime, timezone

import httpx
from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "weather.raw"

OPEN_METEO_HOST = "api.open-meteo.com"
OPEN_METEO_URL = f"https://{OPEN_METEO_HOST}/v1/forecast"

POLL_INTERVAL_SECONDS = 10


CITIES = [
    {
        "id": "VN_HCM",
        "name": "Ho Chi Minh City",
        "latitude": 10.8231,
        "longitude": 106.6297,
    },
    {
        "id": "VN_HANOI",
        "name": "Hanoi",
        "latitude": 21.0285,
        "longitude": 105.8542,
    },
    {
        "id": "VN_DANANG",
        "name": "Da Nang",
        "latitude": 16.0544,
        "longitude": 108.2022,
    },
    {
        "id": "VN_CANTHO",
        "name": "Can Tho",
        "latitude": 10.0452,
        "longitude": 105.7469,
    },
    {
        "id": "VN_HUE",
        "name": "Hue",
        "latitude": 16.4637,
        "longitude": 107.5909,
    },
]


producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "weather-openmeteo-producer",
    }
)


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
        return

    print(
        f"[KAFKA] topic={msg.topic()} "
        f"partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def fetch_weather(client: httpx.Client, city: dict) -> dict:
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current": ",".join(
            [
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "pressure_msl",
                "wind_speed_10m",
                "wind_gusts_10m",
                "weather_code",
            ]
        ),
        "timezone": "UTC",
    }

    response = client.get(
        OPEN_METEO_URL,
        params=params,
        timeout=20,
    )

    response.raise_for_status()

    body = response.json()
    current = body["current"]

    event_time = current["time"]

    if len(event_time) == 16:
        event_time += ":00"

    if not event_time.endswith("Z"):
        event_time += "Z"

    event_id = (
        f"{city['id']}_"
        f"{event_time}"
    )
    return {
        "event_id": event_id,
        "event_type": "WEATHER_OBSERVATION",

        "location_id": city["id"],
        "city": city["name"],

        "latitude": body["latitude"],
        "longitude": body["longitude"],

        "event_time": event_time,
        "ingestion_time": datetime.now(timezone.utc).isoformat(),

        "temperature_c": current.get("temperature_2m"),
        "humidity_pct": current.get("relative_humidity_2m"),
        "precipitation_mm": current.get("precipitation"),
        "pressure_hpa": current.get("pressure_msl"),

        "wind_speed_kmh": current.get("wind_speed_10m"),
        "wind_gust_kmh": current.get("wind_gusts_10m"),

        "weather_code": current.get("weather_code"),

        "source": "OPEN_METEO",
    }


def main():
    print("Weather Producer started")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {TOPIC}")

    with httpx.Client() as client:
        while True:

            for city in CITIES:
                try:
                    event = fetch_weather(client, city)

                    producer.produce(
                        topic=TOPIC,
                        key=event["location_id"],
                        value=json.dumps(event),
                        callback=delivery_report,
                    )

                    producer.poll(0)

                    print(
                        f"[WEATHER] "
                        f"{event['city']} "
                        f"temp={event['temperature_c']}°C "
                        f"humidity={event['humidity_pct']}%"
                    )

                except Exception as exc:
                    print(
                        f"[ERROR] {city['name']}: {exc}"
                    )

            producer.flush()

            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()