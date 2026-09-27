import gzip
import json
import time
from pathlib import Path
from datetime import datetime, timezone

import httpx


BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

START_YEAR = 2020
END_YEAR = 2025

BATCH_SIZE = 10
REQUEST_DELAY_SECONDS = 5

LOCATIONS_FILE = Path(
    "/opt/project/historical/locations.json"
)

OUTPUT_DIR = Path(
    "/opt/project/history-data/historical/raw"
)


HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "pressure_msl",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
]


def load_locations():
    with open(
        LOCATIONS_FILE,
        "r",
        encoding="utf-8"
    ) as file:
        return json.load(file)


def chunks(items, size):
    for index in range(0, len(items), size):
        yield items[index:index + size]


def normalize_time(value):
    if len(value) == 16:
        value += ":00"

    if not value.endswith("Z"):
        value += "Z"

    return value


def fetch_batch(
    client,
    locations,
    year,
    max_retries=8
):
    latitude = ",".join(
        str(x["latitude"])
        for x in locations
    )

    longitude = ",".join(
        str(x["longitude"])
        for x in locations
    )

    params = {
        "latitude": latitude,
        "longitude": longitude,

        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",

        "hourly": ",".join(
            HOURLY_VARIABLES
        ),

        "timezone": "UTC",
    }

    for attempt in range(max_retries):

        try:
            response = client.get(
                BASE_URL,
                params=params,
                timeout=120,
            )

            # Rate limited
            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                if retry_after:
                    wait_seconds = int(
                        retry_after
                    )
                else:
                    # 5, 10, 20, 40, 80...
                    wait_seconds = min(
                        5 * (2 ** attempt),
                        120
                    )

                print(
                    f"[RATE LIMIT] "
                    f"year={year} "
                    f"attempt={attempt + 1}/{max_retries} "
                    f"waiting={wait_seconds}s"
                )

                time.sleep(
                    wait_seconds
                )

                continue

            response.raise_for_status()

            payload = response.json()

            if not isinstance(
                payload,
                list
            ):
                payload = [payload]

            return payload

        except httpx.TimeoutException:

            wait_seconds = min(
                5 * (2 ** attempt),
                120
            )

            print(
                f"[TIMEOUT] "
                f"year={year} "
                f"attempt={attempt + 1}/{max_retries} "
                f"waiting={wait_seconds}s"
            )

            time.sleep(
                wait_seconds
            )

        except httpx.HTTPStatusError as exc:

            # Retry server-side failures
            if (
                exc.response.status_code
                >= 500
            ):

                wait_seconds = min(
                    5 * (2 ** attempt),
                    120
                )

                print(
                    f"[SERVER ERROR] "
                    f"status="
                    f"{exc.response.status_code} "
                    f"attempt="
                    f"{attempt + 1}/{max_retries} "
                    f"waiting="
                    f"{wait_seconds}s"
                )

                time.sleep(
                    wait_seconds
                )

                continue

            raise

    raise RuntimeError(
        f"Failed to download "
        f"year={year} "
        f"after {max_retries} retries"
    )


def convert_location(
    location,
    response
):
    hourly = response["hourly"]

    times = hourly["time"]

    records = []

    for i, raw_time in enumerate(times):

        event_time = normalize_time(
            raw_time
        )

        record = {
            "event_id": (
                f"{location['id']}_"
                f"{event_time}"
            ),

            "event_type":
                "WEATHER_OBSERVATION",

            "location_id":
                location["id"],

            "city":
                location["name"],

            # Actual grid point selected by API
            "latitude":
                response.get(
                    "latitude",
                    location["latitude"]
                ),

            "longitude":
                response.get(
                    "longitude",
                    location["longitude"]
                ),

            "event_time":
                event_time,

            # Will be replaced during replay
            "ingestion_time": None,

            "temperature_c":
                hourly[
                    "temperature_2m"
                ][i],

            "humidity_pct":
                hourly[
                    "relative_humidity_2m"
                ][i],

            "precipitation_mm":
                hourly[
                    "precipitation"
                ][i],

            "pressure_hpa":
                hourly[
                    "pressure_msl"
                ][i],

            "wind_speed_kmh":
                hourly[
                    "wind_speed_10m"
                ][i],

            "wind_gust_kmh":
                hourly[
                    "wind_gusts_10m"
                ][i],

            "weather_code":
                hourly[
                    "weather_code"
                ][i],

            "source":
                "OPEN_METEO_HISTORICAL",
        }

        records.append(record)

    return records

def count_records(path):

    count = 0

    with gzip.open(
        path,
        "rt",
        encoding="utf-8"
    ) as file:

        for _ in file:
            count += 1

    return count


def download_year(
    client,
    locations,
    year
):

    output_path = (
        OUTPUT_DIR
        / f"weather_{year}.jsonl.gz"
    )

    if output_path.exists():

        print(
            f"[SKIP] year={year} "
            f"already exists: "
            f"{output_path}"
        )

        return count_records(
            output_path
        )
    
    print(
        f"\n========== YEAR {year} =========="
    )

    all_records = []

    for batch_number, batch in enumerate(
        chunks(
            locations,
            BATCH_SIZE
        ),
        start=1
    ):

        print(
            f"[DOWNLOAD] "
            f"year={year} "
            f"batch={batch_number} "
            f"locations={len(batch)}"
        )

        payload = fetch_batch(
            client,
            batch,
            year
        )

        if len(payload) != len(batch):
            raise RuntimeError(
                "API response count "
                "does not match location count"
            )

        for location, response in zip(
            batch,
            payload
        ):
            records = convert_location(
                location,
                response
            )

            all_records.extend(
                records
            )

        time.sleep(
            REQUEST_DELAY_SECONDS
        )

    # Important for replay:
    # global event-time ordering.
    all_records.sort(
        key=lambda row: (
            row["event_time"],
            row["location_id"],
        )
    )

    with gzip.open(
        output_path,
        "wt",
        encoding="utf-8"
    ) as file:

        for record in all_records:
            file.write(
                json.dumps(
                    record,
                    ensure_ascii=False
                )
                + "\n"
            )

    print(
        f"[DONE] year={year} "
        f"records={len(all_records):,}"
    )

    print(
        f"[FILE] {output_path}"
    )

    return len(all_records)


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    locations = load_locations()

    print(
        "Historical Weather Downloader"
    )

    print(
        f"Locations: {len(locations)}"
    )

    print(
        f"Years: "
        f"{START_YEAR}-{END_YEAR}"
    )

    total = 0

    with httpx.Client() as client:

        for year in range(
            START_YEAR,
            END_YEAR + 1
        ):

            count = download_year(
                client,
                locations,
                year
            )

            total += count

    print(
        "\n=============================="
    )

    print(
        f"TOTAL RECORDS: {total:,}"
    )

    print(
        "=============================="
    )


if __name__ == "__main__":
    main()