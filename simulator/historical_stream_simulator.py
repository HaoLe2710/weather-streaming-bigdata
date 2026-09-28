import argparse
import gzip
import heapq
import json
import random
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer


DATASET_SOURCE_PATHS = {
    "BENCHMARK_20": Path(
        "/opt/project/history-data/historical/raw"
    ),
    "NATIONWIDE_63": Path(
        "/opt/project/history-data/historical/nationwide_63/raw"
    ),
}


def resolve_dataset_source(dataset, source_override=None):
    """Resolve a named dataset, while preserving explicit legacy --source paths."""
    if source_override:
        return Path(source_override)

    normalized = dataset.strip().upper().replace("-", "_")
    try:
        return DATASET_SOURCE_PATHS[normalized]
    except KeyError as exc:
        raise ValueError(
            "dataset must be benchmark-20 or nationwide-63"
        ) from exc


# =========================================================
# RATE LIMITER
# =========================================================

class RateLimiter:

    def __init__(self, rate):
        self.rate = rate

        self.start_time = (
            time.monotonic()
        )

        self.sent = 0

    def acquire(self):

        if self.rate <= 0:
            return

        self.sent += 1

        # Do not sleep for every single message.
        # Check periodically for better throughput.
        if self.sent % 200 != 0:
            return

        expected_elapsed = (
            self.sent / self.rate
        )

        actual_elapsed = (
            time.monotonic()
            - self.start_time
        )

        sleep_time = (
            expected_elapsed
            - actual_elapsed
        )

        if sleep_time > 0:
            time.sleep(
                sleep_time
            )


# =========================================================
# DATA READER
# =========================================================

def read_historical_records(
    source_dir
):

    source = Path(
        source_dir
    )

    files = sorted(
        source.glob(
            "weather_*.jsonl.gz"
        )
    )

    if not files:
        raise RuntimeError(
            f"No historical files found "
            f"in {source}"
        )

    print(
        f"[SOURCE] files={len(files)}"
    )

    for path in files:

        print(
            f"[SOURCE] reading {path.name}"
        )

        with gzip.open(
            path,
            "rt",
            encoding="utf-8"
        ) as file:

            for line in file:

                if not line.strip():
                    continue

                yield json.loads(
                    line
                )


# =========================================================
# INVALID DATA INJECTION
# =========================================================

def make_invalid(
    record,
    rng
):

    corrupted = dict(
        record
    )

    fault = rng.choice([
        "temperature",
        "humidity",
        "precipitation",
        "location",
    ])

    if fault == "temperature":

        corrupted[
            "temperature_c"
        ] = 999.0

    elif fault == "humidity":

        corrupted[
            "humidity_pct"
        ] = 999.0

    elif fault == "precipitation":

        corrupted[
            "precipitation_mm"
        ] = -999.0

    elif fault == "location":

        corrupted[
            "location_id"
        ] = None

    return (
        corrupted,
        fault
    )


# =========================================================
# PRODUCE ONE KAFKA EVENT
# =========================================================

def send_event(
    producer,
    topic,
    record,
    limiter,
    stats,
    run_id,
    fault_type="NORMAL"
):

    event = dict(
        record
    )

    # The historical event_time stays unchanged.
    #
    # ingestion_time represents the time at which
    # the simulator sends the event to Kafka.
    event[
        "ingestion_time"
    ] = datetime.now(
        timezone.utc
    ).isoformat()

    event[
        "source"
    ] = (
        "OPEN_METEO_HISTORICAL_REPLAY"
    )

    # Extra metadata is useful when inspecting
    # Bronze raw payload.
    #
    # Silver schema may safely ignore these fields.
    event[
        "simulation_run_id"
    ] = run_id

    event[
        "simulation_fault"
    ] = fault_type

    payload = json.dumps(
        event,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    key = (
        event.get(
            "location_id"
        )
        or "INVALID_LOCATION"
    )

    limiter.acquire()

    while True:

        try:

            producer.produce(
                topic=topic,

                key=str(
                    key
                ).encode(
                    "utf-8"
                ),

                value=payload.encode(
                    "utf-8"
                ),
            )

            break

        except BufferError:

            # Local librdkafka queue full.
            producer.poll(
                0.05
            )

    producer.poll(0)

    stats[
        "produced"
    ] += 1


# =========================================================
# MAIN SIMULATION
# =========================================================

def run(args):

    rng = random.Random(
        args.seed
    )

    run_id = str(
        args.run_id or uuid.uuid4()
    )

    producer = Producer({

        "bootstrap.servers":
            args.bootstrap_servers,

        "client.id":
            "historical-weather-replay",

        # Throughput friendly settings
        "linger.ms":
            10,

        "batch.num.messages":
            10000,

        "compression.type":
            "lz4",

        "acks":
            "1",

        "queue.buffering.max.messages":
            500000,
    })

    limiter = RateLimiter(
        args.rate
    )

    stats = {
        "source": 0,
        "produced": 0,

        "normal": 0,
        "duplicate": 0,
        "invalid": 0,
        "late": 0,
        "out_of_order": 0,
    }

    # Heap contains:
    #
    # (
    #   release_at_source_index,
    #   unique_sequence,
    #   record,
    #   fault
    # )
    delayed_events = []

    delayed_sequence = 0

    start_time = time.monotonic()
    generation_start_time = datetime.now(timezone.utc).isoformat()

    last_report_time = (
        start_time
    )


    def emit(
        record,
        fault
    ):

        send_event(
            producer,
            args.topic,
            record,
            limiter,
            stats,
            run_id,
            fault,
        )

        # -----------------------------------------
        # Duplicate injection
        # -----------------------------------------

        if (
            fault != "INVALID"
            and rng.random()
            < args.duplicate_rate
        ):

            send_event(
                producer,
                args.topic,
                record,
                limiter,
                stats,
                run_id,
                "DUPLICATE",
            )

            stats[
                "duplicate"
            ] += 1


    source_path = resolve_dataset_source(
        args.dataset,
        args.source,
    )

    records = read_historical_records(
        source_path
    )


    for source_index, record in enumerate(
        records,
        start=1
    ):

        # =========================================
        # RELEASE DELAYED EVENTS
        # =========================================

        while (
            delayed_events
            and delayed_events[0][0]
            <= source_index
        ):

            (
                _,
                _,
                delayed_record,
                delayed_fault,
            ) = heapq.heappop(
                delayed_events
            )

            emit(
                delayed_record,
                delayed_fault,
            )


        stats[
            "source"
        ] += 1


        # =========================================
        # INVALID EVENT
        # =========================================

        if (
            rng.random()
            < args.invalid_rate
        ):

            (
                record,
                _
            ) = make_invalid(
                record,
                rng
            )

            stats[
                "invalid"
            ] += 1

            emit(
                record,
                "INVALID",
            )


        # =========================================
        # LATE EVENT
        #
        # The event is NOT sent now.
        #
        # Its original event_time remains the same,
        # but it will be released much later.
        # =========================================

        elif (
            rng.random()
            < args.late_rate
        ):

            delayed_sequence += 1

            release_at = (
                source_index
                + args.late_delay_events
            )

            heapq.heappush(
                delayed_events,
                (
                    release_at,
                    delayed_sequence,
                    record,
                    "LATE",
                ),
            )

            stats[
                "late"
            ] += 1


        # =========================================
        # OUT OF ORDER EVENT
        #
        # Smaller random delay.
        # =========================================

        elif (
            rng.random()
            < args.out_of_order_rate
        ):

            delayed_sequence += 1

            delay = rng.randint(
                1,
                args.out_of_order_max_delay
            )

            release_at = (
                source_index
                + delay
            )

            heapq.heappush(
                delayed_events,
                (
                    release_at,
                    delayed_sequence,
                    record,
                    "OUT_OF_ORDER",
                ),
            )

            stats[
                "out_of_order"
            ] += 1


        # =========================================
        # NORMAL
        # =========================================

        else:

            stats[
                "normal"
            ] += 1

            emit(
                record,
                "NORMAL",
            )


        # =========================================
        # PERIODIC METRICS
        # =========================================

        now = time.monotonic()

        if (
            now - last_report_time
            >= 5
        ):

            elapsed = (
                now
                - start_time
            )

            actual_rate = (
                stats["produced"]
                / elapsed
            )

            print(
                "[METRICS] "
                f"source={stats['source']:,} "
                f"produced={stats['produced']:,} "
                f"rate={actual_rate:,.0f} msg/s "
                f"duplicate={stats['duplicate']:,} "
                f"invalid={stats['invalid']:,} "
                f"late={stats['late']:,} "
                f"out_of_order="
                f"{stats['out_of_order']:,} "
                f"buffered={len(delayed_events):,}"
            )

            last_report_time = now


        # =========================================
        # MAX SOURCE EVENTS
        # =========================================

        if (
            args.max_source_events > 0
            and stats["source"]
            >= args.max_source_events
        ):
            break


    # =====================================================
    # FLUSH DELAYED EVENTS
    # =====================================================

    print(
        "[SIMULATOR] "
        f"flushing delayed events="
        f"{len(delayed_events):,}"
    )

    while delayed_events:

        (
            _,
            _,
            delayed_record,
            delayed_fault,
        ) = heapq.heappop(
            delayed_events
        )

        emit(
            delayed_record,
            delayed_fault,
        )

    generation_end_time = datetime.now(timezone.utc).isoformat()
    generation_elapsed = time.monotonic() - start_time

    print(
        "[KAFKA] flushing producer..."
    )

    remaining = producer.flush(
        30
    )


    elapsed = (
        time.monotonic()
        - start_time
    )

    actual_rate = (
        stats["produced"]
        / elapsed
        if elapsed > 0
        else 0
    )


    print()
    print(
        "===================================="
    )

    print(
        "HISTORICAL STREAM SIMULATION COMPLETE"
    )

    print(
        "===================================="
    )

    print(
        f"run_id             : {run_id}"
    )

    print(
        f"source records      : "
        f"{stats['source']:,}"
    )

    print(
        f"kafka messages      : "
        f"{stats['produced']:,}"
    )

    print(
        f"duplicates injected : "
        f"{stats['duplicate']:,}"
    )

    print(
        f"invalid injected    : "
        f"{stats['invalid']:,}"
    )

    print(
        f"late injected       : "
        f"{stats['late']:,}"
    )

    print(
        f"out-of-order        : "
        f"{stats['out_of_order']:,}"
    )

    print(
        f"elapsed             : "
        f"{elapsed:.2f}s"
    )

    print(
        f"actual throughput   : "
        f"{actual_rate:,.0f} msg/s"
    )

    print(
        f"producer remaining  : "
        f"{remaining}"
    )

    print(
        "===================================="
    )

    summary = {
        "scenario": args.scenario,
        "run_id": run_id,
        "topic": args.topic,
        "dataset_id": args.dataset.strip().upper().replace("-", "_"),
        "source_path": str(source_path),
        "source_records": stats["source"],
        "kafka_messages": stats["produced"],
        "normal_generated": stats["normal"],
        "duplicates_generated": stats["duplicate"],
        "invalid_generated": stats["invalid"],
        "late_generated": stats["late"],
        "out_of_order_generated": stats["out_of_order"],
        "requested_replay_rate": args.rate,
        "actual_generation_rate": actual_rate,
        "actual_generated_msgs_sec": (
            stats["produced"] / generation_elapsed
            if generation_elapsed > 0
            else 0
        ),
        "generation_start_time": generation_start_time,
        "generation_end_time": generation_end_time,
        "generation_elapsed_seconds": generation_elapsed,
        "producer_elapsed_seconds": elapsed,
        "duration_seconds": elapsed,
        "producer_remaining": remaining,
        "producer_flush_remaining": remaining,
        "producer_delivery_complete": remaining == 0,
    }

    if args.summary_json:
        summary_path = Path(args.summary_json)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = summary_path.with_suffix(
            summary_path.suffix + ".tmp"
        )
        temporary_path.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(summary_path)

    return summary


# =========================================================
# CLI
# =========================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Historical weather Kafka "
            "stream simulator"
        )
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="broker:19092",
    )

    parser.add_argument(
        "--topic",
        default="weather.replay",
    )

    parser.add_argument(
        "--scenario",
        default=None,
    )

    parser.add_argument(
        "--run-id",
        default=None,
    )

    parser.add_argument(
        "--summary-json",
        default=None,
        help="Optional machine-readable simulator summary path.",
    )

    parser.add_argument(
        "--dataset",
        choices=("benchmark-20", "nationwide-63"),
        default="benchmark-20",
        help=(
            "Select the immutable 20-location benchmark source or the "
            "separate 63-location nationwide source."
        ),
    )

    parser.add_argument(
        "--source",
        default=None,
        help="Optional explicit source directory override.",
    )

    parser.add_argument(
        "--rate",
        type=int,
        default=1000,
        help=(
            "Target Kafka messages "
            "per second. "
            "0 means unlimited."
        ),
    )

    parser.add_argument(
        "--max-source-events",
        type=int,
        default=10000,
        help=(
            "Number of source records "
            "to process. "
            "0 means entire dataset."
        ),
    )

    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.02,
    )

    parser.add_argument(
        "--invalid-rate",
        type=float,
        default=0.01,
    )

    parser.add_argument(
        "--late-rate",
        type=float,
        default=0.05,
    )

    parser.add_argument(
        "--out-of-order-rate",
        type=float,
        default=0.05,
    )

    parser.add_argument(
        "--late-delay-events",
        type=int,
        default=240,
        help=(
            "Delay selected late events "
            "by this many source records."
        ),
    )

    parser.add_argument(
        "--out-of-order-max-delay",
        type=int,
        default=40,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    return parser.parse_args()


if __name__ == "__main__":

    args = parse_args()

    result = run(args)
    if not result["producer_delivery_complete"]:
        raise SystemExit(
            "Kafka producer could not flush every queued message: "
            f"{result['producer_remaining']} message(s) remain."
        )
