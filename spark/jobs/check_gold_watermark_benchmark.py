"""Check the run-scoped Kafka-direct Gold watermark benchmark."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from benchmark_config import (
    BenchmarkConfig,
    KAFKA_WATERMARK_WM10M,
    read_json,
    write_json,
)


CONFIG = BenchmarkConfig.from_environment(
    required=True,
    expected_scenario=KAFKA_WATERMARK_WM10M,
)
INPUT_REFERENCE_PATH = os.getenv("INPUT_REFERENCE_PATH")
RESULT_PATH = os.getenv("RESULT_PATH")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker:19092",
)

if not INPUT_REFERENCE_PATH or not RESULT_PATH:
    raise ValueError(
        "Watermark checker requires INPUT_REFERENCE_PATH and RESULT_PATH."
    )

reference = read_json(INPUT_REFERENCE_PATH)
if reference.get("scenario") != CONFIG.scenario:
    raise ValueError("Watermark input reference scenario does not match the run.")
if reference.get("run_id") != CONFIG.run_id:
    raise ValueError("Watermark input reference run_id does not match the run.")
if reference.get("topic") != CONFIG.topic:
    raise ValueError("Watermark input reference topic does not match the run.")


spark = (
    SparkSession.builder
    .appName(f"CheckKafkaWatermark-{CONFIG.run_id}")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def fault_counts(frame):
    rows = (
        frame.groupBy("simulation_fault")
        .count()
        .collect()
    )
    return {
        str(row["simulation_fault"] or "NULL"): int(row["count"])
        for row in rows
    }


def observation_count(frame, fault_name):
    row = (
        frame.filter(F.col("simulation_fault") == fault_name)
        .agg(
            F.coalesce(
                F.sum("observation_count"),
                F.lit(0),
            ).alias("accepted")
        )
        .first()
    )
    return int(row["accepted"])


try:
    kafka_records = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", CONFIG.topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(
            F.get_json_object(
                F.col("value").cast("string"),
                "$.simulation_run_id",
            ).alias("simulation_run_id"),
            F.get_json_object(
                F.col("value").cast("string"),
                "$.simulation_fault",
            ).alias("simulation_fault"),
        )
        .filter(F.col("simulation_run_id") == CONFIG.run_id)
    )
    gold = (
        spark.read
        .format("delta")
        .load(CONFIG.paths.gold)
        .filter(F.col("simulation_run_id") == CONFIG.run_id)
    )

    input_observations = kafka_records.count()
    input_faults = fault_counts(kafka_records)
    gold_accepted = int(
        gold.agg(
            F.coalesce(
                F.sum("observation_count"),
                F.lit(0),
            ).alias("accepted")
        ).first()["accepted"]
    )
    gold_fault_rows = (
        gold.groupBy("simulation_fault")
        .agg(
            F.sum("observation_count").alias("accepted_observations")
        )
        .collect()
    )
    gold_faults = {
        str(row["simulation_fault"] or "NULL"): int(
            row["accepted_observations"]
        )
        for row in gold_fault_rows
    }

    normal_generated = input_faults.get("NORMAL", 0)
    late_generated = input_faults.get("LATE", 0)
    normal_accepted = observation_count(gold, "NORMAL")
    late_accepted = observation_count(gold, "LATE")
    normal_dropped = normal_generated - normal_accepted
    late_dropped = late_generated - late_accepted
    total_dropped = input_observations - gold_accepted
    late_drop_rate = (
        late_dropped / late_generated * 100.0
        if late_generated > 0 else 0.0
    )
    normal_preservation_rate = (
        normal_accepted / normal_generated * 100.0
        if normal_generated > 0 else 0.0
    )

    duplicate_gold_keys = (
        gold.groupBy(
            "window_start",
            "window_end",
            "location_id",
            "simulation_run_id",
            "simulation_fault",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    expected_input_faults = {
        "NORMAL": int(reference["normal_generated"]),
        "LATE": int(reference["late_generated"]),
    }
    actual_core_faults = {
        fault: input_faults.get(fault, 0)
        for fault in ("NORMAL", "LATE")
    }
    unexpected_fault_records = sum(
        count
        for fault, count in input_faults.items()
        if fault not in {"NORMAL", "LATE"}
    )

    assertions = {
        "simulator_flush_completed": bool(
            reference.get("producer_delivery_complete")
        ),
        "kafka_input_count_matches_simulator": (
            input_observations == int(reference["kafka_messages"])
        ),
        "normal_and_late_counts_match_reference": (
            actual_core_faults == expected_input_faults
        ),
        "no_unexpected_fault_categories": unexpected_fault_records == 0,
        "no_invalid_duplicate_or_out_of_order_generation": (
            int(reference["duplicates_generated"]) == 0
            and int(reference["invalid_generated"]) == 0
            and int(reference["out_of_order_generated"]) == 0
        ),
        "gold_does_not_accept_more_than_input": (
            0 <= gold_accepted <= input_observations
        ),
        "normal_events_are_fully_preserved": normal_dropped == 0,
        "late_events_are_present_for_measurement": late_generated > 0,
        "late_drop_count_is_nonnegative": late_dropped >= 0,
        "normal_and_late_drops_reconcile_to_total": (
            total_dropped == normal_dropped + late_dropped
        ),
        "duplicate_gold_keys_are_zero": duplicate_gold_keys == 0,
    }

    metrics = {
        "input_observations": input_observations,
        "gold_accepted_observations": gold_accepted,
        "total_dropped": total_dropped,
        "late_generated": late_generated,
        "late_accepted": late_accepted,
        "late_dropped": late_dropped,
        "late_drop_rate": round(late_drop_rate, 4),
        "normal_generated": normal_generated,
        "normal_accepted": normal_accepted,
        "normal_dropped": normal_dropped,
        "normal_preservation_rate": round(normal_preservation_rate, 4),
        "duplicate_gold_keys": duplicate_gold_keys,
        "input_fault_distribution": input_faults,
        "gold_fault_distribution": gold_faults,
    }
    result = {
        "schema_version": 1,
        "scenario": CONFIG.scenario,
        "run_id": CONFIG.run_id,
        "git_commit": reference.get("git_commit"),
        "kafka_topic": CONFIG.topic,
        "status": "PASS" if all(assertions.values()) else "FAIL",
        "metrics": metrics,
        "assertions": assertions,
        "gold_path": CONFIG.paths.gold,
        "input_reference_path": INPUT_REFERENCE_PATH,
    }
    write_json(RESULT_PATH, result)
    print("========== KAFKA WATERMARK RESULT ==========")
    print(f"Scenario : {result['scenario']}")
    print(f"Run ID   : {result['run_id']}")
    print(f"Status   : {result['status']}")
    for name, value in metrics.items():
        print(f"{name}: {value}")
    for name, passed in assertions.items():
        print(f"{'PASS' if passed else 'FAIL'}: {name}")
    print(f"Result JSON: {RESULT_PATH}")
finally:
    spark.stop()

if result["status"] != "PASS":
    raise SystemExit("Watermark assertions failed; see result.json.")
