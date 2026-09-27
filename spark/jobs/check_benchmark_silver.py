"""Check a run-scoped B0 Bronze -> Silver/DLQ correctness run."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from benchmark_config import (
    B0_CORRECTNESS,
    BenchmarkConfig,
    read_json,
    write_json,
)
from weather_schema import weather_valid_condition


CONFIG = BenchmarkConfig.from_environment(
    required=True,
    expected_scenario=B0_CORRECTNESS,
)
INPUT_REFERENCE_PATH = os.getenv("INPUT_REFERENCE_PATH")
RESULT_PATH = os.getenv("RESULT_PATH")

if not INPUT_REFERENCE_PATH or not RESULT_PATH:
    raise ValueError(
        "B0 checker requires INPUT_REFERENCE_PATH and RESULT_PATH."
    )

reference = read_json(INPUT_REFERENCE_PATH)
if reference.get("scenario") != CONFIG.scenario:
    raise ValueError("B0 input reference scenario does not match the run.")
if reference.get("run_id") != CONFIG.run_id:
    raise ValueError("B0 input reference run_id does not match the run.")
if reference.get("topic") != CONFIG.topic:
    raise ValueError("B0 input reference topic does not match the run.")


spark = (
    SparkSession.builder
    .appName(f"CheckB0-{CONFIG.run_id}")
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


try:
    bronze = (
        spark.read.format("delta").load(CONFIG.paths.bronze)
        .withColumn(
            "simulation_run_id",
            F.get_json_object("payload", "$.simulation_run_id"),
        )
        .withColumn(
            "simulation_fault",
            F.get_json_object("payload", "$.simulation_fault"),
        )
        .filter(F.col("simulation_run_id") == CONFIG.run_id)
    )
    silver = (
        spark.read.format("delta").load(CONFIG.paths.silver)
        .filter(F.col("simulation_run_id") == CONFIG.run_id)
    )
    dlq = (
        spark.read.format("delta").load(CONFIG.paths.dlq)
        .filter(F.col("simulation_run_id") == CONFIG.run_id)
    )

    bronze_records = bronze.count()
    silver_records = silver.count()
    dlq_records = dlq.count()
    bronze_faults = fault_counts(bronze)
    silver_faults = fault_counts(silver)
    dlq_faults = fault_counts(dlq)

    duplicates = (
        silver.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    quality_violations = (
        silver
        .filter(
            ~F.coalesce(weather_valid_condition(), F.lit(False))
        )
        .count()
    )

    duplicates_generated = int(reference["duplicates_generated"])
    invalid_generated = int(reference["invalid_generated"])
    late_generated = int(reference["late_generated"])
    out_of_order_generated = int(reference["out_of_order_generated"])
    normal_generated = int(reference["normal_generated"])
    expected_bronze_faults = {
        "NORMAL": normal_generated,
        "DUPLICATE": duplicates_generated,
        "INVALID": invalid_generated,
        "LATE": late_generated,
        "OUT_OF_ORDER": out_of_order_generated,
    }

    duplicates_removed = bronze_records - silver_records - dlq_records
    invalid_routed_to_dlq = dlq_faults.get("INVALID", 0)
    expected_silver_records = (
        normal_generated + late_generated + out_of_order_generated
    )

    assertions = {
        "simulator_flush_completed": bool(
            reference.get("producer_delivery_complete")
        ),
        "source_fault_categories_sum_to_source_records": (
            normal_generated
            + invalid_generated
            + late_generated
            + out_of_order_generated
            == int(reference["source_records"])
        ),
        "kafka_message_count_matches_bronze": (
            int(reference["kafka_messages"]) == bronze_records
        ),
        "bronze_fault_distribution_matches_simulator": (
            bronze_faults == expected_bronze_faults
        ),
        "silver_dlq_and_removed_duplicates_conserve_bronze": (
            silver_records + dlq_records + duplicates_removed
            == bronze_records
        ),
        "observed_duplicates_removed_match_injected_duplicates": (
            duplicates_removed == duplicates_generated
        ),
        "silver_contains_all_valid_nonduplicate_events": (
            silver_records == expected_silver_records
        ),
        "all_injected_invalid_events_reached_dlq": (
            invalid_routed_to_dlq == invalid_generated
        ),
        "dlq_contains_only_injected_invalid_events": (
            dlq_records == invalid_routed_to_dlq
        ),
        "silver_has_no_duplicate_event_id_groups": duplicates == 0,
        "silver_quality_violation_count_is_zero": quality_violations == 0,
    }

    metrics = {
        "source_records": int(reference["source_records"]),
        "kafka_messages": int(reference["kafka_messages"]),
        "bronze_records": bronze_records,
        "silver_records": silver_records,
        "dlq_records": dlq_records,
        "duplicates_generated": duplicates_generated,
        "duplicates_removed": duplicates_removed,
        "invalid_generated": invalid_generated,
        "invalid_routed_to_dlq": invalid_routed_to_dlq,
        "normal_generated": normal_generated,
        "late_generated": late_generated,
        "out_of_order_generated": out_of_order_generated,
        "duplicate_event_id_groups": duplicates,
        "quality_violation_count": quality_violations,
        "bronze_fault_distribution": bronze_faults,
        "silver_fault_distribution": silver_faults,
        "dlq_fault_distribution": dlq_faults,
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
        "paths": CONFIG.paths.as_dict(),
        "input_reference_path": INPUT_REFERENCE_PATH,
    }
    write_json(RESULT_PATH, result)
    print("========== B0 CORRECTNESS RESULT ==========")
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
    raise SystemExit("B0 correctness assertions failed; see result.json.")
