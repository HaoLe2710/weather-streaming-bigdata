# Reproducible Correctness Benchmarks

This document covers the two correctness scenarios supported by
`benchmark/run_benchmark.py`. Historical runtime observations supplied during
development remain useful references; each new execution records its own
configuration and result.

## Status vocabulary

Keep these facts separate for every run:

- **Implementation:** the source code and intended behavior exist.
- **Runtime validation:** the component was executed and observed.
- **Reproducibility:** another developer can repeat the run from the repository
  and the documented commands.
- **Persisted result evidence:** the run's manifest, input summary, and checker
  result are stored under `results/benchmarks/`.

The prior B0 and Kafka watermark runs were runtime-verified. Before this
framework, their result artifacts were not persisted. The historical values
below are regression references, not universal pass thresholds.

## Existing benchmark and diagnostic code

| File | Classification | Use |
|---|---|---|
| `spark/jobs/bronze_weather_stream.py` | LIVE / reusable benchmark job | Production Bronze job; benchmark settings supply an isolated topic, path, checkpoint, and bounded `availableNow` trigger. |
| `spark/jobs/silver_weather_stream.py` | LIVE / reusable benchmark job | Production Silver and DLQ jobs; benchmark settings supply isolated inputs, outputs, checkpoints, and a bounded trigger. |
| `spark/jobs/check_benchmark_bronze.py` | CHECKER | Inspects Bronze records and simulator fault metadata. |
| `spark/jobs/check_benchmark_silver.py` | CHECKER | Runs the B0 count, DLQ, deduplication, and quality assertions and writes `result.json`. |
| `spark/jobs/check_watermark_benchmark.py` | DIAGNOSTIC CHECKER | Inspects the earlier Silver watermark experiment. |
| `spark/jobs/check_replay_event_lag.py` | DIAGNOSTIC | Estimates event-time lag from replay arrival ordering. |
| `spark/jobs/gold_watermark_benchmark.py` | DEPRECATED EXPERIMENT | Reads physical Silver Delta file order. It is invalidated as a Kafka arrival-order watermark benchmark. |
| `spark/jobs/gold_kafka_watermark_benchmark.py` | REUSABLE BENCHMARK | Current Kafka-direct Gold watermark implementation. |
| `spark/jobs/check_gold_watermark_benchmark.py` | CHECKER | Requires the Kafka watermark scenario and run identity, reads that run's Kafka input and derived Gold path, and writes `result.json`. |
| `spark/jobs/init_benchmark_bronze.py` | ONE-OFF INITIALIZER | Legacy helper; the run-scoped B0 runner does not use it. |

## B0 correctness

B0 validates that the production Bronze and Silver jobs preserve and classify
the simulator's messages correctly.

```mermaid
flowchart LR
  H[Historical Raw Dataset] --> R[Replay Simulator]
  R --> K[Run-scoped Kafka replay topic]
  K --> B[bronze_weather_stream.py]
  B --> BD[Benchmark Bronze Delta]
  BD --> S[silver_weather_stream.py]
  S --> SD[Benchmark Silver]
  S --> D[Benchmark DLQ]
```

The Kafka topic is unique to the run. Bronze and Silver reuse the production
jobs with run-scoped paths and checkpoints. The live `weather.raw` topic,
live Delta paths, and live checkpoints are not used.

The B0 checker records and asserts:

- source records, Kafka messages, and Bronze records;
- Silver and DLQ records;
- injected and removed duplicates;
- generated and DLQ-routed invalid events;
- NORMAL, LATE, and OUT_OF_ORDER counts;
- duplicate `event_id` groups and Silver quality violations;
- Bronze/Silver/DLQ/duplicate conservation.

The historical B0 reference produced 10,195 Bronze records, 9,903 Silver
records, 97 DLQ records, and 195 removed duplicates. It observed 8,892 NORMAL,
490 LATE, and 521 OUT_OF_ORDER Silver records, zero duplicate groups, and zero
quality violations. A changed result is measured and reported; these counts
are not hard-coded as pass criteria.

## Silver watermark diagnostic

The Silver watermark experiment observed 953 late events generated, 953
accepted, and none dropped. It remains a verified diagnostic and a useful
negative finding. Silver's `dropDuplicatesWithinWatermark` is not the final
general late-event rejection benchmark.

## Delta-order Gold experiment

`gold_watermark_benchmark.py` consumed Silver Delta files using
`maxFilesPerTrigger`. Physical file order does not preserve original Kafka
arrival order. Its result (97 accepted observations; 9,903 dropped, including
940 late and 8,962 normal) is therefore methodologically invalidated and
superseded. Keep it for historical context; do not use it to assess the final
Kafka watermark behavior.

## Kafka-direct Gold watermark

The current watermark scenario sends the replay directly to Kafka and lets
`gold_kafka_watermark_benchmark.py` consume that topic:

```mermaid
flowchart LR
  H[Historical Raw Dataset] --> R[Replay Simulator]
  R --> K[weather.bench.wm10m.<run_id>]
  K --> G[gold_kafka_watermark_benchmark.py]
  G --> D[Run-scoped Benchmark Gold Delta]
```

This path measures event-time window watermark handling, normal-event
preservation, late-event acceptance/rejection, and duplicate Gold keys. It
does not include the B0 Bronze → Silver → DLQ stages.

The established configuration is 10,000 source records, a requested replay
rate of 1,000 messages/second, zero duplicate/invalid/out-of-order rates, a
10% late rate, 4,000-event late delay, seed 42, a 10-minute watermark,
one-hour windows, and 500 Kafka offsets per trigger. A historical run accepted
9,103 of 10,000 observations, preserved all 9,047 NORMAL observations, dropped
897 of 953 LATE observations (94.12%), and produced zero duplicate Gold keys.
The measured late-drop rate is recorded, never required to equal 94.12%.

## Isolation and topic strategy

`spark/jobs/benchmark_config.py` owns scenario names, defaults, topic names,
and run-scoped resource paths. A run ID is used in its topic, Delta paths, and
checkpoints.

Topics use the pattern `weather.bench.<scenario>.<run_id>` and one partition.
Run-scoped topics prevent old messages from contaminating a new run. One
partition also preserves the simulator's overall arrival order for the
watermark scenario. This is a correctness setup; it does not represent a
multi-partition throughput or scalability test. Topics are retained and never
deleted by the runner.

Delta outputs are isolated under:

```text
/opt/project/data/benchmark/runs/{b0|wm10m}/{run_id}/
```

Checkpoints are isolated under:

```text
/opt/project/data/checkpoints/benchmark/runs/{b0|wm10m}/{run_id}/
```

The final watermark checker requires `BENCHMARK_SCENARIO` and `RUN_ID`.
Paths are derived from those values and any conflicting explicit path fails
fast. It cannot silently fall back to the old Delta-order Gold directory.

## Run a benchmark

From Windows PowerShell at the repository root:

```powershell
.\.venv\Scripts\Activate.ps1
docker compose up --build -d broker spark-master spark-worker
python -m pip install -r producer/requirements.txt
python benchmark/run_benchmark.py --scenario b0
python benchmark/run_benchmark.py --scenario wm10m
```

The runner generates a fresh timestamped run ID unless `--run-id` is supplied.
It creates a new one-partition topic and does not reuse or delete prior topics,
paths, or checkpoints. It refuses a dirty code tree so the manifest identifies
the tested commit; uncommitted result JSON from the immediately preceding run
is allowed so both scenarios can be executed before committing their evidence.

Use `--source`, `--max-source-events`, `--rate`, and `--seed` to select a
different bounded replay input. The simulator's actual generation rate is a
load-generation metric only.

The runner uses Spark's `availableNow` trigger for bounded Bronze/Silver
benchmark jobs. Live jobs keep their existing continuous-trigger behavior.
Delta Spark 4.0.0 is pinned for the Spark 4.x image; the same version is used
for its Python package and the Maven artifact resolved by `spark-submit`. Spark
resolves the Delta and Kafka connector packages into `/tmp/spark-ivy`, which is
writable by the container user.

## Run artifacts

Each run writes:

```text
results/benchmarks/{b0|wm10m}/{run_id}/
  manifest.json
  simulator.json
  result.json
```

- `manifest.json` records scenario, run ID, creation/completion times, Git
  commit, input source and limit, topic and partition count, fault settings,
  watermark/window/offset settings, all output/checkpoint paths, software
  versions, artifact paths, status, and result metrics.
- `simulator.json` records actual source/Kafka counts, injected fault counts,
  requested rate, observed generation rate, and producer flush status. The
  checkers use it as the run's input reference.
- `result.json` records checker metrics, named PASS/FAIL assertions, paths,
  scenario, run ID, and Git commit.

Only these small JSON artifacts are stored in the repository. Delta data and
checkpoints remain in the Docker named volume and are not committed.

Inspect recent runs with:

```powershell
Get-ChildItem .\results\benchmarks\b0 -Directory |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1
Get-ChildItem .\results\benchmarks\wm10m -Directory |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1
```

## Load generation versus throughput benchmarking

The replay simulator can generate approximately 1,000 Kafka messages/second
under its smoke configuration. Load-generation capability is implemented and
runtime-verified. A controlled system throughput benchmark is NOT STARTED.
The smoke rate does not measure Spark/Kafka system throughput or scalability.
Those experiments require input/processed rows per second, Kafka lag, batch
duration, CPU, memory, end-to-end latency, and saturation measurements. They
are outside this milestone.
