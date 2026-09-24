# flink_flonk

A hands-on PyFlink pipeline reading real-time trade data from Kafka, built while learning the Flink DataStream API from first principles.

## What it does

- Runs a local Kafka broker (KRaft mode, no ZooKeeper) via Docker
- Reads JSON-encoded trade events (`symbol`, `price`, `timestamp`) from a Kafka topic
- Deserializes with explicit schema enforcement (`JsonRowDeserializationSchema`)
- Assigns event-time watermarks with a 10-second out-of-orderness tolerance
- Aggregates trades into 1-minute tumbling windows, keyed by symbol
- Sums price per symbol per window via a custom `ReduceFunction`
- Sinks results to disk with `FileSink`, using `EXACTLY_ONCE` checkpointing and `OnCheckpointRollingPolicy` to guarantee files finalize cleanly (no dangling `.inprogress` files, no dupes on restart)

## Stack

- Apache Kafka 3.8.0 (KRaft mode, single broker, Docker)
- PyFlink 2.3.0
- Python 3.11 (via `uv`-managed venv, isolated from system Python)
- `flink-sql-connector-kafka` (5.0.0-2.2) for Kafka connectivity

## Setup

```bash
# Kafka (separate compose project)
cd ~/KAFKA && docker compose up -d

# This project
uv venv pyflink_39 --python=3.11
source pyflink_39/bin/activate
uv pip install "setuptools<81"
uv pip install apache-flink --no-build-isolation
# Drop flink-sql-connector-kafka-*.jar into pyflink_39/.../pyflink/lib/
```

## Run

```bash
python trades_reader.py
```

Produces a test message via Kafka's console producer, e.g.:
```json
{"symbol": "BTC", "price": 60000, "timestamp": "2026-09-24T01:30:00"}
```

Output lands in `output/trades_windowed/`, one file per checkpoint interval.

## Known gaps

- Sink is plain `FileSink`, not a real Delta Lake sink — `delta-flink`'s PyFlink support is thin/Java-only as of this version; substituted a checkpointed file sink to prove the same mechanism (checkpoint location, exactly-once, rolling policy).
- Exactly-once kill/restart test designed but not yet run live — protocol: baseline output → send new messages → hard-kill mid-checkpoint-interval → restart → verify sums match hand-calculated totals with no duplicate rows.

## Notable bugs fought and fixed

- `apache-beam` build failure (`ModuleNotFoundError: pkg_resources`) — fixed by pinning `setuptools<81` and using `--no-build-isolation`
- `Duration` vs `Time` — Flink's window assigners expect `Time`, not `Duration`, despite what some doc examples show
- Empty Kafka partitions silently freezing the global watermark (Flink watermark = min across all partition watermarks) — fixed by testing with a single-partition topic
- `FileSink` writing only hidden `.inprogress` files — default rolling policy doesn't roll on checkpoint alone; fixed with `OnCheckpointRollingPolicy`
