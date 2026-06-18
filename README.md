# Aqueduct 🌊

**A fault-tolerant, distributed, real-time Change Data Capture (CDC) streaming pipeline.**

Like its Roman namesake, Aqueduct carries a continuous flow from source to destination through engineered stages. It captures every row-level change in an operational PostgreSQL database and streams it, in near real time, into a stream processor that performs in-flight joins, windowed aggregation, and anomaly detection before landing the results in an analytics layer.

> **Status: in active development.** The [Current Status](#current-status) section tracks exactly what is built versus planned, so nothing here is overstated.

## The Problem

Operational databases are optimized for transactions, not analytics. The traditional way to feed an analytics layer, periodically re-querying the source in large batches, is slow, stale, and heavy on the source. By the time a dashboard refreshes the data is already minutes or hours old, and detecting anomalies (fraud, pricing errors, demand spikes) after the fact is often too late.

## The Solution

Aqueduct taps the database's write-ahead log (WAL), the same internal change log it already keeps for crash recovery, and turns every INSERT, UPDATE, and DELETE into a structured event the moment it happens. Those events flow through a durable, replayable log into a stream processor that enriches, aggregates, and inspects them in real time. Analytics stay continuously fresh, the source database is barely touched, and anomalies surface as they occur.

## Architecture

```
PostgreSQL (WAL)  ->    Debezium   ->    Kafka    ->   Apache Flink   ->   Analytics sink
  source of            CDC reader       durable           joins /            processed
  truth + WAL          of the WAL      event log        windowing /           results
                                                        anomaly detect
```

Every stage scales horizontally, which makes Aqueduct distributed end to end: Kafka partitions topics across brokers, Flink runs operators in parallel across task managers with sharded state, and Debezium runs on the distributed Kafka Connect framework.

| Stage | Role |
| :- | :- |
| **PostgreSQL** | Source of truth. Logical replication (`wal_level=logical`) exposes row-level changes. Seeded with a real, PII-removed dataset; a workload generator produces the live change traffic. |
| **Debezium** | Reads the Postgres WAL and emits each INSERT/UPDATE/DELETE as a structured change event. |
| **Kafka** | Durable, replayable transport that decouples producers from consumers and makes the pipeline fault-tolerant: if a consumer dies, events wait safely until it recovers. |
| **Apache Flink** | Stateful stream processing: in-flight joins, time-windowed aggregation, and real-time anomaly detection. |
| **Analytics sink** | Stores the processed results for querying and dashboards. |

## How It Works

1. **Capture:** A workload generator issues continuous INSERT/UPDATE/DELETE against PostgreSQL; each change is recorded in the WAL.
2. **Stream:** Debezium decodes the WAL and publishes one event per row change to a Kafka topic, preserving change order.
3. **Process:** Flink consumes the change stream and:
   - **joins** order line-items with their orders and the product catalog to enrich each event,
   - **aggregates** over time windows (e.g. revenue per minute, orders per category),
   - **detects anomalies** (abnormal order totals, invalid pricing, demand spikes).
4. **Serve:** Processed results land in the analytics sink, continuously fresh.

## Dataset

Aqueduct is seeded with a real e-commerce dataset (~34M rows), with all PII removed. The seed makes the change traffic realistic; the workload generator drives the throughput.

| Table | Rows | Description |
| :- | :- | :- |
| `order_goods` | ~31.1M | Order line-items: product, quantity, list/deal price |
| `order_info` | ~3.1M | Order headers: user, location, total, date |
| `goods_info` | ~80K | Product catalog: name, brand, category |

## Tech Stack

| Layer | Technology |
| :- | :- |
| **Source database** | PostgreSQL 16 (logical replication) |
| **Change capture** | Debezium |
| **Event transport** | Apache Kafka |
| **Stream processing** | Apache Flink |
| **Data migration** | pgloader (MySQL to PostgreSQL) |
| **Infrastructure** | Docker, Docker Compose |
| **Observability** | Prometheus, Grafana *(planned)* |

## Current Status

### Completed
- **Dataset diagnostics:** verified the source is CDC-suitable (primary keys, schema shape, join keys, volume).
- **Source database:** Dockerized PostgreSQL 16 with `wal_level=logical`.
- **Data migration:** full transactional dataset (~34M rows) loaded from MySQL into PostgreSQL via pgloader, verified row for row.

### In Progress
- **CDC core:** Kafka + Debezium, Postgres publication / replication slot, connector registration.

### Upcoming
- Workload generator for continuous change traffic
- Flink jobs: joins, windowed aggregation, anomaly detection
- Throughput and end-to-end latency benchmarks (reported numbers will reflect measured values)
- Polish: architecture diagram, demo, tests, CI

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Access to the upstream source database (migration step only)

### 1. Start the source database
```bash
docker compose up -d
docker exec -it aqueduct-postgres psql -U cdc -d yami -c "SHOW wal_level;"   # returns: logical
```

### 2. Load the dataset
```bash
./migration/run_migration.sh
```
Migrates the core tables into PostgreSQL using a containerized pgloader, nothing to install locally.
