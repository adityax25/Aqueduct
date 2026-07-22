# Aqueduct 🌊

**A fault-tolerant, distributed, real-time Change Data Capture (CDC) streaming pipeline.**

Like its Roman namesake, Aqueduct carries a continuous flow from source to destination through engineered stages. It captures every row-level change in an operational PostgreSQL database and streams it, in near real time, into a stream processor that performs in-flight joins, windowed aggregation, and anomaly detection before landing the results in an analytics layer.

> **Status: in active development.** The [Current Status](#current-status) section tracks exactly what is built versus planned, so nothing here is overstated.

## The Problem

Operational databases are optimized for transactions, not analytics. The traditional way to feed an analytics layer, periodically re-querying the source in large batches, is slow, stale, and heavy on the source. By the time a dashboard refreshes the data is already minutes or hours old, and detecting anomalies (fraud, pricing errors, demand spikes) after the fact is often too late.

## The Solution

Aqueduct taps the database's write-ahead log (WAL), the same internal change log it already keeps for crash recovery, and turns every INSERT, UPDATE, and DELETE into a structured event the moment it happens. Those events flow through a durable, replayable log into a stream processor that enriches, aggregates, and inspects them in real time. Analytics stay continuously fresh, the source database is barely touched, and anomalies surface as they occur.

## Architecture

```mermaid
%%{init: {'theme':'base','flowchart':{'nodeSpacing':35,'rankSpacing':40},'themeVariables':{'fontFamily':'system-ui, sans-serif','fontSize':'14px','lineColor':'#64748B','textColor':'#1E293B','titleColor':'#0F172A','clusterBkg':'#FFFFFF','edgeLabelBackground':'#FFFFFF'}}}%%
flowchart LR
    subgraph ALL["Aqueduct · system architecture"]
        direction LR
        subgraph SRC["Source database"]
            direction TB
            WG("Workload generator<br/>INSERT/UPDATE/DELETE")
            PG[("PostgreSQL 16<br/>3 source tables")]
            WAL("Write-ahead log<br/>wal_level=logical<br/>pub: aqueduct_pub<br/>slot: aqueduct_slot")
            WG --> PG --> WAL
        end
        subgraph CDC["CDC · Kafka Connect"]
            DBZ("Debezium connector<br/>pgoutput plugin<br/>WAL → change events<br/>REST :8083")
        end
        subgraph KFK["Kafka · KRaft · aqueduct.public.*"]
            direction TB
            INT("internal:<br/>configs · offsets · status")
            T1("order_info")
            T2("order_goods")
            T3("goods_info")
        end
        subgraph PRC["Stream processing"]
            FLINK("Apache Flink<br/>joins · windows<br/>anomaly detection")
        end
        subgraph SRV["Serving"]
            SINK("Analytics sink")
        end
        subgraph OBS["Observability"]
            direction TB
            UI("Kafka UI · :8080")
            PROM("Prometheus<br/>scrapes metrics")
            GRAF("Grafana<br/>dashboards")
            UI ~~~ PROM --> GRAF
        end
    end

    WAL -->|"logical replication"| DBZ
    DBZ -->|"keyed by primary key"| T1 & T2 & T3
    T1 & T2 & T3 --> FLINK
    FLINK --> SINK
    T1 -.-> UI
    T2 -.->|"read only"| UI
    T3 -.-> UI
    CDC -.->|"metrics"| PROM
    KFK -.->|"metrics"| PROM
    PRC -.->|"metrics"| PROM

    classDef source fill:#DBEAFE,stroke:#2563EB,color:#1E3A8A;
    classDef cdc fill:#D1FAE5,stroke:#059669,color:#065F46;
    classDef kafka fill:#FEF3C7,stroke:#D97706,color:#92400E;
    classDef internal fill:#FEF9C3,stroke:#CA8A04,color:#854D0E;
    classDef proc fill:#FCE7F3,stroke:#DB2777,color:#9D174F;
    classDef serve fill:#EDE9FE,stroke:#7C3AED,color:#5B21B6;
    classDef obs fill:#CFFAFE,stroke:#0891B2,color:#155E75;

    class WG,PG,WAL source;
    class DBZ cdc;
    class T1,T2,T3 kafka;
    class INT internal;
    class FLINK proc;
    class SINK serve;
    class UI,PROM,GRAF obs;

    style ALL fill:#FFFFFF,stroke:#CBD5E1,stroke-width:1.5px,color:#0F172A;
    style SRC fill:#EFF6FF,stroke:#2563EB,stroke-width:1px,stroke-dasharray:2 2;
    style CDC fill:#ECFDF5,stroke:#059669,stroke-width:1px,stroke-dasharray:2 2;
    style KFK fill:#FFFBEB,stroke:#D97706,stroke-width:1px,stroke-dasharray:2 2;
    style PRC fill:#FDF2F8,stroke:#DB2777,stroke-width:1px,stroke-dasharray:2 2;
    style SRV fill:#F5F3FF,stroke:#7C3AED,stroke-width:1px,stroke-dasharray:2 2;
    style OBS fill:#ECFEFF,stroke:#0891B2,stroke-width:1px,stroke-dasharray:2 2;
```

Every stage scales horizontally, which makes Aqueduct distributed end-to-end: Kafka partitions topics across brokers, Flink runs operators in parallel across task managers with sharded state, and Debezium runs on the distributed Kafka Connect framework.

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
| **Load generation** | Python (asyncio, asyncpg) |
| **Infrastructure** | Docker, Docker Compose |
| **Observability** | Prometheus, Grafana *(planned)* |

## Current Status

### Completed
- **Dataset diagnostics:** verified the source is CDC-suitable (primary keys, schema shape, join keys, volume).
- **Source database:** Dockerized PostgreSQL 16 with `wal_level=logical`.
- **Data migration:** full transactional dataset (~34M rows) loaded from MySQL into PostgreSQL via pgloader, verified row for row.
- **CDC core:** Kafka (KRaft) + Debezium on Kafka Connect, with the Postgres connector streaming row-level changes into Kafka topics. Verified end to end.
- **Workload generator:** async (asyncio and asyncpg) load driver issuing transactional insert/update/delete in a configurable mix, paced by a token-bucket rate limiter with a burst mode, and exposing Prometheus metrics. Sustains over 12K change events per second into the source in local runs, with zero errors.
- **Stream processing (Flink):** three-way in-flight join (line-items, orders, products), running and tumbling-window aggregation (revenue by category, revenue per window), and real-time anomaly detection, all in Flink SQL over the Debezium topics.

### In Progress
- **Serving layer:** persist the Flink results to a sink and build a live dashboard.

### Upcoming
- End-to-end throughput and latency benchmarks (reported numbers will reflect measured values)
- Observability dashboards (Prometheus and Grafana), and the generator packaged into the stack
- Polish: demo, tests, CI

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (to run the workload generator)
- Access to the upstream source database (migration step only)

Once the stack is up: Flink dashboard at http://localhost:8081, Kafka UI at http://localhost:8080.

### 1. Start the stack
Brings up PostgreSQL, Kafka, Debezium, Kafka UI, and Flink.
```bash
docker compose up -d --build
```

### 2. Load the dataset
```bash
./migration/run_migration.sh
```
Migrates the core tables into PostgreSQL using a containerized pgloader, nothing to install locally.

### 3. Enable change capture
Set REPLICA IDENTITY FULL (so updates carry the full old row), register the Debezium connector, and seed the product-catalog topic once.
```bash
docker exec aqueduct-postgres psql -U cdc -d yami -c \
  "ALTER TABLE order_info REPLICA IDENTITY FULL;
   ALTER TABLE order_goods REPLICA IDENTITY FULL;
   ALTER TABLE goods_info REPLICA IDENTITY FULL;"
./debezium/register-connector.sh
docker exec aqueduct-postgres psql -U cdc -d yami -c "UPDATE goods_info SET goods_name = goods_name;"
```

### 4. Generate change traffic
```bash
pip install -r workload-generator/requirements.txt
DATABASE_URL=postgresql://cdc:cdc_pw@localhost:5432/yami python3 workload-generator/main.py
```
Tune the load with `TARGET_RATE`, `WORKERS`, `BURST`, and `ANOMALY_RATE`.

### 5. Run the stream processing
Open the Flink SQL client and run the queries in [flink/aqueduct.sql](flink/aqueduct.sql).
```bash
docker exec -it aqueduct-flink-jobmanager ./bin/sql-client.sh
```
