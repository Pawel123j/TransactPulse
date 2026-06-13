# TransactPulse

> **Real-Time Big Data Lakehouse for financial transactions.**
> Synthetic transactions flow through Kafka, are processed by Spark Structured
> Streaming, land in a Delta Lake medallion architecture on MinIO, and are served
> to analysts via DuckDB/Trino and a Streamlit dashboard — with Airflow
> orchestrating the batch layers and a fraud-scoring ML model in the loop.

![status](https://img.shields.io/badge/status-in%20development-yellow)
![license](https://img.shields.io/badge/license-MIT-blue)
![python](https://img.shields.io/badge/python-3.11-3776AB)
![docker](https://img.shields.io/badge/run-docker%20compose-2496ED)

---

## Table of contents

- [What is this?](#what-is-this)
- [Architecture](#architecture)
- [Data flow](#data-flow)
- [Tech stack](#tech-stack)
- [Medallion architecture](#medallion-architecture-bronze--silver--gold)
- [Repository layout](#repository-layout)
- [How to run](#how-to-run)
- [Roadmap](#roadmap)
- [Architecture Decision Records](#architecture-decision-records)
- [What this project demonstrates](#what-this-project-demonstrates)
- [License](#license)

---

## What is this?

**TransactPulse** is a portfolio-grade Big Data project that simulates an
end-to-end, real-time **lakehouse** for payment transactions. It is built to
demonstrate the full modern data-engineering toolchain working together on a
single machine via `docker compose`:

- a **synthetic transaction generator** producing realistic payment events
  (log-normal amounts, hourly seasonality, ~0.3 % fraud, late-arriving events);
- **streaming ingestion** from Kafka into a Delta Lake **bronze** layer;
- **data-quality gated** cleansing into a **silver** layer (dedup, currency
  normalization, ISO standardization, quarantine for bad records);
- **analytical aggregates** and **ML fraud scoring** in a **gold** layer;
- **orchestration** of the batch path with Airflow;
- **interactive analytics** through DuckDB/Trino and a Streamlit dashboard.

> ⚠️ **No production data.** Every record is synthetic — generated locally. There
> is zero real banking data anywhere in this project.

---

## Architecture

```mermaid
flowchart LR
    subgraph ING["Ingestion"]
        GEN["Synthetic Transaction Generator<br/>(Faker + custom logic)"]
    end

    subgraph STREAM["Streaming Platform"]
        KAFKA[("Kafka KRaft<br/>topic: transactions.raw")]
    end

    subgraph PROC["Stream Processing"]
        SPARK["Spark Structured Streaming"]
    end

    subgraph LAKE["Lakehouse — Delta Lake on MinIO (S3)"]
        BRONZE[["Bronze<br/>raw, as-is"]]
        SILVER[["Silver<br/>cleansed + DQ"]]
        GOLD[["Gold<br/>aggregates + fraud_score"]]
        QUAR[["Silver / quarantine"]]
    end

    subgraph ML["ML"]
        MODEL["Fraud model<br/>(scikit-learn / XGBoost)"]
    end

    subgraph QUERY["Query & Serving"]
        DUCK["DuckDB / Trino"]
        DASH["Streamlit dashboard"]
    end

    subgraph ORCH["Orchestration"]
        AIRFLOW["Airflow<br/>medallion_pipeline DAG"]
    end

    GEN -->|JSON / Avro| KAFKA
    KAFKA --> SPARK
    SPARK --> BRONZE
    BRONZE --> SILVER
    SILVER -.bad records.-> QUAR
    SILVER --> GOLD
    MODEL -.batch scoring UDF.-> GOLD
    GOLD --> DUCK
    DUCK --> DASH

    AIRFLOW -. triggers .-> SILVER
    AIRFLOW -. triggers .-> GOLD
    AIRFLOW -. triggers .-> MODEL
```

## Data flow

```mermaid
sequenceDiagram
    autonumber
    participant G as Generator
    participant K as Kafka
    participant S as Spark Streaming
    participant B as Delta Bronze
    participant V as Delta Silver
    participant D as Delta Gold
    participant Q as DuckDB / Trino
    participant U as Streamlit

    G->>K: produce transaction events (transactions.raw)
    K->>S: consume micro-batches
    S->>B: append raw + ingestion metadata (exactly-once)
    Note over B,V: Airflow-triggered batch
    B->>V: dedup, cast, normalize, data-quality gates
    V->>D: aggregates + ML fraud scoring
    D->>Q: query gold tables (Delta)
    Q->>U: KPIs, fraud signals, data-quality & drift panels
```

---

## Tech stack

| Layer            | Technology                                   | Role                                            |
| ---------------- | -------------------------------------------- | ----------------------------------------------- |
| Ingestion        | Python, Faker, confluent-kafka               | Synthetic transaction generation + producer     |
| Streaming bus    | **Apache Kafka** (KRaft, no ZooKeeper)       | Durable event backbone (`transactions.raw`)     |
| Stream processing| **PySpark Structured Streaming**             | Kafka → Delta bronze, exactly-once              |
| Storage format   | **Delta Lake**                               | ACID tables, time travel, schema evolution      |
| Object store     | **MinIO** (S3-compatible)                    | Local lakehouse storage (`s3a://lakehouse`)     |
| Data quality     | **Great Expectations**                       | Expectation suites + quarantine                 |
| ML               | scikit-learn / XGBoost                       | Batch fraud scoring as Spark UDF                |
| Orchestration    | **Apache Airflow**                           | `medallion_pipeline` DAG                         |
| Query engine     | **DuckDB / Trino**                           | Analytical SQL over gold tables                 |
| Dashboard        | **Streamlit**                                | KPIs, fraud signals, DQ & drift                 |
| Runtime          | **Docker Compose**                           | One-command stack                               |

> Stack rationale lives in [ADR 0002](docs/adr/0002-wybor-stacku.md).

---

## Medallion architecture (bronze → silver → gold)

The lakehouse follows the **medallion** pattern — data is progressively refined
across three quality tiers, each materialized as Delta tables on MinIO.

| Tier       | Path                                  | Contents                                                                                  |
| ---------- | ------------------------------------- | ----------------------------------------------------------------------------------------- |
| 🥉 **Bronze** | `s3a://lakehouse/bronze/transactions` | Raw events **as-is** from Kafka + ingestion metadata (`ingestion_time`, `source_topic`, `_offset`). No business transformations. |
| 🥈 **Silver** | `s3a://lakehouse/silver/transactions` | Cleansed & conformed: deduped by `transaction_id`, typed, currencies normalized to PLN, ISO country codes, watermarked. Bad records routed to `silver/quarantine`. |
| 🥇 **Gold**   | `s3a://lakehouse/gold/*`              | Business-ready: aggregates (`daily_volume_by_country`, `merchant_category_kpi`, `account_velocity`), `fraud_signals`, and `scored_transactions` with `fraud_score`. |

> Why a lakehouse + medallion instead of a classic data warehouse?
> See [ADR 0001](docs/adr/0001-architektura-medalionowa.md).

---

## Repository layout

```
TransactPulse/
├── ingestion/        # Synthetic transaction generator + Kafka producer
├── streaming/        # PySpark Structured Streaming jobs (bronze)
├── lakehouse/        # Silver/gold transforms, data quality, ML scoring
├── orchestration/    # Airflow DAGs and config
├── analytics/        # SQL queries (DuckDB/Trino)
├── dashboard/        # Streamlit app
├── infra/            # docker-compose, topic creation, infra scripts
├── tests/            # Unit + integration tests
├── docs/
│   └── adr/          # Architecture Decision Records (MADR)
├── LICENSE
└── README.md
```

---

## How to run

> The full `docker compose up` experience is assembled incrementally across the
> roadmap below and finalized in Stage 8. Until then, each stage's README section
> documents how to run that slice.

```bash
# Clone
git clone https://github.com/pawel123j/transactpulse.git
cd transactpulse

# (Stage 3+) bring up the base infrastructure (Kafka + MinIO)
docker compose -f infra/docker-compose.yml up -d

# (Stage 8) bring up the full stack — single command
docker compose up
```

Detailed quickstart with verification steps (Kafka UI, MinIO console, dashboard)
arrives with the corresponding stages.

---

## Roadmap

The project is built in eight checkpointed stages:

- [x] **Stage 1 — Repository foundation & architecture** — structure, README, initial ADRs.
- [ ] **Stage 2 — Synthetic transaction generator** — realistic producer → Kafka.
- [ ] **Stage 3 — Infrastructure** — Kafka (KRaft) + MinIO in Docker.
- [ ] **Stage 4 — Bronze layer** — Spark Structured Streaming → Delta on MinIO.
- [ ] **Stage 5 — Silver layer** — cleansing, dedup, data-quality gates, quarantine.
- [ ] **Stage 6 — Gold layer & ML** — aggregates + fraud scoring + drift metrics.
- [ ] **Stage 7 — Orchestration & query** — Airflow DAG + DuckDB/Trino.
- [ ] **Stage 8 — Dashboard, tests, CI** — Streamlit, full compose, GitHub Actions, `v1.0.0`.

---

## Architecture Decision Records

Significant technical decisions are captured as [ADRs](docs/adr/) in the
[MADR](https://adr.github.io/madr/) format:

| ADR | Title |
| --- | ----- |
| [0000](docs/adr/0000-template.md) | ADR template (MADR) |
| [0001](docs/adr/0001-architektura-medalionowa.md) | Medallion lakehouse architecture |
| [0002](docs/adr/0002-wybor-stacku.md) | Technology stack selection |

---

## What this project demonstrates

For recruiters and reviewers, TransactPulse is designed to prove hands-on command
of: **real-time streaming** (Kafka + Spark Structured Streaming), **lakehouse &
medallion** modeling (Delta Lake), **data quality** engineering (Great
Expectations + quarantine), **orchestration** (Airflow), **ML integration**
(batch scoring + drift), and **idempotent, containerized, reproducible**
infrastructure (`docker compose up`).

---

## License

[MIT](LICENSE) © 2026 Paweł Jankowicz
