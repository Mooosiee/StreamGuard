# StreamGuard: Real-Time Transaction Anomaly Detection & Fraud Intelligence

A production-grade streaming pipeline that ingests transactions in real time, flags suspicious behavior using distributed rule engines, enforces message idempotency, caches account risk profiles via Redis, stores relational audits in PostgreSQL with Alembic migrations, and exposes analytics via FastAPI. Extended with an offline batch machine learning pipeline comparing unsupervised Isolation Forest and supervised Logistic Regression against rule-based verdicts.

## Architecture

```text
       ┌────────────────────────┐         ┌────────────────────────┐
       │ Transaction Generator  │         │  FastAPI (POST /txns)  │
       └───────────┬────────────┘         └───────────┬────────────┘
                   │                                  │
                   │ (event_id, idempotency payload)  │
                   ▼                                  ▼
         ═════════════════════════════════════════════════════════
                       Apache Kafka Topic ("transactions")
         ═════════════════════════════════════════════════════════
                                   │
                                   ▼
                         ┌───────────────────┐
                         │   Kafka Consumer  │
                         └─────────┬─────────┘
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        ▼                          ▼                          ▼
 ┌──────────────┐         ┌──────────────────┐       ┌─────────────────┐
 │ Redis Cache  │         │ Rule Engine      │       │ PostgreSQL (DB) │
 │              │         │                  │       │                 │
 │ • Distributed│◄───────►│ • Amount Flag    │       │ • accounts      │
 │   Velocity   │         │ • Odd-Hour Flag  │──────►│ • transactions  │
 │   Window     │         │ • Shared Velocity│       │   (unique event)│
 │ • Risk Cache │         └──────────────────┘       │ • anomalies     │
 │ • Invalidate │                                    │   (indexed FK)  │
 └──────────────┘                                    └────────┬────────┘
                                                              │
                                                              ▼
                                                     ┌─────────────────┐
                                                     │  FastAPI REST   │
                                                     │ (GET, Profiles) │
                                                     └─────────────────┘
```

```text
Offline Batch Machine Learning Phase:

    transaction_features (Historical feature engineering without leakage)
         │
         ├──► Isolation Forest (Unsupervised) ──► ml_scores
         ├──► Logistic Regression (Supervised) ──► ml_scores_lr
         │
         ▼
    model_comparison (Rules vs Isolation Forest vs Logistic Regression)
         │
         ▼
    FastAPI ML Endpoints & Performance Metrics
```

---

## Core Engineering Implementations

### 1. Unified Event Ingestion (Priority 1)
* **Problem:** Direct database insertions in `POST /transactions` bypassed the message broker and detection rules.
* **Production Fix:** `POST /transactions` validates transaction payloads via Pydantic, generates a unique `event_id`, and publishes to the Kafka `transactions` topic. It responds with `202 Accepted`. All transactions—whether streaming from the generator or submitted via HTTP—follow the exact same event-driven path.

### 2. End-to-End Idempotency & Deduplication (Priority 2)
* **Problem:** Crashes between database commits and Kafka offset commits can trigger message redelivery, producing duplicate financial records.
* **Production Fix:** 
  - Every transaction is tagged with a unique `event_id` (UUID).
  - PostgreSQL schema enforces a `UNIQUE` constraint and index on `transactions.event_id`.
  - The consumer performs deduplication checks and safely catches race-condition duplicate key errors, acknowledging offsets without corrupting data or halting the consumer loop.

### 3. Redis Distributed Sliding Window & Caching (Priority 3)
* **Distributed Velocity Tracking:** Instead of an unshared in-memory dictionary, the rule engine uses Redis Sorted Sets (`ZADD`, `ZREMRANGEBYSCORE`, `ZCARD`, `EXPIRE`) to maintain a cross-consumer 30-second sliding window (`velocity:{account_id}`).
* **Account Risk Profile Caching:** `GET /accounts/{account_id}/risk-profile` serves cached risk evaluations with a 120-second TTL.
* **Targeted Invalidation:** Whenever the Kafka consumer processes a new transaction or flags an anomaly for an account, it automatically invalidates `account:risk:{account_id}` in Redis so the next API read reflects fresh risk data.
* **Graceful Degradation:** A built-in fallback automatically switches to local sliding windows and direct DB queries if Redis is offline during local test runs.

### 4. Database Indexing & Alembic Migrations (Priority 5)
* **Database Migrations:** Configured Alembic with `alembic/env.py` and versioned migration scripts to manage relational DDL changes across environments.
* **Explicit Indexing:**
  - `accounts.account_id` (Primary Key, B-Tree index)
  - `transactions.event_id` (Unique B-Tree index for $O(1)$ idempotency lookups)
  - `transactions.account_id`, `transactions.status`, `transactions.timestamp`
  - Composite Index: `ix_transactions_account_timestamp` on `(account_id, timestamp)` for rapid sliding-window and ML feature aggregation.
  - `anomalies.transaction_id`, `anomalies.risk_score`, `anomalies.detected_at`

### 5. Automated Test Suite (Priority 4)
* 27 automated unit and integration tests under `tests/` covering:
  - **Transaction Validation:** Field presence, types, negative amounts, ISO timestamps.
  - **Rule Engine:** High amount, odd hour, sliding window velocity, multi-rule risk score capping.
  - **Idempotency:** Duplicate event re-delivery, database constraint safety, offset acknowledgement.
  - **Redis Cache & Windowing:** Sliding window boundary pruning, cache hits/misses, TTL, and cache invalidation.
  - **Database Operations:** Schema constraints, foreign key cascades, and joined queries.
  - **REST API:** 202 Accepted Kafka queuing, 422 validation errors, anomaly retrieval, and health checks.

---

## Detection Rules

| Rule | Trigger Condition | Assigned Risk Score | Explainability Reason |
| :--- | :--- | :--- | :--- |
| **High Amount** | `amount > 50,000 PKR` | `+0.35` | `"unusually high amount"` |
| **Distributed Velocity** | `> 4 txns in 30s` (via Redis) | `+0.40` | `"more than 4 transactions in 30s"` |
| **Odd Hour** | `01:00 AM <= timestamp < 05:00 AM` | `+0.25` | `"transaction at unusual hour"` |

Risk scores are summed and capped at `1.0`. Any transaction with at least one trigger is flagged as `"suspicious"` and logged into `anomalies`.

---

## Machine Learning Phase (Batch Analytics)

* **Isolation Forest (Unsupervised):** Learns anomalous data distributions without access to `rule_flag`. Contamination is anchored to historical flag rates.
* **Logistic Regression (Supervised Baseline):** Learns from normalized transaction velocity, rolling averages, and amount deviations without data leakage.

### Model Agreement Summary

| Comparison | Agreement Rate | Key Takeaway |
| :--- | :---: | :--- |
| **Rules ↔ Isolation Forest** | **66.8%** | Diverges on multidimensional outliers that bypass static thresholds |
| **Rules ↔ Logistic Regression** | **99.5%** | Accurately models non-linear decision boundaries of the rules |
| **Isolation Forest ↔ Logistic Regression** | **66.3%** | Distinguishes rule-mimicking behavior from structural anomaly signals |

---

## Running the Project

### 1. Start Infrastructure (Docker Compose)

```bash
docker compose up -d
```
Starts:
* **Kafka Broker (KRaft mode):** `localhost:9092`
* **PostgreSQL 16:** `localhost:5433`
* **Redis 7 (Alpine):** `localhost:6379`

### 2. Apply Database Migrations (Alembic)

```bash
python -m alembic upgrade head
```

### 3. Run the Automated Test Suite

```bash
python -m pytest -v
```

### 4. Start the Ingestion & Detection Pipeline

Open separate terminals:

**Terminal 1: Kafka Consumer (Detection & Persistence)**
```bash
python consumer.py
```

**Terminal 2: Transaction Generator (Simulated Stream)**
```bash
python transaction_generator.py
```

**Terminal 3: REST API Server**
```bash
uvicorn main:app --reload
```

Interactive OpenAPI / Swagger Documentation available at:
`http://localhost:8000/docs`

---

## REST API Specification

| Method | Endpoint | Status | Description |
| :--- | :--- | :---: | :--- |
| `GET` | `/health` | `200` | System dependency health check (PostgreSQL, Kafka, Redis) |
| `POST` | `/transactions` | `202` | Enqueues transaction to Kafka for asynchronous evaluation |
| `GET` | `/transactions` | `200` | Fetches recent stored transactions |
| `GET` | `/anomalies` | `200` | Fetches flagged anomalies joined with transaction audit data |
| `GET` | `/anomalies/{id}` | `200` | Fetches anomaly details by ID |
| `GET` | `/accounts/{id}/risk-profile` | `200` | Cached risk profile with automatic DB fallback & Redis TTL |
| `DELETE` | `/accounts/{id}/cache` | `200` | Manually invalidates account profile cache |
| `GET` | `/ml/isolation-forest` | `200` | Returns Isolation Forest anomaly scores |
| `GET` | `/ml/logistic-regression` | `200` | Returns Logistic Regression prediction probabilities |
| `GET` | `/ml/comparison` | `200` | Combined comparative results across Rules & ML models |

---

## Tech Stack

* **Language:** Python 3.10+
* **Event Streaming:** Apache Kafka (KRaft mode), `kafka-python`
* **Distributed Cache & State:** Redis 7, `redis-py`
* **Database & ORM:** PostgreSQL 16, SQLAlchemy 2.0
* **Schema Migrations:** Alembic
* **API Framework:** FastAPI, Pydantic v2, Uvicorn
* **Machine Learning:** scikit-learn, pandas, NumPy
* **Testing & CI:** pytest, httpx
* **Containerization:** Docker, Docker Compose
