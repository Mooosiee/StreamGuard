import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import socket
from sqlalchemy import select, func, text

from schema import (
    engine,
    transactions,
    anomalies,
    ml_scores,
    ml_scores_lr,
    model_comparison,
)
from redis_client import (
    get_cached_account_risk,
    cache_account_risk,
    invalidate_account_cache,
    get_redis_client,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

app = FastAPI(
    title="StreamGuard - Real-Time Fraud & Anomaly Detection API",
    description="Event-driven transaction processing, real-time fraud rules, and Redis caching.",
    version="2.0.0",
)

# Shared / lazy Kafka producer
_kafka_producer = None


def _is_service_reachable(host_port: str, timeout: float = 0.2) -> bool:
    try:
        parts = host_port.split(":")
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 9092
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def get_producer():
    """Lazily initialize Kafka producer to avoid startup stalls when Kafka is offline."""
    global _kafka_producer
    if _kafka_producer is None:
        if not _is_service_reachable(KAFKA_BOOTSTRAP_SERVERS, timeout=0.2):
            return None

        try:
            from kafka import KafkaProducer
            _kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=1000,
                api_version_auto_timeout_ms=1000,
            )
        except Exception as exc:
            logger.warning(f"Could not connect KafkaProducer to {KAFKA_BOOTSTRAP_SERVERS}: {exc}")
            return None
    return _kafka_producer


def set_producer(producer):
    """Allows injecting mock producer for automated tests."""
    global _kafka_producer
    _kafka_producer = producer


# ==========================================
# Pydantic Schemas
# ==========================================

class TransactionCreate(BaseModel):
    account_id: str = Field(..., description="Unique account identifier (e.g. acc_001)")
    amount: float = Field(..., gt=0, description="Transaction amount (must be positive)")
    location: str = Field(..., description="Origin city or location")
    merchant: str = Field(..., description="Merchant name")
    event_id: Optional[str] = Field(None, description="Optional client-supplied idempotent event ID")
    timestamp: Optional[str] = Field(None, description="ISO-8601 UTC timestamp")


class TransactionQueuedResponse(BaseModel):
    status: str
    event_id: str
    account_id: str
    amount: float
    location: str
    merchant: str
    timestamp: str
    message: str


# ==========================================
# Health Check Endpoint
# ==========================================

@app.get("/health", tags=["System"])
def health_check():
    """Reports status of PostgreSQL, Kafka, and Redis dependencies."""
    health_status = {"status": "ok", "components": {}}

    # Check Database
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health_status["components"]["postgres"] = "healthy"
    except Exception as exc:
        health_status["components"]["postgres"] = f"unhealthy: {exc}"
        health_status["status"] = "degraded"

    # Check Redis
    redis_client = get_redis_client()
    if redis_client:
        health_status["components"]["redis"] = "healthy"
    else:
        health_status["components"]["redis"] = "unavailable (operating in fallback mode)"

    # Check Kafka Producer
    producer = get_producer()
    health_status["components"]["kafka"] = "healthy" if producer else "disconnected"

    return health_status


# ==========================================
# Ingestion Endpoint (Priority 1 & 2)
# ==========================================

@app.post(
    "/transactions",
    response_model=TransactionQueuedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Transactions"],
)
def submit_transaction(transaction: TransactionCreate):
    """
    Ingests a transaction asynchronously via Kafka.
    
    Guarantees:
    - Assigns an idempotent `event_id` if not provided.
    - Publishes event to Kafka topic `transactions`.
    - Consumer asynchronously evaluates rules and writes to PostgreSQL.
    - Prevents bypassing fraud detection.
    """
    event_id = transaction.event_id or str(uuid.uuid4())
    ts = transaction.timestamp or datetime.now(timezone.utc).isoformat()

    payload = {
        "event_id": event_id,
        "account_id": transaction.account_id,
        "amount": transaction.amount,
        "location": transaction.location,
        "merchant": transaction.merchant,
        "timestamp": ts,
    }

    producer = get_producer()
    if producer is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka ingestion service temporarily unavailable. Transaction not queued.",
        )

    try:
        producer.send(TOPIC, value=payload)
        producer.flush(timeout=3)
    except Exception as exc:
        logger.error(f"Failed to publish transaction {event_id} to Kafka: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to enqueue transaction to Kafka broker: {exc}",
        )

    return TransactionQueuedResponse(
        status="queued",
        event_id=event_id,
        account_id=transaction.account_id,
        amount=transaction.amount,
        location=transaction.location,
        merchant=transaction.merchant,
        timestamp=ts,
        message="Transaction queued to Kafka for real-time risk assessment.",
    )


# ==========================================
# Query Endpoints
# ==========================================

@app.get("/transactions", tags=["Transactions"])
def get_transactions(limit: int = 20):
    """Return the most recent transactions stored in the database."""
    with engine.connect() as conn:
        query = (
            select(transactions)
            .order_by(transactions.c.timestamp.desc())
            .limit(limit)
        )
        rows = conn.execute(query).mappings().all()
        return {
            "count": len(rows),
            "transactions": [dict(row) for row in rows],
        }


@app.get("/anomalies", tags=["Anomalies"])
def get_anomalies(limit: int = 20):
    """Return the most recent flagged anomalies, joined with transaction details."""
    with engine.connect() as conn:
        query = (
            select(
                anomalies.c.id,
                anomalies.c.transaction_id,
                anomalies.c.risk_score,
                anomalies.c.reason,
                anomalies.c.detected_at,
                transactions.c.event_id,
                transactions.c.account_id,
                transactions.c.amount,
                transactions.c.merchant,
                transactions.c.location,
            )
            .join(
                transactions,
                transactions.c.transaction_id == anomalies.c.transaction_id,
            )
            .order_by(anomalies.c.detected_at.desc())
            .limit(limit)
        )
        rows = conn.execute(query).mappings().all()
        return {
            "count": len(rows),
            "anomalies": [dict(row) for row in rows],
        }


@app.get("/anomalies/{anomaly_id}", tags=["Anomalies"])
def get_anomaly_by_id(anomaly_id: int):
    """Return one specific anomaly by its id."""
    with engine.connect() as conn:
        query = (
            select(
                anomalies.c.id,
                anomalies.c.transaction_id,
                anomalies.c.risk_score,
                anomalies.c.reason,
                anomalies.c.detected_at,
                transactions.c.event_id,
                transactions.c.account_id,
                transactions.c.amount,
                transactions.c.merchant,
                transactions.c.location,
            )
            .join(
                transactions,
                transactions.c.transaction_id == anomalies.c.transaction_id,
            )
            .where(anomalies.c.id == anomaly_id)
        )
        row = conn.execute(query).mappings().first()

        if row is None:
            raise HTTPException(
                status_code=404,
                detail="Anomaly not found",
            )

        return dict(row)


# ==========================================
# Account Risk Profiles & Redis Caching (Priority 3)
# ==========================================

@app.get("/accounts/{account_id}/risk-profile", tags=["Accounts"])
def get_account_risk_profile(account_id: str):
    """
    Returns an aggregated risk summary for an account.
    Demonstrates Redis caching with TTL and automatic invalidation.
    """
    # 1. Attempt Redis Cache Read
    cached_profile = get_cached_account_risk(account_id)
    if cached_profile:
        return {
            "source": "cache",
            "profile": cached_profile,
        }

    # 2. Cache Miss: Compute from PostgreSQL
    with engine.connect() as conn:
        # Total transaction count and sum
        total_txns = conn.execute(
            select(func.count(transactions.c.transaction_id))
            .where(transactions.c.account_id == account_id)
        ).scalar() or 0

        avg_amount = conn.execute(
            select(func.avg(transactions.c.amount))
            .where(transactions.c.account_id == account_id)
        ).scalar() or 0.0

        # Anomalies count
        flagged_txns = conn.execute(
            select(func.count(anomalies.c.id))
            .join(transactions, transactions.c.transaction_id == anomalies.c.transaction_id)
            .where(transactions.c.account_id == account_id)
        ).scalar() or 0

        # Most recent anomaly
        recent_anomaly = conn.execute(
            select(anomalies.c.risk_score, anomalies.c.reason, anomalies.c.detected_at)
            .join(transactions, transactions.c.transaction_id == anomalies.c.transaction_id)
            .where(transactions.c.account_id == account_id)
            .order_by(anomalies.c.detected_at.desc())
            .limit(1)
        ).mappings().first()

    anomaly_ratio = (flagged_txns / total_txns) if total_txns > 0 else 0.0

    if flagged_txns >= 3 or anomaly_ratio > 0.4:
        risk_tier = "HIGH"
    elif flagged_txns >= 1 or anomaly_ratio > 0.1:
        risk_tier = "MEDIUM"
    else:
        risk_tier = "LOW"

    profile_data = {
        "account_id": account_id,
        "total_transactions": total_txns,
        "average_amount": round(float(avg_amount), 2),
        "flagged_anomalies_count": flagged_txns,
        "anomaly_rate": round(anomaly_ratio, 3),
        "risk_tier": risk_tier,
        "last_anomaly": {
            "risk_score": float(recent_anomaly["risk_score"]),
            "reason": recent_anomaly["reason"],
            "detected_at": recent_anomaly["detected_at"].isoformat(),
        } if recent_anomaly else None,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }

    # 3. Store in Redis with 120s TTL
    cache_account_risk(account_id, profile_data, ttl_seconds=120)

    return {
        "source": "database",
        "profile": profile_data,
    }


@app.delete("/accounts/{account_id}/cache", tags=["Accounts"])
def invalidate_account_profile_cache(account_id: str):
    """Manually invalidate an account's risk cache in Redis."""
    invalidate_account_cache(account_id)
    return {"message": f"Cache invalidated for account {account_id}"}


# ==========================================
# ML Endpoints
# ==========================================

@app.get("/ml/isolation-forest", tags=["ML"])
def get_isolation_forest_results(limit: int = 20):
    """Return Isolation Forest anomaly results."""
    with engine.connect() as conn:
        query = (
            select(
                ml_scores.c.transaction_id,
                ml_scores.c.anomaly_score,
                ml_scores.c.ml_anomaly,
            )
            .order_by(ml_scores.c.anomaly_score.asc())
            .limit(limit)
        )
        rows = conn.execute(query).mappings().all()
        return {
            "count": len(rows),
            "results": [dict(row) for row in rows],
        }


@app.get("/ml/logistic-regression", tags=["ML"])
def get_logistic_regression_results(limit: int = 20):
    """Return Logistic Regression predictions."""
    with engine.connect() as conn:
        query = (
            select(
                ml_scores_lr.c.transaction_id,
                ml_scores_lr.c.lr_prediction,
                ml_scores_lr.c.lr_probability,
            )
            .order_by(ml_scores_lr.c.lr_probability.desc())
            .limit(limit)
        )
        rows = conn.execute(query).mappings().all()
        return {
            "count": len(rows),
            "results": [dict(row) for row in rows],
        }


@app.get("/ml/comparison", tags=["ML"])
def get_model_comparison(limit: int = 20):
    """Return Rules, Isolation Forest and Logistic Regression results together."""
    with engine.connect() as conn:
        query = (
            select(
                model_comparison.c.transaction_id,
                model_comparison.c.account_id,
                model_comparison.c.amount,
                model_comparison.c.rule_flag,
                model_comparison.c.ml_anomaly,
                model_comparison.c.anomaly_score,
                model_comparison.c.lr_prediction,
                model_comparison.c.lr_probability,
            )
            .order_by(model_comparison.c.transaction_id.desc())
            .limit(limit)
        )
        rows = conn.execute(query).mappings().all()
        return {
            "count": len(rows),
            "results": [dict(row) for row in rows],
        }
