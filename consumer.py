import json
import logging
import os
import uuid
from datetime import datetime

from dotenv import load_dotenv
try:
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from schema import engine, accounts, transactions, anomalies
from rules import detect_anomaly
from redis_client import invalidate_account_cache

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "fraud-detection-group")


def get_kafka_consumer():
    """Factory to create KafkaConsumer instance."""
    if KafkaConsumer is None:
        raise RuntimeError("kafka-python package is not installed")
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def ensure_account_exists(conn, account_id: str):
    """Ensure an account record exists to satisfy foreign key constraints."""
    exists = conn.execute(
        select(accounts.c.account_id).where(accounts.c.account_id == account_id)
    ).first()

    if not exists:
        conn.execute(
            insert(accounts).values(
                account_id=account_id,
                owner_name=f"User {account_id}",
            )
        )


def validate_transaction(txn: dict):
    """
    Validates payload structure and data types.
    Ensures account_id, amount, location, merchant, and timestamp are well-formed.
    """
    if not isinstance(txn, dict):
        return False, "Payload must be a JSON object"

    required_fields = [
        "account_id",
        "amount",
        "location",
        "merchant",
        "timestamp",
    ]

    for field in required_fields:
        if field not in txn or txn[field] is None:
            return False, f"Missing field: {field}"

    if not isinstance(txn["amount"], (int, float)):
        return False, "Amount must be a number"

    if txn["amount"] < 0:
        return False, "Amount cannot be negative"

    try:
        datetime.fromisoformat(txn["timestamp"])
    except (ValueError, TypeError):
        return False, "Invalid timestamp format (ISO-8601 required)"

    return True, None


def process_transaction(txn: dict, bind_engine=None):
    """
    Ingests and processes a transaction with strict idempotency:
    1. Validates payload format.
    2. Guarantees unique event_id (deduplication check).
    3. Runs fraud detection rules.
    4. Persists transaction and anomaly (if flagged) atomically.
    5. Invalidates cached account risk profile in Redis.
    
    Returns:
        bool: True if message was successfully processed or recognized as duplicate (commit offset).
              False if message is permanently invalid (poison pill handling).
    """
    is_valid, error_msg = validate_transaction(txn)
    if not is_valid:
        logger.warning(f"Invalid transaction rejected: {error_msg}")
        return False

    # Ensure transaction has a unique event_id for idempotency
    event_id = txn.get("event_id")
    if not event_id:
        event_id = str(uuid.uuid4())
        txn["event_id"] = event_id

    db_engine = bind_engine or engine

    try:
        with db_engine.begin() as conn:
            # Step 1: Idempotency check in database
            existing = conn.execute(
                select(transactions.c.transaction_id).where(
                    transactions.c.event_id == event_id
                )
            ).first()

            if existing:
                logger.info(
                    f"[IDEMPOTENT] Duplicate event '{event_id}' already processed as txn_id={existing[0]}. Skipping."
                )
                return True

            # Step 2: Ensure account exists
            ensure_account_exists(conn, txn["account_id"])

            # Step 3: Run real-time detection rules
            status, risk_score, reasons = detect_anomaly(txn)

            # Step 4: Persist transaction
            result = conn.execute(
                insert(transactions).values(
                    event_id=event_id,
                    account_id=txn["account_id"],
                    amount=txn["amount"],
                    location=txn["location"],
                    merchant=txn["merchant"],
                    status=status,
                    timestamp=datetime.fromisoformat(txn["timestamp"]),
                )
            )

            txn_id = result.inserted_primary_key[0]

            # Step 5: Persist anomaly record if suspicious
            if status == "suspicious":
                conn.execute(
                    insert(anomalies).values(
                        transaction_id=txn_id,
                        risk_score=risk_score,
                        reason=", ".join(reasons),
                    )
                )
                logger.warning(
                    f"FLAGGED txn {txn_id} (event: {event_id}): {reasons} (risk: {risk_score})"
                )
            else:
                logger.info(f"Stored normal txn {txn_id} (event: {event_id})")

            # Step 6: Invalidate cached account risk profile in Redis
            invalidate_account_cache(txn["account_id"])

        return True

    except IntegrityError as exc:
        # Race condition or duplicate event_id constraint hit
        if "unique" in str(exc).lower() or "event_id" in str(exc).lower():
            logger.warning(f"[IDEMPOTENT] Caught unique constraint on event_id '{event_id}'. Safe to commit offset.")
            return True
        logger.error(f"Database IntegrityError processing event '{event_id}': {exc}")
        return False
    except Exception as exc:
        logger.error(f"Unexpected error processing event '{event_id}': {exc}", exc_info=True)
        return False


def run_consumer():
    """Main consumer loop."""
    logger.info(
        f"Connecting consumer to '{TOPIC}' at {KAFKA_BOOTSTRAP_SERVERS}. Press Ctrl+C to stop."
    )
    consumer = get_kafka_consumer()

    try:
        for message in consumer:
            try:
                processed = process_transaction(message.value)
                if processed:
                    consumer.commit()
                else:
                    logger.warning(
                        f"Unprocessable message at offset {message.offset} committed as dead-letter."
                    )
                    consumer.commit()
            except Exception as e:
                logger.error(f"Error handling message at offset {message.offset}: {e}")
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
