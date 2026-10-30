import json
import logging

from datetime import datetime

from kafka import KafkaConsumer

from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from schema import engine, accounts, transactions, anomalies
from rules import detect_anomaly


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


TOPIC = "transactions"


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="fraud-detection-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)


def ensure_account_exists(conn, account_id):
    exists = conn.execute(
        select(accounts).where(accounts.c.account_id == account_id)
    ).first()

    if not exists:
        conn.execute(
            insert(accounts).values(account_id=account_id)
        )


def validate_transaction(txn: dict):
    required_fields = [
        "account_id",
        "amount",
        "location",
        "merchant",
        "timestamp",
    ]

    for field in required_fields:
        if field not in txn:
            return False, f"Missing field: {field}"

    if not isinstance(txn["amount"], (int, float)):
        return False, "Amount must be a number"

    try:
        datetime.fromisoformat(txn["timestamp"])
    except (ValueError, TypeError):
        return False, "Invalid timestamp"

    return True, None


def process_transaction(txn: dict):

    is_valid, error_msg = validate_transaction(txn)

    if not is_valid:
        logger.warning(f"Invalid transaction: {error_msg}")
        return False

    with engine.begin() as conn:

        ensure_account_exists(conn, txn["account_id"])

        status, risk_score, reasons = detect_anomaly(txn)

        result = conn.execute(
            insert(transactions).values(
                account_id=txn["account_id"],
                amount=txn["amount"],
                location=txn["location"],
                merchant=txn["merchant"],
                status=status,
                timestamp=datetime.fromisoformat(txn["timestamp"]),
            )
        )

        txn_id = result.inserted_primary_key[0]

        if status == "suspicious":

            conn.execute(
                insert(anomalies).values(
                    transaction_id=txn_id,
                    risk_score=risk_score,
                    reason=", ".join(reasons),
                )
            )

            logger.warning(
                f"FLAGGED txn {txn_id}: "
                f"{reasons} (risk {risk_score})"
            )

        else:
            logger.info(f"Stored normal txn {txn_id}")

    return True


if __name__ == "__main__":

    logger.info(
        f"Listening on '{TOPIC}' "
        "and writing to Postgres. Ctrl+C to stop."
    )

    try:

        for message in consumer:

            try:

                processed = process_transaction(message.value)

                if processed:
                    consumer.commit()

                else:
                    logger.warning(
                        "Invalid message was not committed."
                    )

            except IntegrityError as e:

                logger.error(
                    f"DB error on this transaction, "
                    f"skipping commit: {e}"
                )

                break

    except KeyboardInterrupt:

        logger.info("Stopped.")

    finally:

        consumer.close()