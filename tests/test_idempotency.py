import uuid
from sqlalchemy import select, func
from consumer import process_transaction
import schema


def test_consumer_persists_new_transaction(test_db_engine):
    event_id = str(uuid.uuid4())
    txn = {
        "event_id": event_id,
        "account_id": "acc_user_1",
        "amount": 2500.0,
        "location": "Lahore",
        "merchant": "Foodpanda",
        "timestamp": "2026-10-30T10:00:00",
    }

    result = process_transaction(txn, bind_engine=test_db_engine)
    assert result is True

    with test_db_engine.connect() as conn:
        row = conn.execute(
            select(schema.transactions).where(schema.transactions.c.event_id == event_id)
        ).mappings().first()
        assert row is not None
        assert row["amount"] == 2500.0
        assert row["status"] == "normal"


def test_idempotent_duplicate_event_handling(test_db_engine):
    """
    Simulates Kafka re-delivery or consumer restart:
    The same event_id processed multiple times must NOT create duplicate rows.
    """
    event_id = "evt_idempotency_12345"
    txn = {
        "event_id": event_id,
        "account_id": "acc_user_2",
        "amount": 1800.0,
        "location": "Karachi",
        "merchant": "Daraz",
        "timestamp": "2026-10-30T10:05:00",
    }

    # First delivery
    first_result = process_transaction(txn, bind_engine=test_db_engine)
    assert first_result is True

    # Second delivery (duplicate)
    second_result = process_transaction(txn, bind_engine=test_db_engine)
    # Must return True to acknowledge offset and prevent pipeline hang
    assert second_result is True

    # Third delivery (duplicate)
    third_result = process_transaction(txn, bind_engine=test_db_engine)
    assert third_result is True

    with test_db_engine.connect() as conn:
        count = conn.execute(
            select(func.count(schema.transactions.c.transaction_id)).where(
                schema.transactions.c.event_id == event_id
            )
        ).scalar()
        assert count == 1, "Duplicate records were created despite idempotency check"


def test_consumer_flags_and_persists_anomaly(test_db_engine):
    event_id = str(uuid.uuid4())
    flagged_txn = {
        "event_id": event_id,
        "account_id": "acc_user_3",
        "amount": 95000.0,  # exceeds 50000 threshold
        "location": "Islamabad",
        "merchant": "Unknown POS",
        "timestamp": "2026-10-30T11:00:00",
    }

    result = process_transaction(flagged_txn, bind_engine=test_db_engine)
    assert result is True

    with test_db_engine.connect() as conn:
        txn_row = conn.execute(
            select(schema.transactions).where(schema.transactions.c.event_id == event_id)
        ).mappings().first()
        assert txn_row is not None
        assert txn_row["status"] == "suspicious"

        anomaly_row = conn.execute(
            select(schema.anomalies).where(schema.anomalies.c.transaction_id == txn_row["transaction_id"])
        ).mappings().first()
        assert anomaly_row is not None
        assert "unusually high amount" in anomaly_row["reason"]
        assert anomaly_row["risk_score"] >= 0.35
