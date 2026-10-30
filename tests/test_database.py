from datetime import datetime, timezone
from sqlalchemy import insert, select
import schema


def test_database_table_creation(test_db_engine):
    """Verifies that all required tables and columns are defined."""
    table_names = schema.metadata.tables.keys()
    assert "accounts" in table_names
    assert "transactions" in table_names
    assert "anomalies" in table_names
    assert "ml_scores" in table_names
    assert "ml_scores_lr" in table_names
    assert "model_comparison" in table_names


def test_transactions_indexes_defined():
    """Verifies that performance and idempotency indexes are present."""
    index_names = [idx.name for idx in schema.transactions.indexes]
    assert "ix_transactions_event_id" in index_names or any("event_id" in idx.name for idx in schema.transactions.indexes)
    assert any("account_id" in idx.name for idx in schema.transactions.indexes)
    assert any("status" in idx.name for idx in schema.transactions.indexes)
    assert any("timestamp" in idx.name for idx in schema.transactions.indexes)


def test_account_creation_and_transaction_fk(test_db_engine):
    with test_db_engine.begin() as conn:
        conn.execute(
            insert(schema.accounts).values(
                account_id="acc_db_01",
                owner_name="Database Test User",
            )
        )
        res = conn.execute(
            insert(schema.transactions).values(
                event_id="evt_db_test_01",
                account_id="acc_db_01",
                amount=1500.00,
                location="Lahore",
                merchant="Daraz",
                status="normal",
                timestamp=datetime.now(timezone.utc),
            )
        )
        txn_id = res.inserted_primary_key[0]

        # Insert anomaly linked to transaction
        conn.execute(
            insert(schema.anomalies).values(
                transaction_id=txn_id,
                risk_score=0.88,
                reason="test anomaly",
            )
        )

    # Verify query with join
    with test_db_engine.connect() as conn:
        query = (
            select(
                schema.transactions.c.transaction_id,
                schema.transactions.c.event_id,
                schema.transactions.c.account_id,
                schema.anomalies.c.risk_score,
                schema.anomalies.c.reason,
            )
            .join(schema.anomalies, schema.anomalies.c.transaction_id == schema.transactions.c.transaction_id)
            .where(schema.transactions.c.transaction_id == txn_id)
        )
        row = conn.execute(query).mappings().first()
        assert row is not None
        assert row["event_id"] == "evt_db_test_01"
        assert float(row["risk_score"]) == 0.88
