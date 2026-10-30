from dotenv import load_dotenv
import os
load_dotenv()  # take environment variables from .env.
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Numeric, DateTime, ForeignKey, select, insert
)
from datetime import datetime
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_URL = f"postgresql://postgres:{DB_PASSWORD}@localhost:5433/fraud_detection"
engine = create_engine(DB_URL, echo=False)  
metadata = MetaData()
# accounts 
accounts = Table(
    "accounts",
    metadata,
    Column("account_id", String(50), primary_key=True),
    Column("owner_name", String(100)),
    Column("created_at", DateTime, default=datetime.utcnow),
)

# transactions
transactions = Table(
    "transactions",
    metadata,
    Column("transaction_id", Integer, primary_key=True, autoincrement=True),
    Column("account_id", String(50), ForeignKey("accounts.account_id"), nullable=False),
    Column("amount", Numeric(12, 2), nullable=False),
    Column("location", String(100)),
    Column("merchant", String(100)),
    Column("status", String(20), default="normal"),  # 'normal' or 'suspicious'
    Column("timestamp", DateTime, default=datetime.utcnow),
)

# anomalies
anomalies = Table(
    "anomalies",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("transaction_id", Integer, ForeignKey("transactions.transaction_id"), nullable=False),
    Column("risk_score", Numeric(4, 2)),   # e.g. 0.82
    Column("reason", String(255)),          # e.g. "unusually high amount"
    Column("detected_at", DateTime, default=datetime.utcnow),
)
# ML scores from Isolation Forest
ml_scores = Table(
    "ml_scores",
    metadata,
    Column("transaction_id", Integer, primary_key=True),
    Column("anomaly_score", Numeric),
    Column("ml_anomaly", Integer),
)
# ML scores from Logistic Regression
ml_scores_lr = Table(
    "ml_scores_lr",
    metadata,
    Column("transaction_id", Integer, primary_key=True),
    Column("lr_prediction", Integer),
    Column("lr_probability", Numeric),
)
# Combined comparison of Rules + Isolation Forest + Logistic Regression
model_comparison = Table(
    "model_comparison",
    metadata,
    Column("transaction_id", Integer, primary_key=True),
    Column("account_id", String(50)),
    Column("amount", Numeric(12, 2)),
    Column("rule_flag", Integer),
    Column("ml_anomaly", Integer),
    Column("anomaly_score", Numeric),
    Column("lr_prediction", Integer),
    Column("lr_probability", Numeric),
)
def create_tables():
    metadata.create_all(engine)
    print(">>> All database tables ready.")


def sanity_check():
    with engine.begin() as conn:
        conn.execute(insert(accounts).values(account_id="acc_001", owner_name="Test User"))

        result = conn.execute(
            insert(transactions).values(
                account_id="acc_001",
                amount=99999.00,
                location="Lahore",
                merchant="Unknown POS",
                status="suspicious",
            )
        )
        txn_id = result.inserted_primary_key[0]

        conn.execute(
            insert(anomalies).values(
                transaction_id=txn_id,
                risk_score=0.91,
                reason="unusually high amount",
            )
        )

    # Join it back
    with engine.connect() as conn:
        query = (
            select(
                transactions.c.transaction_id,
                transactions.c.amount,
                transactions.c.status,
                anomalies.c.risk_score,
                anomalies.c.reason,
            )
            .join(anomalies, anomalies.c.transaction_id == transactions.c.transaction_id)
        )
        for row in conn.execute(query):
            print(">>> Joined result:", row)


if __name__ == "__main__":
    create_tables()
    sanity_check()
