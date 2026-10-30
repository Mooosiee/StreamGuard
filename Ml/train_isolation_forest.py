
import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import IsolationForest
import os
from dotenv import load_dotenv

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")
CONN_STRING = f"postgresql://postgres:{DB_PASSWORD}@localhost:5433/fraud_detection"

# Features Isolation Forest is allowed to see.
# rule_flag is deliberately excluded - IF must learn "anomalous" purely
# from data shape, with zero exposure to what the rules decided.
FEATURE_COLS = [
    "amount", "hour", "day_of_week", "is_weekend",
    "transactions_last_30_sec", "transactions_last_5_min", "transactions_last_1_hour",
    "rolling_avg_amount", "amount_deviation", "risk_score",
]

def load_feature_table(engine) -> pd.DataFrame:
    query = "SELECT * FROM transaction_features"
    return pd.read_sql(query, engine)

def pick_contamination(df: pd.DataFrame) -> float:
    """
    Anchor Isolation Forest's expected anomaly rate to roughly how often
    the rule engine actually flags things. This uses rule_flag only to
    choose a hyperparameter (how many anomalies to expect), NOT as a
    training input - IF still never sees rule_flag during .fit().
    """
    rate = df["rule_flag"].mean()
    # clamp to a sane range - IsolationForest expects (0, 0.5]
    return float(min(max(rate, 0.01), 0.5))

def train_and_score(df: pd.DataFrame) -> pd.DataFrame:
    X = df[FEATURE_COLS]

    contamination = pick_contamination(df)
    print(f"Using contamination={contamination:.4f} (based on rule_flag rate)")

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
    )
    model.fit(X)
    # decision_function: higher = more normal, lower/negative = more anomalous
    df["anomaly_score"] = model.decision_function(X)
    # predict: -1 = anomaly, 1 = normal -> convert to 1/0 flag
    df["ml_anomaly"] = (model.predict(X) == -1).astype(int)

    return df[["transaction_id", "anomaly_score", "ml_anomaly"]]
def run():
    engine = create_engine(CONN_STRING)

    df = load_feature_table(engine)
    scores_df = train_and_score(df)

    scores_df.to_sql("ml_scores", engine, if_exists="replace", index=False)
    print(f"Wrote {len(scores_df)} rows to ml_scores")
    print(scores_df.head())

    n_flagged = scores_df["ml_anomaly"].sum()
    print(f"Isolation Forest flagged {n_flagged} / {len(scores_df)} transactions as anomalous")

    return scores_df


if __name__ == "__main__":
    run()