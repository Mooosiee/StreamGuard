
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
 
load_dotenv()
 
DB_PASSWORD = os.getenv("DB_PASSWORD")
CONN_STRING = f"postgresql://postgres:{DB_PASSWORD}@localhost:5433/fraud_detection"
 
 
def load_all_scores(engine) -> pd.DataFrame:
    query = """
        SELECT
            f.transaction_id,
            f.account_id,
            f.amount,
            f.rule_flag,
            m.ml_anomaly,
            m.anomaly_score,
            l.lr_prediction,
            l.lr_probability
        FROM transaction_features f
        LEFT JOIN ml_scores m ON m.transaction_id = f.transaction_id
        LEFT JOIN ml_scores_lr l ON l.transaction_id = f.transaction_id
    """
    return pd.read_sql(query, engine)
 
 
def agreement_rate(a: pd.Series, b: pd.Series) -> float:
    return float((a == b).mean())
 
 
def compute_agreements(df: pd.DataFrame) -> dict:
    return {
        "Rules <-> Isolation Forest": agreement_rate(df["rule_flag"], df["ml_anomaly"]),
        "Rules <-> Logistic Regression": agreement_rate(df["rule_flag"], df["lr_prediction"]),
        "Isolation Forest <-> Logistic Regression": agreement_rate(df["ml_anomaly"], df["lr_prediction"]),
    }
 
 
def find_disagreements(df: pd.DataFrame) -> dict:
    return {
        # IF caught something the rules completely missed
        "IF flagged, Rules missed": df[(df["ml_anomaly"] == 1) & (df["rule_flag"] == 0)],
        # Rules flagged it, but IF (with no exposure to rules) disagreed
        "Rules flagged, IF missed": df[(df["rule_flag"] == 1) & (df["ml_anomaly"] == 0)],
        # LR disagrees with the very labels it was trained on - borderline/ambiguous cases
        "LR disagreed with its own training label (Rules)": df[df["rule_flag"] != df["lr_prediction"]],
        # All three disagree  genuinely ambiguous transactions
         "One model disagreed with the other two": df[
            (df["rule_flag"] != df["ml_anomaly"]) & (df["ml_anomaly"] != df["lr_prediction"])
        ],
    }
 
 
def run():
    engine = create_engine(CONN_STRING)
    df = load_all_scores(engine)
 
    print(f"Loaded {len(df)} transactions with all three model outputs\n")
 
    print("Agreement rates:")
    agreements = compute_agreements(df)
    for pair, rate in agreements.items():
        print(f"{pair}: {rate:.1%}")
 
    print(" Disagreement cases")
    disagreements = find_disagreements(df)
    for label, subset in disagreements.items():
        print(f"\n{label}: {len(subset)} transactions")
        if len(subset) > 0:
            cols = ["transaction_id", "account_id", "amount", "rule_flag",
                    "ml_anomaly", "anomaly_score", "lr_prediction", "lr_probability"]
            print(subset[cols].head(5).to_string(index=False))
 
    # Save the full comparison table for the write-up
    df.to_sql("model_comparison", engine, if_exists="replace", index=False)
    print(f"\nWrote {len(df)} rows to model_comparison")
 
    return df, agreements, disagreements
 
 
if __name__ == "__main__":
    run()
 