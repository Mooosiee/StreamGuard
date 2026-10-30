import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    precision_score, recall_score, f1_score,
    confusion_matrix, roc_auc_score, classification_report
)
import os
from dotenv import load_dotenv

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")
CONN_STRING = f"postgresql://postgres:{DB_PASSWORD}@localhost:5433/fraud_detection"

FEATURE_COLS = [
    "amount",
    "hour",
    "day_of_week",
    "is_weekend",
    "transactions_last_30_sec",
    "transactions_last_5_min",
    "transactions_last_1_hour",
    "rolling_avg_amount",
    "amount_deviation",
]

TARGET_COL = "rule_flag"


def load_feature_table(engine) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM transaction_features", engine)


def train_and_evaluate(df: pd.DataFrame):
    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        stratify=y,
        random_state=42,
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    model = LogisticRegression(
        max_iter=1000,
        random_state=42,
    )

    model.fit(X_train_scaled, y_train)

    y_pred = model.predict(X_test_scaled)
    y_proba = model.predict_proba(X_test_scaled)[:, 1]

    print("\nLogistic Regression Evaluation")
    print(f"Precision: {precision_score(y_test, y_pred):.3f}")
    print(f"Recall:    {recall_score(y_test, y_pred):.3f}")
    print(f"F1:        {f1_score(y_test, y_pred):.3f}")
    print(f"ROC-AUC:   {roc_auc_score(y_test, y_proba):.3f}")

    print("\nConfusion matrix:")
    print(confusion_matrix(y_test, y_pred))

    print("\nFull report:")
    print(
        classification_report(
            y_test,
            y_pred,
            target_names=["normal", "suspicious"]
        )
    )

    coef_df = pd.DataFrame({
        "feature": FEATURE_COLS,
        "coefficient": model.coef_[0]
    }).sort_values(
        "coefficient",
        key=abs,
        ascending=False
    )

    print("\nFeature coefficients (sorted by magnitude):")
    print(coef_df.to_string(index=False))

    X_all_scaled = scaler.transform(X)

    df["lr_prediction"] = model.predict(X_all_scaled)
    df["lr_probability"] = model.predict_proba(X_all_scaled)[:, 1]

    return (
        df[["transaction_id", "lr_prediction", "lr_probability"]],
        coef_df
    )


def run():
    engine = create_engine(CONN_STRING)

    df = load_feature_table(engine)

    scores_df, coef_df = train_and_evaluate(df)

    scores_df.to_sql(
        "ml_scores_lr",
        engine,
        if_exists="replace",
        index=False
    )

    print(f"\nWrote {len(scores_df)} rows to ml_scores_lr")

    return scores_df, coef_df


if __name__ == "__main__":
    run()