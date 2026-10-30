import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

#Config

DB_PASSWORD = os.getenv("DB_PASSWORD")
CONN_STRING = (
    f"postgresql://postgres:{DB_PASSWORD}"
    f"@localhost:5433/fraud_detection"
)

ROLLING_WINDOW_FOR_AVG = 20
#LOAD DATA
def load_transactions(engine) -> pd.DataFrame:
    """
    Load transactions and their anomaly information from PostgreSQL.
    """
    query = """
        SELECT
            t.transaction_id,
            t.account_id,
            t.amount,
            t.timestamp,
            t.status,
            a.risk_score
        FROM transactions t
        LEFT JOIN anomalies a
            ON a.transaction_id = t.transaction_id
        ORDER BY t.account_id, t.timestamp
    """
    df = pd.read_sql(
        query,
        engine,
        parse_dates=["timestamp"]
    )
    df["risk_score"] = df["risk_score"].fillna(0).astype(float)

    return df
  
#VELOCITY FEATURES 

def compute_velocity_features(df: pd.DataFrame) -> pd.DataFrame:

    df = df.sort_values(
        ["account_id", "timestamp"]
    ).reset_index(drop=True)

    counts_30sec = []
    counts_5min = []
    counts_1hr = []

    for account_id, group in df.groupby("account_id"):

        ts_values = group["timestamp"].values

        for i, current_ts in enumerate(ts_values):

            window_30sec_start = (
                current_ts - np.timedelta64(30, "s")
            )

            window_5min_start = (
                current_ts - np.timedelta64(5, "m")
            )

            window_1hr_start = (
                current_ts - np.timedelta64(1, "h")
            )

            # Only transactions BEFORE the current transaction
            prior = ts_values[:i]

            counts_30sec.append(
                int(
                    (
                        (prior >= window_30sec_start)
                        & (prior < current_ts)
                    ).sum()
                )
            )

            counts_5min.append(
                int(
                    (
                        (prior >= window_5min_start)
                        & (prior < current_ts)
                    ).sum()
                )
            )

            counts_1hr.append(
                int(
                    (
                        (prior >= window_1hr_start)
                        & (prior < current_ts)
                    ).sum()
                )
            )

    df["transactions_last_30_sec"] = counts_30sec
    df["transactions_last_5_min"] = counts_5min
    df["transactions_last_1_hour"] = counts_1hr

    return df


# AMOUNT FEATURES

def compute_rolling_amount_features(
    df: pd.DataFrame
) -> pd.DataFrame:

    df = df.sort_values(
        ["account_id", "timestamp"]
    ).reset_index(drop=True)

    df["rolling_avg_amount"] = (
        df.groupby("account_id")["amount"]
        .transform(
            lambda s:
            s.shift(1)
            .rolling(
                window=ROLLING_WINDOW_FOR_AVG,
                min_periods=1
            )
            .mean()
        )
    )

    # No previous transaction = no previous average.
    # For now, use the current amount so deviation becomes 0.
    df["rolling_avg_amount"] = (
        df["rolling_avg_amount"]
        .fillna(df["amount"])
    )

    df["amount_deviation"] = (
        df["amount"] - df["rolling_avg_amount"]
    )

    return df


# TIME FEATURES

def compute_time_features(
    df: pd.DataFrame
) -> pd.DataFrame:

    df["hour"] = df["timestamp"].dt.hour

    df["day_of_week"] = (
        df["timestamp"].dt.dayofweek
    )

    df["is_weekend"] = (
        df["day_of_week"].isin([5, 6]).astype(int)
    )

    return df


#RULE LABEL 

def compute_rule_flag(
    df: pd.DataFrame
) -> pd.DataFrame:

    df["rule_flag"] = (
        df["status"] == "suspicious"
    ).astype(int)

    return df
#BUILD FEATURE TABLE 

def build_feature_table():

    engine = create_engine(CONN_STRING)

    df = load_transactions(engine)

    df = compute_velocity_features(df)

    df = compute_rolling_amount_features(df)

    df = compute_time_features(df)

    df = compute_rule_flag(df)

    # Columns we keep in our ML dataset
    feature_cols = [
        "transaction_id",
        "account_id",

        # ML features
        "amount",
        "hour",
        "day_of_week",
        "is_weekend",
        "transactions_last_30_sec",
        "transactions_last_5_min",
        "transactions_last_1_hour",
        "rolling_avg_amount",
        "amount_deviation",
        "risk_score",

        # Target for Logistic Regression
        "rule_flag"
    ]

    feature_df = df[feature_cols]

    feature_df.to_sql(
        "transaction_features",
        engine,
        if_exists="replace",
        index=False
    )

    print(
        f"Wrote {len(feature_df)} rows "
        "to transaction_features"
    )

    print(feature_df.head())

    return feature_df


if __name__ == "__main__":
    build_feature_table()