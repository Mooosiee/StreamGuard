from datetime import datetime
from redis_client import record_and_get_velocity

AMOUNT_THRESHOLD = 50000       # PKR - flag anything above this
ODD_HOUR_START = 1              # 1 AM
ODD_HOUR_END = 5                # 5 AM

WINDOW_SECONDS = 30             # look at the last 30 seconds
VELOCITY_THRESHOLD = 4          # more than 4 transactions in that window = suspicious

AMOUNT_RISK = 0.35
VELOCITY_RISK = 0.40
ODD_HOUR_RISK = 0.25


def check_velocity(account_id: str, ts: datetime) -> bool:
    """
    Checks if this transaction exceeds the sliding window velocity threshold.
    Uses distributed Redis Sorted Sets (with local fallback) to ensure state
    is shared accurately across multiple parallel consumer instances.
    """
    count = record_and_get_velocity(account_id, ts, window_seconds=WINDOW_SECONDS)
    return count > VELOCITY_THRESHOLD


def detect_anomaly(transaction: dict):
    """
    Evaluates a transaction dict against fraud detection rules.
    transaction: dict with keys account_id, amount, merchant, location, timestamp
    Returns: (status, risk_score, reasons)
    """
    reasons = []
    risk_score = 0.0

    ts = datetime.fromisoformat(transaction["timestamp"])

    # Rule 1: High amount threshold
    if transaction["amount"] > AMOUNT_THRESHOLD:
        reasons.append("unusually high amount")
        risk_score += AMOUNT_RISK

    # Rule 2: Distributed velocity sliding window check
    if check_velocity(transaction["account_id"], ts):
        reasons.append(
            f"more than {VELOCITY_THRESHOLD} transactions in {WINDOW_SECONDS}s"
        )
        risk_score += VELOCITY_RISK

    # Rule 3: Odd hours (unusual nocturnal activity)
    if ODD_HOUR_START <= ts.hour < ODD_HOUR_END:
        reasons.append("transaction at unusual hour")
        risk_score += ODD_HOUR_RISK

    status = "suspicious" if reasons else "normal"
    risk_score = round(min(1.0, risk_score), 2)

    return status, risk_score, reasons
