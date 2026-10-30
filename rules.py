
from datetime import datetime, timedelta
from collections import deque

AMOUNT_THRESHOLD = 50000       # PKR - flag anything above this
ODD_HOUR_START = 1              # 1 AM
ODD_HOUR_END = 5                # 5 AM

WINDOW_SECONDS = 30              # look at the last 30 seconds
VELOCITY_THRESHOLD = 4           # more than 4 transactions in that window = suspicious

AMOUNT_RISK = 0.35
VELOCITY_RISK = 0.40
ODD_HOUR_RISK = 0.25
# deque of recent datetime objects
_recent_transactions: dict[str, deque] = {}

def _update_and_check_velocity(account_id: str, ts: datetime) -> bool:
    """Add this transaction's timestamp to the account's window, drop old ones,
    return True if the account is over the velocity threshold."""
    if account_id not in _recent_transactions:
        _recent_transactions[account_id] = deque()

    window = _recent_transactions[account_id]
    window.append(ts)

    cutoff = ts - timedelta(seconds=WINDOW_SECONDS)
    while window and window[0] < cutoff:
        window.popleft()

    return len(window) > VELOCITY_THRESHOLD

def detect_anomaly(transaction: dict):
    """
    transaction: dict with keys account_id, amount, merchant, location, timestamp
    Returns: (status, risk_score, reasons)
    """

    reasons = []
    risk_score = 0.0

    ts = datetime.fromisoformat(transaction["timestamp"])

    # Rule 1: amount too high
    if transaction["amount"] > AMOUNT_THRESHOLD:
        reasons.append("unusually high amount")
        risk_score += AMOUNT_RISK

    # Rule 2: velocity
    if _update_and_check_velocity(transaction["account_id"], ts):
        reasons.append(
            f"more than {VELOCITY_THRESHOLD} transactions in {WINDOW_SECONDS}s"
        )
        risk_score += VELOCITY_RISK

    # Rule 3: odd hour
    if ODD_HOUR_START <= ts.hour < ODD_HOUR_END:
        reasons.append("transaction at unusual hour")
        risk_score += ODD_HOUR_RISK

    status = "suspicious" if reasons else "normal"

  
    risk_score = round(min(1.0, risk_score), 2)

    return status, risk_score, reasons
