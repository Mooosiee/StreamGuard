from datetime import datetime
from rules import (
    detect_anomaly,
    AMOUNT_THRESHOLD,
    ODD_HOUR_START,
    ODD_HOUR_END,
    VELOCITY_THRESHOLD,
    WINDOW_SECONDS,
)
import redis_client


def test_normal_transaction_rule():
    redis_client.reset_redis_connection_for_testing(None)

    txn = {
        "account_id": "acc_001",
        "amount": 500.0,
        "location": "Lahore",
        "merchant": "Foodpanda",
        "timestamp": "2026-10-30T14:30:00",  # 2:30 PM (normal hour)
    }
    status, risk_score, reasons = detect_anomaly(txn)
    assert status == "normal"
    assert risk_score == 0.0
    assert len(reasons) == 0


def test_high_amount_rule():
    redis_client.reset_redis_connection_for_testing(None)

    txn = {
        "account_id": "acc_001",
        "amount": AMOUNT_THRESHOLD + 10000,
        "location": "Karachi",
        "merchant": "Unknown POS",
        "timestamp": "2026-10-30T12:00:00",
    }
    status, risk_score, reasons = detect_anomaly(txn)
    assert status == "suspicious"
    assert risk_score >= 0.35
    assert "unusually high amount" in reasons


def test_odd_hour_rule():
    redis_client.reset_redis_connection_for_testing(None)

    # 3:00 AM is within ODD_HOUR_START (1) and ODD_HOUR_END (5)
    txn = {
        "account_id": "acc_002",
        "amount": 200.0,
        "location": "Islamabad",
        "merchant": "Uber",
        "timestamp": "2026-10-30T03:00:00",
    }
    status, risk_score, reasons = detect_anomaly(txn)
    assert status == "suspicious"
    assert risk_score >= 0.25
    assert "transaction at unusual hour" in reasons


def test_velocity_rule():
    redis_client.reset_redis_connection_for_testing(None)

    account_id = "acc_velocity_test"
    base_time = "2026-10-30T15:00:"

    # Submit VELOCITY_THRESHOLD + 1 transactions within WINDOW_SECONDS
    for i in range(VELOCITY_THRESHOLD):
        txn = {
            "account_id": account_id,
            "amount": 100.0,
            "location": "Lahore",
            "merchant": "Daraz",
            "timestamp": f"{base_time}{i:02d}",
        }
        status, _, _ = detect_anomaly(txn)
        assert status == "normal"

    # The 5th transaction should trigger the velocity rule
    over_limit_txn = {
        "account_id": account_id,
        "amount": 100.0,
        "location": "Lahore",
        "merchant": "Daraz",
        "timestamp": f"{base_time}{VELOCITY_THRESHOLD:02d}",
    }
    status, risk_score, reasons = detect_anomaly(over_limit_txn)
    assert status == "suspicious"
    assert risk_score >= 0.40
    assert any("transactions in" in r for r in reasons)


def test_combined_multiple_rules_capped():
    redis_client.reset_redis_connection_for_testing(None)

    account_id = "acc_combined"
    base_time = "2026-10-30T03:00:"  # Odd hour

    # Flood velocity
    for i in range(VELOCITY_THRESHOLD):
        detect_anomaly({
            "account_id": account_id,
            "amount": 100.0,
            "location": "Lahore",
            "merchant": "Daraz",
            "timestamp": f"{base_time}{i:02d}",
        })

    # High amount + Odd hour + Velocity triggered simultaneously
    flagged_txn = {
        "account_id": account_id,
        "amount": 150000.0,  # High amount
        "location": "Unknown",
        "merchant": "ATM Out of Area",
        "timestamp": f"{base_time}{VELOCITY_THRESHOLD:02d}",  # Odd hour + velocity
    }
    status, risk_score, reasons = detect_anomaly(flagged_txn)
    assert status == "suspicious"
    assert len(reasons) == 3
    assert risk_score == 1.0  # Capped at 1.0 max
