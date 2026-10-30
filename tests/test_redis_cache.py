from datetime import datetime, timedelta
import redis_client


def test_sliding_window_velocity_tracking():
    redis_client.reset_redis_connection_for_testing(None)

    account_id = "acc_test_window"
    now = datetime(2026, 10, 30, 12, 0, 0)

    # 1st txn at T+0s
    c1 = redis_client.record_and_get_velocity(account_id, now, window_seconds=30)
    assert c1 == 1

    # 2nd txn at T+5s
    c2 = redis_client.record_and_get_velocity(account_id, now + timedelta(seconds=5), window_seconds=30)
    assert c2 == 2

    # 3rd txn at T+15s
    c3 = redis_client.record_and_get_velocity(account_id, now + timedelta(seconds=15), window_seconds=30)
    assert c3 == 3

    # 4th txn at T+40s (1st txn at T+0s should now slide out of the 30s window)
    c4 = redis_client.record_and_get_velocity(account_id, now + timedelta(seconds=40), window_seconds=30)
    # Txns inside window are now: T+15s, T+40s -> count = 2 (T+0s and T+5s expired)
    assert c4 == 2


def test_account_risk_cache_and_invalidation():
    redis_client.reset_redis_connection_for_testing(None)

    account_id = "acc_cache_test"
    sample_profile = {
        "account_id": account_id,
        "risk_tier": "LOW",
        "flagged_anomalies_count": 0,
        "total_transactions": 10,
    }

    # Cache initially empty
    assert redis_client.get_cached_account_risk(account_id) is None

    # Write to cache with TTL
    redis_client.cache_account_risk(account_id, sample_profile, ttl_seconds=60)

    # Cache hit
    cached = redis_client.get_cached_account_risk(account_id)
    assert cached is not None
    assert cached["risk_tier"] == "LOW"
    assert cached["total_transactions"] == 10

    # Cache Invalidation
    redis_client.invalidate_account_cache(account_id)
    assert redis_client.get_cached_account_risk(account_id) is None


def test_fast_path_idempotency_marker():
    redis_client.reset_redis_connection_for_testing(None)

    event_id = "evt_fast_path_001"

    # First time seen -> True
    first_seen = redis_client.mark_event_seen(event_id, ttl_seconds=300)
    assert first_seen is True

    # Second time seen -> False (duplicate detected!)
    second_seen = redis_client.mark_event_seen(event_id, ttl_seconds=300)
    assert second_seen is False
