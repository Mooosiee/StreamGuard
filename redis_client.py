import json
import logging
import os
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# In-memory fallbacks when Redis is not reachable (e.g. during local tests)
_in_memory_velocity: Dict[str, deque] = {}
_in_memory_cache: Dict[str, Dict[str, Any]] = {}
_in_memory_seen_events: set = set()

_redis_instance = None
_redis_available: Optional[bool] = None


import socket

def _is_redis_reachable(host: str, port: int, timeout: float = 0.2) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def get_redis_client():
    """Returns a connected Redis client instance, or None if unavailable."""
    global _redis_instance, _redis_available

    if _redis_available is False:
        return None

    if _redis_instance is not None:
        return _redis_instance

    if not _is_redis_reachable(REDIS_HOST, REDIS_PORT, timeout=0.2):
        _redis_available = False
        _redis_instance = None
        logger.warning(
            f"Redis unreachable at {REDIS_HOST}:{REDIS_PORT}. Operating with graceful in-memory fallback."
        )
        return None

    try:
        import redis
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            socket_timeout=1.5,
            socket_connect_timeout=1.5,
            retry_on_timeout=False,
            decode_responses=True,
        )
        client.ping()
        _redis_instance = client
        _redis_available = True
        logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        return _redis_instance
    except Exception as exc:
        _redis_available = False
        _redis_instance = None
        logger.warning(
            f"Redis unavailable ({exc}). Operating with graceful in-memory fallback."
        )
        return None


def reset_redis_connection_for_testing(client=None):
    """Allows test fixtures to inject mock or reset redis connection state."""
    global _redis_instance, _redis_available, _in_memory_velocity, _in_memory_cache, _in_memory_seen_events
    _redis_instance = client
    _redis_available = True if client is not None else None
    _in_memory_velocity.clear()
    _in_memory_cache.clear()
    _in_memory_seen_events.clear()


# ==========================================
# 1. Distributed Sliding Window Velocity
# ==========================================

def record_and_get_velocity(
    account_id: str,
    ts: datetime,
    window_seconds: int = 30,
) -> int:
    """
    Records this transaction timestamp and returns the total number of
    transactions observed for this account within the sliding window.
    
    Uses Redis Sorted Set (ZADD / ZREMRANGEBYSCORE / ZCARD) to ensure
    cross-instance shared state across multiple distributed consumers.
    Falls back cleanly to in-memory sliding window when Redis is offline.
    """
    client = get_redis_client()
    score = ts.timestamp()

    if client:
        try:
            key = f"velocity:{account_id}"
            member = f"{score}:{uuid.uuid4().hex[:8]}"
            cutoff = score - window_seconds

            pipe = client.pipeline()
            pipe.zadd(key, {member: score})
            pipe.zremrangebyscore(key, "-inf", f"({cutoff}")
            pipe.zcard(key)
            pipe.expire(key, max(window_seconds * 4, 120))
            results = pipe.execute()

            # results[2] is the ZCARD after pruning
            return int(results[2])
        except Exception as exc:
            logger.warning(f"Redis velocity check failed ({exc}), falling back to in-memory")

    # Fallback to local in-memory sliding window
    if account_id not in _in_memory_velocity:
        _in_memory_velocity[account_id] = deque()

    window = _in_memory_velocity[account_id]
    window.append(ts)
    cutoff_dt = ts - timedelta(seconds=window_seconds)
    while window and window[0] < cutoff_dt:
        window.popleft()

    return len(window)


# ==========================================
# 2. Account Risk Profile & Cache Management
# ==========================================

def get_cached_account_risk(account_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve cached risk profile or summary for an account."""
    client = get_redis_client()
    key = f"account:risk:{account_id}"

    if client:
        try:
            data = client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as exc:
            logger.warning(f"Failed to read risk cache from Redis: {exc}")

    # Fallback check
    cached = _in_memory_cache.get(key)
    if cached:
        expires_at = cached.get("_expires_at")
        if expires_at and datetime.now(timezone.utc) > expires_at:
            del _in_memory_cache[key]
            return None
        return {k: v for k, v in cached.items() if k != "_expires_at"}
    return None


def cache_account_risk(account_id: str, profile_data: Dict[str, Any], ttl_seconds: int = 120) -> None:
    """Store account risk profile in cache with TTL."""
    client = get_redis_client()
    key = f"account:risk:{account_id}"

    if client:
        try:
            client.setex(key, ttl_seconds, json.dumps(profile_data))
            return
        except Exception as exc:
            logger.warning(f"Failed to write risk cache to Redis: {exc}")

    # Fallback storage
    data_with_expiry = dict(profile_data)
    data_with_expiry["_expires_at"] = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
    _in_memory_cache[key] = data_with_expiry


def invalidate_account_cache(account_id: str) -> None:
    """Invalidate cached risk profile when a new transaction/anomaly is processed."""
    client = get_redis_client()
    key = f"account:risk:{account_id}"

    if client:
        try:
            client.delete(key)
        except Exception as exc:
            logger.warning(f"Failed to invalidate risk cache in Redis: {exc}")

    _in_memory_cache.pop(key, None)


# ==========================================
# 3. Redis Fast-Path Idempotency Check
# ==========================================

def mark_event_seen(event_id: str, ttl_seconds: int = 86400) -> bool:
    """
    Returns True if this is the first time the event is seen (atomically marks it seen).
    Returns False if already seen (duplicate).
    """
    client = get_redis_client()
    key = f"event:{event_id}"

    if client:
        try:
            # SET NX returns True if key was set, None/False if already existed
            res = client.set(key, "1", ex=ttl_seconds, nx=True)
            return bool(res)
        except Exception as exc:
            logger.warning(f"Redis idempotency check failed ({exc}), falling back to database")

    # In-memory fallback
    if event_id in _in_memory_seen_events:
        return False
    _in_memory_seen_events.add(event_id)
    return True
