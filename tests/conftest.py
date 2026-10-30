import os
from unittest.mock import MagicMock
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

# Set testing environment variables before importing app modules
os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ["REDIS_HOST"] = "localhost"
os.environ["REDIS_PORT"] = "6379"

import schema
import main
import redis_client


@pytest.fixture(scope="session")
def test_db_engine():
    """Creates a thread-safe shared in-memory SQLite engine with the full application schema."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False,
    )
    schema.metadata.create_all(engine)
    return engine


@pytest.fixture(autouse=True)
def setup_test_db(test_db_engine, monkeypatch):
    """Binds the in-memory engine to schema and main modules for each test."""
    monkeypatch.setattr(schema, "engine", test_db_engine)
    monkeypatch.setattr(main, "engine", test_db_engine)
    
    # Clean tables before each test
    with test_db_engine.begin() as conn:
        conn.execute(schema.anomalies.delete())
        conn.execute(schema.transactions.delete())
        conn.execute(schema.accounts.delete())

    # Reset in-memory cache and redis mock state
    redis_client.reset_redis_connection_for_testing(None)
    yield test_db_engine


@pytest.fixture
def mock_kafka_producer(monkeypatch):
    """Provides a mocked Kafka producer for API tests."""
    mock_producer = MagicMock()
    mock_producer.send.return_value = None
    mock_producer.flush.return_value = None
    main.set_producer(mock_producer)
    yield mock_producer
    main.set_producer(None)


@pytest.fixture
def client(mock_kafka_producer):
    """FastAPI TestClient with mocked Kafka producer."""
    return TestClient(main.app)
