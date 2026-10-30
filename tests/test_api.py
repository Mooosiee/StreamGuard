from datetime import datetime, timezone
from sqlalchemy import insert
import schema


def now_utc():
    return datetime.now(timezone.utc)


def test_health_check_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "components" in data


def test_post_transaction_enqueues_to_kafka(client, mock_kafka_producer):
    payload = {
        "account_id": "acc_api_1",
        "amount": 7500.0,
        "location": "Lahore",
        "merchant": "Careem",
    }
    response = client.post("/transactions", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "queued"
    assert data["account_id"] == "acc_api_1"
    assert data["amount"] == 7500.0
    assert "event_id" in data
    assert len(data["event_id"]) > 0

    # Verify mock producer was called to send to Kafka topic
    mock_kafka_producer.send.assert_called_once()
    mock_kafka_producer.flush.assert_called_once()


def test_post_transaction_negative_amount_rejected(client):
    payload = {
        "account_id": "acc_api_1",
        "amount": -500.0,
        "location": "Lahore",
        "merchant": "Careem",
    }
    response = client.post("/transactions", json=payload)
    # Pydantic validation error
    assert response.status_code == 422


def test_get_transactions_endpoint(client, test_db_engine):
    with test_db_engine.begin() as conn:
        conn.execute(insert(schema.accounts).values(account_id="acc_api_query"))
        conn.execute(
            insert(schema.transactions).values(
                event_id="evt_query_01",
                account_id="acc_api_query",
                amount=300.0,
                location="Karachi",
                merchant="Daraz",
                status="normal",
                timestamp=now_utc(),
            )
        )

    response = client.get("/transactions?limit=10")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] >= 1
    assert data["transactions"][0]["event_id"] == "evt_query_01"


def test_get_anomalies_endpoint(client, test_db_engine):
    with test_db_engine.begin() as conn:
        conn.execute(schema.insert(schema.accounts).values(account_id="acc_anomaly_query"))
        res = conn.execute(
            schema.insert(schema.transactions).values(
                event_id="evt_anomaly_01",
                account_id="acc_anomaly_query",
                amount=99000.0,
                location="Lahore",
                merchant="Unknown POS",
                status="suspicious",
                timestamp=now_utc(),
            )
        )
        txn_id = res.inserted_primary_key[0]
        conn.execute(
            schema.insert(schema.anomalies).values(
                transaction_id=txn_id,
                risk_score=0.95,
                reason="unusually high amount",
                detected_at=now_utc(),
            )
        )

    response = client.get("/anomalies")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] >= 1
    assert data["anomalies"][0]["risk_score"] == 0.95


def test_get_anomaly_by_id_found_and_not_found(client, test_db_engine):
    with test_db_engine.begin() as conn:
        conn.execute(schema.insert(schema.accounts).values(account_id="acc_single_anom"))
        res = conn.execute(
            schema.insert(schema.transactions).values(
                event_id="evt_single_01",
                account_id="acc_single_anom",
                amount=88000.0,
                location="Lahore",
                merchant="ATM",
                status="suspicious",
                timestamp=now_utc(),
            )
        )
        txn_id = res.inserted_primary_key[0]
        anom_res = conn.execute(
            schema.insert(schema.anomalies).values(
                transaction_id=txn_id,
                risk_score=0.75,
                reason="unusually high amount",
                detected_at=now_utc(),
            )
        )
        anom_id = anom_res.inserted_primary_key[0]

    # Existing
    res = client.get(f"/anomalies/{anom_id}")
    assert res.status_code == 200
    assert res.json()["id"] == anom_id

    # Not found
    not_found_res = client.get("/anomalies/999999")
    assert not_found_res.status_code == 404


def test_account_risk_profile_caching_endpoint(client, test_db_engine):
    account_id = "acc_risk_profile_test"
    with test_db_engine.begin() as conn:
        conn.execute(schema.insert(schema.accounts).values(account_id=account_id))
        for i in range(3):
            conn.execute(
                schema.insert(schema.transactions).values(
                    event_id=f"evt_rp_{i}",
                    account_id=account_id,
                    amount=500.0 * (i + 1),
                    location="Lahore",
                    merchant="Careem",
                    status="normal",
                    timestamp=now_utc(),
                )
            )

    # 1. First request computes from DB
    res1 = client.get(f"/accounts/{account_id}/risk-profile")
    assert res1.status_code == 200
    data1 = res1.json()
    assert data1["source"] == "database"
    assert data1["profile"]["total_transactions"] == 3
    assert data1["profile"]["risk_tier"] == "LOW"

    # 2. Second request hits cache
    res2 = client.get(f"/accounts/{account_id}/risk-profile")
    assert res2.status_code == 200
    data2 = res2.json()
    assert data2["source"] == "cache"
    assert data2["profile"]["total_transactions"] == 3

    # 3. Invalidate cache
    del_res = client.delete(f"/accounts/{account_id}/cache")
    assert del_res.status_code == 200

    # 4. Third request is database miss again
    res3 = client.get(f"/accounts/{account_id}/risk-profile")
    assert res3.status_code == 200
    data3 = res3.json()
    assert data3["source"] == "database"
