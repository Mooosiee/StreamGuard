import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ACCOUNTS = ["acc_001", "acc_002", "acc_003", "acc_004", "acc_005"]
MERCHANTS = ["Daraz", "Foodpanda", "Careem", "Unknown POS", "Utility Bill"]
LOCATIONS = ["Lahore", "Karachi", "Islamabad", "Faisalabad", "Gujranwala"]


def get_kafka_producer():
    """Factory to create KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def generate_transaction():
    """
    Builds a simulated transaction payload with a globally unique event_id.
    Deliberately introduces ~15% high-amount anomalies for the detection pipeline.
    """
    is_weird = random.random() < 0.15

    amount = (
        round(random.uniform(50000, 200000), 2)
        if is_weird
        else round(random.uniform(100, 15000), 2)
    )

    return {
        "event_id": str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNTS),
        "amount": amount,
        "merchant": random.choice(MERCHANTS),
        "location": random.choice(LOCATIONS),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    producer = get_kafka_producer()
    print(f">>> Producing transactions to topic '{TOPIC}' on {KAFKA_BOOTSTRAP_SERVERS}. Ctrl+C to stop.")
    try:
        while True:
            txn = generate_transaction()
            producer.send(TOPIC, value=txn)
            producer.flush()
            print(">>> Sent:", txn)
            time.sleep(random.uniform(1, 3))
    except KeyboardInterrupt:
        print("\n>>> Stopped producing.")
    finally:
        producer.close()
