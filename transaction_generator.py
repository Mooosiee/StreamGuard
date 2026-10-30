 
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
 
TOPIC = "transactions"
 
# Connects to the Kafka broker running in Docker 
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # auto-convert 
)
ACCOUNTS = ["acc_001", "acc_002", "acc_003", "acc_004", "acc_005"]
MERCHANTS = ["Daraz", "Foodpanda", "Careem", "Unknown POS", "Utility Bill"]
LOCATIONS = ["Lahore", "Karachi", "Islamabad", "Faisalabad", "Gujranwala"]
 
 
def generate_transaction():
    """Build one fake transaction. Occasionally generate a deliberately 'weird' one
    (huge amount) so your consumer/detection logic later has something real to catch."""
    is_weird = random.random() < 0.15  # ~15% of transactions look suspicious
 
    amount = round(random.uniform(50000, 200000), 2) if is_weird else round(random.uniform(100, 15000), 2)
 
    return {
        "account_id": random.choice(ACCOUNTS),
        "amount": amount,
        "merchant": random.choice(MERCHANTS),
        "location": random.choice(LOCATIONS),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
 
 
if __name__ == "__main__":
    print(f">>> Producing transactions to topic '{TOPIC}'. Ctrl+C to stop.")
    try:
        while True:
            txn = generate_transaction()
            producer.send(TOPIC, value=txn)
            producer.flush() 
            print(">>> Sent:", txn)
            time.sleep(random.uniform(1, 3))  # simulate real-time arrival
    except KeyboardInterrupt:
        print("\n>>> Stopped producing.")
        producer.close()
 
