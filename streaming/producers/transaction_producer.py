import os
import uuid
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv
from streaming.schema import Transaction

load_dotenv()

# Realistic user profiles — each user has typical spending behaviour
USER_PROFILES = {
    "user_001": {"avg_amount": 45.0,  "stddev": 15.0,  "home_country": "KE", "city": "Nairobi"},
    "user_002": {"avg_amount": 120.0, "stddev": 40.0,  "home_country": "KE", "city": "Mombasa"},
    "user_003": {"avg_amount": 25.0,  "stddev": 8.0,   "home_country": "KE", "city": "Kisumu"},
    "user_004": {"avg_amount": 200.0, "stddev": 60.0,  "home_country": "KE", "city": "Nairobi"},
    "user_005": {"avg_amount": 75.0,  "stddev": 20.0,  "home_country": "KE", "city": "Nakuru"},
}

MERCHANT_CATEGORIES = [
    "groceries", "fuel", "restaurant", "pharmacy",
    "electronics", "clothing", "transport", "utilities",
]

MERCHANTS = {
    "groceries":   ["Naivas Supermarket", "Quickmart", "Carrefour"],
    "fuel":        ["Total Energies", "Shell", "Rubis"],
    "restaurant":  ["Java House", "KFC Kenya", "Artcaffe"],
    "pharmacy":    ["Goodlife Pharmacy", "Haltons", "City Pharmacy"],
    "electronics": ["Safaricom Shop", "Phone Place", "iStore"],
    "clothing":    ["Mr Price", "Woolworths", "Zara Kenya"],
    "transport":   ["Uber", "Bolt", "Little Cab"],
    "utilities":   ["Kenya Power", "Nairobi Water", "Safaricom"],
}


def _normal_transaction(user_id: str) -> Transaction:
    """Generate a normal transaction matching the user's typical behaviour."""
    profile  = USER_PROFILES[user_id]
    category = random.choice(MERCHANT_CATEGORIES)
    amount   = max(1.0, random.gauss(profile["avg_amount"], profile["stddev"]))

    return Transaction(
        transaction_id=   str(uuid.uuid4()),
        user_id=          user_id,
        amount=           round(amount, 2),
        currency=         "KES",
        merchant_name=    random.choice(MERCHANTS[category]),
        merchant_category=category,
        country=          profile["home_country"],
        city=             profile["city"],
        timestamp=        datetime.now(timezone.utc).isoformat(),
        is_anomaly=       False,
    )


def _anomalous_transaction(user_id: str, anomaly_type: str) -> Transaction:
    """Generate an anomalous transaction for testing detection."""
    profile = USER_PROFILES[user_id]

    if anomaly_type == "high_amount":
        # 10-20x the user's normal spend
        amount = profile["avg_amount"] * random.uniform(10, 20)
        country = profile["home_country"]
        city    = profile["city"]

    elif anomaly_type == "foreign_transaction":
        amount  = profile["avg_amount"] * random.uniform(1, 3)
        country = random.choice(["NG", "ZA", "GH", "TZ", "UG"])
        city    = random.choice(["Lagos", "Johannesburg", "Accra", "Dar es Salaam"])

    elif anomaly_type == "rapid_succession":
        # Normal amount but flagged as part of rapid succession
        amount  = profile["avg_amount"] * random.uniform(0.8, 1.2)
        country = profile["home_country"]
        city    = profile["city"]

    else:
        amount  = profile["avg_amount"] * 15
        country = profile["home_country"]
        city    = profile["city"]

    category = random.choice(MERCHANT_CATEGORIES)
    return Transaction(
        transaction_id=   str(uuid.uuid4()),
        user_id=          user_id,
        amount=           round(amount, 2),
        currency=         "KES",
        merchant_name=    random.choice(MERCHANTS[category]),
        merchant_category=category,
        country=          country,
        city=             city,
        timestamp=        datetime.now(timezone.utc).isoformat(),
        is_anomaly=       True,
        anomaly_type=     anomaly_type,
    )


def run_producer(
    transactions_per_second: float = 2.0,
    anomaly_rate: float = 0.05,
    max_transactions: int = None,
):
    """
    Continuously publishes transactions to the Kafka transactions topic.

    anomaly_rate controls what fraction of transactions are anomalous.
    Set max_transactions for finite runs during testing.
    """
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: v,
    )

    topic    = os.getenv("TRANSACTIONS_TOPIC", "transactions")
    interval = 1.0 / transactions_per_second
    count    = 0
    anomaly_types = ["high_amount", "foreign_transaction", "rapid_succession"]

    print(f"[producer] Starting — {transactions_per_second} tx/s, "
          f"anomaly rate {anomaly_rate:.0%}")
    print(f"[producer] Publishing to topic: {topic}")

    try:
        while True:
            user_id = random.choice(list(USER_PROFILES.keys()))

            if random.random() < anomaly_rate:
                tx = _anomalous_transaction(
                    user_id, random.choice(anomaly_types)
                )
                print(f"[producer] ANOMALY {tx.anomaly_type}: "
                      f"{tx.user_id} KES {tx.amount:.0f} in {tx.country}")
            else:
                tx = _normal_transaction(user_id)

            producer.send(topic, tx.to_kafka_bytes())
            count += 1

            if count % 20 == 0:
                print(f"[producer] {count} transactions published")
                producer.flush()

            if max_transactions and count >= max_transactions:
                print(f"[producer] Reached limit of {max_transactions} transactions.")
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n[producer] Stopped. Total published: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_producer(transactions_per_second=2.0, anomaly_rate=0.05)
