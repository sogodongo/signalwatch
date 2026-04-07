import sys
import time
sys.path.insert(0, ".")

from streaming.producers.transaction_producer import run_producer
from streaming.consumers.transaction_consumer import consume_transactions
from streaming.schema import Transaction

received = []

def handle_transaction(tx: Transaction):
    received.append(tx)
    anomaly_tag = f"[ANOMALY: {tx.anomaly_type}]" if tx.is_anomaly else ""
    print(f"[consumer] {tx.user_id} | KES {tx.amount:.0f} | "
          f"{tx.merchant_category} | {tx.country} {anomaly_tag}")

# Produce first — all messages land in Kafka before consumer starts
print("Publishing 10 transactions...")
run_producer(
    transactions_per_second=10.0,
    anomaly_rate=0.20,
    max_transactions=10,
)

print("\nConsuming from beginning of topic...")
time.sleep(1)

consume_transactions(
    callback=handle_transaction,
    group_id="test-consumer-2",
    auto_offset_reset="earliest",
    max_messages=10,
)

print(f"\nTotal received : {len(received)}")
print(f"Anomalies      : {sum(1 for t in received if t.is_anomaly)}")
print(f"Normal         : {sum(1 for t in received if not t.is_anomaly)}")
