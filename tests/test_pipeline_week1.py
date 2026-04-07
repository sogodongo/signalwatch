import sys
import time
sys.path.insert(0, ".")

from streaming.producers.transaction_producer import run_producer
from streaming.consumers.transaction_consumer import consume_transactions
from detection.pipeline import process_transaction
from streaming.schema import Transaction

# First build a baseline — run normal transactions only to warm up windows
print("=== Building baselines (no anomalies) ===\n")
run_producer(
    transactions_per_second=10.0,
    anomaly_rate=0.0,
    max_transactions=30,
)

time.sleep(1)

# Now run with anomalies and process through the full pipeline
print("\n=== Running detection pipeline ===\n")
run_producer(
    transactions_per_second=5.0,
    anomaly_rate=0.20,
    max_transactions=20,
)

time.sleep(1)

print("\n=== Consuming and processing through detection pipeline ===\n")
consume_transactions(
    callback=process_transaction,
    group_id="detection-pipeline-test",
    auto_offset_reset="earliest",
    max_messages=50,
)
