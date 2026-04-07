import sys
sys.path.insert(0, ".")

from streaming.producers.transaction_producer import run_producer

# Publish 20 transactions at 5/second — finishes in ~4 seconds
run_producer(
    transactions_per_second=5.0,
    anomaly_rate=0.15,
    max_transactions=20,
)
