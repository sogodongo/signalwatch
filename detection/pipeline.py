import sys
sys.path.insert(0, ".")

from streaming.schema import Transaction
from detection.window_processor import WindowProcessor
from detection.anomaly_detector import detect_anomalies
from detection.anomaly_publisher import publish_anomaly

_processor = WindowProcessor()


def process_transaction(tx: Transaction):
    """
    Full detection pipeline for a single transaction:
    1. Update the user's sliding window
    2. Run anomaly detection
    3. Publish to anomalies topic if detected
    """
    stats    = _processor.process(tx.user_id, tx.amount)
    detected = detect_anomalies(tx, stats)

    if detected:
        publish_anomaly(tx, stats, detected)
    else:
        print(f"[pipeline] Clean: {tx.user_id} KES {tx.amount:.0f} "
              f"| window count={stats['count']}")
