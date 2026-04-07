import sys
sys.path.insert(0, ".")

from streaming.schema import Transaction
from detection.window_processor import WindowProcessor
from detection.anomaly_detector import detect_anomalies
from datetime import datetime, timezone
import uuid

processor = WindowProcessor(window_seconds=300)

# Build a realistic baseline for user_001
baseline = [42.0, 38.0, 51.0, 45.0, 40.0, 47.0, 43.0, 39.0, 46.0, 44.0]
for amount in baseline:
    processor.process("user_001", amount)

print("Baseline established for user_001")
print(f"Stats: {processor.get_stats('user_001')}\n")
print("=" * 70)

def make_tx(user_id, amount, country="KE") -> Transaction:
    return Transaction(
        transaction_id=   str(uuid.uuid4()),
        user_id=          user_id,
        amount=           amount,
        currency=         "KES",
        merchant_name=    "Test Merchant",
        merchant_category="groceries",
        country=          country,
        city=             "Nairobi",
        timestamp=        datetime.now(timezone.utc).isoformat(),
    )

test_cases = [
    ("Normal transaction",        make_tx("user_001", 48.0)),
    ("Slightly high",             make_tx("user_001", 65.0)),
    ("High amount anomaly",       make_tx("user_001", 850.0)),
    ("Extreme amount",            make_tx("user_001", 5000.0)),
    ("Foreign transaction",       make_tx("user_001", 45.0, country="NG")),
    ("Foreign + high amount",     make_tx("user_001", 900.0, country="ZA")),
]

for label, tx in test_cases:
    stats    = processor.process(tx.user_id, tx.amount)
    detected = detect_anomalies(tx, stats)

    print(f"\n{label}: KES {tx.amount:.0f} ({tx.country})")
    if detected:
        for a in detected:
            print(f"  ANOMALY [{a['severity'].upper()}] {a['type']}: "
                  f"score={a['score']} — {a['explanation']}")
    else:
        print(f"  Clean — no anomalies detected")
