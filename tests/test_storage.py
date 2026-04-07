import sys
import uuid
sys.path.insert(0, ".")

from datetime import datetime, timezone
from streaming.schema import Transaction, AnomalyEvent
from response.response_engine import handle_anomaly
from storage.event_store import get_recent_anomalies, get_metrics

def make_event(user_id, amount, country, anomalies, mean, stddev):
    tx = Transaction(
        transaction_id=   str(uuid.uuid4()),
        user_id=          user_id,
        amount=           amount,
        currency=         "KES",
        merchant_name=    "Test Merchant",
        merchant_category="electronics",
        country=          country,
        city=             "Nairobi",
        timestamp=        datetime.now(timezone.utc).isoformat(),
    )
    return AnomalyEvent(
        anomaly_id=        str(uuid.uuid4()),
        transaction=       tx,
        window_stats={"mean": mean, "stddev": stddev, "count": 6, "ready": True},
        anomalies=         anomalies,
        combined_severity= anomalies[0]["severity"],
        detected_at=       datetime.now(timezone.utc).isoformat(),
    )

# Process 2 events
events = [
    make_event("user_002", 2500.0, "NG",
        [{"type": "amount_zscore", "severity": "critical", "score": 21.0,
          "explanation": "21 std devs from mean"},
         {"type": "geographic", "severity": "high", "score": 1.0,
          "explanation": "Transaction in NG"}],
        mean=120.0, stddev=40.0),
    make_event("user_003", 22.0, "KE",
        [{"type": "velocity", "severity": "medium", "score": 1.5,
          "explanation": "12 transactions in window"}],
        mean=25.0, stddev=8.0),
]

for event in events:
    handle_anomaly(event)

print(f"\n{'='*60}")
print("AUDIT TRAIL")
print(f"{'='*60}")
recent = get_recent_anomalies(limit=5)
for r in recent:
    ts = r["detected_at"]
    if hasattr(ts, "isoformat"):
        ts = ts.isoformat()
    print(f"\nAnomaly  : {str(r['anomaly_id'])[:16]}")
    print(f"User     : {r['user_id']} KES {r['amount']:.0f} in {r['country']}")
    print(f"Severity : {r['combined_severity']}")
    print(f"Threat   : {r['threat_level']} confidence={r['confidence']}")
    print(f"Action   : {r['final_action']} → {r['status']}")

print(f"\n{'='*60}")
print("METRICS")
print(f"{'='*60}")
metrics = get_metrics()
print(f"Total anomalies : {metrics['total_anomalies']}")
print(f"Avg confidence  : {metrics['avg_confidence']}")
print(f"By action       : {metrics['by_action']}")
print(f"By severity     : {metrics['by_severity']}")
