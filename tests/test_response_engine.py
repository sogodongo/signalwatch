import sys
import uuid
sys.path.insert(0, ".")

from datetime import datetime, timezone
from streaming.schema import Transaction, AnomalyEvent
from response.response_engine import handle_anomaly


def make_event(user_id, amount, country, category, anomalies, mean, stddev):
    tx = Transaction(
        transaction_id=   str(uuid.uuid4()),
        user_id=          user_id,
        amount=           amount,
        currency=         "KES",
        merchant_name=    "Test Merchant",
        merchant_category=category,
        country=          country,
        city=             "Nairobi" if country == "KE" else "Lagos",
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


test_events = [
    make_event(
        "user_002", 2500.0, "NG", "electronics",
        [
            {"type": "amount_zscore", "severity": "critical", "score": 21.0,
             "explanation": "Amount KES 2500 is 21 std devs from mean KES 120"},
            {"type": "geographic", "severity": "high", "score": 1.0,
             "explanation": "Transaction in NG but home country is KE"},
        ],
        mean=120.0, stddev=40.0,
    ),
    make_event(
        "user_001", 850.0, "KE", "electronics",
        [{"type": "amount_zscore", "severity": "high", "score": 18.5,
          "explanation": "Amount KES 850 is 18.5 std devs from mean KES 44"}],
        mean=44.0, stddev=4.4,
    ),
    make_event(
        "user_003", 22.0, "KE", "transport",
        [{"type": "velocity", "severity": "medium", "score": 1.5,
          "explanation": "12 transactions in window — threshold is 8"}],
        mean=25.0, stddev=8.0,
    ),
]

print("=" * 70)
print("SIGNALWATCH RESPONSE ENGINE TEST")
print("=" * 70)

for event in test_events:
    outcome = handle_anomaly(event)
    print(f"\nTransaction : {outcome['transaction']['user_id']} "
          f"KES {outcome['transaction']['amount']:.0f} "
          f"in {outcome['transaction']['country']}")
    print(f"Threat      : {outcome['assessment']['threat_level']} "
          f"(confidence={outcome['assessment']['confidence']})")
    print(f"Action      : {outcome['action_result']['action'].upper()} "
          f"→ {outcome['action_result']['status']}")
    if outcome['decision']['downgraded']:
        print(f"Downgraded  : {outcome['decision']['original_action']} "
              f"→ {outcome['decision']['final_action']}")
    print("-" * 70)
