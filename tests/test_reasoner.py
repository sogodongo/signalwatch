import sys
import uuid
sys.path.insert(0, ".")

from datetime import datetime, timezone
from streaming.schema import Transaction, AnomalyEvent
from reasoning.llm_reasoner import reason_about_anomaly


def make_anomaly_event(
    user_id, amount, country, merchant_category,
    anomalies, mean, stddev, count
) -> AnomalyEvent:
    tx = Transaction(
        transaction_id=   str(uuid.uuid4()),
        user_id=          user_id,
        amount=           amount,
        currency=         "KES",
        merchant_name=    "Test Merchant",
        merchant_category=merchant_category,
        country=          country,
        city=             "Nairobi" if country == "KE" else "Unknown",
        timestamp=        datetime.now(timezone.utc).isoformat(),
    )
    return AnomalyEvent(
        anomaly_id=        str(uuid.uuid4()),
        transaction=       tx,
        window_stats={
            "mean": mean, "stddev": stddev,
            "count": count, "ready": True,
        },
        anomalies=         anomalies,
        combined_severity= anomalies[0]["severity"] if anomalies else "low",
        detected_at=       datetime.now(timezone.utc).isoformat(),
    )


test_cases = [
    {
        "label": "High amount — likely Christmas shopping",
        "event": make_anomaly_event(
            user_id="user_001", amount=850.0, country="KE",
            merchant_category="electronics",
            anomalies=[{
                "type": "amount_zscore", "severity": "high",
                "score": 18.5,
                "explanation": "Amount KES 850 is 18.5 std devs from mean KES 44"
            }],
            mean=44.0, stddev=4.4, count=6,
        ),
    },
    {
        "label": "Foreign transaction + high amount — likely fraud",
        "event": make_anomaly_event(
            user_id="user_002", amount=2500.0, country="NG",
            merchant_category="electronics",
            anomalies=[
                {
                    "type": "amount_zscore", "severity": "critical",
                    "score": 21.0,
                    "explanation": "Amount KES 2500 is 21 std devs from mean KES 120"
                },
                {
                    "type": "geographic", "severity": "high",
                    "score": 1.0,
                    "explanation": "Transaction in NG but home country is KE"
                },
            ],
            mean=120.0, stddev=40.0, count=5,
        ),
    },
    {
        "label": "Velocity only — probably legitimate burst",
        "event": make_anomaly_event(
            user_id="user_003", amount=22.0, country="KE",
            merchant_category="transport",
            anomalies=[{
                "type": "velocity", "severity": "medium",
                "score": 1.5,
                "explanation": "12 transactions in window — threshold is 8"
            }],
            mean=25.0, stddev=8.0, count=12,
        ),
    },
]

print("=" * 70)
print("SIGNALWATCH THREAT ASSESSMENTS")
print("=" * 70)

for case in test_cases:
    print(f"\n--- {case['label']} ---")
    result = reason_about_anomaly(case["event"])
    print(f"Threat level  : {result.threat_level}")
    print(f"Action        : {result.recommended_action}")
    print(f"Confidence    : {result.confidence}")
    print(f"False positive: {result.false_positive_likelihood}")
    print(f"Human review  : {result.requires_human_review}")
    print(f"Assessment    : {result.assessment[:200]}")
    if result.key_risk_factors:
        print(f"Risk factors  : {result.key_risk_factors}")
    print("-" * 70)
