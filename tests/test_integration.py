import sys
import time
sys.path.insert(0, ".")

from streaming.producers.transaction_producer import run_producer
from streaming.consumers.transaction_consumer import consume_transactions
from streaming.consumers.transaction_consumer import create_consumer
from detection.pipeline import process_transaction, _processor
from detection.anomaly_publisher import publish_anomaly
from detection.anomaly_detector import detect_anomalies
from storage.models import init_db
from storage.event_store import get_recent_anomalies, get_metrics
from streaming.schema import Transaction, AnomalyEvent
from response.response_engine import handle_anomaly

# Track results during the test run
results = {
    "transactions_processed": 0,
    "anomalies_detected":     0,
    "anomalies_reasoned":     0,
    "actions": {
        "block":   0,
        "alert":   0,
        "review":  0,
        "monitor": 0,
    }
}


def full_pipeline_callback(tx: Transaction):
    """
    Runs a transaction through the complete SignalWatch pipeline:
    window → detect → reason → respond → store
    """
    results["transactions_processed"] += 1

    stats    = _processor.process(tx.user_id, tx.amount)
    detected = detect_anomalies(tx, stats)

    if not detected:
        return

    results["anomalies_detected"] += 1

    # Build anomaly event
    from detection.anomaly_publisher import _combined_severity
    import uuid
    from datetime import datetime, timezone

    event = AnomalyEvent(
        anomaly_id=        str(uuid.uuid4()),
        transaction=       tx,
        window_stats=      stats,
        anomalies=         detected,
        combined_severity= _combined_severity(detected),
        detected_at=       datetime.now(timezone.utc).isoformat(),
    )

    # Run full reasoning + response + storage
    try:
        outcome = handle_anomaly(event)
        action  = outcome["action_result"]["action"]
        results["actions"][action] = results["actions"].get(action, 0) + 1
        results["anomalies_reasoned"] += 1
        print(f"[integration] {tx.user_id} KES {tx.amount:.0f} → "
              f"{outcome['assessment']['threat_level']} → {action}")
    except Exception as e:
        print(f"[integration] Reasoning failed for {tx.transaction_id[:16]}: {e}")


def run_integration_test():
    init_db()

    print("=" * 70)
    print("SIGNALWATCH INTEGRATION TEST")
    print("=" * 70)

    # Step 1: Build baselines with normal transactions
    print("\n[Step 1] Building user baselines (30 normal transactions)...")
    run_producer(
        transactions_per_second=10.0,
        anomaly_rate=0.0,
        max_transactions=30,
    )
    time.sleep(1)

    # Step 2: Publish mixed transactions
    print("\n[Step 2] Publishing 20 transactions with 20% anomaly rate...")
    run_producer(
        transactions_per_second=5.0,
        anomaly_rate=0.20,
        max_transactions=20,
    )
    time.sleep(1)

    # Step 3: Consume and process through full pipeline
    print("\n[Step 3] Processing through full pipeline...\n")
    consume_transactions(
        callback=full_pipeline_callback,
        group_id="integration-test",
        auto_offset_reset="earliest",
        max_messages=50,
    )

    # Step 4: Verify audit trail
    print(f"\n[Step 4] Verifying audit trail...")
    recent  = get_recent_anomalies(limit=10)
    metrics = get_metrics()

    print(f"\n{'='*70}")
    print("INTEGRATION TEST RESULTS")
    print(f"{'='*70}")
    print(f"Transactions processed : {results['transactions_processed']}")
    print(f"Anomalies detected     : {results['anomalies_detected']}")
    print(f"Anomalies reasoned     : {results['anomalies_reasoned']}")
    print(f"Actions taken          : {results['actions']}")
    print(f"\nAudit trail records    : {metrics['total_anomalies']}")
    print(f"Average confidence     : {metrics['avg_confidence']}")
    print(f"Actions in DB          : {metrics['by_action']}")

    print(f"\n--- Recent audit records ---")
    for r in recent[:3]:
        ts = r["detected_at"]
        if hasattr(ts, "isoformat"):
            ts = ts.isoformat()
        print(f"  {r['user_id']} KES {r['amount']:.0f} | "
              f"{r['combined_severity']} | "
              f"{r['threat_level']} | "
              f"{r['final_action']} → {r['status']}")

    passed = (
        results["transactions_processed"] > 0 and
        metrics["total_anomalies"] > 0
    )

    print(f"\n{'OVERALL: PASS' if passed else 'OVERALL: FAIL'}")
    print("=" * 70)
    return passed


if __name__ == "__main__":
    success = run_integration_test()
    sys.exit(0 if success else 1)
