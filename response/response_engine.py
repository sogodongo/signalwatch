from streaming.schema import AnomalyEvent
from reasoning.llm_reasoner import reason_about_anomaly
from reasoning.threat_classifier import classify_response
from storage.models import init_db
from storage.event_store import (
    store_anomaly_event,
    store_threat_assessment,
    store_response_action,
)
from response.actions import (
    block_transaction,
    send_fraud_alert,
    queue_for_review,
    increase_monitoring,
)


def handle_anomaly(event: AnomalyEvent) -> dict:
    """
    Full response pipeline for an anomaly event:
    1. LLM reasons about the anomaly
    2. Classifier applies confidence thresholds
    3. Response action is executed
    4. Returns complete outcome record for audit logging
    """
    tx = event.transaction

    print(f"\n[response_engine] Processing anomaly: "
          f"{tx.user_id} KES {tx.amount:.0f} "
          f"| signals={[a['type'] for a in event.anomalies]}")

    # Step 1: LLM assessment
    assessment = reason_about_anomaly(event)

    # Step 2: Classify final action
    decision = classify_response(assessment)

    # Step 3: Execute action
    action = decision["final_action"]

    if action == "block":
        result = block_transaction(
            user_id=        tx.user_id,
            transaction_id= tx.transaction_id,
            amount=         tx.amount,
            reason=         assessment.assessment[:200],
        )

    elif action == "alert":
        result = send_fraud_alert(
            user_id=        tx.user_id,
            transaction_id= tx.transaction_id,
            amount=         tx.amount,
            threat_level=   decision["threat_level"],
            assessment=     assessment.assessment,
        )

    elif action == "review":
        result = queue_for_review(
            user_id=        tx.user_id,
            transaction_id= tx.transaction_id,
            amount=         tx.amount,
            assessment=     assessment.assessment,
        )

    else:
        result = increase_monitoring(
            user_id=        tx.user_id,
            transaction_id= tx.transaction_id,
            amount=         tx.amount,
        )

    # Persist everything to the audit trail
    init_db()
    store_anomaly_event(event)
    store_threat_assessment(event.anomaly_id, assessment)
    store_response_action(event.anomaly_id, decision, result)

    return {
        "anomaly_id":   event.anomaly_id,
        "transaction":  tx.model_dump(),
        "assessment":   assessment.model_dump(),
        "decision":     decision,
        "action_result":result,
    }
