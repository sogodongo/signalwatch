import json
from sqlalchemy import text
from storage.models import engine
from streaming.schema import AnomalyEvent
from reasoning.llm_reasoner import ThreatAssessment


def store_anomaly_event(event: AnomalyEvent) -> str:
    """
    Persists an anomaly event to the database.
    Returns the anomaly_id. Uses ON CONFLICT DO NOTHING for idempotency
    — replaying a Kafka topic won't create duplicate records.
    """
    tx    = event.transaction
    stats = event.window_stats

    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO anomaly_events (
                anomaly_id, user_id, transaction_id, amount, currency,
                merchant_name, merchant_category, country,
                anomaly_signals, combined_severity,
                window_mean, window_stddev, window_count, detected_at
            ) VALUES (
                :anomaly_id, :user_id, :transaction_id, :amount, :currency,
                :merchant_name, :merchant_category, :country,
                :anomaly_signals, :combined_severity,
                :window_mean, :window_stddev, :window_count, :detected_at
            )
            ON CONFLICT (anomaly_id) DO NOTHING
        """), {
            "anomaly_id":        event.anomaly_id,
            "user_id":           tx.user_id,
            "transaction_id":    tx.transaction_id,
            "amount":            tx.amount,
            "currency":          tx.currency,
            "merchant_name":     tx.merchant_name,
            "merchant_category": tx.merchant_category,
            "country":           tx.country,
            "anomaly_signals":   json.dumps([a["type"] for a in event.anomalies]),
            "combined_severity": event.combined_severity,
            "window_mean":       stats.get("mean"),
            "window_stddev":     stats.get("stddev"),
            "window_count":      stats.get("count", 0),
            "detected_at":       event.detected_at,
        })
        conn.commit()

    print(f"[store] Anomaly event stored: {event.anomaly_id[:16]}")
    return event.anomaly_id


def store_threat_assessment(
    anomaly_id: str,
    assessment: ThreatAssessment,
):
    """Persists the LLM threat assessment linked to its anomaly event."""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO threat_assessments (
                anomaly_id, threat_level, assessment,
                recommended_action, confidence,
                false_positive_likelihood, requires_human_review,
                key_risk_factors
            ) VALUES (
                :anomaly_id, :threat_level, :assessment,
                :recommended_action, :confidence,
                :false_positive_likelihood, :requires_human_review,
                :key_risk_factors
            )
        """), {
            "anomaly_id":              anomaly_id,
            "threat_level":            assessment.threat_level,
            "assessment":              assessment.assessment,
            "recommended_action":      assessment.recommended_action,
            "confidence":              assessment.confidence,
            "false_positive_likelihood": assessment.false_positive_likelihood,
            "requires_human_review":   assessment.requires_human_review,
            "key_risk_factors":        json.dumps(assessment.key_risk_factors),
        })
        conn.commit()

    print(f"[store] Assessment stored: {assessment.threat_level} "
          f"confidence={assessment.confidence}")


def store_response_action(
    anomaly_id: str,
    decision: dict,
    action_result: dict,
):
    """Persists the response action taken for an anomaly."""
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO response_actions (
                anomaly_id, final_action, original_action,
                downgraded, status, action_detail
            ) VALUES (
                :anomaly_id, :final_action, :original_action,
                :downgraded, :status, :action_detail
            )
        """), {
            "anomaly_id":     anomaly_id,
            "final_action":   decision["final_action"],
            "original_action":decision["original_action"],
            "downgraded":     decision["downgraded"],
            "status":         action_result.get("status", "unknown"),
            "action_detail":  json.dumps(action_result),
        })
        conn.commit()

    print(f"[store] Action stored: {decision['final_action']} "
          f"({action_result.get('status')})")


def get_recent_anomalies(limit: int = 20) -> list[dict]:
    """Returns recent anomalies with their assessments and actions."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                ae.anomaly_id, ae.user_id, ae.amount, ae.currency,
                ae.country, ae.anomaly_signals, ae.combined_severity,
                ae.detected_at,
                ta.threat_level, ta.confidence, ta.recommended_action,
                ra.final_action, ra.status
            FROM anomaly_events ae
            LEFT JOIN threat_assessments ta ON ae.anomaly_id = ta.anomaly_id
            LEFT JOIN response_actions   ra ON ae.anomaly_id = ra.anomaly_id
            ORDER BY ae.detected_at DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()
    return [dict(r._mapping) for r in rows]


def get_metrics() -> dict:
    """Returns detection and response metrics for the dashboard."""
    with engine.connect() as conn:
        total = conn.execute(
            text("SELECT COUNT(*) FROM anomaly_events")
        ).scalar()

        by_action = conn.execute(text("""
            SELECT final_action, COUNT(*) as count
            FROM response_actions
            GROUP BY final_action
            ORDER BY count DESC
        """)).fetchall()

        by_severity = conn.execute(text("""
            SELECT combined_severity, COUNT(*) as count
            FROM anomaly_events
            GROUP BY combined_severity
            ORDER BY count DESC
        """)).fetchall()

        avg_confidence = conn.execute(text("""
            SELECT ROUND(AVG(confidence)::numeric, 3)
            FROM threat_assessments
        """)).scalar()

    return {
        "total_anomalies":  total,
        "avg_confidence":   float(avg_confidence) if avg_confidence else 0.0,
        "by_action":        [dict(r._mapping) for r in by_action],
        "by_severity":      [dict(r._mapping) for r in by_severity],
    }
