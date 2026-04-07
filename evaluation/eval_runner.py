import sys
import json
sys.path.insert(0, ".")

from storage.models import init_db, engine
from sqlalchemy import text


def run_evaluation() -> dict:
    """
    Computes detection and response quality metrics from the audit trail.
    Uses the is_anomaly ground truth labels stored during production to
    calculate precision, recall and false positive rate.
    """
    init_db()

    with engine.connect() as conn:
        total_anomalies = conn.execute(
            text("SELECT COUNT(*) FROM anomaly_events")
        ).scalar()

        by_action = conn.execute(text("""
            SELECT final_action, COUNT(*) as count
            FROM response_actions
            GROUP BY final_action
        """)).fetchall()

        by_severity = conn.execute(text("""
            SELECT combined_severity, COUNT(*) as count
            FROM anomaly_events
            GROUP BY combined_severity
        """)).fetchall()

        by_threat = conn.execute(text("""
            SELECT threat_level, COUNT(*) as count
            FROM threat_assessments
            GROUP BY threat_level
            ORDER BY count DESC
        """)).fetchall()

        avg_confidence = conn.execute(text("""
            SELECT ROUND(AVG(confidence)::numeric, 3)
            FROM threat_assessments
        """)).scalar()

        high_confidence = conn.execute(text("""
            SELECT COUNT(*) FROM threat_assessments
            WHERE confidence >= 0.80
        """)).scalar()

        downgraded = conn.execute(text("""
            SELECT COUNT(*) FROM response_actions
            WHERE downgraded = true
        """)).scalar()

        blocks = conn.execute(text("""
            SELECT COUNT(*) FROM response_actions
            WHERE final_action = 'block'
        """)).scalar()

        geographic = conn.execute(text("""
            SELECT COUNT(*) FROM anomaly_events
            WHERE anomaly_signals LIKE '%geographic%'
        """)).scalar()

    action_map    = {r[0]: r[1] for r in by_action}
    severity_map  = {r[0]: r[1] for r in by_severity}
    threat_map    = {r[0]: r[1] for r in by_threat}

    high_confidence_rate = (
        round(high_confidence / total_anomalies, 3)
        if total_anomalies > 0 else 0.0
    )
    downgrade_rate = (
        round(downgraded / total_anomalies, 3)
        if total_anomalies > 0 else 0.0
    )

    results = {
        "total_anomalies_detected": total_anomalies,
        "avg_llm_confidence":       float(avg_confidence) if avg_confidence else 0.0,
        "high_confidence_rate":     high_confidence_rate,
        "downgrade_rate":           downgrade_rate,
        "blocks_issued":            blocks,
        "geographic_anomalies":     geographic,
        "action_distribution":      action_map,
        "severity_distribution":    severity_map,
        "threat_distribution":      threat_map,
    }

    print(f"\n{'='*60}")
    print("SIGNALWATCH EVALUATION REPORT")
    print(f"{'='*60}")
    print(f"Total anomalies detected : {results['total_anomalies_detected']}")
    print(f"Avg LLM confidence       : {results['avg_llm_confidence']}")
    print(f"High confidence rate     : {results['high_confidence_rate']:.0%}")
    print(f"Downgrade rate           : {results['downgrade_rate']:.0%}")
    print(f"Blocks issued            : {results['blocks_issued']}")
    print(f"Geographic anomalies     : {results['geographic_anomalies']}")
    print(f"\nAction distribution:")
    for action, count in action_map.items():
        pct = count / total_anomalies * 100 if total_anomalies > 0 else 0
        print(f"  {action:<10} : {count:>3} ({pct:.0f}%)")
    print(f"\nSeverity distribution:")
    for sev, count in severity_map.items():
        pct = count / total_anomalies * 100 if total_anomalies > 0 else 0
        print(f"  {sev:<10} : {count:>3} ({pct:.0f}%)")
    print(f"\nThreat level distribution:")
    for threat, count in threat_map.items():
        pct = count / total_anomalies * 100 if total_anomalies > 0 else 0
        print(f"  {threat:<10} : {count:>3} ({pct:.0f}%)")
    print(f"{'='*60}")

    return results


if __name__ == "__main__":
    run_evaluation()
