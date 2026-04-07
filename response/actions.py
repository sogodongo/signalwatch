from datetime import datetime, timezone


def block_transaction(
    user_id: str,
    transaction_id: str,
    amount: float,
    reason: str,
) -> dict:
    """Decline the transaction and notify the user."""
    print(f"[action] BLOCK: {user_id} transaction {transaction_id[:16]} "
          f"KES {amount:.0f} — {reason}")
    return {
        "action":         "block",
        "user_id":        user_id,
        "transaction_id": transaction_id,
        "status":         "blocked",
        "reason":         reason,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "user_notified":  True,
    }


def send_fraud_alert(
    user_id: str,
    transaction_id: str,
    amount: float,
    threat_level: str,
    assessment: str,
) -> dict:
    """Allow the transaction but alert the fraud team."""
    print(f"[action] ALERT: {threat_level.upper()} threat — "
          f"{user_id} KES {amount:.0f} | {assessment[:80]}")
    return {
        "action":         "alert",
        "user_id":        user_id,
        "transaction_id": transaction_id,
        "status":         "allowed_with_alert",
        "threat_level":   threat_level,
        "alert_sent_to":  "fraud-team@company.com",
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }


def queue_for_review(
    user_id: str,
    transaction_id: str,
    amount: float,
    assessment: str,
) -> dict:
    """Queue the transaction for human review within 1 hour."""
    print(f"[action] REVIEW: {user_id} KES {amount:.0f} queued for human review")
    return {
        "action":          "review",
        "user_id":         user_id,
        "transaction_id":  transaction_id,
        "status":          "queued_for_review",
        "review_deadline": "1 hour",
        "queue":           "fraud-review-queue",
        "timestamp":       datetime.now(timezone.utc).isoformat(),
    }


def increase_monitoring(
    user_id: str,
    transaction_id: str,
    amount: float,
) -> dict:
    """Allow the transaction and flag user for increased monitoring."""
    print(f"[action] MONITOR: {user_id} flagged for increased monitoring")
    return {
        "action":           "monitor",
        "user_id":          user_id,
        "transaction_id":   transaction_id,
        "status":           "allowed_with_monitoring",
        "monitoring_level": "elevated",
        "timestamp":        datetime.now(timezone.utc).isoformat(),
    }
