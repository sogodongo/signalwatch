import os
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv
from streaming.schema import Transaction, AnomalyEvent

load_dotenv()

_producer = None


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=lambda v: v,
        )
    return _producer


def _combined_severity(anomalies: list[dict]) -> str:
    """
    Derives the overall severity from the list of individual anomaly signals.
    Takes the highest severity found across all signals.
    """
    severity_rank = {"low": 1, "medium": 2, "high": 3, "critical": 4}
    if not anomalies:
        return "none"
    highest = max(anomalies, key=lambda a: severity_rank.get(a["severity"], 0))
    return highest["severity"]


def publish_anomaly(
    tx: Transaction,
    window_stats: dict,
    anomalies: list[dict],
) -> str:
    """
    Packages a detected anomaly and publishes it to the anomalies topic.
    Returns the anomaly_id for audit logging.

    This is the handoff point between the detection layer and the
    reasoning layer — everything the LLM needs is in one event.
    """
    topic = os.getenv("ANOMALIES_TOPIC", "anomalies")

    event = AnomalyEvent(
        anomaly_id=        str(uuid.uuid4()),
        transaction=       tx,
        window_stats=      window_stats,
        anomalies=         anomalies,
        combined_severity= _combined_severity(anomalies),
        detected_at=       datetime.now(timezone.utc).isoformat(),
    )

    _get_producer().send(topic, event.to_kafka_bytes())
    _get_producer().flush()

    print(f"[publisher] Anomaly published: {event.anomaly_id[:16]} "
          f"| {tx.user_id} KES {tx.amount:.0f} "
          f"| severity={event.combined_severity} "
          f"| signals={[a['type'] for a in anomalies]}")

    return event.anomaly_id
