import uuid
import os
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from api.schemas import InjectAnomalyRequest, AnomalyResponse
from storage.models import init_db, engine
from storage.event_store import get_recent_anomalies, get_metrics
from streaming.schema import Transaction, AnomalyEvent
from response.response_engine import handle_anomaly
from sqlalchemy import text

load_dotenv()

app = FastAPI(
    title="SignalWatch API",
    description="Real-time anomaly detection and response engine for payment transactions.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()


@app.get("/health")
def health():
    return {"status": "ok", "service": "signalwatch"}


@app.get("/stream/status")
def stream_status():
    """Returns Kafka topic stats and pipeline health."""
    try:
        from kafka import KafkaAdminClient
        from kafka.admin import NewTopic

        admin = KafkaAdminClient(
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            )
        )
        topics = admin.list_topics()
        admin.close()
        kafka_status = "healthy"
        topic_list   = topics
    except Exception as e:
        kafka_status = f"unavailable: {str(e)[:50]}"
        topic_list   = []

    return {
        "kafka_status":       kafka_status,
        "topics":             topic_list,
        "transactions_topic": os.getenv("TRANSACTIONS_TOPIC", "transactions"),
        "anomalies_topic":    os.getenv("ANOMALIES_TOPIC", "anomalies"),
    }


@app.get("/anomalies")
def list_anomalies(limit: int = 20):
    """Returns the most recent anomaly events with threat assessments."""
    anomalies = get_recent_anomalies(limit=limit)
    for a in anomalies:
        if hasattr(a.get("detected_at"), "isoformat"):
            a["detected_at"] = a["detected_at"].isoformat()
    return {"count": len(anomalies), "anomalies": anomalies}


@app.get("/anomalies/{anomaly_id}")
def get_anomaly(anomaly_id: str):
    """Returns full audit detail for a specific anomaly event."""
    with engine.connect() as conn:
        event = conn.execute(text("""
            SELECT ae.*, ta.threat_level, ta.assessment, ta.confidence,
                   ta.false_positive_likelihood, ta.requires_human_review,
                   ta.key_risk_factors,
                   ra.final_action, ra.original_action,
                   ra.downgraded, ra.status, ra.action_detail
            FROM anomaly_events ae
            LEFT JOIN threat_assessments ta ON ae.anomaly_id = ta.anomaly_id
            LEFT JOIN response_actions   ra ON ae.anomaly_id = ra.anomaly_id
            WHERE ae.anomaly_id = :anomaly_id
        """), {"anomaly_id": anomaly_id}).fetchone()

    if not event:
        raise HTTPException(status_code=404, detail="Anomaly not found")

    result = dict(event._mapping)
    if hasattr(result.get("detected_at"), "isoformat"):
        result["detected_at"] = result["detected_at"].isoformat()
    if hasattr(result.get("assessed_at"), "isoformat"):
        result["assessed_at"] = result["assessed_at"].isoformat()

    return result


@app.get("/metrics")
def metrics():
    """Returns detection and response metrics."""
    return get_metrics()


@app.post("/inject")
def inject_anomaly(request: InjectAnomalyRequest):
    """
    Manually inject a synthetic anomaly for testing.
    Runs the full LLM reasoning + response pipeline and returns the outcome.
    Useful for demoing the system without running the Kafka producer.
    """
    severity_map = {
        "amount_zscore":  "critical",
        "geographic":     "high",
        "velocity":       "medium",
        "amount_iqr":     "medium",
    }

    mean   = 45.0
    stddev = 10.0
    score  = round((request.amount - mean) / stddev, 2) if stddev > 0 else 0

    anomaly_signal = {
        "type":        request.anomaly_type,
        "severity":    severity_map.get(request.anomaly_type, "medium"),
        "score":       score,
        "explanation": f"Synthetic test anomaly: {request.anomaly_type}",
    }

    if request.country != "KE":
        anomaly_signals = [
            anomaly_signal,
            {
                "type":        "geographic",
                "severity":    "high",
                "score":       1.0,
                "explanation": f"Transaction in {request.country} — home country is KE",
            },
        ]
    else:
        anomaly_signals = [anomaly_signal]

    tx = Transaction(
        transaction_id=   str(uuid.uuid4()),
        user_id=          request.user_id,
        amount=           request.amount,
        currency=         "KES",
        merchant_name=    "Test Merchant",
        merchant_category=request.merchant_category,
        country=          request.country,
        city=             "Nairobi" if request.country == "KE" else "Unknown",
        timestamp=        datetime.now(timezone.utc).isoformat(),
    )

    event = AnomalyEvent(
        anomaly_id=        str(uuid.uuid4()),
        transaction=       tx,
        window_stats={
            "mean": mean, "stddev": stddev,
            "count": 6, "ready": True,
        },
        anomalies=         anomaly_signals,
        combined_severity= anomaly_signals[0]["severity"],
        detected_at=       datetime.now(timezone.utc).isoformat(),
    )

    try:
        outcome = handle_anomaly(event)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "anomaly_id":   outcome["anomaly_id"],
        "user_id":      request.user_id,
        "amount":       request.amount,
        "country":      request.country,
        "threat_level": outcome["assessment"]["threat_level"],
        "confidence":   outcome["assessment"]["confidence"],
        "action":       outcome["action_result"]["action"],
        "status":       outcome["action_result"]["status"],
        "assessment":   outcome["assessment"]["assessment"][:300],
    }
