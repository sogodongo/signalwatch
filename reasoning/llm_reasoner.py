import os
import json
from openai import OpenAI
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from streaming.schema import AnomalyEvent

load_dotenv()

_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

REASONER_SYSTEM_PROMPT = """You are SignalWatch's fraud analyst.
You receive anomalous payment transactions with statistical signals and
produce structured threat assessments.

You have access to:
- The transaction details (amount, merchant, country, timestamp)
- The user's rolling window statistics (mean, stddev, transaction count)
- The specific anomaly signals detected (Z-score, IQR, velocity, geographic)

Your assessment must consider:
1. Are the anomaly signals coherent — do they tell a consistent story?
2. Is the merchant category consistent with the transaction amount?
3. Does the geographic signal combined with other signals increase risk?
4. What is the most likely explanation — fraud, false positive, or edge case?

Threat levels:
- critical: Almost certainly fraud, block immediately
- high: Strong fraud indicators, block and alert
- medium: Suspicious but ambiguous, flag for review
- low: Likely false positive, allow with monitoring

Recommended actions:
- block: Decline the transaction immediately
- alert: Allow but send alert to fraud team
- review: Queue for human review within 1 hour
- monitor: Allow and increase monitoring on this user

Respond ONLY with valid JSON matching this exact schema — no other fields, no nested objects:
{
  "threat_level": "critical|high|medium|low",
  "assessment": "one paragraph explanation as a plain string",
  "recommended_action": "block|alert|review|monitor",
  "confidence": 0.0 to 1.0,
  "false_positive_likelihood": "low|medium|high",
  "requires_human_review": true or false,
  "key_risk_factors": ["factor1", "factor2"]
}"""

REASONER_USER_TEMPLATE = """Analyze this anomalous transaction:

Transaction:
- User: {user_id}
- Amount: {currency} {amount}
- Merchant: {merchant_name} ({merchant_category})
- Location: {city}, {country}
- Time: {timestamp}

User's normal behaviour (last 5 minutes):
- Mean transaction: {currency} {mean}
- Std deviation: {currency} {stddev}
- Transactions in window: {count}

Anomaly signals detected:
{anomaly_signals}

Produce your threat assessment."""


class ThreatAssessment(BaseModel):
    threat_level:            str
    assessment:              str
    recommended_action:      str
    confidence:              float = Field(ge=0.0, le=1.0)
    false_positive_likelihood: str
    requires_human_review:   bool
    key_risk_factors:        list[str] = []


def reason_about_anomaly(event: AnomalyEvent) -> ThreatAssessment:
    """
    Produces a structured threat assessment for an anomaly event.
    The LLM reads transaction context + statistical signals together
    to make a more nuanced decision than statistics alone.
    """
    tx    = event.transaction
    stats = event.window_stats

    signals_text = "\n".join([
        f"- [{a['severity'].upper()}] {a['type']}: {a['explanation']}"
        for a in event.anomalies
    ])

    user_message = REASONER_USER_TEMPLATE.format(
        user_id=          tx.user_id,
        currency=         tx.currency,
        amount=           tx.amount,
        merchant_name=    tx.merchant_name,
        merchant_category=tx.merchant_category,
        city=             tx.city,
        country=          tx.country,
        timestamp=        tx.timestamp,
        mean=             stats.get("mean", "unknown"),
        stddev=           stats.get("stddev", "unknown"),
        count=            stats.get("count", 0),
        anomaly_signals=  signals_text,
    )

    print(f"[reasoner] Assessing: {tx.user_id} "
          f"{tx.currency} {tx.amount:.0f} in {tx.country}...")

    response = _client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        messages=[
            {"role": "system", "content": REASONER_SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        response_format={"type": "json_object"},
    )

    raw    = response.choices[0].message.content
    data   = json.loads(raw)
    result = ThreatAssessment(**data)

    print(f"[reasoner] → threat={result.threat_level} "
          f"action={result.recommended_action} "
          f"confidence={result.confidence} "
          f"fp={result.false_positive_likelihood}")

    return result
