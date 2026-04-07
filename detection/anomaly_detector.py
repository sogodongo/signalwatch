import os
import math
from dotenv import load_dotenv
from streaming.schema import Transaction
from detection.window_processor import WindowProcessor

load_dotenv()

ZSCORE_THRESHOLD    = float(os.getenv("ZSCORE_THRESHOLD", "3.0"))
VELOCITY_THRESHOLD  = 8
HOME_COUNTRIES      = {
    "user_001": "KE",
    "user_002": "KE",
    "user_003": "KE",
    "user_004": "KE",
    "user_005": "KE",
}


def _zscore(value: float, mean: float, stddev: float) -> float:
    if stddev == 0:
        return 0.0
    return abs((value - mean) / stddev)


def _iqr_upper_bound(mean: float, stddev: float) -> float:
    # Approximate Q1/Q3 from mean and stddev assuming roughly normal distribution
    # In production you'd track actual percentiles in the window
    q1 = mean - 0.674 * stddev
    q3 = mean + 0.674 * stddev
    iqr = q3 - q1
    return q3 + 1.5 * iqr


def detect_anomalies(
    tx: Transaction,
    window_stats: dict,
) -> list[dict]:
    """
    Runs all anomaly detectors against a transaction and its window stats.
    Returns a list of detected anomalies — empty list means no anomalies.

    Each anomaly dict contains:
      type, severity (low/medium/high/critical), score, and explanation.
    """
    anomalies = []

    # --- Amount anomaly detection ---
    if window_stats.get("ready") and window_stats["stddev"] is not None:
        mean   = window_stats["mean"]
        stddev = window_stats["stddev"]

        zscore = _zscore(tx.amount, mean, stddev)
        iqr_upper = _iqr_upper_bound(mean, stddev)

        if zscore >= ZSCORE_THRESHOLD:
            severity = (
                "critical" if zscore >= 10 else
                "high"     if zscore >= 6  else
                "medium"
            )
            anomalies.append({
                "type":        "amount_zscore",
                "severity":    severity,
                "score":       round(zscore, 2),
                "explanation": (
                    f"Amount KES {tx.amount:.0f} is {zscore:.1f} standard deviations "
                    f"from user mean of KES {mean:.0f} (stddev={stddev:.0f})"
                ),
            })

        elif tx.amount > iqr_upper and tx.amount > mean * 2:
            anomalies.append({
                "type":        "amount_iqr",
                "severity":    "medium",
                "score":       round(tx.amount / iqr_upper, 2),
                "explanation": (
                    f"Amount KES {tx.amount:.0f} exceeds IQR upper bound "
                    f"of KES {iqr_upper:.0f}"
                ),
            })

    # --- Velocity anomaly detection ---
    count = window_stats.get("count", 0)
    if count >= VELOCITY_THRESHOLD:
        severity = "high" if count >= VELOCITY_THRESHOLD * 2 else "medium"
        anomalies.append({
            "type":        "velocity",
            "severity":    severity,
            "score":       round(count / VELOCITY_THRESHOLD, 2),
            "explanation": (
                f"{count} transactions in the last window — "
                f"threshold is {VELOCITY_THRESHOLD}"
            ),
        })

    # --- Geographic anomaly detection ---
    home_country = HOME_COUNTRIES.get(tx.user_id)
    if home_country and tx.country != home_country:
        anomalies.append({
            "type":        "geographic",
            "severity":    "high",
            "score":       1.0,
            "explanation": (
                f"Transaction in {tx.country} but user's home country is {home_country}"
            ),
        })

    return anomalies
