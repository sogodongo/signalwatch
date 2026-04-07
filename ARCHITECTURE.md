# SignalWatch — System Architecture

This document explains the technical architecture of SignalWatch, the reasoning behind each major design decision, and the trade-offs considered during development.

---

## System overview

SignalWatch is a real-time streaming anomaly detection and response engine for payment transactions. It combines statistical detection with LLM reasoning to distinguish genuine fraud from false positives.

The core pipeline:
```
TransactionProducer → [transactions topic] → WindowProcessor → AnomalyDetector
                                                                      ↓
                                              [anomalies topic] → LLM Reasoner
                                                                      ↓
                                                            ResponseEngine → PostgreSQL
```

---

## Streaming layer

### Apache Kafka — two-topic architecture

**Decision:** Separate `transactions` and `anomalies` topics rather than processing inline.

**Reasoning:** Publishing detected anomalies back to Kafka decouples the detection layer from the reasoning layer. The LLM reasoner, the dashboard, and the compliance logger all consume from `anomalies` independently — none of them know about each other. Adding a new consumer never requires touching the detector.

**Trade-off:** Two Kafka topics adds operational complexity over a direct function call. For a single-process system, this is over-engineering. For a multi-service deployment, it's the correct architecture.

### kafka-python consumer groups

**Decision:** Named consumer groups with offset tracking over manual offset management.

**Reasoning:** Consumer groups give Kafka responsibility for tracking what's been processed. If the consumer crashes and restarts, it resumes from the last committed offset automatically — no messages lost. Manual offset management would require implementing this reliability ourselves.

**Trade-off:** `auto_commit_interval_ms=1000` means up to 1 second of messages could be reprocessed after a crash. For fraud detection this is acceptable — processing a transaction twice is safer than missing it.

---

## Detection layer

### Sliding window processor — deque-based, in-memory

**Decision:** Pure Python deque over Redis or a time-series database for window state.

**Reasoning:** For a single-process system, in-memory state is 10-100x faster than a Redis round-trip. The deque provides O(1) append and eviction. Window state is intentionally ephemeral — if the process restarts, windows rebuild within minutes as new transactions flow in.

**Trade-off:** In-memory state doesn't survive process restarts. For a distributed multi-process deployment, Redis would be required for shared window state across workers.

### MIN_WINDOW_SIZE = 5

**Decision:** Require at least 5 transactions before enabling anomaly detection for a user.

**Reasoning:** Z-score computed from 1 or 2 data points is statistically meaningless — the standard deviation is too unstable. Waiting for 5 transactions produces a meaningful baseline before any detection fires. This dramatically reduces false positives for new users at the cost of missing anomalies in the first 5 transactions.

### Four detection methods

**Z-score** — primary method for amount anomalies. Sensitive to deviations from the user's rolling mean. Threshold of 3.0σ means only 0.3% of normal transactions are flagged.

**IQR bounds** — secondary method for skewed distributions. Catches outliers even when the mean has been elevated by previous anomalies. Complements Z-score rather than replacing it.

**Velocity** — counts transactions in the window. Rapid succession of transactions is a fraud signal independent of amounts. Threshold of 8 per 5-minute window catches card testing patterns.

**Geographic** — deterministic home-country check. Zero false positives for users who never travel. High signal-to-noise ratio makes it a reliable secondary signal.

---

## Reasoning layer

### GPT-4o with structured JSON output

**Decision:** LLM reasoning over pure rule-based escalation.

**Reasoning:** Statistical signals are context-free — a Z-score of 18 means "unusual" but not "fraud." The LLM adds merchant context (electronics purchases are legitimately high-value), temporal awareness (holiday periods spike spending), and signal coherence assessment (foreign + high amount + unusual merchant = very suspicious; velocity alone = probably legitimate).

**Trade-off:** LLM reasoning adds ~2-3 seconds latency and ~$0.003 cost per anomaly. For a fraud detection system where a false block costs customer goodwill and a missed fraud costs real money, this is justified.

### Confidence threshold downgrade pattern

**Decision:** Downgrade actions when confidence falls below threshold (block requires 0.80, alert requires 0.65).

**Reasoning:** Irreversible actions (block) taken with low confidence cause unnecessary customer friction. The downgrade chain — block → review → monitor — ensures uncertain decisions default to the safer reversible action. This is a critical safety property for any autonomous system.

---

## Storage layer

### Three-table audit schema

`anomaly_events` — immutable detection record. Written once when an anomaly is detected.
`threat_assessments` — reasoning record. What the LLM concluded and its confidence.
`response_actions` — execution record. What action was taken and whether it was downgraded.

**Decision:** Three separate tables over a single denormalized events table.

**Reasoning:** Separating detection, reasoning, and action records allows independent querying of each layer. "How often did the LLM recommend block but we downgraded to review?" requires joining assessments and actions — only possible with separate tables.

### ON CONFLICT DO NOTHING for idempotency

Every write uses `ON CONFLICT (anomaly_id) DO NOTHING`. Replaying a Kafka topic for debugging never creates duplicate audit records.

---

## Evaluation metrics

From the current test corpus (23 anomalies):

| Metric | Value |
|--------|-------|
| Avg LLM confidence | 0.787 |
| High confidence rate (≥0.80) | 74% |
| Downgrade rate | 0% |
| Geographic anomalies | 35% of total |
| Action distribution | 57% monitor, 26% review, 13% alert, 4% block |

---

## Known limitations and future work

- **In-memory window state** — process restart loses window history. Production requires Redis for distributed state.
- **Mock tools** — all response actions are mock implementations. Production requires real payment processor, alerting, and CRM integrations.
- **Single Kafka partition** — no parallelism. Production needs multiple partitions with partition-key-based routing (user_id as key) to ensure ordering per user.
- **Synchronous LLM calls** — each anomaly blocks until the LLM responds. Production needs async processing with a reasoning worker pool.
- **Home country hardcoded** — user profiles are static. Production requires a user database with dynamic home country tracking.
