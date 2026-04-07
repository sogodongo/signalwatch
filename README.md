# SignalWatch — Real-Time Anomaly Detection and Response Engine

A production-grade real-time streaming intelligence system that detects anomalous payment transactions as they happen and uses GPT-4o to reason about whether they represent genuine threats.

---

## What it does

A transaction arrives: *user_001 charges KES 4,200 at an electronics store in Lagos.*

SignalWatch:
1. Ingests the transaction from the Kafka stream in real time
2. Updates the user's 5-minute sliding window statistics
3. Computes Z-score, IQR bounds, velocity, and geographic signals
4. Detects: amount 95σ from mean, foreign country — publishes to anomalies topic
5. LLM reasoner reads the anomaly and assesses threat context
6. Response engine issues block, alert, or escalation based on severity
7. Full audit trail written to PostgreSQL

Total detection latency: under 100ms. LLM reasoning: under 3 seconds.

---

## Architecture
```
TransactionProducer → [transactions topic] → WindowProcessor
                                                    ↓
                                         AnomalyDetector (Z-score + IQR + geo)
                                                    ↓
                                          [anomalies topic]
                                                    ↓
                                           LLM Reasoner (GPT-4o)
                                                    ↓
                                          ResponseEngine (block/alert/escalate)
                                                    ↓
                                         PostgreSQL audit store
```

---

## Detection methods

| Method | What it catches |
|--------|----------------|
| Z-score | Amounts deviating from user's rolling mean |
| IQR bounds | Outliers in skewed spending distributions |
| Velocity | Too many transactions in the window |
| Geographic | Transactions outside user's home country |

---

## Stack

| Component | Technology |
|-----------|------------|
| Event streaming | Apache Kafka 7.5 |
| Stream processing | Python kafka-python |
| Statistical detection | Z-score + IQR (numpy-free, pure Python) |
| AI reasoning | GPT-4o |
| Audit store | PostgreSQL 15 |
| API | FastAPI |
| Dashboard | Streamlit (live feed) |

---

## Quick start
```bash
git clone https://github.com/sogodongo/signalwatch
cd signalwatch
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Add OPENAI_API_KEY to .env
docker compose up -d
python3 streaming/producers/transaction_producer.py
```

---

## Project status

| Week | Focus | Status |
|------|-------|--------|
| Week 1 | Streaming foundation + detection | Done |
| Week 2 | AI reasoning + response engine | In progress |
| Week 3 | Dashboard + evaluation + docs | Upcoming |
