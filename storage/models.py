import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://signalwatch:signalwatch@localhost:5434/signalwatch"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def init_db():
    """Creates all SignalWatch tables if they don't exist."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS anomaly_events (
                id               SERIAL PRIMARY KEY,
                anomaly_id       TEXT UNIQUE NOT NULL,
                user_id          TEXT NOT NULL,
                transaction_id   TEXT NOT NULL,
                amount           FLOAT NOT NULL,
                currency         TEXT,
                merchant_name    TEXT,
                merchant_category TEXT,
                country          TEXT,
                anomaly_signals  TEXT,
                combined_severity TEXT,
                window_mean      FLOAT,
                window_stddev    FLOAT,
                window_count     INTEGER,
                detected_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS threat_assessments (
                id                      SERIAL PRIMARY KEY,
                anomaly_id              TEXT REFERENCES anomaly_events(anomaly_id),
                threat_level            TEXT,
                assessment              TEXT,
                recommended_action      TEXT,
                confidence              FLOAT,
                false_positive_likelihood TEXT,
                requires_human_review   BOOLEAN,
                key_risk_factors        TEXT,
                assessed_at             TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS response_actions (
                id              SERIAL PRIMARY KEY,
                anomaly_id      TEXT REFERENCES anomaly_events(anomaly_id),
                final_action    TEXT NOT NULL,
                original_action TEXT,
                downgraded      BOOLEAN DEFAULT FALSE,
                status          TEXT,
                action_detail   TEXT,
                executed_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        conn.commit()
    print("[storage] Database tables initialized.")
