import sys
import time
sys.path.insert(0, ".")

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from storage.models import init_db, engine
from storage.event_store import get_recent_anomalies, get_metrics
from sqlalchemy import text

st.set_page_config(
    page_title="SignalWatch",
    page_icon="",
    layout="wide",
)

st.title("SignalWatch")
st.caption("Real-time anomaly detection and response engine — payment transaction monitoring")

init_db()

# Auto-refresh every 5 seconds
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 3, 30, 5)
st.sidebar.divider()
st.sidebar.markdown("**Detection thresholds**")
st.sidebar.markdown(f"Z-score threshold: `3.0σ`")
st.sidebar.markdown(f"Velocity threshold: `8 tx/window`")
st.sidebar.markdown(f"Window size: `5 minutes`")
st.sidebar.divider()
st.sidebar.markdown("**Topics**")
st.sidebar.code("transactions\nanomalies")

# Load data
metrics  = get_metrics()
anomalies = get_recent_anomalies(limit=50)

# Convert datetimes
for a in anomalies:
    if hasattr(a.get("detected_at"), "isoformat"):
        a["detected_at"] = a["detected_at"].isoformat()

# --- Metrics row ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total anomalies", metrics["total_anomalies"])
with col2:
    blocks = next(
        (r["count"] for r in metrics["by_action"] if r["final_action"] == "block"), 0
    )
    st.metric("Blocks", blocks)
with col3:
    st.metric("Avg confidence", f"{metrics['avg_confidence']:.2f}")
with col4:
    reviews = next(
        (r["count"] for r in metrics["by_action"] if r["final_action"] == "review"), 0
    )
    st.metric("Pending review", reviews)

st.divider()

# --- Charts row ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Actions taken")
    if metrics["by_action"]:
        df_action = pd.DataFrame(metrics["by_action"])
        color_map = {
            "block":   "#E24B4A",
            "alert":   "#EF9F27",
            "review":  "#7F77DD",
            "monitor": "#1D9E75",
        }
        df_action["color"] = df_action["final_action"].map(
            lambda x: color_map.get(x, "#888780")
        )
        fig = px.bar(
            df_action,
            x="final_action",
            y="count",
            color="final_action",
            color_discrete_map=color_map,
            labels={"final_action": "Action", "count": "Count"},
        )
        fig.update_layout(showlegend=False, height=300)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No actions recorded yet.")

with col_right:
    st.subheader("Severity distribution")
    if metrics["by_severity"]:
        df_sev = pd.DataFrame(metrics["by_severity"])
        color_map_sev = {
            "critical": "#E24B4A",
            "high":     "#EF9F27",
            "medium":   "#7F77DD",
            "low":      "#1D9E75",
        }
        fig2 = px.pie(
            df_sev,
            names="combined_severity",
            values="count",
            color="combined_severity",
            color_discrete_map=color_map_sev,
        )
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No severity data yet.")

st.divider()

# --- Live anomaly feed ---
st.subheader("Live anomaly feed")

if anomalies:
    df = pd.DataFrame(anomalies)

    # Severity color coding
    def severity_badge(level):
        colors = {
            "critical": "🔴",
            "high":     "🟠",
            "medium":   "🟡",
            "low":      "🟢",
        }
        return colors.get(str(level).lower(), "⚪")

    def action_badge(action):
        icons = {
            "block":   "🚫",
            "alert":   "⚠️",
            "review":  "👁️",
            "monitor": "📊",
        }
        return icons.get(str(action).lower(), "❓")

    display_cols = [
        "user_id", "amount", "country",
        "combined_severity", "threat_level",
        "confidence", "final_action", "detected_at"
    ]
    available = [c for c in display_cols if c in df.columns]
    df_display = df[available].copy()

    if "combined_severity" in df_display.columns:
        df_display["combined_severity"] = df_display["combined_severity"].apply(
            lambda x: f"{severity_badge(x)} {x}"
        )
    if "final_action" in df_display.columns:
        df_display["final_action"] = df_display["final_action"].apply(
            lambda x: f"{action_badge(x)} {x}"
        )
    if "detected_at" in df_display.columns:
        df_display["detected_at"] = df_display["detected_at"].apply(
            lambda x: str(x)[:19] if x else ""
        )

    st.dataframe(df_display, use_container_width=True, height=400)
else:
    st.info("No anomalies detected yet. Start the transaction producer to see live detections.")

st.divider()

# --- Per-user risk table ---
st.subheader("Per-user anomaly summary")
try:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                ae.user_id,
                COUNT(*) as total_anomalies,
                SUM(CASE WHEN ra.final_action = 'block' THEN 1 ELSE 0 END) as blocks,
                SUM(CASE WHEN ra.final_action = 'alert' THEN 1 ELSE 0 END) as alerts,
                ROUND(AVG(ta.confidence)::numeric, 3) as avg_confidence,
                MAX(ae.detected_at) as last_seen
            FROM anomaly_events ae
            LEFT JOIN threat_assessments ta ON ae.anomaly_id = ta.anomaly_id
            LEFT JOIN response_actions   ra ON ae.anomaly_id = ra.anomaly_id
            GROUP BY ae.user_id
            ORDER BY total_anomalies DESC
        """)).fetchall()

    if rows:
        df_users = pd.DataFrame([dict(r._mapping) for r in rows])
        if "last_seen" in df_users.columns:
            df_users["last_seen"] = df_users["last_seen"].apply(
                lambda x: x.isoformat()[:19] if hasattr(x, "isoformat") else str(x)[:19]
            )
        st.dataframe(df_users, use_container_width=True)
    else:
        st.info("No user data yet.")
except Exception as e:
    st.error(f"Could not load user data: {e}")

# Auto-refresh
time.sleep(refresh_interval)
st.rerun()
