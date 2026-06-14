"""TransactPulse — Streamlit analytics dashboard.

Reads the gold layer (via DuckDB over Delta on MinIO) and the data-quality / drift
reports, presenting KPIs, volume by country, fraud signals, and DQ/drift panels.

Run locally::

    pip install -r dashboard/requirements.txt
    streamlit run dashboard/app.py
"""

from __future__ import annotations

from pathlib import Path

import plotly.express as px
import streamlit as st

from dashboard.data_access import (
    CATEGORY_KPI_SQL,
    DAILY_TREND_SQL,
    ISO2_TO_ISO3,
    KPI_SQL,
    TOP_SCORED_SQL,
    TOP_SIGNALS_SQL,
    VOLUME_BY_COUNTRY_SQL,
    DashboardConfig,
    open_gold,
    query_df,
    read_json_report,
)

st.set_page_config(page_title="TransactPulse", page_icon="💳", layout="wide")


@st.cache_resource(show_spinner=False)
def _connection():
    config = DashboardConfig.from_env()
    return open_gold(config), config


def _has_rows(df) -> bool:
    return df is not None and not df.empty


def render_header() -> None:
    st.title("💳 TransactPulse — real-time transaction analytics")
    st.caption(
        "Synthetic financial transactions · Kafka → Spark → Delta (medallion) → "
        "DuckDB · fraud scoring & drift. All data is synthetic."
    )


def render_kpis(con) -> None:
    df = query_df(con, KPI_SQL)
    if not _has_rows(df):
        st.info("No gold data yet — start the pipeline (generator → bronze → silver → gold).")
        return
    row = df.iloc[0]
    total_tx = int(row["total_tx"])
    fraud = int(row["total_fraud"])
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Transactions", f"{total_tx:,}")
    c2.metric("Volume (PLN)", f"{float(row['total_volume_pln']):,.0f}")
    c3.metric("Fraud labelled", f"{fraud:,}")
    c4.metric("Fraud rate", f"{(fraud / total_tx if total_tx else 0):.3%}")


def render_volume(con) -> None:
    st.subheader("Volume by country")
    df = query_df(con, VOLUME_BY_COUNTRY_SQL)
    if not _has_rows(df):
        st.info("No volume data yet.")
        return
    df["iso3"] = df["country"].map(ISO2_TO_ISO3)
    left, right = st.columns([3, 2])
    with left:
        fig = px.choropleth(
            df.dropna(subset=["iso3"]),
            locations="iso3",
            color="total_volume_pln",
            hover_name="country",
            color_continuous_scale="Blues",
            title="Total volume (PLN)",
        )
        fig.update_layout(margin={"l": 0, "r": 0, "t": 40, "b": 0}, height=380)
        st.plotly_chart(fig, use_container_width=True)
    with right:
        bar = px.bar(
            df.head(10),
            x="country",
            y="total_volume_pln",
            color="fraud_count",
            color_continuous_scale="Reds",
            title="Top countries",
        )
        bar.update_layout(margin={"l": 0, "r": 0, "t": 40, "b": 0}, height=380)
        st.plotly_chart(bar, use_container_width=True)


def render_trend(con) -> None:
    st.subheader("Daily transaction & fraud trend")
    df = query_df(con, DAILY_TREND_SQL)
    if not _has_rows(df):
        st.info("No trend data yet.")
        return
    fig = px.line(df, x="event_date", y=["tx_count", "fraud_count"], markers=True)
    fig.update_layout(margin={"l": 0, "r": 0, "t": 10, "b": 0}, height=320, legend_title="")
    st.plotly_chart(fig, use_container_width=True)


def render_fraud(con) -> None:
    st.subheader("🚨 Fraud signals & ML scoring")
    cat = query_df(con, CATEGORY_KPI_SQL)
    if _has_rows(cat):
        fig = px.bar(cat, x="merchant_category", y="fraud_rate", title="Fraud rate by category")
        fig.update_layout(margin={"l": 0, "r": 0, "t": 40, "b": 0}, height=320)
        st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.markdown("**Top ML-scored transactions (flagged)**")
        scored = query_df(con, TOP_SCORED_SQL)
        st.dataframe(scored, use_container_width=True, height=320) if _has_rows(
            scored
        ) else st.info("No scored transactions yet.")
    with right:
        st.markdown("**Strongest rule-based signals**")
        signals = query_df(con, TOP_SIGNALS_SQL)
        st.dataframe(signals, use_container_width=True, height=320) if _has_rows(
            signals
        ) else st.info("No fraud signals yet.")


def render_quality(config: DashboardConfig) -> None:
    st.subheader("✅ Data quality")
    report = read_json_report(Path(config.reports_dir) / "silver_quality_report.json")
    if report is None:
        st.info("No data-quality report yet (runs with the silver job).")
        return
    metrics = report.get("metrics", {})
    checks = report.get("checks", [])
    passed = sum(1 for c in checks if c.get("passed"))
    c1, c2, c3 = st.columns(3)
    c1.metric("Silver rows", f"{int(metrics.get('row_count', 0)):,}")
    c2.metric("Quarantined", f"{int(metrics.get('quarantined_count', 0)):,}")
    c3.metric("Checks passed", f"{passed}/{len(checks)}")
    st.dataframe(checks, use_container_width=True)


def render_drift(config: DashboardConfig) -> None:
    st.subheader("📈 Model drift")
    report = read_json_report(Path(config.reports_dir) / "drift_report.json")
    if report is None:
        st.info("No drift report yet (runs with the gold drift step).")
        return
    rows = [{"feature": k, **v} for k, v in report.get("metrics", {}).items()]
    st.caption(f"Model: {report.get('model_kind', '?')} · generated {report.get('generated', '?')}")
    st.dataframe(rows, use_container_width=True)


def main() -> None:
    render_header()
    con, config = _connection()

    render_kpis(con)
    st.divider()
    render_volume(con)
    render_trend(con)
    st.divider()
    render_fraud(con)
    st.divider()
    q, d = st.columns(2)
    with q:
        render_quality(config)
    with d:
        render_drift(config)


# Streamlit re-runs the whole script on every interaction.
main()
