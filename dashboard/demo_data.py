"""Small, fixed synthetic Gold snapshot for a quick, reproducible demo.

This sample is illustrative and is not generated from a fraud model. It keeps
the dashboard easy to review without Kafka, MinIO, Spark, or network services;
the full streaming pipeline remains the integration environment.
"""

from __future__ import annotations

from datetime import date

import duckdb
import pandas as pd


def register_demo_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Register stable, explicitly synthetic tables matching the Gold schemas."""
    daily = pd.DataFrame(
        [
            (date(2026, 10, 20), "PL", 1480, 184250.00, 23),
            (date(2026, 10, 20), "DE", 630, 119820.00, 8),
            (date(2026, 10, 20), "GB", 485, 92715.00, 12),
            (date(2026, 10, 21), "PL", 1725, 215990.00, 31),
            (date(2026, 10, 21), "DE", 710, 133410.00, 11),
            (date(2026, 10, 21), "GB", 552, 106210.00, 9),
            (date(2026, 10, 22), "PL", 1960, 248730.00, 28),
            (date(2026, 10, 22), "DE", 805, 157450.00, 14),
            (date(2026, 10, 22), "GB", 608, 120875.00, 10),
            (date(2026, 10, 23), "PL", 2105, 274180.00, 35),
            (date(2026, 10, 23), "DE", 862, 170945.00, 18),
            (date(2026, 10, 23), "GB", 670, 134340.00, 13),
        ],
        columns=["event_date", "country", "tx_count", "total_volume_pln", "fraud_count"],
    )
    categories = pd.DataFrame(
        [
            ("GROCERIES", 3000, 20, 20 / 3000, 450000.0),
            ("ELECTRONICS", 2250, 80, 80 / 2250, 650000.0),
            ("TRAVEL", 1600, 42, 42 / 1600, 380000.0),
            ("RETAIL", 5742, 70, 70 / 5742, 478915.0),
        ],
        columns=["merchant_category", "tx_count", "fraud_count", "fraud_rate", "total_volume_pln"],
    )
    scored = pd.DataFrame(
        [
            ("demo-tx-1042", "acct-019", 18490.0, "PL", "ELECTRONICS", 0.98, True, True),
            ("demo-tx-1088", "acct-044", 12320.0, "DE", "TRAVEL", 0.93, True, True),
            ("demo-tx-1107", "acct-012", 8940.0, "GB", "ELECTRONICS", 0.87, False, True),
            ("demo-tx-1159", "acct-031", 6420.0, "PL", "TRAVEL", 0.79, True, True),
        ],
        columns=[
            "transaction_id",
            "account_id",
            "amount_pln",
            "country",
            "merchant_category",
            "fraud_score",
            "is_fraud_label",
            "fraud_flag",
        ],
    )
    signals = pd.DataFrame(
        [
            ("demo-tx-1042", "acct-019", 18490.0, "PL", "ONLINE", "ELECTRONICS", 5),
            ("demo-tx-1088", "acct-044", 12320.0, "DE", "ONLINE", "TRAVEL", 4),
            ("demo-tx-1159", "acct-031", 6420.0, "PL", "CARD_PRESENT", "TRAVEL", 3),
        ],
        columns=[
            "transaction_id",
            "account_id",
            "amount_pln",
            "country",
            "channel",
            "merchant_category",
            "signal_count",
        ],
    )
    velocity = pd.DataFrame(
        [("acct-019", 8, 60, 1300.0), ("acct-044", 5, 30, 970.0), ("acct-031", 4, 60, 410.0)],
        columns=["account_id", "event_count", "window_minutes", "median_amount"],
    )

    for name, frame in (
        ("daily_volume_by_country", daily),
        ("merchant_category_kpi", categories),
        ("scored_transactions", scored),
        ("fraud_signals", signals),
        ("account_velocity", velocity),
    ):
        con.register(name, frame)
