"""Streamlit runtime regressions. Live S3/Kafka coverage lives in test_live_pipeline."""

from pathlib import Path

import duckdb
import pytest
from streamlit.testing.v1 import AppTest

from dashboard import data_access

APP = Path(__file__).parents[1] / "dashboard/app.py"


def _connection():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE daily_volume_by_country AS
        SELECT DATE '2026-10-20' event_date, 'PL' country, 1 tx_count,
               100.0 total_volume_pln, 1 fraud_count;
        CREATE TABLE merchant_category_kpi AS
        SELECT 'RETAIL' merchant_category, 1 tx_count, 1 fraud_count,
               1.0 fraud_rate, 100.0 total_volume_pln;
        CREATE TABLE scored_transactions AS
        SELECT 'tx' transaction_id, 'acct' account_id, 100.0 amount_pln,
               'PL' country, 'RETAIL' merchant_category, 0.95 fraud_score,
               TRUE is_fraud_label, TRUE fraud_flag;
        CREATE TABLE fraud_signals AS
        SELECT 'tx' transaction_id, 'acct' account_id, 100.0 amount_pln,
               'PL' country, 'ONLINE' channel, 'RETAIL' merchant_category, 3 signal_count;
    """)
    return con


@pytest.mark.parametrize("flagged", [True, False])
def test_dashboard_renders_both_nonempty_and_empty_fraud_panels(monkeypatch, tmp_path, flagged):
    def open_test_gold(config):
        con = _connection()
        if not flagged:
            con.execute("DELETE FROM scored_transactions; DELETE FROM fraud_signals")
        return con

    monkeypatch.setattr(data_access, "open_gold", open_test_gold)
    monkeypatch.setenv("REPORTS_DIR", str(tmp_path))
    app = AppTest.from_file(str(APP)).run(timeout=15)
    assert not app.exception
    assert not app.error
    assert app.metric[0].value == "1"
    if not flagged:
        assert any(info.value == "No scored transactions yet." for info in app.info)


def test_dashboard_outage_is_visible_without_exposing_credentials(monkeypatch):
    def failed_storage(config):
        raise OSError("private-credential-test")

    monkeypatch.setattr(data_access, "open_gold", failed_storage)
    app = AppTest.from_file(str(APP)).run(timeout=15)
    assert not app.exception
    assert len(app.error) == 1
    assert "unavailable" in app.error[0].value
    assert "private-credential-test" not in app.error[0].value
    assert not app.metric
