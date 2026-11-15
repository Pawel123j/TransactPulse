# Dashboard — Streamlit analytics UI

Reads the gold layer (via DuckDB over Delta on MinIO) and the data-quality / drift
reports, presenting:

- **KPIs** — transactions, volume (PLN), fraud count & rate.
- **Volume by country** — local Plotly bar charts for volume and transaction count.
- **Daily trend** — transactions and fraud over time.
- **Fraud** — fraud rate by category, top ML-scored transactions, strongest
  rule-based signals.
- **Data quality** — silver rows, quarantined count, checks passed.
- **Drift** — PSI/KS of `fraud_score` / `amount_pln` vs the training reference.

The dashboard degrades gracefully: panels show an informational message when their
table/report does not exist yet. The synthetic demo uses local charts and does not
fetch map geometry or other assets from a CDN.

## Run

### Self-contained portfolio demo (no services)

From the repository root, install the dashboard dependencies and start the fixed
synthetic sample:

```bash
pip install -r dashboard/requirements.txt
DEMO_MODE=synthetic streamlit run dashboard/app.py
```

The app labels these figures as synthetic and does not claim they came from the
Spark pipeline or model.

### In the full stack

```bash
docker compose up -d --build
# open http://localhost:8501
```

### Standalone (against a running infra + gold)

```bash
pip install -r dashboard/requirements.txt
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin123
export LAKEHOUSE_BUCKET=lakehouse REPORTS_DIR=docs
streamlit run dashboard/app.py
```

## Layout

- [`app.py`](app.py) — Streamlit UI (panels + Plotly charts).
- [`data_access.py`](data_access.py) — DuckDB connection, query strings, report
  loading (unit-tested in `tests/test_dashboard_data.py`).
