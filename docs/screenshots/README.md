# Screenshots

This directory holds the screenshots referenced from the root `README.md`.

They are **not** generated automatically: every UI in this project is served by
a container, so capturing them requires a running Docker daemon and a browser.
The exact capture procedure — which services to start, which URL to open and
what each image must show — is in
[`HUMAN_ACTION_REQUIRED.md`](../../HUMAN_ACTION_REQUIRED.md) at the repository
root.

## Expected files

| File | Source | What it must show |
| ---- | ------ | ----------------- |
| `dashboard-overview.png` | Streamlit, `http://localhost:8501` | The landing view over the gold layer: transaction volume, fraud-rate tiles and the time-series chart, populated with real generated data |
| `dashboard-fraud.png` | Streamlit, `http://localhost:8501` | The fraud-scoring view: scored transactions with their model score, filtered to the high-risk band |
| `airflow-dag.png` | Airflow, `http://localhost:8088` | The `medallion_pipeline` DAG in **Graph** view after a successful run — every task green |
| `airflow-runs.png` | Airflow, `http://localhost:8088` | The DAG's run history (Grid view) showing at least one complete green run |
| `minio-lakehouse.png` | MinIO console, `http://localhost:9001` | The bucket browser showing the `bronze/`, `silver/` and `gold/` prefixes with Delta files present |

## Conventions

- PNG, captured at a viewport of at least 1440×900.
- Crop browser chrome; keep the application UI only.
- No real data appears anywhere in this project — every record is synthetic, so
  no redaction is required.
- Keep each file under ~500 KB (resize rather than compress to a blur).
