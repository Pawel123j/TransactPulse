# Screenshots

This directory holds real screenshots referenced from the root `README.md`.
`dashboard-synthetic-demo.png` is captured from the local Streamlit app in
`DEMO_MODE=synthetic`. Full-stack captures require a running Compose deployment
and are still pending.

## Expected files

| File | Source | What it must show |
| ---- | ------ | ----------------- |
| `dashboard-synthetic-demo.png` | Streamlit offline demo, `http://localhost:8501` | Clearly labelled fixed synthetic data, KPI tiles and populated charts; not a model or pipeline result |
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
