# Lakehouse — Silver layer (cleansing + data quality)

Batch job refining the Delta **bronze** table into a clean, conformed,
deduplicated **silver** table, with a **quarantine** table for bad records and a
**data-quality report**. See [ADR 0005](../docs/adr/0005-data-quality-i-kwarantanna.md).

- Source: `s3a://lakehouse/bronze/transactions`
- Sink (clean): `s3a://lakehouse/silver/transactions` — partitioned by `event_date`,
  Z-ordered by `account_id`
- Sink (rejected): `s3a://lakehouse/silver/quarantine` — with `dq_errors` reasons
- Report: `docs/data_quality/`

## What it does

1. **Cast & extract** the bronze `data` struct into typed columns (`event_time`,
   `amount`, …).
2. **Normalize**: upper-case currency/country/channel, lower-case
   `merchant_category`, join the [synthetic FX table](fx_rates.py) and compute
   `amount_pln`.
3. **Data-quality gate** ([`dq_rules.py`](dq_rules.py)): split rows into valid vs.
   quarantine; every failing rule is recorded per row in `dq_errors`.
4. **Deduplicate** valid rows by `transaction_id` (keep latest by `ingestion_time`).
5. **Upsert** into silver via idempotent Delta **MERGE** (late-data tolerant within
   the watermark horizon).
6. **OPTIMIZE … ZORDER BY (account_id)**.
7. **Report**: Great Expectations suite + Spark metrics → Markdown/JSON in
   `docs/data_quality/`.

## Silver schema

`transaction_id, event_time, event_date, account_id, amount, currency,
fx_rate_to_pln, amount_pln, merchant_category, country, channel, device_id,
is_fraud_label, ingestion_time, _offset`

## Data-quality rules

Defined once in [`dq_rules.py`](dq_rules.py) as `Rule` objects carrying **both** a
pure-Python predicate (unit-tested) and a null-safe Spark SQL expression (used by
the gate): non-null `transaction_id` / `account_id` / `event_time`,
`0 < amount ≤ 1,000,000`, and allowed `currency` / `country` / `channel` /
`merchant_category`.

The Great Expectations suite ([`quality.py`](quality.py)) mirrors these rules and
runs *off the critical path* for observability. A representative report lives at
[`docs/data_quality/silver_quality_report.example.md`](../docs/data_quality/silver_quality_report.example.md)
and the suite at
[`docs/data_quality/expectation_suite.json`](../docs/data_quality/expectation_suite.json).

## Run

```bash
# In Docker (reuses the spark image; override the entrypoint):
docker compose -f infra/docker-compose.yml run --rm \
  --entrypoint /opt/spark/bin/spark-submit spark-bronze \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /app/lakehouse/silver_job.py

# From the host:
pip install -r lakehouse/requirements.txt
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  lakehouse/silver_job.py
```

> The silver job is wired into the Airflow `medallion_pipeline` DAG in Stage 7.

## Gold layer & ML scoring (Stage 6)

The gold job ([`gold_job.py`](gold_job.py)) reads silver and writes analytical
Delta tables plus an ML-scored table, then computes drift.

| Gold table                  | Grain                     | Contents                                            |
| --------------------------- | ------------------------- | --------------------------------------------------- |
| `daily_volume_by_country`   | day × country             | tx count, volume/avg PLN, fraud count & rate        |
| `merchant_category_kpi`     | merchant category         | tx count, distinct accounts, volume, fraud rate     |
| `account_velocity`          | account × day             | tx count, total/max PLN, distinct countries/devices |
| `fraud_signals`             | transaction               | rule-based boolean signals + `signal_count`         |
| `scored_transactions`       | transaction               | silver + `fraud_score` + `fraud_flag`               |

### Fraud scoring (ADR 0006)

- The model is a swappable artifact behind [`FraudModel`](ml/model.py); inference
  runs as a vectorized Spark **`pandas_udf`** ([`ml/scoring.py`](ml/scoring.py)),
  loading the model once per executor.
- A baseline **scikit-learn** model is trained on synthetic data:

  ```bash
  python -m lakehouse.ml.train_model --samples 60000 --seed 42
  # -> models/fraud_model.pkl (gitignored) + models/fraud_model.meta.json (committed)
  ```

- If the artifact is missing/unloadable, scoring **falls back** to a deterministic
  heuristic model, so gold always produces `scored_transactions`.

### Drift ([`drift.py`](drift.py))

PSI and KS of the live `fraud_score` / `amount_pln` versus the model's training
reference are written to `docs/drift/` — see the example
[`drift_report.example.md`](../docs/drift/drift_report.example.md).

## Tests

- `tests/test_dq_rules.py` — pure-Python rule logic (no Spark).
- `tests/test_quality.py` — expectation suite + report rendering (no Spark).
- `tests/test_features.py` — feature engineering (pure + pandas-vectorized).
- `tests/test_drift.py` — PSI / KS metrics (pure Python).
- `tests/test_model_scoring.py` — model wrapper + pandas scoring (no Spark).
- `tests/test_silver_transform.py`, `tests/test_gold_aggregates.py` — Spark
  transforms (skipped without PySpark; run in CI).
