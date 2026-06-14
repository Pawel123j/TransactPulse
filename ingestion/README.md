# Ingestion — synthetic transaction generator

Generates realistic synthetic financial transactions and publishes them to the
Kafka topic `transactions.raw` (JSON on the wire — see
[ADR 0003](../docs/adr/0003-format-serializacji.md)).

> ⚠️ Fully synthetic data. No real banking data is used anywhere.

## Record schema

| Field               | Type    | Notes                                              |
| ------------------- | ------- | -------------------------------------------------- |
| `transaction_id`    | string  | UUID4                                              |
| `timestamp`         | string  | ISO-8601 UTC event time (may be late-arriving)     |
| `account_id`        | string  | Stable account id (`ACC-…`)                        |
| `amount`            | number  | > 0, log-normal, 2 decimals                        |
| `currency`          | string  | ISO 4217, derived from `country`                   |
| `merchant_category` | string  | MCC-like bucket                                    |
| `country`           | string  | ISO 3166-1 alpha-2                                 |
| `channel`           | string  | `ONLINE` / `POS` / `ATM` / `MOBILE`                |
| `device_id`         | string  | Device id (`DEV-…`)                                |
| `is_fraud_label`    | boolean | Synthetic ground-truth fraud flag (~0.3 %)         |

## Realistic patterns

- **Log-normal amounts** with high-value fraud outliers.
- **~0.3 % fraud**, skewed toward card-not-present channels, riskier merchant
  categories, and foreign country/device (account-takeover style).
- **Hourly seasonality** — emission rate modulated by a day/night weight curve.
- **Late-arriving events** — a configurable fraction carry a backdated timestamp
  to exercise watermarking later (Stage 5).
- **Stable entity pools** (accounts ↔ devices) so per-account aggregation and
  velocity features are meaningful downstream.

## Install

```bash
pip install -r ingestion/requirements.txt
```

## Usage

```bash
# Stream ~50 tx/s to Kafka for 60 seconds
python -m ingestion --rate 50 --duration 60 --bootstrap-servers localhost:9092

# Burst mode, deterministic
python -m ingestion --rate 30 --burst --seed 42

# No Kafka needed: print JSON to stdout (great for a quick look / smoke test)
python -m ingestion --rate 5 --duration 3 --dry-run
```

### Configuration

CLI flags override environment variables, which override defaults.

| Env var                     | CLI flag              | Default            |
| --------------------------- | --------------------- | ------------------ |
| `GEN_RATE`                  | `--rate`              | `20`               |
| `GEN_DURATION_SECONDS`      | `--duration`          | `0` (until Ctrl-C) |
| `GEN_FRAUD_RATE`            | `--fraud-rate`        | `0.003`            |
| `GEN_LATE_EVENT_RATE`       | —                     | `0.02`             |
| `GEN_BURST_ENABLED`         | `--burst`             | `false`            |
| `GEN_APPLY_SEASONALITY`     | `--no-seasonality`    | `true`             |
| `GEN_SEED`                  | `--seed`              | random             |
| `KAFKA_BOOTSTRAP_SERVERS`   | `--bootstrap-servers` | `localhost:9092`   |
| `KAFKA_TOPIC`               | `--topic`             | `transactions.raw` |

## Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```
