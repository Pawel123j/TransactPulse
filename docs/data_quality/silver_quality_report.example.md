# Silver data-quality report

- **Generated:** 2026-06-13T20:09:46+00:00
- **Suite:** `transactpulse.silver.transactions`
- **Overall:** ✅ PASS (5/5 checks passed)

## Metrics

| Metric | Value |
| ------ | ----- |
| Rows (silver) | 28741 |
| Distinct transaction_id | 28741 |
| Duplicate ids | 0 |
| Null transaction_id | 0 |
| Null account_id | 0 |
| Null amount | 0 |
| Amount min / max | 0.32 / 98234.11 |
| Avg amount (PLN) | 187.44 |
| Fraud count / rate | 86 / 0.00299 |
| Quarantined records | 53 |

## Checks

| Check | Result |
| ----- | ------ |
| `no_null_transaction_id` | ✅ |
| `no_null_account_id` | ✅ |
| `no_null_amount` | ✅ |
| `transaction_id_unique` | ✅ |
| `amount_within_bounds` | ✅ |
