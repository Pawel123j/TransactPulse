-- TransactPulse — example analytical queries over the gold layer.
--
-- These run through DuckDB (ADR 0008). The runner registers each gold Delta table
-- as a view of the same name before executing this file:
--   daily_volume_by_country, merchant_category_kpi, account_velocity,
--   fraud_signals, scored_transactions
-- Statements are separated by semicolons.

-- 1. Top 10 countries by total transaction volume (PLN).
SELECT
    country,
    SUM(tx_count)            AS tx_count,
    ROUND(SUM(total_volume_pln), 2) AS total_volume_pln,
    SUM(fraud_count)         AS fraud_count
FROM daily_volume_by_country
GROUP BY country
ORDER BY total_volume_pln DESC
LIMIT 10;

-- 2. Merchant categories ranked by fraud rate (min 100 transactions).
SELECT
    merchant_category,
    tx_count,
    fraud_count,
    ROUND(fraud_count * 1.0 / tx_count, 5) AS fraud_rate,
    total_volume_pln
FROM merchant_category_kpi
WHERE tx_count >= 100
ORDER BY fraud_rate DESC;

-- 3. Daily fraud trend across the platform.
SELECT
    event_date,
    SUM(tx_count)    AS tx_count,
    SUM(fraud_count) AS fraud_count,
    ROUND(SUM(fraud_count) * 1.0 / NULLIF(SUM(tx_count), 0), 5) AS fraud_rate
FROM daily_volume_by_country
GROUP BY event_date
ORDER BY event_date;

-- 4. High-velocity accounts: many transactions/day across multiple countries.
SELECT
    account_id,
    event_date,
    tx_count,
    distinct_countries,
    distinct_devices,
    total_amount_pln,
    max_amount_pln
FROM account_velocity
WHERE tx_count >= 5 AND distinct_countries >= 2
ORDER BY tx_count DESC, distinct_countries DESC
LIMIT 25;

-- 5. Strongest rule-based fraud signals (most signals fired).
SELECT
    transaction_id,
    account_id,
    amount_pln,
    country,
    channel,
    merchant_category,
    signal_count
FROM fraud_signals
WHERE signal_count >= 3
ORDER BY signal_count DESC, amount_pln DESC
LIMIT 25;

-- 6. Highest ML-scored transactions flagged as fraud.
SELECT
    transaction_id,
    account_id,
    amount_pln,
    country,
    merchant_category,
    ROUND(fraud_score, 4) AS fraud_score,
    is_fraud_label
FROM scored_transactions
WHERE fraud_flag = TRUE
ORDER BY fraud_score DESC
LIMIT 25;

-- 7. Model lift: average score for labelled fraud vs non-fraud.
SELECT
    is_fraud_label,
    COUNT(*)                       AS n,
    ROUND(AVG(fraud_score), 4)     AS avg_fraud_score
FROM scored_transactions
GROUP BY is_fraud_label
ORDER BY is_fraud_label;
