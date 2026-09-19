# Athena demo queries

Use workgroup `rtmsp-dev-payments` (`terraform output -raw athena_workgroup`). Silver and gold are Iceberg tables registered by the Glue jobs (DDL under `sql/athena/gold/` only if a table is missing).

Substitute the date filter for the day you generated events. GMV is captured amount only (`dim_status.is_revenue` or `fact_payment.is_revenue`).

## Top merchants by GMV

```sql
SELECT
  m.merchant_id,
  d.full_date,
  SUM(f.amount_usd) AS gmv_usd,
  COUNT(*) AS captured_count,
  COUNT(DISTINCT f.user_id) AS unique_users,
  SUM(f.amount_usd) / COUNT(*) AS aov_usd
FROM rtmsp_dev_payments.fact_payment f
JOIN rtmsp_dev_payments.dim_merchant m ON f.merchant_id = m.merchant_id
JOIN rtmsp_dev_payments.dim_date d ON f.date_key = d.date_key
JOIN rtmsp_dev_payments.dim_status s ON f.status = s.status
WHERE f.year = '2026'
  AND f.month = '08'
  AND f.day = '23'
  AND s.is_revenue = true
GROUP BY m.merchant_id, d.full_date
ORDER BY gmv_usd DESC
LIMIT 10;
```

## Highest failure rate (at least 5 transactions)

```sql
SELECT
  m.merchant_id,
  d.full_date,
  COUNT(*) AS txn_count,
  SUM(CASE WHEN f.status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
  CAST(SUM(CASE WHEN f.status = 'failed' THEN 1 ELSE 0 END) AS DOUBLE)
    / COUNT(*) AS failure_rate,
  SUM(CASE WHEN f.status = 'failed' THEN f.amount_usd ELSE 0 END) AS failed_usd
FROM rtmsp_dev_payments.fact_payment f
JOIN rtmsp_dev_payments.dim_merchant m ON f.merchant_id = m.merchant_id
JOIN rtmsp_dev_payments.dim_date d ON f.date_key = d.date_key
WHERE f.year = '2026'
  AND f.month = '08'
GROUP BY m.merchant_id, d.full_date
HAVING COUNT(*) >= 5
ORDER BY failure_rate DESC, failed_count DESC
LIMIT 10;
```

## Daily status mix

```sql
SELECT
  d.full_date,
  s.status,
  COUNT(*) AS txn_count,
  SUM(f.amount_usd) AS amount_usd,
  COUNT(DISTINCT f.merchant_id) AS unique_merchants,
  COUNT(DISTINCT f.user_id) AS unique_users
FROM rtmsp_dev_payments.fact_payment f
JOIN rtmsp_dev_payments.dim_date d ON f.date_key = d.date_key
JOIN rtmsp_dev_payments.dim_status s ON f.status = s.status
WHERE f.year = '2026'
  AND f.month = '08'
  AND f.day = '23'
GROUP BY d.full_date, s.status
ORDER BY s.status;
```

## Large-ticket captures

```sql
SELECT
  f.event_id,
  f.merchant_id,
  f.user_id,
  f.amount_usd,
  f.amount_band,
  d.full_date,
  f.event_hour
FROM rtmsp_dev_payments.fact_payment f
JOIN rtmsp_dev_payments.dim_date d ON f.date_key = d.date_key
JOIN rtmsp_dev_payments.dim_status s ON f.status = s.status
WHERE f.year = '2026'
  AND f.month = '08'
  AND f.day = '23'
  AND s.is_revenue = true
  AND f.amount_band = 'large'
ORDER BY f.amount_usd DESC;
```
