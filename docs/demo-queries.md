# Athena demo queries

Use workgroup `rtmsp-dev-payments` (`terraform output -raw athena_workgroup`). Create the gold tables from `sql/athena/gold/` first.

Substitute the date filter for the day you generated events.

## Top merchants by GMV

```sql
SELECT
  merchant_id,
  event_date,
  gmv_usd,
  txn_count,
  unique_users,
  aov_usd,
  capture_rate
FROM rtmsp_dev_payments.merchant_daily_metrics
WHERE year = '2026' AND month = '08' AND day = '23'
ORDER BY gmv_usd DESC
LIMIT 10;
```

## Highest failure rate (at least 5 transactions)

```sql
SELECT
  merchant_id,
  event_date,
  failed_count,
  txn_count,
  failure_rate,
  failed_usd
FROM rtmsp_dev_payments.merchant_daily_metrics
WHERE year = '2026'
  AND month = '08'
  AND txn_count >= 5
ORDER BY failure_rate DESC, failed_count DESC
LIMIT 10;
```

## Daily status mix

```sql
SELECT
  event_date,
  status,
  txn_count,
  amount_usd,
  unique_merchants,
  unique_users
FROM rtmsp_dev_payments.payment_status_daily
WHERE year = '2026' AND month = '08' AND day = '23'
ORDER BY status;
```

## Large-ticket captures (silver)

```sql
SELECT
  event_id,
  merchant_id,
  user_id,
  amount_usd,
  amount_band,
  event_timestamp
FROM rtmsp_dev_payments.payment_events
WHERE year = '2026'
  AND month = '08'
  AND day = '23'
  AND is_revenue = true
  AND amount_band = 'large'
ORDER BY amount_usd DESC;
```
