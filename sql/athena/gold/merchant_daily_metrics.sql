-- Gold mart: one row per merchant × event day.
--
-- Substitute payments_gold_bucket from Terraform output.

CREATE EXTERNAL TABLE IF NOT EXISTS rtmsp_dev_payments.merchant_daily_metrics (
  merchant_id string,
  event_date string,
  txn_count bigint,
  unique_users bigint,
  captured_count bigint,
  authorized_count bigint,
  failed_count bigint,
  refunded_count bigint,
  gmv_usd double,
  authorized_usd double,
  failed_usd double,
  refunded_usd double,
  aov_usd double,
  capture_rate double,
  failure_rate double,
  large_ticket_count bigint
)
PARTITIONED BY (
  year string,
  month string,
  day string
)
STORED AS PARQUET
LOCATION 's3://${payments_gold_bucket}/payments/merchant_daily_metrics/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.year.type' = 'integer',
  'projection.year.range' = '2026,2030',
  'projection.month.type' = 'integer',
  'projection.month.range' = '1,12',
  'projection.month.digits' = '2',
  'projection.day.type' = 'integer',
  'projection.day.range' = '1,31',
  'projection.day.digits' = '2',
  'storage.location.template' = 's3://${payments_gold_bucket}/payments/merchant_daily_metrics/year=${year}/month=${month}/day=${day}/'
);
