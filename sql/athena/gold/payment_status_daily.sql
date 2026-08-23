-- Gold mart: daily payment outcome mix.
--
-- Substitute payments_gold_bucket from Terraform output.

CREATE EXTERNAL TABLE IF NOT EXISTS rtmsp_dev_payments.payment_status_daily (
  status string,
  event_date string,
  txn_count bigint,
  amount_usd double,
  unique_merchants bigint,
  unique_users bigint
)
PARTITIONED BY (
  year string,
  month string,
  day string
)
STORED AS PARQUET
LOCATION 's3://${payments_gold_bucket}/payments/payment_status_daily/'
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
  'storage.location.template' = 's3://${payments_gold_bucket}/payments/payment_status_daily/year=${year}/month=${month}/day=${day}/'
);
