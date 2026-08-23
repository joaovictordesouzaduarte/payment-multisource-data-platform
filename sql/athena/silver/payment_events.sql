-- Silver conformed payment events (Parquet, event-time partitions).
-- Source of truth is payments_lake.to_silver; Glue writes this prefix.
--
-- Substitute payments_silver_bucket from Terraform output.

CREATE EXTERNAL TABLE IF NOT EXISTS rtmsp_dev_payments.payment_events (
  event_id string,
  merchant_id string,
  user_id string,
  amount_usd double,
  amount_cents bigint,
  currency string,
  status string,
  event_timestamp string,
  schema_version int,
  ingest_source string,
  is_captured boolean,
  is_authorized boolean,
  is_failed boolean,
  is_refunded boolean,
  is_revenue boolean,
  amount_band string,
  event_date string,
  event_hour int,
  ingest_year string,
  ingest_month string,
  ingest_day string,
  ingest_hour string,
  ingest_lag_hours int
)
PARTITIONED BY (
  year string,
  month string,
  day string
)
STORED AS PARQUET
LOCATION 's3://${payments_silver_bucket}/payments/payment_events/'
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
  'storage.location.template' = 's3://${payments_silver_bucket}/payments/payment_events/year=${year}/month=${month}/day=${day}/'
);
