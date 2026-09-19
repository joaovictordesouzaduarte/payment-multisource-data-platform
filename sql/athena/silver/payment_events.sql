-- Silver Iceberg payment events (event-time identity partitions).
-- Glue bronze→silver registers this table; run this only if it is missing.
-- If a Hive/Parquet table already exists at this name, drop it first:
--   DROP TABLE rtmsp_dev_payments.payment_events;
--
-- Substitute payments_silver_bucket from Terraform output.

CREATE TABLE IF NOT EXISTS rtmsp_dev_payments.payment_events (
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
  ingest_lag_hours int,
  year string,
  month string,
  day string
)
PARTITIONED BY (year, month, day)
LOCATION 's3://${payments_silver_bucket}/payments/payment_events/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
  --'format-version' = '2'
);
