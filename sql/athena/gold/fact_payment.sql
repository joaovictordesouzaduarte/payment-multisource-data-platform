-- Gold Iceberg fact: one row per payment event.
-- Glue silver→gold registers this table; run this only if it is missing.
-- If a Hive/Parquet table already exists at this name, drop it first:
--   DROP TABLE rtmsp_dev_payments.fact_payment;
--
-- Substitute payments_gold_bucket from Terraform output.

CREATE TABLE IF NOT EXISTS rtmsp_dev_payments.fact_payment (
  event_id string,
  date_key int,
  merchant_id string,
  user_id string,
  status string,
  amount_usd double,
  amount_cents bigint,
  is_revenue boolean,
  amount_band string,
  event_hour int,
  ingest_lag_hours int,
  year string,
  month string,
  day string
)
PARTITIONED BY (year, month, day)
LOCATION 's3://${payments_gold_bucket}/payments/fact_payment/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'format-version' = '2'
);
