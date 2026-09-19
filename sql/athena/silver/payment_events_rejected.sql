-- Silver Iceberg quarantine: bronze rows that failed quality gates.
-- Glue bronze→silver registers this table; run this only if it is missing.
-- If a Hive/JSON table already exists at this name, drop it first:
--   DROP TABLE rtmsp_dev_payments.payment_events_rejected;
--
-- Substitute payments_silver_bucket from Terraform output.

CREATE TABLE IF NOT EXISTS rtmsp_dev_payments.payment_events_rejected (
  event_id string,
  merchant_id string,
  user_id string,
  amount_usd string,
  currency string,
  status string,
  event_timestamp string,
  schema_version string,
  ingest_source string,
  rejection_reason string
)
LOCATION 's3://${payments_silver_bucket}/payments/payment_events_rejected/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'format-version' = '2'
);
