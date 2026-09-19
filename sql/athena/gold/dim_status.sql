-- Gold Iceberg payment-status dimension.
-- Glue silver→gold registers this table; run this only if it is missing.
-- If a Hive/Parquet table already exists at this name, drop it first:
--   DROP TABLE rtmsp_dev_payments.dim_status;
--
-- Substitute payments_gold_bucket from Terraform output.

CREATE TABLE IF NOT EXISTS rtmsp_dev_payments.dim_status (
  status string,
  is_revenue boolean
)
LOCATION 's3://${payments_gold_bucket}/payments/dim_status/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'format-version' = '2'
);
