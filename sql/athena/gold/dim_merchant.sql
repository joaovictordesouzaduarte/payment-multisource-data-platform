-- Gold Iceberg Type 1 merchant dimension (natural key only).
-- Glue silver→gold registers this table; run this only if it is missing.
-- If a Hive/Parquet table already exists at this name, drop it first:
--   DROP TABLE rtmsp_dev_payments.dim_merchant;
--
-- Substitute payments_gold_bucket from Terraform output.

CREATE TABLE IF NOT EXISTS rtmsp_dev_payments.dim_merchant (
  merchant_id string
)
LOCATION 's3://${payments_gold_bucket}/payments/dim_merchant/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'format-version' = '2'
);
