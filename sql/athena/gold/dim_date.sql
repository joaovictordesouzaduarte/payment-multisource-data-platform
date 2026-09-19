-- Gold Iceberg Type 0 date dimension (calendar 2026–2030).
-- Glue silver→gold registers this table; run this only if it is missing.
-- If a Hive/Parquet table already exists at this name, drop it first:
--   DROP TABLE rtmsp_dev_payments.dim_date;
--
-- Substitute payments_gold_bucket from Terraform output.

CREATE TABLE IF NOT EXISTS rtmsp_dev_payments.dim_date (
  date_key int,
  full_date string,
  year int,
  month int,
  day int,
  month_name string,
  day_of_week string,
  is_weekend boolean
)
LOCATION 's3://${payments_gold_bucket}/payments/dim_date/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'format-version' = '2'
);
