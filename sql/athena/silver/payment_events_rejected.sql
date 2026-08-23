-- Silver quarantine: bronze rows that failed quality gates.
--
-- Substitute payments_silver_bucket from Terraform output.

CREATE EXTERNAL TABLE IF NOT EXISTS rtmsp_dev_payments.payment_events_rejected (
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
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://${payments_silver_bucket}/payments/payment_events_rejected/'
TBLPROPERTIES (
  'classification' = 'json'
);
