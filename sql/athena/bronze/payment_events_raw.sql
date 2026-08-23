-- Bronze external table for Firehose GZIP NDJSON.
-- Source of truth for Athena / Glue Catalog (not Iceberg — raw landing stays files).
--
-- Apply in Athena (database rtmsp_dev_payments) after substituting the bronze bucket:
--   terraform output -raw payments_bronze_bucket
--
-- Example: rtmsp-dev-payments-bronze-01a0b697

CREATE EXTERNAL TABLE IF NOT EXISTS rtmsp_dev_payments.payment_events_raw (
  event_id string,
  merchant_id string,
  user_id string,
  amount_usd double,
  currency string,
  status string,
  event_timestamp string,
  schema_version int,
  ingest_source string
)
PARTITIONED BY (
  year string,
  month string,
  day string,
  hour string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://rtmsp-dev-payments-bronze-547ddd40/payments/raw/'
TBLPROPERTIES (
  'classification' = 'json',
  'compressionType' = 'gzip',
  'projection.enabled' = 'true',
  'projection.year.type' = 'integer',
  'projection.year.range' = '2026,2030',
  'projection.month.type' = 'integer',
  'projection.month.range' = '1,12',
  'projection.month.digits' = '2',
  'projection.day.type' = 'integer',
  'projection.day.range' = '1,31',
  'projection.day.digits' = '2',
  'projection.hour.type' = 'integer',
  'projection.hour.range' = '0,23',
  'projection.hour.digits' = '2',
  'storage.location.template' = 's3://rtmsp-dev-payments-bronze-547ddd40/payments/raw/year=${year}/month=${month}/day=${day}/hour=${hour}/'
);
