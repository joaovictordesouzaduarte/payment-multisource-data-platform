# Payment Data Platform — Kinesis → Firehose → S3 → Glue batch

This is a **payments data platform**, not a real-time stream-processing system. Kinesis is one ingest source. Firehose writes bronze files so Glue can process them in batch.

## Architecture

```text
Payment Gateway Producer (simulator / FastAPI)
        │  PutRecords (partition key = merchant_id)
        ▼
Amazon Kinesis Data Streams  (payments source)
        │  Firehose reads the stream
        ▼
Amazon Kinesis Data Firehose
        │  GZIP JSON, buffered (64 MiB / 60s)
        ▼
S3 Bronze  (raw landing)
  payments/raw/year=YYYY/month=MM/day=DD/hour=HH/   ← delivery time
  payments/errors/...
        │  Glue crawler (on-demand, incremental)
        ▼
Glue Data Catalog  (bronze_raw)
        │  Glue 6.0 SDP job (pipelines/sdp/payments)
        ▼
S3 Silver (Iceberg v2)
  bronze_payments / silver_payments / silver_payments_rejected
        │
        ▼
Athena workgroup  (results on gold/athena-results/)
```

## Design choices

| Decision | Rationale |
|---|---|
| Kinesis Data Streams | Payments source (not a stream processor) |
| Firehose → S3 | Durable bronze files sized for Glue batch (avoids tiny objects) |
| Glue 6.0 SDP + Athena | Spark Declarative Pipeline (bronze→silver); Athena for SQL |
| Partition key = `merchant_id` | Per-merchant ordering on the source stream |
| Line-delimited JSON + `schema_version` | Glue / Athena friendly |
| Glue crawler on `payments/raw/` only | Catalogs bronze Hive partitions; excludes `payments/errors/` |
| Incremental crawler (`CRAWL_NEW_FOLDERS_ONLY`) | First run is full; later runs add new hour prefixes only |
| Silver/gold as Apache Iceberg v2 | ACID commits Athena can query; Glue 6.0 can write v3, but Athena cannot read it yet |
| Silver partitioned by event time | Firehose hour is delivery time; analytics need `event_timestamp` |
| Quarantine rejected rows | Quality without a separate DQ product |
| GMV = captured only | Authorized/failed/refunded are not revenue |
| Terraform IaC (interactive Docker workspace) | Review `terraform plan -out=tfplan` then `apply tfplan` |

AWS resource names still use the `rtmsp-*` prefix so existing stacks are not recreated.

## Payment event contract

| Field | Type | Notes |
|---|---|---|
| `event_id` | string (UUID) | Dedup key |
| `merchant_id` | string | Partition + join key |
| `user_id` | string | Payer identifier |
| `amount_usd` | float `> 0` | Payment amount |
| `currency` | string | ISO-4217, default `USD` |
| `status` | enum | `authorized` / `captured` / `failed` / `refunded` |
| `event_timestamp` | datetime (UTC) | Event time |
| `schema_version` | int | Starts at `1` |
| `ingest_source` | string | Always `payment_gateway` |

Do not invent card brand, country, fees, or FX. Transforms only use this contract.

## S3 medallion layout

| Layer | Prefix | Storage |
|---|---|---|
| Bronze | `payments/raw/` (Firehose), `payments/errors/` | S3 Standard |
| Silver | Iceberg `payments/payment_events/`, `payments/payment_events_rejected/` | S3 Standard |
| Gold | Not deployed yet (SDP is bronze→silver only) | — |

Bronze Athena DDL: [`sql/athena/bronze/payment_events_raw.sql`](../sql/athena/bronze/payment_events_raw.sql) (Hive/JSON + partition projection). Silver Iceberg tables are registered by the SDP job. Substitute bucket names from Terraform output.

The bronze crawler targets `s3://{bronze}/payments/raw/` (table level 3). Schema policy is `LOG`. The crawler infers JSON types (`event_timestamp` is typically a string); `PaymentEvent` remains the contract. Do not crawl silver: Iceberg metadata is the catalog. If Hive/Parquet tables already exist under the same names, drop them before the SDP job runs.

## Silver transforms

Implemented as Spark Declarative Pipeline views in [`pipelines/sdp/payments/`](../pipelines/sdp/payments/).

**`bronze_payments`** — Firehose JSON as landed, plus `source_file`.

**`silver_payments`**

- Cast `amount_usd` to decimal; normalize status/currency; parse timestamp
- Quarantine rows with missing required fields or negative amount
- Derive `amount_band` (micro/small/medium/large)
- Dedup on `event_id` (keep latest `event_timestamp`)

**`silver_payments_rejected`** — rows with one or more `rejection_reasons`.

Gold star (fact + dims) is designed but not in SDP yet.

## Later

- Gold star (`fact_payment` + dims) as an SDP transformation
- Iceberg v3 when Athena can read format-version 3
- SCD Type 2 on `dim_merchant` (derived `failure_tier`) in dbt on Athena
- Optional extra payment-adjacent sources (settlements or chargebacks on S3)
- Glue Schema Registry
- Remote Terraform state
