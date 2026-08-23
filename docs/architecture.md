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
        │  Glue job bronze→silver  (src/payments_lake)
        ▼
S3 Silver
  payments/payment_events/year=/month=/day=/         ← event time
  payments/payment_events_rejected/
        │  Glue job silver→gold
        ▼
S3 Gold
  payments/merchant_daily_metrics/
  payments/payment_status_daily/
        │
        ▼
Athena workgroup  (results on gold/athena-results/)
```

## Design choices

| Decision | Rationale |
|---|---|
| Kinesis Data Streams | Payments source (not a stream processor) |
| Firehose → S3 | Durable bronze files sized for Glue batch (avoids tiny objects) |
| Glue batch + Athena | Transform and query; no Flink / Lambda stream job |
| Partition key = `merchant_id` | Per-merchant ordering on the source stream |
| Line-delimited JSON + `schema_version` | Glue / Athena friendly |
| Glue crawler on `payments/raw/` only | Catalogs bronze Hive partitions; excludes `payments/errors/` |
| Incremental crawler (`CRAWL_NEW_FOLDERS_ONLY`) | First run is full; later runs add new hour prefixes only |
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
| Silver | `payments/payment_events/`, `payments/payment_events_rejected/` | S3 Standard |
| Gold | `payments/merchant_daily_metrics/`, `payments/payment_status_daily/` | S3 Standard |

Bronze Athena DDL: [`sql/athena/bronze/payment_events_raw.sql`](../sql/athena/bronze/payment_events_raw.sql). Silver/gold DDL under `sql/athena/silver/` and `sql/athena/gold/`. Substitute bucket names from Terraform output.

The bronze crawler targets `s3://{bronze}/payments/raw/` (table level 3). Schema policy is `LOG`. The crawler infers JSON types (`event_timestamp` is typically a string); `PaymentEvent` remains the contract. Silver/gold tables are defined by DDL, not crawled.

## Silver and gold transforms

Implemented in [`src/payments_lake/`](../src/payments_lake/) and invoked by [`jobs/glue/`](../jobs/glue/).

**Silver `payment_events`**

- Parse types (`amount_usd` → Decimal, `event_timestamp` → UTC, `currency` uppercase)
- Dedup on `event_id` (keep latest `event_timestamp`)
- Reject: missing required fields, non-UUID `event_id`, `amount_usd <= 0`, unknown status, bad currency, unparseable timestamp
- Derive: `amount_cents`, status flags, `is_revenue`, `amount_band` (micro/small/medium/large), `event_date` / `event_hour`, Firehose ingest partitions, `ingest_lag_hours`

**Silver `payment_events_rejected`** — original bronze fields plus `rejection_reason`.

**Gold `merchant_daily_metrics`** — merchant × `event_date`: txn count, unique users, status counts, GMV, AOV, capture/failure rates, large-ticket count.

**Gold `payment_status_daily`** — status × `event_date`: txn count, amount, unique merchants/users.

## Later

- Optional extra payment-adjacent sources (settlements or chargebacks on S3)
- Glue Schema Registry
- Remote Terraform state
