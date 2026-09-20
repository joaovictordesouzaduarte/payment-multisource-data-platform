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
        │
Glue Schema Registry  (JSON, BACKWARD_ALL)
        │  latest PaymentEvent schema → bronze Spark read
        ▼
Glue 6.0 SDP job  (pipelines/sdp/payments)
        │
        ├─ bronze_payments
        ├─ silver_payments / silver_payments_rejected
        └─ fact_payments / fact_payments_rejected     ← gold
        │
        ▼
Athena workgroup  (results on gold/athena-results/)
```

## Design choices

| Decision | Rationale |
|---|---|
| Kinesis Data Streams | Payments source (not a stream processor) |
| Firehose → S3 | Durable bronze files sized for Glue batch (avoids tiny objects) |
| Glue Schema Registry | Bronze reads the latest JSON contract instead of a hardcoded Spark schema |
| `PaymentEvent` allows extras | Simulator (and future producers) can land optional fields; unknown keys are not dropped |
| Glue 6.0 SDP + Athena | Spark Declarative Pipeline (bronze→silver→gold); Athena for SQL |
| Partition key = `merchant_id` | Per-merchant ordering on the source stream |
| Line-delimited JSON + `schema_version` | Glue / Athena friendly; `2` marks events that carry extras |
| Glue crawler on `payments/raw/` only | Catalogs bronze Hive partitions; excludes `payments/errors/` |
| Incremental crawler (`CRAWL_NEW_FOLDERS_ONLY`) | First run is full; later runs add new hour prefixes only |
| Silver/gold as Apache Iceberg v2 | ACID commits Athena can query; Glue 6.0 can write v3, but Athena cannot read it yet |
| Silver partitioned by event time | Firehose hour is delivery time; analytics need `event_timestamp` |
| Quarantine rejected rows | Quality without a separate DQ product |
| Gold daily grain | `fact_payments` aggregates silver by day / merchant / status / country / currency |
| GMV = captured only | Authorized/failed/refunded are not revenue; gold still keeps authorized and refunded in `fact_payments` |
| Terraform IaC (interactive Docker workspace) | Review `terraform plan -out=tfplan` then `apply tfplan` |

AWS resource names still use the `rtmsp-*` prefix so existing stacks are not recreated.

## Payment event contract

Core fields on `PaymentEvent` (`src/payments_gateway/models.py`):

| Field | Type | Notes |
|---|---|---|
| `event_id` | string (UUID) | Dedup key |
| `merchant_id` | string | Partition + join key |
| `user_id` | string | Payer identifier |
| `amount_usd` | float `> 0` | Payment amount |
| `currency` | string | ISO-4217, default `USD` |
| `status` | enum | `authorized` / `captured` / `failed` / `refunded` |
| `event_timestamp` | datetime (UTC) | Event time |
| `schema_version` | int | `1` = core only; `2` = extras present |
| `ingest_source` | string | Always `payment_gateway` |

The model uses `extra="allow"`. The simulator may attach any of these extras (~40% of events): `country`, `payment_method`, `channel`, `card_brand`, `fx_rate`.

Glue Registry schema ([`infra/terraform/schemas/payment_event.json`](../infra/terraform/schemas/payment_event.json)) is the bronze read contract: core payload fields plus `country` and `source_file`. `schema_version` and `ingest_source` stay on the producer; they are not in the registry schema. Compatibility is `BACKWARD_ALL`.

Silver and gold currently consume `country` from extras (`US` when missing). Other extras land in bronze when present but are not projected into silver.

## S3 medallion layout

| Layer | Location | Storage |
|---|---|---|
| Bronze | `s3://{bronze}/payments/raw/` (Firehose), `payments/errors/` | S3 Standard |
| SDP artifacts | `s3://{silver}/glue/pipelines/payments/` (code + state) | S3 Standard |
| Iceberg warehouse | `s3://{silver}/iceberg-warehouse/` | S3 Standard |
| Silver tables | Catalog: `bronze_payments`, `silver_payments`, `silver_payments_rejected` | Iceberg v2 |
| Gold tables | Catalog: `fact_payments`, `fact_payments_rejected` | Iceberg v2 |
| Athena results | `s3://{gold}/athena-results/` | S3 Standard |

Bronze Athena DDL: [`sql/athena/bronze/payment_events_raw.sql`](../sql/athena/bronze/payment_events_raw.sql) (Hive/JSON + partition projection). Iceberg tables are registered by the SDP job. Substitute bucket names from Terraform output.

The bronze crawler targets `s3://{bronze}/payments/raw/` (table level 3). Schema policy is `LOG`. The crawler infers JSON types (`event_timestamp` is typically a string); the Registry schema plus `PaymentEvent` remain the contract. Do not crawl silver or gold: Iceberg metadata is the catalog. If Hive/Parquet tables already exist under the same names, drop them before the SDP job runs.

Terraform: registry + schema in [`infra/terraform/registry.tf`](../infra/terraform/registry.tf) and [`infra/terraform/schema.tf`](../infra/terraform/schema.tf). Outputs include `glue_schema_registry` and `glue_events_schema`.

## SDP transforms

Implemented in [`pipelines/sdp/payments/`](../pipelines/sdp/payments/).

**`bronze_payments`** — Firehose JSON read with the latest Registry schema, plus `source_file`. Streaming table; `write.spark.accept-any-schema` is on so later registry versions can land.

**`silver_payments`**

- Cast `amount_usd` to decimal; normalize status/currency; parse timestamp
- Quarantine rows with missing required fields or negative amount
- Derive per-event `amount_band` (`<10` micro, `<50` small, `<100` medium, else large)
- Dedup on `event_id` (keep latest `event_timestamp`)
- Keep `country` for gold
- Partitioned by event-time `year` / `month` / `day`

**`silver_payments_rejected`** — rows with one or more `rejection_reasons`.

**`fact_payments_temp`** — daily grain from `silver_payments`: `year`, `month`, `day`, `merchant_id`, `status`, `country` (`US` if null), `currency`. Metrics: `amount` (sum of `amount_decimal`), `txn_count`, `unique_users`. Aggregated `amount_band`: `<100` micro, `<250` small, `<1000` medium, else large. `full_date` is `year-month-day`.

**`fact_payments`** — that grain where `status <> 'failed'`. Partitioned by `full_date`, `country`.

**`fact_payments_rejected`** — the same grain where `status = 'failed'`. Partitioned by `full_date`, `country`.

GMV queries should still filter `status = 'captured'`. `sql/athena/gold/` star DDL (`fact_payment` + dims) is leftover from an earlier design; SDP gold is the two `fact_payments*` tables.

## Later

- Iceberg v3 when Athena can read format-version 3
- SCD Type 2 on `dim_merchant` (derived `failure_tier`) in dbt on Athena
- Optional extra payment-adjacent sources (settlements or chargebacks on S3)
- Remote Terraform state
