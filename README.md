# Payment Data Platform

Batch payments lake on AWS: a typed gateway publishes events to **Kinesis**, **Firehose** lands durable bronze files on S3, and **Glue** turns them into silver events and gold marts that **Athena** can query.

Payment events must be queryable, partitioned, and batch-friendly — not processed as a real-time stream.

```text
Payment Gateway (simulator / FastAPI)
        │  PutRecords (partition key = merchant_id)
        ▼
Kinesis Data Streams
        │
        ▼
Firehose  →  S3 bronze (GZIP NDJSON, Hive hour partitions)
        │
        ▼
Glue 6.0 SDP (bronze→silver)  →  Iceberg v2 silver (typed events + quarantine)
        │
        ▼
Athena workgroup
```

**Stack:** Python 3.10 · FastAPI · Pydantic · uv · Terraform · Kinesis · Firehose · S3 · Glue 6.0 (Spark 4.1.1) · Apache Iceberg v2 · Athena · CloudWatch · pytest · Ruff

CI (pytest, Ruff, `terraform fmt` / `validate`) runs on every push via [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

Design rationale lives in [docs/architecture.md](docs/architecture.md). Analyst queries: [docs/demo-queries.md](docs/demo-queries.md).

## What the lake does

| Layer | Grain | What you get |
|---|---|---|
| Bronze | raw Firehose files | NDJSON as landed; partitioned by **delivery** time |
| Silver `payment_events` | one row per event | Iceberg v2 (Athena-readable); typed, deduped on `event_id` |
| Silver `payment_events_rejected` | bad bronze rows | Iceberg v2 quality quarantine (`rejection_reason`) |
| Gold (later) | — | Not in SDP yet; star schema is still designed, not deployed |

SDP transforms live in `pipelines/sdp/payments/`. GMV is **captured** amount only.

## Repository layout

```text
infra/terraform/     S3, Kinesis, Firehose, Glue crawler + SDP job, Athena, IAM, alarms
src/payments_gateway/  Event contract, Kinesis producer, simulator, FastAPI
src/payments_lake/     Shared lake helpers (catalog naming, local Iceberg)
pipelines/sdp/         Glue 6.0 Spark Declarative Pipeline (bronze→silver)
sql/athena/            Bronze / silver / gold DDL
tests/                 Gateway + lake unit tests
docs/                  Architecture, IAM policy, demo queries
data/                  Local bronze samples + Hadoop Iceberg warehouse (gitignored)
```

## Prerequisites

- Python 3.10+ and [uv](https://docs.astral.sh/uv/)
- Docker in WSL (Terraform runs in a container; no local Terraform install)
- AWS credentials that can create Kinesis, Firehose, S3, Glue, IAM, Athena, CloudWatch, SNS  
  (set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in `.env`)

The deploy identity needs [docs/iam-deploy-policy.json](docs/iam-deploy-policy.json). Attach it from the Terraform workspace (mounted at `/docs/iam-deploy-policy.json`):

```bash
aws iam put-user-policy \
  --user-name iac \
  --policy-name rtmsp-terraform-deploy \
  --policy-document file:///docs/iam-deploy-policy.json
```

If you cannot grant SNS/CloudWatch alarm permissions, set `enable_alarms = false` in `infra/terraform/terraform.tfvars`.

## 1. Deploy infrastructure

```bash
chmod +x infra/tf.sh
./infra/tf.sh up
./infra/tf.sh shell
```

Inside the container:

```bash
terraform init
terraform plan -out=tfplan
terraform show tfplan
terraform apply tfplan
terraform output
exit
```

Then `./infra/tf.sh down`. Apply never re-plans; you apply the saved `tfplan`.

Useful outputs: `kinesis_stream_name`, `medallion_buckets`, `glue_payments_sdp_job`, `athena_workgroup`. Attach `producer_iam_policy_arn` to the principal that runs the simulator or API.

## 2. Install the app

```bash
uv sync --group dev
cp .env.example .env
# set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, KINESIS_STREAM_NAME
```

## 3. Publish events

```bash
uv run payments-simulator --rate 10 --duration 30
```

Or the HTTP API (OpenAPI at http://127.0.0.1:8080/docs):

```bash
uv run uvicorn payments_gateway.api:app --reload --port 8080
curl -X POST http://127.0.0.1:8080/payments/events/generate \
  -H "Content-Type: application/json" \
  -d '{"count": 50}'
```

After the Firehose buffer (~60s), GZIP JSON lands under:

```text
s3://{bronze}/payments/raw/year=.../month=.../day=.../hour=.../
```

Catalog bronze (crawler is on-demand):

```bash
aws glue start-crawler --name rtmsp-dev-payments-bronze-raw
```

## 4. Run silver (SDP)

```bash
aws glue start-job-run --job-name rtmsp-dev-payments-sdp
```

The Glue 6.0 Spark Declarative Pipeline reads Firehose bronze and materializes `bronze_payments`, `silver_payments`, and `silver_payments_rejected` in the Glue Catalog. Gold is not in SDP yet. If Hive/Parquet tables already exist under those names, drop them first. Athena DDL under `sql/athena/silver/` is only needed if a table is missing. Use workgroup `rtmsp-dev-payments`. Example queries are in [docs/demo-queries.md](docs/demo-queries.md).

## 5. Tests

```bash
uv run pytest -q
uv run ruff check src tests jobs
```

Transforms (`to_silver` / `to_gold`) are pytest. To run the **Glue job scripts** without uploading them, build the Glue 5 image and spark-submit inside the container (Spark 3.5 / Iceberg 1.7 — not a Glue 6.0 bitwise match). Writes go to a local Hadoop warehouse under `data/`; the container does not use AWS credentials.

```bash
aws s3 sync s3://<bronze-bucket>/payments/raw/year=2026/month=08/day=23/ \
  data/bronze/payments/raw/year=2026/month=08/day=23/

docker compose -f infra/docker-compose.glue.yml up -d --build
docker compose -f infra/docker-compose.glue.yml exec glue bash
```

Inside the container:

```bash
spark-submit jobs/glue/bronze_to_silver.py \
  --JOB_NAME local-bronze-to-silver \
  --GLUE_DATABASE rtmsp_dev_payments \
  --ICEBERG_CATALOG local \
  --ICEBERG_WAREHOUSE file:///home/hadoop/workspace/data/warehouse \
  --BRONZE_PATH file:///home/hadoop/workspace/data/bronze/payments/raw/ \
  --SILVER_PATH file:///home/hadoop/workspace/data/silver/payments/payment_events/ \
  --REJECTED_PATH file:///home/hadoop/workspace/data/silver/payments/payment_events_rejected/
```

`<bronze-bucket>` is `terraform output -raw payments_bronze_bucket`. Sync a Hive day/hour prefix only. `spark-submit jobs/glue/silver_to_gold.py` with the gold `file://` paths when you want gold. `docker compose -f infra/docker-compose.glue.yml down` stops the workspace.

## Success checks

1. `terraform apply tfplan` creates the stream, Firehose, medallion buckets, Glue crawler + SDP job, Athena workgroup, IAM, and alarms.
2. Simulator `PutRecords` succeed (CloudWatch `IncomingRecords` increases).
3. Within ~1–2 minutes, objects land in the bronze prefix.
4. Bronze crawler creates `bronze_raw` partitioned by year/month/day/hour.
5. Glue SDP job writes Iceberg `bronze_payments`, `silver_payments`, and `silver_payments_rejected`.
6. Athena queries in [docs/demo-queries.md](docs/demo-queries.md) return silver rows.

## Cost and teardown

Glue job runs (G.1X × 2 workers) and Kinesis/Firehose/S3 incur AWS charges. Disable alarms with `enable_alarms = false` to skip SNS/CloudWatch alarm spend.

```bash
./infra/tf.sh shell
# inside: terraform plan -destroy -out=tfplan && terraform show tfplan && terraform apply tfplan
```

Empty S3 buckets first if destroy fails on remaining objects.

## What I would do next

- Gold star (`fact_payment` + dims) as an SDP transformation
- Iceberg v3 when Athena can read format-version 3 (Glue 6.0 already can write it)
- SCD Type 2 on `dim_merchant` (as-of `failure_tier`) in dbt on Athena
- A second source (settlements or chargebacks on S3) so the lake is literally multi-source
- Glue Schema Registry for contract evolution
- Remote Terraform state (S3 + DynamoDB lock) if this is shared

This repo is one payments source end-to-end. Completing that path is the current scope.

## License

[MIT](LICENSE)
