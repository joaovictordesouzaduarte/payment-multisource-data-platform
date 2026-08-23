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
Glue bronze→silver  →  S3 silver (typed events + quarantine, event-time partitions)
        │
        ▼
Glue silver→gold    →  S3 gold (merchant_daily_metrics, payment_status_daily)
        │
        ▼
Athena workgroup
```

**Stack:** Python 3.10 · FastAPI · Pydantic · uv · Terraform · Kinesis · Firehose · S3 · Glue · Athena · CloudWatch · pytest · Ruff

CI (pytest, Ruff, `terraform fmt` / `validate`) runs on every push via [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

Design rationale lives in [docs/architecture.md](docs/architecture.md). Analyst queries: [docs/demo-queries.md](docs/demo-queries.md).

## What the lake does

| Layer | Grain | What you get |
|---|---|---|
| Bronze | raw Firehose files | NDJSON as landed; partitioned by **delivery** time |
| Silver `payment_events` | one row per event | typed, deduped on `event_id`, partitioned by **event** time |
| Silver `payment_events_rejected` | bad bronze rows | quality quarantine (`rejection_reason`) |
| Gold `merchant_daily_metrics` | merchant × day | GMV, AOV, capture/failure rate, unique users |
| Gold `payment_status_daily` | status × day | outcome mix |

Transforms are testable Python in `src/payments_lake/` (Glue jobs are thin wrappers). GMV is **captured** amount only.

## Repository layout

```text
infra/terraform/     S3, Kinesis, Firehose, Glue crawler + ETL jobs, Athena, IAM, alarms
src/payments_gateway/  Event contract, Kinesis producer, simulator, FastAPI
src/payments_lake/     Bronze→silver→gold transforms (unit-tested)
jobs/glue/             Glue entrypoints
sql/athena/            Bronze / silver / gold DDL
tests/                 Gateway + lake unit tests
docs/                  Architecture, IAM policy, demo queries
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

Useful outputs: `kinesis_stream_name`, `medallion_buckets`, `glue_bronze_to_silver_job`, `glue_silver_to_gold_job`, `athena_workgroup`. Attach `producer_iam_policy_arn` to the principal that runs the simulator or API.

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

## 4. Run silver and gold

```bash
aws glue start-job-run --job-name rtmsp-dev-payments-bronze-to-silver
aws glue start-job-run --job-name rtmsp-dev-payments-silver-to-gold
```

Create Athena tables from `sql/athena/**` (substitute bucket names from `terraform output`). Use workgroup `rtmsp-dev-payments`. Example queries are in [docs/demo-queries.md](docs/demo-queries.md).

## 5. Tests

```bash
uv run pytest -q
uv run ruff check src tests jobs
```

## Success checks

1. `terraform apply tfplan` creates the stream, Firehose, medallion buckets, Glue crawler + jobs, Athena workgroup, IAM, and alarms.
2. Simulator `PutRecords` succeed (CloudWatch `IncomingRecords` increases).
3. Within ~1–2 minutes, objects land in the bronze prefix.
4. Bronze crawler creates `bronze_raw` partitioned by year/month/day/hour.
5. Glue bronze→silver writes `payments/payment_events/` (event-time partitions). Rejected rows go to `payments/payment_events_rejected/`.
6. Glue silver→gold writes `merchant_daily_metrics` and `payment_status_daily`.
7. Athena queries in [docs/demo-queries.md](docs/demo-queries.md) return rows.

## Cost and teardown

Glue job runs (G.1X × 2 workers) and Kinesis/Firehose/S3 incur AWS charges. Disable alarms with `enable_alarms = false` to skip SNS/CloudWatch alarm spend.

```bash
./infra/tf.sh shell
# inside: terraform plan -destroy -out=tfplan && terraform show tfplan && terraform apply tfplan
```

Empty S3 buckets first if destroy fails on remaining objects.

## What I would do next

- A second source (settlements or chargebacks on S3) so the lake is literally multi-source
- Glue Schema Registry for contract evolution
- Remote Terraform state (S3 + DynamoDB lock) if this is shared

This repo is one payments source end-to-end. Completing that path is the current scope.

## License

[MIT](LICENSE)
