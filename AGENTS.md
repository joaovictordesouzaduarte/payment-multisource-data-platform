# Agent notes

This is a **payments data platform** (batch lake), not a real-time stream processor. A typed gateway publishes events to **Kinesis**. **Firehose** lands durable bronze files on S3. **Glue 6.0 SDP** turns them into Iceberg silver tables that **Athena** can query.

Resource names use the `rtmsp-*` prefix so existing stacks are not recreated.

**Architecture (source of truth):** [docs/architecture.md](docs/architecture.md). Read that file before changing ingest, storage, transforms, or IaC. Analyst queries: [docs/demo-queries.md](docs/demo-queries.md). IAM for deploy: [docs/iam-deploy-policy.json](docs/iam-deploy-policy.json).

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

Gold star (`fact_payment` + dims) is designed in `sql/athena/gold/` but is **not** in SDP yet. Do not invent a second source, Iceberg v3, or Glue Schema Registry usage unless the task asks for it.

## Requirements

Agents and local work need the following before touching AWS or running the producer.

### Tooling

- Python 3.10+ and [uv](https://docs.astral.sh/uv/)
- Docker (Terraform and local Glue run in containers; no host Terraform or AWS CLI)
- WSL is the expected environment for `infra/tf.sh` on Windows

**Stack:** Python 3.10 · FastAPI · Pydantic · uv · Terraform 1.15.8 · Kinesis · Firehose · S3 · Glue 6.0 (Spark 4.1.1) · Apache Iceberg v2 · Athena · CloudWatch · pytest · Ruff · Black

### AWS shared config (required)

The repo expects a gitignored **`.aws/`** directory at the project root with both files:

```text
.aws/config
.aws/credentials
```

These are the standard AWS CLI shared-config files. Never commit them. `.aws/` is already in `.gitignore`.

`.aws/config`:

```ini
[default]
region = us-east-1
output = json
```

`.aws/credentials`:

```ini
[default]
aws_access_key_id = <key>
aws_secret_access_key = <secret>
```

Add `aws_session_token` under `[default]` when using temporary credentials.

The deploy identity must be able to create Kinesis, Firehose, S3, Glue, IAM, Athena, CloudWatch, and SNS. Attach [docs/iam-deploy-policy.json](docs/iam-deploy-policy.json) to that principal. If SNS/CloudWatch alarm permissions are missing, set `enable_alarms = false` in `infra/terraform/terraform.tfvars`.

### App environment

Copy `.env.example` to `.env` (also gitignored). The gateway and simulator read it via `payments_gateway.config.Settings`. Required values:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `KINESIS_STREAM_NAME` (default in the example: `rtmsp-dev-payments-events`)

Optional: `AWS_REGION` / `AWS_DEFAULT_REGION` (default `us-east-1`), `KINESIS_BATCH_SIZE`, `KINESIS_MAX_RETRIES`, `LOG_LEVEL`.

The Terraform Compose workspace (`infra/docker-compose.terraform.yml`) loads `.env` and the image entrypoint writes `/root/.aws/config` and `/root/.aws/credentials` inside the container.

### Terraform overrides

If `infra/terraform/terraform.tfvars` is missing, `./infra/tf.sh up` copies it from `terraform.tfvars.example`. Do not commit `*.tfvars` or Terraform state.

## Local AWS debugging

Any debug of AWS services from the **local** environment must use the running container image `rtmsp-terraform:1.15.8` (Compose service `terraform` in `infra/docker-compose.terraform.yml`). Do not call `aws` or `terraform` on the host.

```bash
./infra/tf.sh up
./infra/tf.sh shell
```

Use the AWS CLI and Terraform inside that shell. Credentials come from `.env` (materialized as `/root/.aws/config` and `/root/.aws/credentials` by `infra/docker-entrypoint.sh`). Host `.aws/` is the local shared-config requirement; do not rely on a host-installed AWS CLI.

Inside the shell:

```bash
terraform init
terraform plan -out=tfplan
terraform show tfplan
terraform apply tfplan
```

Apply never re-plans; apply the saved `tfplan`. Then `./infra/tf.sh down` when done.

Useful outputs: `kinesis_stream_name`, `medallion_buckets`, `glue_payments_sdp_job`, `athena_workgroup`, `producer_iam_policy_arn`.

## Repository layout

```text
docs/                      Architecture, IAM policy, demo Athena queries
infra/terraform/           S3, Kinesis, Firehose, Glue crawler + SDP, Athena, IAM, alarms
infra/tf.sh                Docker Terraform workspace (only way to run terraform/aws locally)
pipelines/sdp/payments/    Glue 6.0 Spark Declarative Pipeline (bronze→silver)
src/payments_gateway/      Event contract, Kinesis producer, simulator, FastAPI
sql/athena/                Bronze / silver / gold DDL (gold not deployed by SDP)
tests/                     Gateway + transform unit tests
data/                      Local bronze samples + Hadoop Iceberg warehouse (gitignored)
```

| Path | Role |
|---|---|
| `src/payments_gateway/models.py` | `PaymentEvent` contract — do not invent extra business fields |
| `src/payments_gateway/producer.py` | Batched Kinesis `PutRecords` |
| `src/payments_gateway/simulator.py` | CLI event generator (`payments-simulator`) |
| `src/payments_gateway/api.py` | Thin FastAPI injector — no helper functions that start with `_` |
| `src/payments_gateway/config.py` | Settings from `.env` |
| `pipelines/sdp/payments/transformations/` | SDP views: bronze, silver, rejected, gold stub |
| `infra/terraform/` | All AWS resources; local state on the host bind-mount |

## Payment event contract

Transforms and producers only use this contract. Do not add card brand, country, fees, or FX.

| Field | Type | Notes |
|---|---|---|
| `event_id` | string (UUID) | Dedup key |
| `merchant_id` | string | Kinesis partition key + join key |
| `user_id` | string | Payer identifier |
| `amount_usd` | float `> 0` | Payment amount |
| `currency` | string | ISO-4217, default `USD` |
| `status` | enum | `authorized` / `captured` / `failed` / `refunded` |
| `event_timestamp` | datetime (UTC) | Event time (silver partitions use this, not Firehose delivery hour) |
| `schema_version` | int | Starts at `1` |
| `ingest_source` | string | Always `payment_gateway` |

GMV is **captured** amount only. Authorized, failed, and refunded are not revenue.

## Lake layers

| Layer | Grain | What you get |
|---|---|---|
| Bronze | raw Firehose files | GZIP NDJSON under `payments/raw/year=/month=/day=/hour=` (delivery time) |
| Silver `silver_payments` | one row per event | Iceberg v2; typed; deduped on `event_id` |
| Silver `silver_payments_rejected` | bad bronze rows | Iceberg v2 quarantine (`rejection_reasons`) |
| Gold | star schema | Designed in SQL, not in SDP yet |

Bronze crawler is on-demand and incremental (`CRAWL_NEW_FOLDERS_ONLY`) on `payments/raw/` only. Do not crawl silver — Iceberg metadata is the catalog. If Hive/Parquet tables already exist under the SDP table names, drop them before the job runs. Athena workgroup: `rtmsp-dev-payments`.

## Common commands

```bash
# App
uv sync --group dev
uv run payments-simulator --rate 10 --duration 30
uv run uvicorn payments_gateway.api:app --reload --port 8080

# Quality
uv run pytest -q
uv run ruff check src tests jobs
uv run black src tests pipelines

# After Firehose lands (~60s), catalog bronze then run silver
# (inside the Terraform workspace shell)
aws glue start-crawler --name rtmsp-dev-payments-bronze-raw
aws glue start-job-run --job-name rtmsp-dev-payments-sdp
```

CI (`.github/workflows/ci.yml`) runs pytest, Ruff, and `terraform fmt` / `validate` on every push.

Local Glue job replay (Spark 3.5 / Iceberg 1.7 — not a Glue 6.0 bitwise match) uses `infra/docker-compose.glue.yml` and writes to `data/`. That container does not use AWS credentials.

## Agent conventions

- Read [docs/architecture.md](docs/architecture.md) before changing pipeline or AWS design.
- Match existing naming, layout, and style. Prefer the smallest change that solves the request.
- Reuse existing helpers; put every function that starts with `_` in a business/lib module, never in an API module (`src/payments_gateway/api.py`).
- Format Python with Black (line length 88, Python 3.10) before finishing.
- Respond and comment in English unless asked otherwise.
- Conventional commits (`feat`, `fix`, `chore`, `docs`, `refactor`, `test`). Commit only when asked.
- Do not commit `.env`, `.aws/`, `*.tfstate`, `terraform.tfvars`, or other secrets.
- Do not call `aws` or `terraform` on the host; use `./infra/tf.sh shell`.
- Do not write exploits, attack procedures, or expand IAM beyond what the task needs.
- Gold, Iceberg v3, SCD Type 2, extra sources, and remote Terraform state are later work — see architecture “Later” — unless the user asks for them.
