# ---------------------------------------------------------------------------
# Glue batch jobs: bronze → silver → gold (scripts + payments_lake zip on S3)
# ---------------------------------------------------------------------------

data "archive_file" "payments_src" {
  type        = "zip"
  source_dir  = "${path.module}/../../src"
  output_path = "${path.module}/build/payments_src.zip"
  excludes    = ["**/__pycache__", "**/*.pyc"]
}

resource "aws_s3_object" "payments_src_zip" {
  bucket = local.payments_silver_bucket.id
  key    = "glue/lib/payments_src.zip"
  source = data.archive_file.payments_src.output_path
  etag   = data.archive_file.payments_src.output_md5
}

resource "aws_s3_object" "glue_bronze_to_silver" {
  bucket = local.payments_silver_bucket.id
  key    = "glue/scripts/bronze_to_silver.py"
  source = "${path.module}/../../jobs/glue/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../../jobs/glue/bronze_to_silver.py")
}

resource "aws_s3_object" "glue_silver_to_gold" {
  bucket = local.payments_silver_bucket.id
  key    = "glue/scripts/silver_to_gold.py"
  source = "${path.module}/../../jobs/glue/silver_to_gold.py"
  etag   = filemd5("${path.module}/../../jobs/glue/silver_to_gold.py")
}

resource "aws_glue_job" "bronze_to_silver" {
  name              = "${var.project_name}-${var.environment}-payments-bronze-to-silver"
  role_arn          = aws_iam_role.glue_crawler.arn
  description       = "Type, validate, and re-partition bronze payment events by event time."
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.payments_silver_bucket.bucket}/glue/scripts/bronze_to_silver.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--extra-py-files"                   = "s3://${local.payments_silver_bucket.bucket}/glue/lib/payments_src.zip"
    "--BRONZE_PATH"                      = "s3://${local.payments_bronze_bucket.bucket}/payments/raw/"
    "--SILVER_PATH"                      = "s3://${local.payments_silver_bucket.bucket}/payments/payment_events/"
    "--REJECTED_PATH"                    = "s3://${local.payments_silver_bucket.bucket}/payments/payment_events_rejected/"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  depends_on = [
    aws_s3_object.glue_bronze_to_silver,
    aws_s3_object.payments_src_zip,
    aws_iam_role_policy.glue_crawler,
  ]
}

resource "aws_glue_job" "silver_to_gold" {
  name              = "${var.project_name}-${var.environment}-payments-silver-to-gold"
  role_arn          = aws_iam_role.glue_crawler.arn
  description       = "Aggregate silver payment events into merchant daily and status daily marts."
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.payments_silver_bucket.bucket}/glue/scripts/silver_to_gold.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--extra-py-files"                   = "s3://${local.payments_silver_bucket.bucket}/glue/lib/payments_src.zip"
    "--SILVER_PATH"                      = "s3://${local.payments_silver_bucket.bucket}/payments/payment_events/"
    "--MERCHANT_DAILY_PATH"              = "s3://${local.payments_gold_bucket.bucket}/payments/merchant_daily_metrics/"
    "--STATUS_DAILY_PATH"                = "s3://${local.payments_gold_bucket.bucket}/payments/payment_status_daily/"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  depends_on = [
    aws_s3_object.glue_silver_to_gold,
    aws_s3_object.payments_src_zip,
    aws_iam_role_policy.glue_crawler,
  ]
}
