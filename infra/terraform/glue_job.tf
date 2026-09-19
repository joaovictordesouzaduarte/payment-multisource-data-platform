# ---------------------------------------------------------------------------
# Glue 6.0 Spark Declarative Pipeline (bronze → silver)
# ---------------------------------------------------------------------------

locals {
  pipeline_dir  = "${path.module}/../../pipelines/sdp/payments"
  pipeline_name = "${var.project_name}-${var.environment}-payments-sdp"
  pipeline_yaml = templatefile("${local.pipeline_dir}/spark-pipeline.yml.tftpl", {
    pipeline_name = local.pipeline_name
    catalog       = "glue_catalog"
    database      = local.glue_database_name
    bucket        = local.payments_silver_bucket.bucket
  })
  bronze_py = templatefile("${local.pipeline_dir}/transformations/01_bronze.py.tftpl", {
    bronze_path = "s3://${local.payments_bronze_bucket.bucket}/payments/raw/"
  })
}

resource "aws_s3_object" "pipeline_yaml" {
  bucket  = local.payments_silver_bucket.id
  key     = "glue/pipelines/payments/spark-pipeline.yml"
  content = local.pipeline_yaml
  etag    = md5(local.pipeline_yaml)
}

resource "aws_s3_object" "pipeline_bronze" {
  bucket  = local.payments_silver_bucket.id
  key     = "glue/pipelines/payments/transformations/01_bronze.py"
  content = local.bronze_py
  etag    = md5(local.bronze_py)
}

resource "aws_s3_object" "pipeline_silver" {
  bucket = local.payments_silver_bucket.id
  key    = "glue/pipelines/payments/transformations/02_silver.py"
  source = "${local.pipeline_dir}/transformations/02_silver.py"
  etag   = filemd5("${local.pipeline_dir}/transformations/02_silver.py")
}

resource "aws_glue_job" "payments_sdp" {
  name              = local.pipeline_name
  role_arn          = aws_iam_role.glue_crawler.arn
  description       = "SDP bronze→silver: Firehose JSON to Iceberg silver_payments and quarantine."
  glue_version      = "6.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 20
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${local.payments_silver_bucket.bucket}/glue/pipelines/payments/"
  }

  default_arguments = {
    "--enable-spark-declarative-pipeline" = "true"
    "--datalake-formats"                  = "iceberg"
    "--conf"                              = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--enable-continuous-cloudwatch-log"  = "true"
  }

  depends_on = [
    aws_s3_object.pipeline_yaml,
    aws_s3_object.pipeline_bronze,
    aws_s3_object.pipeline_silver,
    aws_iam_role_policy.glue_crawler,
  ]
}
