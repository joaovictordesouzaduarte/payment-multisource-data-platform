# ---------------------------------------------------------------------------
# Glue Data Catalog + bronze crawler (payments/raw Hive partitions)
# ---------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

locals {
  glue_database_name = replace("${var.project_name}_${var.environment}_payments", "-", "_")
}

resource "aws_glue_catalog_database" "payments" {
  name        = local.glue_database_name
  description = "Payments lake catalog. Bronze crawler writes bronze_raw from Firehose NDJSON."
}

data "aws_iam_policy_document" "glue_crawler_assume" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "glue_crawler" {
  name               = "${var.project_name}-${var.environment}-payments-glue-crawler"
  assume_role_policy = data.aws_iam_policy_document.glue_crawler_assume.json
}

data "aws_iam_policy_document" "glue_crawler" {
  statement {
    sid    = "GlueCatalogCrawlerAndInteractiveSessions"
    effect = "Allow"
    actions = [
      "glue:CreateDatabase",
      "glue:DeleteDatabase",
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:UpdateDatabase",
      "glue:CreateCrawler",
      "glue:DeleteCrawler",
      "glue:GetCrawler",
      "glue:GetCrawlers",
      "glue:ListCrawlers",
      "glue:UpdateCrawler",
      "glue:StartCrawler",
      "glue:StopCrawler",
      "glue:GetCrawlerMetrics",
      "glue:CreateTable",
      "glue:DeleteTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:UpdateTable",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:BatchUpdatePartition",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:DeletePartition",
      "glue:UpdatePartition",
      "glue:BatchGetPartition",
      "glue:GetClassifier",
      "glue:GetClassifiers",
      "glue:TagResource",
      "glue:UntagResource",
      "glue:GetTags",
      "glue:CreateSession",
      "glue:GetSession",
      "glue:ListSessions",
      "glue:DeleteSession",
      "glue:StopSession",
      "glue:RunStatement",
      "glue:GetStatement",
      "glue:ListStatements",
      "glue:CancelStatement",
      "glue:CreateJob",
      "glue:DeleteJob",
      "glue:GetJob",
      "glue:GetJobs",
      "glue:UpdateJob",
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = ["*"]
  }

  statement {
    sid     = "PassSelfToGlue"
    effect  = "Allow"
    actions = ["iam:PassRole"]
    resources = [
      "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-${var.environment}-payments-glue-crawler",
    ]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["glue.amazonaws.com"]
    }
  }

  statement {
    sid    = "ListMedallion"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      local.payments_bronze_bucket.arn,
      local.payments_silver_bucket.arn,
      local.payments_gold_bucket.arn,
    ]
  }

  statement {
    sid     = "ReadBronzeRaw"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${local.payments_bronze_bucket.arn}/payments/raw/*",
    ]
  }

  statement {
    sid    = "ReadWriteSilverGold"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [
      "${local.payments_silver_bucket.arn}/*",
      "${local.payments_gold_bucket.arn}/*",
    ]
  }

  statement {
    sid    = "GlueRuntimeLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/crawlers",
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/crawlers:log-stream:*",
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/sessions",
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/sessions:log-stream:*",
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/jobs/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_crawler" {
  name   = "${var.project_name}-${var.environment}-payments-glue-crawler"
  role   = aws_iam_role.glue_crawler.id
  policy = data.aws_iam_policy_document.glue_crawler.json
}

resource "aws_glue_crawler" "payments_bronze_raw" {
  name          = "${var.project_name}-${var.environment}-payments-bronze-raw"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.payments.name
  table_prefix  = "bronze_"
  description   = "Infer bronze_raw from Firehose GZIP NDJSON under payments/raw/. Incremental after the first run."

  recrawl_policy {
    recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY"
  }

  schema_change_policy {
    update_behavior = "LOG"
    delete_behavior = "LOG"
  }

  s3_target {
    path = "s3://${local.payments_bronze_bucket.bucket}/payments/raw/"
  }

  # Table level 3 = s3://bucket/payments/raw (Hive year/month/day/hour stay partitions).
  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy     = "CombineCompatibleSchemas"
      TableLevelConfiguration = 3
    }
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
      Tables = {
        TableThreshold = 1
      }
    }
  })

  schedule = var.glue_crawler_schedule != "" ? var.glue_crawler_schedule : null

  depends_on = [aws_iam_role_policy.glue_crawler]
}
