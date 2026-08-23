# ---------------------------------------------------------------------------
# Firehose delivery role — read Kinesis, write S3, write CloudWatch Logs
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "firehose_assume" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${var.project_name}-${var.environment}-payments-firehose"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
}

data "aws_iam_policy_document" "firehose" {
  statement {
    sid    = "ReadKinesis"
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
      "kinesis:SubscribeToShard",
    ]
    resources = [aws_kinesis_stream.payments_events.arn]
  }

  statement {
    sid    = "WriteS3"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      local.payments_bronze_bucket.arn,
      "${local.payments_bronze_bucket.arn}/*",
    ]
  }

  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:PutLogEvents",
      "logs:CreateLogStream",
    ]
    resources = [
      "${aws_cloudwatch_log_group.firehose.arn}:*",
    ]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "${var.project_name}-${var.environment}-payments-firehose"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

# ---------------------------------------------------------------------------
# Producer IAM user/policy — PutRecords only (local simulator / API)
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "producer" {
  statement {
    sid    = "PutPaymentsEvents"
    effect = "Allow"
    actions = [
      "kinesis:PutRecord",
      "kinesis:PutRecords",
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.payments_events.arn]
  }
}

resource "aws_iam_policy" "producer" {
  name        = "${var.project_name}-${var.environment}-payments-producer"
  description = "Least-privilege policy for the payments gateway producer."
  policy      = data.aws_iam_policy_document.producer.json
}
