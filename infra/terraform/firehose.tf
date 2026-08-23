resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${var.project_name}-${var.environment}-payments-to-s3"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_stream" "firehose_s3_delivery" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

resource "aws_kinesis_firehose_delivery_stream" "payments_to_s3" {
  name        = "${var.project_name}-${var.environment}-payments-to-s3"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.payments_events.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = local.payments_bronze_bucket.arn

    # Hive-style partitions via Firehose delivery timestamp
    prefix              = "payments/raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "payments/errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"

    buffering_size     = var.firehose_buffer_size_mb
    buffering_interval = var.firehose_buffer_interval_seconds
    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_s3_delivery.name
    }
  }

  depends_on = [
    aws_iam_role_policy.firehose,
    aws_cloudwatch_log_stream.firehose_s3_delivery,
  ]
}
