resource "aws_sns_topic" "payments_alarms" {
  count = var.enable_alarms ? 1 : 0

  name = "${var.project_name}-${var.environment}-payments-alarms"
}

resource "aws_sns_topic_subscription" "payments_alarms_email" {
  count = var.enable_alarms && var.alarm_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.payments_alarms[0].arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

locals {
  alarm_actions = var.enable_alarms ? [aws_sns_topic.payments_alarms[0].arn] : []
}

resource "aws_cloudwatch_metric_alarm" "firehose_delivery_errors" {
  count = var.enable_alarms ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-firehose-delivery-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "DeliveryToS3.DataFreshness"
  namespace           = "AWS/Firehose"
  period              = 300
  statistic           = "Maximum"
  threshold           = 900
  alarm_description   = "Firehose delivery lag exceeds 15 minutes for payments bronze landing."
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions

  dimensions = {
    DeliveryStreamName = aws_kinesis_firehose_delivery_stream.payments_to_s3.name
  }
}

resource "aws_cloudwatch_metric_alarm" "firehose_throttle" {
  count = var.enable_alarms ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-firehose-throttled"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ThrottledRecords"
  namespace           = "AWS/Firehose"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Firehose is throttling records on the payments delivery stream."
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  dimensions = {
    DeliveryStreamName = aws_kinesis_firehose_delivery_stream.payments_to_s3.name
  }
}

resource "aws_cloudwatch_metric_alarm" "kinesis_write_throttled" {
  count = var.enable_alarms ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-kinesis-write-throttled"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "WriteProvisionedThroughputExceeded"
  namespace           = "AWS/Kinesis"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Payments Kinesis stream is rejecting writes due to throughput limits."
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  dimensions = {
    StreamName = aws_kinesis_stream.payments_events.name
  }
}

resource "aws_cloudwatch_metric_alarm" "kinesis_iterator_age" {
  count = var.enable_alarms ? 1 : 0

  alarm_name          = "${var.project_name}-${var.environment}-kinesis-iterator-age"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 60
  statistic           = "Maximum"
  threshold           = 60000
  alarm_description   = "Kinesis consumer iterator age exceeds 60s for downstream consumers."
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  dimensions = {
    StreamName = aws_kinesis_stream.payments_events.name
  }
}
