resource "aws_kinesis_stream" "payments_events" {
  name             = "${var.project_name}-${var.environment}-payments-events"
  retention_period = var.kinesis_retention_hours

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "IteratorAgeMilliseconds",
  ]
}
