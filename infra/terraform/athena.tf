resource "aws_athena_workgroup" "payments" {
  name = "${var.project_name}-${var.environment}-payments"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${local.payments_gold_bucket.bucket}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
