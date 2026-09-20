resource "aws_glue_schema" "events" {
  schema_name       = "${var.project_name}-${var.environment}-events"
  registry_arn      = aws_glue_registry.registry.arn
  data_format       = "JSON"
  compatibility     = "BACKWARD_ALL"
  description       = "Payment gateway event contract for bronze JSON landing."
  schema_definition = file("${path.module}/schemas/payment_event.json")
}
