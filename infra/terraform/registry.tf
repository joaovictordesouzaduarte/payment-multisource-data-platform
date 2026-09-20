resource "aws_glue_registry" "registry" {
  registry_name = "${var.project_name}-${var.environment}-registry"
  description   = "Schema registry for ${var.project_name} ${var.environment} payment events."

  tags = {
    Name = "sdp-registry"
  }
}
