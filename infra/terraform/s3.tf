resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  # Medallion layers for Glue / Athena analytics.
  # Tag values must use only S3-safe characters: letters, numbers, spaces
  # and + - = . _ : / @  (parentheses are rejected with InvalidTag).
  medallion_layers = {
    bronze = { purpose = "raw-landing-firehose" }
    silver = { purpose = "cleansed-conformed-glue" }
    gold   = { purpose = "business-marts-athena" }
  }
}

resource "aws_s3_bucket" "medallion" {
  for_each = local.medallion_layers

  bucket = "${var.project_name}-${var.environment}-payments-${each.key}-${random_id.bucket_suffix.hex}"

  tags = {
    Layer   = each.key
    Purpose = each.value.purpose
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "medallion" {
  for_each = local.medallion_layers

  bucket = aws_s3_bucket.medallion[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = false
  }
}

resource "aws_s3_bucket_public_access_block" "medallion" {
  for_each = local.medallion_layers

  bucket = aws_s3_bucket.medallion[each.key].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Convenience aliases so existing Firehose / IAM references stay readable
locals {
  payments_bronze_bucket = aws_s3_bucket.medallion["bronze"]
  payments_silver_bucket = aws_s3_bucket.medallion["silver"]
  payments_gold_bucket   = aws_s3_bucket.medallion["gold"]
}
