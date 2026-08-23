# Preserve existing bronze bucket when upgrading from the single-bucket layout.
# Safe no-ops on a fresh apply (source addresses absent from state).

moved {
  from = aws_s3_bucket.payments_bronze
  to   = aws_s3_bucket.medallion["bronze"]
}

moved {
  from = aws_s3_bucket_server_side_encryption_configuration.payments_bronze
  to   = aws_s3_bucket_server_side_encryption_configuration.medallion["bronze"]
}

moved {
  from = aws_s3_bucket_public_access_block.payments_bronze
  to   = aws_s3_bucket_public_access_block.medallion["bronze"]
}
