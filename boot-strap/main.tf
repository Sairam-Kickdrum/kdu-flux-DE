

locals {
  bucket_name = "${var.creator}-${var.bucket_name_prefix}"

  table_name = "${var.creator}-${var.dynamodb_table_name}"

  common_tags = {
    Name        = "${var.creator}-DE"
    Creator     = var.creator
    Purpose     = var.purpose
  }
}


resource "aws_s3_bucket" "tf_state" {
  bucket        = local.bucket_name
  force_destroy = false

  tags = local.common_tags

}

resource "aws_s3_bucket_ownership_controls" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}


resource "aws_s3_bucket_public_access_block" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  rule {
    id     = "state-noncurrent-version-retention"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }

  depends_on = [aws_s3_bucket_versioning.tf_state]
}


resource "aws_dynamodb_table" "tf_locks" {
  name         = local.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = local.common_tags
}