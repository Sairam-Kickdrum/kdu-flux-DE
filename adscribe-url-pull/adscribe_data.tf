data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

data "aws_dynamodb_table" "adscribe" {
  name = var.adscribe_dynamodb_table_name
}

data "aws_s3_bucket" "adscribe" {
  bucket = var.adscribe_bucket_name
}
