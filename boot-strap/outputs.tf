output "tf_state_bucket_name" {
  description = "S3 bucket name for Terraform remote state"
  value       = aws_s3_bucket.tf_state.bucket
}

output "tf_state_bucket_region" {
  description = "Region of the S3 bucket"
  value       = var.aws_region
}

output "tf_lock_table_name" {
  description = "DynamoDB table name for Terraform state locking"
  value       = aws_dynamodb_table.tf_locks.name
}