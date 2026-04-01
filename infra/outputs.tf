output "bucket_name" {
  value = module.bucket.bucket_name
}

output "bucket_arn" {
  value = module.bucket.bucket_arn
}

output "dynamodb_table_name" {
  value = module.dynamodb.table_name
}

output "dynamodb_table_arn" {
  value = module.dynamodb.table_arn
}
