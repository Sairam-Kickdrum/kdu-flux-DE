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

output "sqs_queue_name" {
  value = module.sqs.queue_name
}

output "sqs_queue_arn" {
  value = module.sqs.queue_arn
}

output "sqs_queue_url" {
  value = module.sqs.queue_url
}

output "lambda_function_name" {
  value = module.lambda.function_name
}

output "lambda_function_arn" {
  value = module.lambda.function_arn
}

output "glue_job_name" {
  value = module.glue_job.job_name
}

output "glue_job_arn" {
  value = module.glue_job.job_arn
}

output "step_function_name" {
  value = module.step_function.state_machine_name
}

output "step_function_arn" {
  value = module.step_function.state_machine_arn
}

output "pipeline_config_s3_uri" {
  value = "s3://${module.bucket.bucket_name}/${aws_s3_object.pipeline_config.key}"
}
