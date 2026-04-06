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

output "redshift_s3_access_role_arn" {
  value = aws_iam_role.redshift_s3_access.arn
}

output "glue_stage_load_job_name" {
  value = module.glue_stage_load_job.job_name
}

output "glue_final_promote_job_name" {
  value = module.glue_final_promote_job.job_name
}

output "dashboard_api_invoke_url" {
  value = module.dashboard_api.api_invoke_url
}

output "dashboard_api_gateway_id" {
  value = module.dashboard_api.api_gateway_id
}

output "dashboard_api_lambda_function_names" {
  value = module.dashboard_api.lambda_function_names
}

output "dashboard_api_lambda_role_names" {
  value = module.dashboard_api.lambda_role_names
}

output "dashboard_api_redshift_secret_arn_effective" {
  value = local.dashboard_api_effective_secret_arn
}
