output "lambda_function_name" {
  description = "Deployed Lambda function name."
  value       = module.adscribe_url_pull_lambda.function_name
}

output "lambda_function_arn" {
  description = "Deployed Lambda function ARN."
  value       = module.adscribe_url_pull_lambda.function_arn
}

output "lambda_role_arn" {
  description = "IAM role ARN attached to the Lambda function."
  value       = module.adscribe_url_pull_lambda.role_arn
}

output "adscribe_step_function_arn" {
  description = "ARN of the Adscribe raw landing Step Functions state machine."
  value       = aws_sfn_state_machine.adscribe_raw_landing.arn
}

output "adscribe_glue_job_name" {
  description = "Name of the Adscribe raw landing Glue job."
  value       = aws_glue_job.adscribe_raw_landing.name
}
