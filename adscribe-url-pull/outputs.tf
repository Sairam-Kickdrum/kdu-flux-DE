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
