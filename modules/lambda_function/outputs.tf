output "function_name" {
  description = "Lambda function name."
  value       = aws_lambda_function.this.function_name
}

output "function_arn" {
  description = "Lambda function ARN."
  value       = aws_lambda_function.this.arn
}

output "role_arn" {
  description = "IAM role ARN used by the Lambda function."
  value       = aws_iam_role.this.arn
}
