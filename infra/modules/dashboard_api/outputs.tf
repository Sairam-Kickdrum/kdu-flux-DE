output "enabled" {
  value = var.enabled
}

output "api_invoke_url" {
  value = var.enabled ? "https://${aws_api_gateway_rest_api.this[0].id}.execute-api.${data.aws_region.current.name}.amazonaws.com/${aws_api_gateway_stage.this[0].stage_name}" : ""
}

output "api_gateway_id" {
  value = var.enabled ? aws_api_gateway_rest_api.this[0].id : ""
}

output "lambda_function_names" {
  value = var.enabled ? { for k, v in aws_lambda_function.this : k => v.function_name } : {}
}

output "lambda_role_names" {
  value = var.enabled ? { for k, v in aws_iam_role.lambda : k => v.name } : {}
}
