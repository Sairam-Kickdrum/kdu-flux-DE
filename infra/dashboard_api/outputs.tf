output "api_invoke_url" {
  value = "https://${aws_api_gateway_rest_api.this.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.this.stage_name}"
}

output "api_gateway_id" {
  value = aws_api_gateway_rest_api.this.id
}

output "redshift_secret_arn" {
  value = var.redshift_secret_arn
}

output "lambda_names" {
  value = {
    kpi             = module.lambda_kpi.function_name
    revenue_daily   = module.lambda_revenue_daily.function_name
    revenue_monthly = module.lambda_revenue_monthly.function_name
    breakdown       = module.lambda_breakdown.function_name
    details         = module.lambda_details.function_name
  }
}

output "lambda_role_names" {
  value = {
    kpi             = module.lambda_kpi.role_name
    revenue_daily   = module.lambda_revenue_daily.role_name
    revenue_monthly = module.lambda_revenue_monthly.role_name
    breakdown       = module.lambda_breakdown.role_name
    details         = module.lambda_details.role_name
  }
}
