locals {
  name_prefix        = "${var.project_name}-${var.environment}-analytics"
  api_deploy_version = "v2"
  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Service     = "dashboard-analytics-api"
    },
    var.tags
  )

  lambda_environment = {
    REDSHIFT_WORKGROUP_NAME = var.redshift_workgroup_name
    REDSHIFT_DATABASE       = var.redshift_database
    REDSHIFT_SECRET_ARN     = var.redshift_secret_arn
    REDSHIFT_FACT_TABLE     = var.redshift_fact_table
  }
}

resource "aws_security_group" "lambda" {
  count = var.create_lambda_security_group ? 1 : 0

  name        = "${local.name_prefix}-lambda-sg"
  description = "Security group for analytics lambdas"
  vpc_id      = var.lambda_security_group_vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

locals {
  effective_lambda_sg_ids = concat(
    var.lambda_security_group_ids,
    var.create_lambda_security_group ? [aws_security_group.lambda[0].id] : []
  )
}

module "lambda_kpi" {
  source = "./modules/lambda_endpoint"

  function_name         = "${local.name_prefix}-kpi"
  description           = "Returns total revenue and total orders"
  handler               = "handler.lambda_handler"
  lambda_zip_file       = "${path.root}/lambda_dist/kpi.zip"
  timeout               = var.lambda_timeout_seconds
  memory_size           = var.lambda_memory_size
  log_retention_days    = var.lambda_log_retention_days
  environment_variables = local.lambda_environment
  secret_arn            = var.redshift_secret_arn
  attach_vpc            = var.attach_lambda_to_vpc
  subnet_ids            = var.lambda_subnet_ids
  security_group_ids    = local.effective_lambda_sg_ids
  tags                  = local.common_tags
}

module "lambda_revenue_daily" {
  source = "./modules/lambda_endpoint"

  function_name         = "${local.name_prefix}-revenue-daily"
  description           = "Returns revenue trend by day"
  handler               = "handler.lambda_handler"
  lambda_zip_file       = "${path.root}/lambda_dist/revenue_daily.zip"
  timeout               = var.lambda_timeout_seconds
  memory_size           = var.lambda_memory_size
  log_retention_days    = var.lambda_log_retention_days
  environment_variables = local.lambda_environment
  secret_arn            = var.redshift_secret_arn
  attach_vpc            = var.attach_lambda_to_vpc
  subnet_ids            = var.lambda_subnet_ids
  security_group_ids    = local.effective_lambda_sg_ids
  tags                  = local.common_tags
}

module "lambda_revenue_monthly" {
  source = "./modules/lambda_endpoint"

  function_name         = "${local.name_prefix}-revenue-monthly"
  description           = "Returns revenue trend by month"
  handler               = "handler.lambda_handler"
  lambda_zip_file       = "${path.root}/lambda_dist/revenue_monthly.zip"
  timeout               = var.lambda_timeout_seconds
  memory_size           = var.lambda_memory_size
  log_retention_days    = var.lambda_log_retention_days
  environment_variables = local.lambda_environment
  secret_arn            = var.redshift_secret_arn
  attach_vpc            = var.attach_lambda_to_vpc
  subnet_ids            = var.lambda_subnet_ids
  security_group_ids    = local.effective_lambda_sg_ids
  tags                  = local.common_tags
}

module "lambda_breakdown" {
  source = "./modules/lambda_endpoint"

  function_name         = "${local.name_prefix}-breakdown"
  description           = "Returns breakdown by discount_code/show/product"
  handler               = "handler.lambda_handler"
  lambda_zip_file       = "${path.root}/lambda_dist/breakdown.zip"
  timeout               = var.lambda_timeout_seconds
  memory_size           = var.lambda_memory_size
  log_retention_days    = var.lambda_log_retention_days
  environment_variables = local.lambda_environment
  secret_arn            = var.redshift_secret_arn
  attach_vpc            = var.attach_lambda_to_vpc
  subnet_ids            = var.lambda_subnet_ids
  security_group_ids    = local.effective_lambda_sg_ids
  tags                  = local.common_tags
}

module "lambda_details" {
  source = "./modules/lambda_endpoint"

  function_name         = "${local.name_prefix}-details"
  description           = "Returns paginated details table rows"
  handler               = "handler.lambda_handler"
  lambda_zip_file       = "${path.root}/lambda_dist/details.zip"
  timeout               = var.lambda_timeout_seconds
  memory_size           = var.lambda_memory_size
  log_retention_days    = var.lambda_log_retention_days
  environment_variables = local.lambda_environment
  secret_arn            = var.redshift_secret_arn
  attach_vpc            = var.attach_lambda_to_vpc
  subnet_ids            = var.lambda_subnet_ids
  security_group_ids    = local.effective_lambda_sg_ids
  tags                  = local.common_tags
}

resource "aws_api_gateway_rest_api" "this" {
  name        = "${local.name_prefix}-api"
  description = "Analytics API for dashboard widgets"

  endpoint_configuration {
    types = ["REGIONAL"]
  }

  tags = local.common_tags
}

resource "aws_api_gateway_resource" "analytics" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_rest_api.this.root_resource_id
  path_part   = "analytics"
}

resource "aws_api_gateway_resource" "kpi" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.analytics.id
  path_part   = "kpi"
}

resource "aws_api_gateway_resource" "revenue" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.analytics.id
  path_part   = "revenue"
}

resource "aws_api_gateway_resource" "revenue_daily" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.revenue.id
  path_part   = "daily"
}

resource "aws_api_gateway_resource" "revenue_monthly" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.revenue.id
  path_part   = "monthly"
}

resource "aws_api_gateway_resource" "breakdown" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.analytics.id
  path_part   = "breakdown"
}

resource "aws_api_gateway_resource" "details" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  parent_id   = aws_api_gateway_resource.analytics.id
  path_part   = "details"
}

resource "aws_api_gateway_request_validator" "query_validator" {
  rest_api_id                 = aws_api_gateway_rest_api.this.id
  name                        = "${local.name_prefix}-query-validator"
  validate_request_body       = false
  validate_request_parameters = true
}

locals {
  endpoint_configs = {
    kpi = {
      resource_id  = aws_api_gateway_resource.kpi.id
      invoke_arn   = module.lambda_kpi.invoke_arn
      function_arn = module.lambda_kpi.function_arn
      request_parameters = {
        "method.request.querystring.client"     = false
        "method.request.querystring.start_date" = false
        "method.request.querystring.end_date"   = false
      }
    }
    revenue_daily = {
      resource_id  = aws_api_gateway_resource.revenue_daily.id
      invoke_arn   = module.lambda_revenue_daily.invoke_arn
      function_arn = module.lambda_revenue_daily.function_arn
      request_parameters = {
        "method.request.querystring.client"     = false
        "method.request.querystring.start_date" = false
        "method.request.querystring.end_date"   = false
      }
    }
    revenue_monthly = {
      resource_id  = aws_api_gateway_resource.revenue_monthly.id
      invoke_arn   = module.lambda_revenue_monthly.invoke_arn
      function_arn = module.lambda_revenue_monthly.function_arn
      request_parameters = {
        "method.request.querystring.client"     = false
        "method.request.querystring.start_date" = false
        "method.request.querystring.end_date"   = false
      }
    }
    breakdown = {
      resource_id  = aws_api_gateway_resource.breakdown.id
      invoke_arn   = module.lambda_breakdown.invoke_arn
      function_arn = module.lambda_breakdown.function_arn
      request_parameters = {
        "method.request.querystring.client"     = false
        "method.request.querystring.start_date" = false
        "method.request.querystring.end_date"   = false
        "method.request.querystring.group_by"   = false
      }
    }
    details = {
      resource_id  = aws_api_gateway_resource.details.id
      invoke_arn   = module.lambda_details.invoke_arn
      function_arn = module.lambda_details.function_arn
      request_parameters = {
        "method.request.querystring.client"     = false
        "method.request.querystring.start_date" = false
        "method.request.querystring.end_date"   = false
        "method.request.querystring.limit"      = false
        "method.request.querystring.offset"     = false
      }
    }
  }
}

resource "aws_api_gateway_method" "get" {
  for_each = local.endpoint_configs

  rest_api_id          = aws_api_gateway_rest_api.this.id
  resource_id          = each.value.resource_id
  http_method          = "GET"
  authorization        = "NONE"
  request_validator_id = aws_api_gateway_request_validator.query_validator.id
  request_parameters   = each.value.request_parameters
}

resource "aws_api_gateway_integration" "get" {
  for_each = local.endpoint_configs

  rest_api_id             = aws_api_gateway_rest_api.this.id
  resource_id             = each.value.resource_id
  http_method             = aws_api_gateway_method.get[each.key].http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = each.value.invoke_arn
}

resource "aws_lambda_permission" "apigw_invoke" {
  for_each = local.endpoint_configs

  statement_id  = "AllowApiGatewayInvoke-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = each.value.function_arn
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this.execution_arn}/*/GET/*"
}

resource "aws_api_gateway_method" "options" {
  for_each = local.endpoint_configs

  rest_api_id   = aws_api_gateway_rest_api.this.id
  resource_id   = each.value.resource_id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "options" {
  for_each = local.endpoint_configs

  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = each.value.resource_id
  http_method = aws_api_gateway_method.options[each.key].http_method
  type        = "MOCK"

  request_templates = {
    "application/json" = "{\"statusCode\": 200}"
  }
}

resource "aws_api_gateway_method_response" "options" {
  for_each = local.endpoint_configs

  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = each.value.resource_id
  http_method = aws_api_gateway_method.options[each.key].http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true
    "method.response.header.Access-Control-Allow-Methods" = true
    "method.response.header.Access-Control-Allow-Origin"  = true
  }
}

resource "aws_api_gateway_integration_response" "options" {
  for_each = local.endpoint_configs

  rest_api_id = aws_api_gateway_rest_api.this.id
  resource_id = each.value.resource_id
  http_method = aws_api_gateway_method.options[each.key].http_method
  status_code = aws_api_gateway_method_response.options[each.key].status_code

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'*'"
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
    "method.response.header.Access-Control-Allow-Origin"  = "'*'"
  }
}

resource "aws_api_gateway_gateway_response" "default_4xx" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  response_type = "DEFAULT_4XX"

  response_parameters = {
    "gatewayresponse.header.Access-Control-Allow-Origin"  = "'*'"
    "gatewayresponse.header.Access-Control-Allow-Headers" = "'*'"
    "gatewayresponse.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
  }
}

resource "aws_api_gateway_gateway_response" "default_5xx" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  response_type = "DEFAULT_5XX"

  response_parameters = {
    "gatewayresponse.header.Access-Control-Allow-Origin"  = "'*'"
    "gatewayresponse.header.Access-Control-Allow-Headers" = "'*'"
    "gatewayresponse.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
  }
}

resource "aws_api_gateway_deployment" "this" {
  rest_api_id = aws_api_gateway_rest_api.this.id

  depends_on = [
    aws_api_gateway_integration.get,
    aws_api_gateway_integration.options,
    aws_api_gateway_integration_response.options
  ]

  triggers = {
    redeploy = sha1(jsonencode({
      endpoints = local.endpoint_configs
      version   = local.api_deploy_version
    }))
  }
}

resource "aws_api_gateway_stage" "this" {
  rest_api_id   = aws_api_gateway_rest_api.this.id
  deployment_id = aws_api_gateway_deployment.this.id
  stage_name    = var.api_stage_name
  tags          = local.common_tags
}

resource "aws_api_gateway_method_settings" "all" {
  rest_api_id = aws_api_gateway_rest_api.this.id
  stage_name  = aws_api_gateway_stage.this.stage_name
  method_path = "*/*"

  settings {
    metrics_enabled        = true
    logging_level          = "INFO"
    throttling_rate_limit  = var.api_throttling_rate_limit
    throttling_burst_limit = var.api_throttling_burst_limit
  }
}
