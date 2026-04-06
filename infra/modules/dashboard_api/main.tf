locals {
  create = var.enabled

  name_prefix = "${var.project_name}-${var.environment}-analytics"

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
    REDSHIFT_HOST           = var.redshift_host
    REDSHIFT_WORKGROUP_NAME = split(".", var.redshift_host)[0]
    REDSHIFT_PORT           = tostring(var.redshift_port)
    REDSHIFT_DATABASE       = var.redshift_database
    REDSHIFT_SECRET_ARN     = var.redshift_secret_arn
    CLIENT_UPLOAD_TABLE     = var.client_upload_table
    ADSCRIBE_TABLE          = var.ascribe_table
    DEBUG_DB_IDENTITY       = tostring(var.dashboard_api_debug_db_identity)
  }

  endpoints = {
    kpi = {
      path_parts  = ["analytics", "kpi"]
      description = "Returns KPI totals"
      zip_file    = var.lambda_kpi_zip
      request_parameters = {
        "method.request.querystring.domain"     = false
        "method.request.querystring.client"     = false
        "method.request.querystring.start_date" = false
        "method.request.querystring.end_date"   = false
      }
    }
    revenue_daily = {
      path_parts  = ["analytics", "revenue", "daily"]
      description = "Returns revenue by day"
      zip_file    = var.lambda_revenue_daily_zip
      request_parameters = {
        "method.request.querystring.domain"     = false
        "method.request.querystring.client"     = false
        "method.request.querystring.start_date" = false
        "method.request.querystring.end_date"   = false
      }
    }
    revenue_monthly = {
      path_parts  = ["analytics", "revenue", "monthly"]
      description = "Returns revenue by month"
      zip_file    = var.lambda_revenue_monthly_zip
      request_parameters = {
        "method.request.querystring.domain"     = false
        "method.request.querystring.client"     = false
        "method.request.querystring.start_date" = false
        "method.request.querystring.end_date"   = false
      }
    }
    breakdown = {
      path_parts  = ["analytics", "breakdown"]
      description = "Returns breakdown by dimension"
      zip_file    = var.lambda_breakdown_zip
      request_parameters = {
        "method.request.querystring.domain"     = false
        "method.request.querystring.client"     = false
        "method.request.querystring.start_date" = false
        "method.request.querystring.end_date"   = false
        "method.request.querystring.dimension"  = false
        "method.request.querystring.top_n"      = false
      }
    }
    details = {
      path_parts  = ["analytics", "details"]
      description = "Returns detail rows"
      zip_file    = var.lambda_details_zip
      request_parameters = {
        "method.request.querystring.domain"        = false
        "method.request.querystring.client"        = false
        "method.request.querystring.start_date"    = false
        "method.request.querystring.end_date"      = false
        "method.request.querystring.client_name"   = false
        "method.request.querystring.show_name"     = false
        "method.request.querystring.discount_code" = false
        "method.request.querystring.limit"         = false
        "method.request.querystring.offset"        = false
        "method.request.querystring.sort_by"       = false
        "method.request.querystring.sort_order"    = false
      }
    }
  }
}

data "aws_region" "current" {}

resource "aws_security_group" "lambda" {
  count = local.create && var.create_lambda_security_group ? 1 : 0

  name        = "${local.name_prefix}-lambda-sg"
  description = "Security group for dashboard analytics lambdas"
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
  lambda_sg_ids = concat(
    var.lambda_security_group_ids,
    local.create && var.create_lambda_security_group ? [aws_security_group.lambda[0].id] : []
  )
}

resource "aws_iam_role" "lambda" {
  for_each = local.create ? local.endpoints : {}

  name = "${local.name_prefix}-${replace(each.key, "_", "-")}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "lambda" {
  for_each = local.create ? local.endpoints : {}

  name = "${local.name_prefix}-${replace(each.key, "_", "-")}-policy"
  role = aws_iam_role.lambda[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Effect = "Allow"
          Action = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ]
          Resource = "*"
        },
        {
          Effect = "Allow"
          Action = [
            "redshift-data:ExecuteStatement",
            "redshift-data:DescribeStatement",
            "redshift-data:GetStatementResult"
          ]
          Resource = "*"
        },
        {
          Effect = "Allow"
          Action = [
            "redshift-serverless:GetCredentials"
          ]
          Resource = "*"
        },
      ],
      trimspace(var.redshift_secret_arn) != "" ? [
        {
          Effect = "Allow"
          Action = [
            "secretsmanager:GetSecretValue"
          ]
          Resource = var.redshift_secret_arn
        }
      ] : [],
      var.attach_lambda_to_vpc ? [
        {
          Effect = "Allow"
          Action = [
            "ec2:CreateNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DeleteNetworkInterface",
            "ec2:AssignPrivateIpAddresses",
            "ec2:UnassignPrivateIpAddresses"
          ]
          Resource = "*"
        }
      ] : []
    )
  })
}

resource "aws_cloudwatch_log_group" "lambda" {
  for_each = local.create ? local.endpoints : {}

  name              = "/aws/lambda/${local.name_prefix}-${replace(each.key, "_", "-")}"
  retention_in_days = var.lambda_log_retention_days

  tags = local.common_tags
}

resource "aws_lambda_function" "this" {
  for_each = local.create ? local.endpoints : {}

  function_name    = "${local.name_prefix}-${replace(each.key, "_", "-")}"
  role             = aws_iam_role.lambda[each.key].arn
  runtime          = "python3.11"
  handler          = "handler.lambda_handler"
  filename         = each.value.zip_file
  source_code_hash = filebase64sha256(each.value.zip_file)
  timeout          = var.lambda_timeout_seconds
  memory_size      = var.lambda_memory_size

  environment {
    variables = local.lambda_environment
  }

  dynamic "vpc_config" {
    for_each = var.attach_lambda_to_vpc ? [1] : []
    content {
      subnet_ids         = var.lambda_subnet_ids
      security_group_ids = local.lambda_sg_ids
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda]

  tags = local.common_tags
}

resource "aws_api_gateway_rest_api" "this" {
  count = local.create ? 1 : 0

  name = "${local.name_prefix}-api"

  endpoint_configuration {
    types = ["REGIONAL"]
  }

  tags = local.common_tags
}

resource "aws_api_gateway_resource" "analytics" {
  count = local.create ? 1 : 0

  rest_api_id = aws_api_gateway_rest_api.this[0].id
  parent_id   = aws_api_gateway_rest_api.this[0].root_resource_id
  path_part   = "analytics"
}

resource "aws_api_gateway_resource" "revenue" {
  count = local.create ? 1 : 0

  rest_api_id = aws_api_gateway_rest_api.this[0].id
  parent_id   = aws_api_gateway_resource.analytics[0].id
  path_part   = "revenue"
}

resource "aws_api_gateway_resource" "leaf" {
  for_each = local.create ? {
    kpi             = { parent = "analytics", path_part = "kpi" }
    breakdown       = { parent = "analytics", path_part = "breakdown" }
    details         = { parent = "analytics", path_part = "details" }
    revenue_daily   = { parent = "revenue", path_part = "daily" }
    revenue_monthly = { parent = "revenue", path_part = "monthly" }
  } : {}

  rest_api_id = aws_api_gateway_rest_api.this[0].id
  parent_id   = each.value.parent == "analytics" ? aws_api_gateway_resource.analytics[0].id : aws_api_gateway_resource.revenue[0].id
  path_part   = each.value.path_part
}

resource "aws_api_gateway_request_validator" "query" {
  count = local.create ? 1 : 0

  rest_api_id                 = aws_api_gateway_rest_api.this[0].id
  name                        = "${local.name_prefix}-query-validator"
  validate_request_body       = false
  validate_request_parameters = true
}

resource "aws_api_gateway_method" "get" {
  for_each = local.create ? local.endpoints : {}

  rest_api_id          = aws_api_gateway_rest_api.this[0].id
  resource_id          = aws_api_gateway_resource.leaf[each.key].id
  http_method          = "GET"
  authorization        = "NONE"
  request_validator_id = aws_api_gateway_request_validator.query[0].id
  request_parameters   = each.value.request_parameters
}

resource "aws_api_gateway_integration" "get" {
  for_each = local.create ? local.endpoints : {}

  rest_api_id             = aws_api_gateway_rest_api.this[0].id
  resource_id             = aws_api_gateway_resource.leaf[each.key].id
  http_method             = aws_api_gateway_method.get[each.key].http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.this[each.key].invoke_arn
}

resource "aws_lambda_permission" "apigw" {
  for_each = local.create ? local.endpoints : {}

  statement_id  = "AllowInvoke-${replace(each.key, "_", "-")}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this[each.key].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.this[0].execution_arn}/*/GET/*"
}

resource "aws_api_gateway_method" "options" {
  for_each = local.create ? local.endpoints : {}

  rest_api_id   = aws_api_gateway_rest_api.this[0].id
  resource_id   = aws_api_gateway_resource.leaf[each.key].id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "options" {
  for_each = local.create ? local.endpoints : {}

  rest_api_id = aws_api_gateway_rest_api.this[0].id
  resource_id = aws_api_gateway_resource.leaf[each.key].id
  http_method = aws_api_gateway_method.options[each.key].http_method
  type        = "MOCK"

  request_templates = {
    "application/json" = "{\"statusCode\": 200}"
  }
}

resource "aws_api_gateway_method_response" "options" {
  for_each = local.create ? local.endpoints : {}

  rest_api_id = aws_api_gateway_rest_api.this[0].id
  resource_id = aws_api_gateway_resource.leaf[each.key].id
  http_method = aws_api_gateway_method.options[each.key].http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true
    "method.response.header.Access-Control-Allow-Methods" = true
    "method.response.header.Access-Control-Allow-Origin"  = true
  }
}

resource "aws_api_gateway_integration_response" "options" {
  for_each = local.create ? local.endpoints : {}

  rest_api_id = aws_api_gateway_rest_api.this[0].id
  resource_id = aws_api_gateway_resource.leaf[each.key].id
  http_method = aws_api_gateway_method.options[each.key].http_method
  status_code = aws_api_gateway_method_response.options[each.key].status_code

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'*'"
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
    "method.response.header.Access-Control-Allow-Origin"  = "'*'"
  }
}

resource "aws_api_gateway_gateway_response" "default_4xx" {
  count = local.create ? 1 : 0

  rest_api_id   = aws_api_gateway_rest_api.this[0].id
  response_type = "DEFAULT_4XX"

  response_parameters = {
    "gatewayresponse.header.Access-Control-Allow-Origin"  = "'*'"
    "gatewayresponse.header.Access-Control-Allow-Headers" = "'*'"
    "gatewayresponse.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
  }
}

resource "aws_api_gateway_gateway_response" "default_5xx" {
  count = local.create ? 1 : 0

  rest_api_id   = aws_api_gateway_rest_api.this[0].id
  response_type = "DEFAULT_5XX"

  response_parameters = {
    "gatewayresponse.header.Access-Control-Allow-Origin"  = "'*'"
    "gatewayresponse.header.Access-Control-Allow-Headers" = "'*'"
    "gatewayresponse.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
  }
}

resource "aws_api_gateway_deployment" "this" {
  count = local.create ? 1 : 0

  rest_api_id = aws_api_gateway_rest_api.this[0].id

  depends_on = [
    aws_api_gateway_integration.get,
    aws_api_gateway_integration.options,
    aws_api_gateway_integration_response.options
  ]

  triggers = {
    redeploy = sha1(jsonencode(local.endpoints))
  }
}

resource "aws_api_gateway_stage" "this" {
  count = local.create ? 1 : 0

  rest_api_id   = aws_api_gateway_rest_api.this[0].id
  deployment_id = aws_api_gateway_deployment.this[0].id
  stage_name    = var.api_stage_name

  tags = local.common_tags
}

resource "aws_api_gateway_method_settings" "all" {
  count = local.create ? 1 : 0

  rest_api_id = aws_api_gateway_rest_api.this[0].id
  stage_name  = aws_api_gateway_stage.this[0].stage_name
  method_path = "*/*"

  settings {
    metrics_enabled        = true
    logging_level          = "OFF"
    throttling_rate_limit  = var.api_throttling_rate_limit
    throttling_burst_limit = var.api_throttling_burst_limit
  }
}
