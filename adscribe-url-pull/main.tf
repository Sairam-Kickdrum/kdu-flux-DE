locals {
  common_tags = {
    Name        = var.lambda_name
    Creator     = var.creator
    Purpose     = var.purpose
    Service     = "adscribe-url-pull"
    Environment = "de"
  }
}

module "adscribe_url_pull_lambda" {
  source = "../modules/lambda_function"

  function_name         = var.lambda_name
  source_dir            = "${path.module}/lambda_src"
  handler               = "app.lambda_handler"
  runtime               = "python3.14"
  timeout               = var.lambda_timeout
  memory_size           = var.lambda_memory_size
  log_retention_in_days = var.log_retention_in_days
  environment_variables = {
    ADSCRIBE_API_URL     = var.adscribe_api_url
    LOOKBACK_DAYS        = "3"
    MAX_RANGE_DAYS       = "7"
    HTTP_TIMEOUT_SECONDS = "30"
    DYNAMODB_TABLE       = "kdu-flux-dynamodb-table-de"
    STEP_FUNCTION_ARN    = aws_sfn_state_machine.adscribe_raw_landing.arn
  }
  tags = local.common_tags
}
