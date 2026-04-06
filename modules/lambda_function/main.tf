data "archive_file" "package" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${path.root}/${var.function_name}.zip"
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name               = "${var.function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "basic_execution" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.log_retention_in_days

  tags = var.tags
}

resource "aws_lambda_function" "this" {
  function_name = var.function_name
  role          = aws_iam_role.this.arn
  filename      = data.archive_file.package.output_path
  handler       = var.handler
  runtime       = var.runtime
  timeout       = var.timeout
  memory_size   = var.memory_size
  architectures = var.architectures

  source_code_hash = data.archive_file.package.output_base64sha256

  dynamic "environment" {
    for_each = length(var.environment_variables) > 0 ? [1] : []

    content {
      variables = var.environment_variables
    }
  }

  tags = var.tags

  depends_on = [
    aws_cloudwatch_log_group.this,
    aws_iam_role_policy_attachment.basic_execution,
  ]
}
