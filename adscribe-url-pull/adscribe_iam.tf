resource "aws_iam_role" "adscribe_glue" {
  name = "${var.adscribe_glue_job_name}-role-2"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "adscribe_glue" {
  name = "${var.adscribe_glue_job_name}-policy"
  role = aws_iam_role.adscribe_glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowGlueLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*",
          "arn:${data.aws_partition.current.partition}:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*:log-stream:*"
        ]
      },
      {
        Sid    = "AllowAdscribeBucketList"
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = data.aws_s3_bucket.adscribe.arn
      },
      {
        Sid    = "AllowAdscribeBucketObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Resource = [
          "${data.aws_s3_bucket.adscribe.arn}/${var.adscribe_glue_script_s3_key}",
          "${data.aws_s3_bucket.adscribe.arn}/raw/adscribe/*",
          "${data.aws_s3_bucket.adscribe.arn}/processed/adscribe/*",
          "${data.aws_s3_bucket.adscribe.arn}/quarantine/adscribe/*",
          "${data.aws_s3_bucket.adscribe.arn}/pipeline/config/adscribe_pipeline_config.json"
        ]
      },
      {
        Sid    = "AllowAdscribeDynamoDbUpdates"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem"
        ]
        Resource = data.aws_dynamodb_table.adscribe.arn
      }
    ]
  })
}

resource "aws_iam_role" "adscribe_step_functions" {
  name = "${var.adscribe_step_function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "adscribe_step_functions" {
  name = "${var.adscribe_step_function_name}-policy"
  role = aws_iam_role.adscribe_step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowGlueJobExecution"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun"
        ]
        Resource = aws_glue_job.adscribe_raw_landing.arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "adscribe_lambda_trigger" {
  name = "${var.lambda_name}-adscribe-raw-landing"
  role = "${var.lambda_name}-role"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowAdscribeDynamoDb"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem"
        ]
        Resource = data.aws_dynamodb_table.adscribe.arn
      },
      {
        Sid      = "AllowAdscribeStateMachineExecution"
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = aws_sfn_state_machine.adscribe_raw_landing.arn
      }
    ]
  })

  depends_on = [module.adscribe_url_pull_lambda]
}
