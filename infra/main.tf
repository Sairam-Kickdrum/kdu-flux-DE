module "bucket" {
  source = "./modules/s3_bucket"

  bucket_name   = var.client_upload_bucket_name
  force_destroy = var.force_destroy

  tags = {
    Name    = var.client_upload_bucket_name
    Project = var.project_name
    Purpose = "DE-Mini-Project"
  }
}

module "dynamodb" {
  source = "./modules/dynamodb"

  table_name                    = var.dynamodb_table_name
  billing_mode                  = var.dynamodb_billing_mode
  hash_key                      = var.dynamodb_hash_key
  hash_key_type                 = var.dynamodb_hash_key_type
  deletion_protection_enabled   = var.dynamodb_deletion_protection_enabled
  enable_point_in_time_recovery = var.dynamodb_enable_point_in_time_recovery

  tags = {
    Name    = var.dynamodb_table_name
    Project = var.project_name
    Purpose = "DynamoDB-Table"
  }
}

module "sqs" {
  source = "./modules/sqs"

  queue_name                 = var.sqs_queue_name
  visibility_timeout_seconds = var.sqs_visibility_timeout_seconds
  message_retention_seconds  = var.sqs_message_retention_seconds
  receive_wait_time_seconds  = var.sqs_receive_wait_time_seconds

  tags = {
    Name    = var.sqs_queue_name
    Project = var.project_name
    Purpose = "S3-Message-Queue"
  }
}

resource "aws_s3_object" "client_upload_prefix_placeholders" {
  for_each = toset(var.client_upload_client_names)

  bucket  = module.bucket.bucket_name
  key     = "raw/client_uploads/${each.value}/"
  content = ""
}

resource "aws_s3_object" "pipeline_config" {
  bucket       = module.bucket.bucket_name
  key          = var.pipeline_config_s3_key
  source       = "${path.root}/../pipeline/config/client_pipeline_config.json"
  etag         = filemd5("${path.root}/../pipeline/config/client_pipeline_config.json")
  content_type = "application/json"
}

resource "aws_s3_object" "glue_script" {
  bucket       = module.bucket.bucket_name
  key          = var.glue_script_s3_key
  source       = "${path.root}/../pipeline/glue/jobs/client_upload_etl.py"
  etag         = filemd5("${path.root}/../pipeline/glue/jobs/client_upload_etl.py")
  content_type = "text/x-python"
}

module "glue_job" {
  source = "./modules/glue_job"

  job_name        = var.glue_job_name
  iam_role_name   = var.glue_job_iam_role_name
  bucket_arn      = module.bucket.bucket_arn
  script_location = "s3://${module.bucket.bucket_name}/${aws_s3_object.glue_script.key}"
  temp_dir        = "s3://${module.bucket.bucket_name}/${var.glue_temp_dir_prefix}"

  tags = {
    Name    = var.glue_job_name
    Project = var.project_name
    Purpose = "Client-Upload-ETL"
  }
}

locals {
  step_function_definition = templatefile(
    "${path.root}/../statemachine/client_upload_orchestration.asl.json.tftpl",
    {
      glue_job_name     = module.glue_job.job_name
      redshift_jdbc_url = var.redshift_jdbc_url
      redshift_table    = var.redshift_table
      redshift_user     = var.redshift_user
      redshift_password = var.redshift_password
    }
  )
}

module "step_function" {
  source = "./modules/step_function"

  state_machine_name = var.step_function_state_machine_name
  iam_role_name      = var.step_function_iam_role_name
  glue_job_arn       = module.glue_job.job_arn
  definition         = local.step_function_definition

  tags = {
    Name    = var.step_function_state_machine_name
    Project = var.project_name
    Purpose = "Client-Upload-Orchestration"
  }
}

module "lambda" {
  source = "./modules/lambda"

  function_name                   = var.lambda_function_name
  runtime                         = var.lambda_runtime
  handler                         = var.lambda_handler
  lambda_package_file             = var.lambda_package_file
  timeout                         = var.lambda_timeout
  memory_size                     = var.lambda_memory_size
  sqs_queue_arn                   = module.sqs.queue_arn
  sqs_batch_size                  = var.lambda_sqs_batch_size
  dynamodb_table_arn              = module.dynamodb.table_arn
  step_function_state_machine_arn = module.step_function.state_machine_arn
  source_bucket_arn               = module.bucket.bucket_arn
  environment_variables = {
    IDEMPOTENCY_TABLE_NAME = module.dynamodb.table_name
    STEP_FUNCTION_ARN      = module.step_function.state_machine_arn
    SOURCE_BUCKET_NAME     = module.bucket.bucket_name
    CONFIG_S3_URI          = "s3://${module.bucket.bucket_name}/${aws_s3_object.pipeline_config.key}"
    PIPELINE_CONFIG_PATH   = "config/client_pipeline_config.json"
  }

  tags = {
    Name    = var.lambda_function_name
    Project = var.project_name
    Purpose = "Queue-Processor"
  }
}

resource "aws_sqs_queue_policy" "allow_s3_send_message" {
  queue_url = module.sqs.queue_url
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowS3SendMessage"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action   = "sqs:SendMessage"
        Resource = module.sqs.queue_arn
        Condition = {
          ArnLike = {
            "aws:SourceArn" = module.bucket.bucket_arn
          }
        }
      }
    ]
  })
}

resource "aws_s3_bucket_notification" "s3_to_sqs_upload_trigger" {
  bucket = module.bucket.bucket_name

  queue {
    queue_arn     = module.sqs.queue_arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = var.s3_upload_trigger_prefix
  }

  depends_on = [aws_sqs_queue_policy.allow_s3_send_message]
}
