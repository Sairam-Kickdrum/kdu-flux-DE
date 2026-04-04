variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-southeast-1"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "kduflux"
}

variable "client_upload_bucket_name" {
  description = "S3 bucket name for client uploads"
  type        = string
}

variable "force_destroy" {
  description = "Allow bucket deletion with objects inside"
  type        = bool
  default     = false
}

variable "dynamodb_table_name" {
  description = "DynamoDB table name"
  type        = string
}

variable "dynamodb_billing_mode" {
  description = "Billing mode for the DynamoDB table"
  type        = string
  default     = "PAY_PER_REQUEST"
}

variable "dynamodb_hash_key" {
  description = "Partition key name for the DynamoDB table"
  type        = string
  default     = "key"
}

variable "dynamodb_hash_key_type" {
  description = "Partition key type for the DynamoDB table"
  type        = string
  default     = "S"
}

variable "dynamodb_deletion_protection_enabled" {
  description = "Enable deletion protection for the DynamoDB table"
  type        = bool
  default     = false
}

variable "dynamodb_enable_point_in_time_recovery" {
  description = "Enable point-in-time recovery for the DynamoDB table"
  type        = bool
  default     = true
}

variable "sqs_queue_name" {
  description = "SQS queue name"
  type        = string
}

variable "sqs_visibility_timeout_seconds" {
  description = "Visibility timeout for the SQS queue in seconds"
  type        = number
  default     = 120
}

variable "sqs_message_retention_seconds" {
  description = "Message retention period for the SQS queue in seconds"
  type        = number
  default     = 345600
}

variable "sqs_receive_wait_time_seconds" {
  description = "Long polling wait time for ReceiveMessage in seconds"
  type        = number
  default     = 20
}

variable "lambda_function_name" {
  description = "Lambda function name"
  type        = string
}

variable "lambda_runtime" {
  description = "Runtime for the Lambda function"
  type        = string
}

variable "lambda_handler" {
  description = "Handler for the Lambda function"
  type        = string
}

variable "lambda_package_file" {
  description = "Path to the Lambda deployment package zip file"
  type        = string
  default     = "../pipeline/lambda/dist/client_upload_orchestrator.zip"
}

variable "lambda_timeout" {
  description = "Timeout for the Lambda function in seconds"
  type        = number
  default     = 60
}

variable "lambda_memory_size" {
  description = "Memory size for the Lambda function in MB"
  type        = number
  default     = 256
}

variable "lambda_sqs_batch_size" {
  description = "Number of SQS messages sent to Lambda per invocation"
  type        = number
  default     = 10
}

variable "pipeline_config_s3_key" {
  description = "S3 object key for the client pipeline config"
  type        = string
  default     = "pipeline/config/client_pipeline_config.json"
}

variable "client_transform_config_base_prefix" {
  description = "S3 prefix where per-client transformation configs are stored"
  type        = string
  default     = "config"
}

variable "client_transform_config_version_file" {
  description = "Config version file name for each client config"
  type        = string
  default     = "v1.json"
}

variable "glue_script_s3_key" {
  description = "S3 object key for the Glue ETL script"
  type        = string
  default     = "pipeline/glue/client_upload_etl.py"
}

variable "glue_temp_dir_prefix" {
  description = "S3 prefix for Glue temporary files"
  type        = string
  default     = "pipeline/glue/tmp/"
}

variable "glue_job_name" {
  description = "Glue job name"
  type        = string
}

variable "glue_job_iam_role_name" {
  description = "IAM role name for Glue job"
  type        = string
}

variable "step_function_state_machine_name" {
  description = "Step Functions state machine name"
  type        = string
}

variable "step_function_iam_role_name" {
  description = "IAM role name for Step Functions"
  type        = string
}

variable "redshift_s3_access_role_name" {
  description = "IAM role name used by Redshift to access S3 pipeline data"
  type        = string
  default     = "kdu-flux-redshift-s3-access-role-de"
}

variable "redshift_jdbc_url" {
  description = "Redshift JDBC URL"
  type        = string
}

variable "redshift_table" {
  description = "Redshift target table"
  type        = string
}

variable "redshift_user" {
  description = "Redshift username"
  type        = string
}

variable "redshift_password" {
  description = "Redshift password"
  type        = string
  sensitive   = true
}

variable "client_upload_client_names" {
  description = "Client names used for S3 placeholder prefixes under raw/client_uploads"
  type        = list(string)
  default     = ["alpha", "beta", "gamma"]
}

variable "s3_upload_trigger_prefix" {
  description = "S3 prefix filter for upload trigger notifications sent to SQS"
  type        = string
  default     = "raw/client_uploads/"
}
