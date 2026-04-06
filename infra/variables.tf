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

variable "glue_stage_load_script_s3_key" {
  description = "S3 object key for the Glue Redshift staging load script"
  type        = string
  default     = "pipeline/glue/redshift_stage_load.py"
}

variable "glue_final_promote_script_s3_key" {
  description = "S3 object key for the Glue Redshift final promote script"
  type        = string
  default     = "pipeline/glue/redshift_final_promote.py"
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

variable "glue_stage_load_job_name" {
  description = "Glue job name for loading transformed data into Redshift staging table"
  type        = string
}

variable "glue_stage_load_job_iam_role_name" {
  description = "IAM role name for Glue staging load job"
  type        = string
}

variable "glue_final_promote_job_name" {
  description = "Glue job name for promoting data from staging table to final table"
  type        = string
}

variable "glue_final_promote_job_iam_role_name" {
  description = "IAM role name for Glue final promote job"
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

variable "redshift_host" {
  description = "Deprecated: no longer used directly (kept for backward compatibility)"
  type        = string
  default     = ""
}

variable "redshift_port" {
  description = "Redshift endpoint port"
  type        = number
  default     = 5439
}

variable "redshift_workgroup_name" {
  description = "Optional override for Redshift workgroup name; when empty it is derived from redshift_jdbc_url"
  type        = string
  default     = ""
}

variable "redshift_database" {
  description = "Redshift database name"
  type        = string
}

variable "redshift_secret_arn" {
  description = "Deprecated: no longer used; secret is created by Terraform from redshift_user/redshift_password"
  type        = string
  default     = ""
}

variable "redshift_schema" {
  description = "Redshift schema used by staging/final tables"
  type        = string
  default     = "public"
}

variable "redshift_staging_table" {
  description = "Redshift staging table name"
  type        = string
  default     = "fact_client_uploads_staging"
}

variable "redshift_final_table" {
  description = "Redshift final table name"
  type        = string
  default     = "fact_client_uploads"
}

variable "redshift_cleanup_staging_rows" {
  description = "Whether final promote lambda should clean staging rows for the completed load_id"
  type        = bool
  default     = true
}

variable "staging_loader_lambda_function_name" {
  description = "Function name for Redshift staging loader lambda"
  type        = string
}

variable "staging_loader_lambda_role_name" {
  description = "IAM role name for Redshift staging loader lambda"
  type        = string
}

variable "staging_loader_lambda_package_file" {
  description = "Package zip path for Redshift staging loader lambda"
  type        = string
}

variable "staging_loader_lambda_timeout" {
  description = "Timeout seconds for Redshift staging loader lambda"
  type        = number
  default     = 180
}

variable "staging_loader_lambda_memory_size" {
  description = "Memory MB for Redshift staging loader lambda"
  type        = number
  default     = 256
}

variable "final_promote_lambda_function_name" {
  description = "Function name for Redshift final promote lambda"
  type        = string
}

variable "final_promote_lambda_role_name" {
  description = "IAM role name for Redshift final promote lambda"
  type        = string
}

variable "final_promote_lambda_package_file" {
  description = "Package zip path for Redshift final promote lambda"
  type        = string
}

variable "final_promote_lambda_timeout" {
  description = "Timeout seconds for Redshift final promote lambda"
  type        = number
  default     = 180
}

variable "final_promote_lambda_memory_size" {
  description = "Memory MB for Redshift final promote lambda"
  type        = number
  default     = 256
}

variable "redshift_jdbc_url" {
  description = "Redshift JDBC URL"
  type        = string

  validation {
    condition     = can(regex("^jdbc:redshift://", var.redshift_jdbc_url))
    error_message = "redshift_jdbc_url must start with 'jdbc:redshift://'."
  }
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

variable "dashboard_api_enabled" {
  description = "Enable dashboard analytics API module"
  type        = bool
  default     = false
}

variable "dashboard_api_environment" {
  description = "Environment suffix for dashboard API naming"
  type        = string
  default     = "de"
}

variable "dashboard_api_stage_name" {
  description = "API Gateway stage name for dashboard API"
  type        = string
  default     = "v1"
}

variable "dashboard_api_redshift_secret_arn" {
  description = "Secrets Manager secret ARN containing Redshift username/password for dashboard API lambdas"
  type        = string
  default     = ""
}

variable "dashboard_api_client_upload_table" {
  description = "Client upload analytics table name"
  type        = string
  default     = "fact_client_uploads"
}

variable "dashboard_api_ascribe_table" {
  description = "Ascribe analytics table name"
  type        = string
  default     = "flux_ascribe_performance"
}

variable "dashboard_api_lambda_timeout_seconds" {
  description = "Dashboard API lambda timeout in seconds"
  type        = number
  default     = 30
}

variable "dashboard_api_lambda_memory_size" {
  description = "Dashboard API lambda memory in MB"
  type        = number
  default     = 256
}

variable "dashboard_api_lambda_log_retention_days" {
  description = "Dashboard API lambda log retention"
  type        = number
  default     = 14
}

variable "dashboard_api_attach_lambda_to_vpc" {
  description = "Attach dashboard API lambdas to VPC"
  type        = bool
  default     = false
}

variable "dashboard_api_lambda_subnet_ids" {
  description = "Subnet IDs for dashboard API lambdas when VPC attachment is enabled"
  type        = list(string)
  default     = []
}

variable "dashboard_api_lambda_security_group_ids" {
  description = "Security group IDs for dashboard API lambdas"
  type        = list(string)
  default     = []
}

variable "dashboard_api_create_lambda_security_group" {
  description = "Whether dashboard API module should create a dedicated lambda security group"
  type        = bool
  default     = false
}

variable "dashboard_api_lambda_security_group_vpc_id" {
  description = "VPC ID for dashboard API lambda security group creation"
  type        = string
  default     = ""
}

variable "dashboard_api_throttling_rate_limit" {
  description = "Dashboard API throttling rate limit"
  type        = number
  default     = 50
}

variable "dashboard_api_throttling_burst_limit" {
  description = "Dashboard API throttling burst limit"
  type        = number
  default     = 100
}

variable "dashboard_api_debug_db_identity" {
  description = "Enable debug DB identity logging in dashboard KPI lambda"
  type        = bool
  default     = false
}
