variable "aws_region" {
  description = "AWS region to deploy the Lambda function into."
  type        = string
  default     = "ap-southeast-1"
}

variable "creator" {
  description = "Tag value that identifies the resource owner."
  type        = string
  default     = "kdu-flux"
}

variable "purpose" {
  description = "Tag value that describes the workload purpose."
  type        = string
  default     = "DE-Mini-Project"
}

variable "lambda_name" {
  description = "Name of the Lambda function."
  type        = string
  default     = "flux-de-adscribe-url-pull"
}

variable "adscribe_api_url" {
  description = "Adscribe API endpoint used to request a presigned CSV download URL."
  type        = string
  default     = "https://i500x8ofql.execute-api.us-east-1.amazonaws.com/prod/generate-csv"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 60
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB."
  type        = number
  default     = 256
}

variable "log_retention_in_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 14
}

variable "adscribe_dynamodb_table_name" {
  description = "Existing DynamoDB table used for Adscribe batch tracking."
  type        = string
  default     = "kdu-flux-dynamodb-table-de"
}

variable "adscribe_bucket_name" {
  description = "Existing S3 bucket used for Adscribe raw landing."
  type        = string
  default     = "kduflux-de-bucket"
}

variable "adscribe_glue_job_name" {
  description = "Glue Python Shell job name for Adscribe raw landing."
  type        = string
  default     = "flux-de-adscribe-raw-landing"
}

variable "adscribe_step_function_name" {
  description = "Step Functions state machine name for Adscribe raw landing."
  type        = string
  default     = "flux-de-adscribe-raw-landing"
}

variable "adscribe_glue_script_s3_key" {
  description = "S3 key used to store the Glue Python Shell script."
  type        = string
  default     = "glue-scripts/adscribe/adscribe_raw_landing.py"
}

variable "adscribe_redshift_user" {
  description = "Redshift username passed to the Adscribe Glue job as a default argument."
  type        = string
}

variable "adscribe_redshift_password" {
  description = "Redshift password passed to the Adscribe Glue job as a default argument."
  type        = string
  sensitive   = true
}

variable "adscribe_redshift_iam_role_arn" {
  description = "IAM role ARN passed to the Adscribe Glue job so Redshift COPY can read processed Adscribe parquet."
  type        = string
}
