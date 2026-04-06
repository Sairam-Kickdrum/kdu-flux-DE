variable "enabled" {
  description = "Whether to create dashboard analytics API resources"
  type        = bool
  default     = false
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
}

variable "environment" {
  description = "Environment suffix for names"
  type        = string
  default     = "de"
}

variable "api_stage_name" {
  description = "API Gateway stage name"
  type        = string
  default     = "v1"
}

variable "redshift_host" {
  description = "Redshift host"
  type        = string
}

variable "redshift_port" {
  description = "Redshift port"
  type        = number
  default     = 5439
}

variable "redshift_database" {
  description = "Redshift database"
  type        = string
}

variable "redshift_secret_arn" {
  description = "Secrets Manager secret ARN for Redshift credentials"
  type        = string

  validation {
    condition     = !var.enabled || trimspace(var.redshift_secret_arn) != ""
    error_message = "redshift_secret_arn must be set when dashboard_api is enabled."
  }
}

variable "client_upload_table" {
  description = "Fact table name for client upload domain"
  type        = string
  default     = "fact_client_uploads"
}

variable "ascribe_table" {
  description = "Fact table name for ascribe domain"
  type        = string
  default     = "flux_ascribe_performance"
}

variable "lambda_kpi_zip" {
  description = "Zip path for KPI lambda"
  type        = string
}

variable "lambda_revenue_daily_zip" {
  description = "Zip path for revenue daily lambda"
  type        = string
}

variable "lambda_revenue_monthly_zip" {
  description = "Zip path for revenue monthly lambda"
  type        = string
}

variable "lambda_breakdown_zip" {
  description = "Zip path for breakdown lambda"
  type        = string
}

variable "lambda_details_zip" {
  description = "Zip path for details lambda"
  type        = string
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout"
  type        = number
  default     = 30
}

variable "lambda_memory_size" {
  description = "Lambda memory"
  type        = number
  default     = 256
}

variable "lambda_log_retention_days" {
  description = "CloudWatch log retention"
  type        = number
  default     = 14
}

variable "attach_lambda_to_vpc" {
  description = "Attach lambdas to VPC"
  type        = bool
  default     = false
}

variable "lambda_subnet_ids" {
  description = "Lambda subnet ids"
  type        = list(string)
  default     = []
}

variable "lambda_security_group_ids" {
  description = "Existing Lambda security groups"
  type        = list(string)
  default     = []
}

variable "create_lambda_security_group" {
  description = "Whether to create lambda SG"
  type        = bool
  default     = false
}

variable "lambda_security_group_vpc_id" {
  description = "VPC ID if creating lambda SG"
  type        = string
  default     = ""
}

variable "api_throttling_rate_limit" {
  description = "API throttle rate"
  type        = number
  default     = 50
}

variable "api_throttling_burst_limit" {
  description = "API throttle burst"
  type        = number
  default     = 100
}

variable "tags" {
  description = "Extra tags"
  type        = map(string)
  default     = {}
}

variable "dashboard_api_debug_db_identity" {
  description = "Enable debug logging of current_user/current_database and secret username in KPI lambda"
  type        = bool
  default     = false
}
