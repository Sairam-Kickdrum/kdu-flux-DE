variable "aws_region" {
  type    = string
  default = "ap-southeast-1"
}

variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "redshift_workgroup_name" {
  type = string
}

variable "redshift_database" {
  type = string
}

variable "redshift_secret_arn" {
  type = string
}

variable "redshift_fact_table" {
  type = string
}

variable "api_stage_name" {
  type    = string
  default = "v1"
}

variable "api_throttling_rate_limit" {
  type    = number
  default = 50
}

variable "api_throttling_burst_limit" {
  type    = number
  default = 100
}

variable "lambda_timeout_seconds" {
  type    = number
  default = 30
}

variable "lambda_memory_size" {
  type    = number
  default = 512
}

variable "lambda_log_retention_days" {
  type    = number
  default = 30
}

variable "attach_lambda_to_vpc" {
  type    = bool
  default = false
}

variable "lambda_subnet_ids" {
  type    = list(string)
  default = []
}

variable "lambda_security_group_ids" {
  type    = list(string)
  default = []
}

variable "create_lambda_security_group" {
  type    = bool
  default = false
}

variable "lambda_security_group_vpc_id" {
  type    = string
  default = ""
}
