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
