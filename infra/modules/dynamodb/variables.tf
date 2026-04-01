variable "table_name" {
  description = "Name of the DynamoDB table"
  type        = string
}

variable "billing_mode" {
  description = "Billing mode for the DynamoDB table"
  type        = string
  default     = "PAY_PER_REQUEST"
}

variable "hash_key" {
  description = "Partition key name for the DynamoDB table"
  type        = string
  default     = "key"
}

variable "hash_key_type" {
  description = "Partition key type for the DynamoDB table"
  type        = string
  default     = "S"
}

variable "deletion_protection_enabled" {
  description = "Enable deletion protection for the DynamoDB table"
  type        = bool
  default     = false
}

variable "enable_point_in_time_recovery" {
  description = "Enable point-in-time recovery for the DynamoDB table"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags for the DynamoDB table"
  type        = map(string)
  default     = {}
}
