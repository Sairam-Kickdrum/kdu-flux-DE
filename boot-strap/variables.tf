variable "aws_region" {
  description = "Region to create the backend resources"
  type        = string
  default     = "ap-southeast-1"
}

# variable "environment" {
#   description = "Environment name: (dev, prod)"
#   type        = string
#   default     = "dev"
# }

variable "creator" {
  description = "Tag: Creator of these resources"
  type        = string
  default     = "kdu-flux"
}

variable "purpose" {
  description = "Tag: purpose of these resources"
  type        = string
  default     = "DE-Mini-Project"
}

variable "bucket_name_prefix" {
  description = "Prefix for the S3 bucket name. A random suffix will be added to ensure global uniqueness."
  type        = string
  default     = "tf-state-de"
}

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table for state locking"
  type        = string
  default     = "tf-state-locks-de"
}