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
