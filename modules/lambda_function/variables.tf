variable "function_name" {
  description = "Name of the Lambda function."
  type        = string
}

variable "source_dir" {
  description = "Directory that contains the Lambda source code."
  type        = string
}

variable "handler" {
  description = "Lambda handler entrypoint."
  type        = string
}

variable "runtime" {
  description = "Lambda runtime."
  type        = string
}

variable "timeout" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 60
}

variable "memory_size" {
  description = "Lambda memory size in MB."
  type        = number
  default     = 256
}

variable "log_retention_in_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 14
}

variable "architectures" {
  description = "Lambda instruction set architectures."
  type        = list(string)
  default     = ["x86_64"]
}

variable "environment_variables" {
  description = "Environment variables to inject into the Lambda function."
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Tags to apply to all supported resources."
  type        = map(string)
  default     = {}
}
