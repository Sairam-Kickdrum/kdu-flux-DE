variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "runtime" {
  description = "Runtime for the Lambda function"
  type        = string
}

variable "handler" {
  description = "Handler for the Lambda function"
  type        = string
}

variable "lambda_package_file" {
  description = "Optional path to the Lambda deployment package zip file"
  type        = string
  default     = null
}

variable "timeout" {
  description = "Timeout for the Lambda function in seconds"
  type        = number
  default     = 60
}

variable "memory_size" {
  description = "Memory size for the Lambda function in MB"
  type        = number
  default     = 256
}

variable "sqs_queue_arn" {
  description = "ARN of the SQS queue that triggers Lambda"
  type        = string
}

variable "sqs_batch_size" {
  description = "Number of messages to send to Lambda in each batch"
  type        = number
  default     = 10
}

variable "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table for read and write operations"
  type        = string
}

variable "step_function_state_machine_arn" {
  description = "ARN of the Step Functions state machine to start"
  type        = string
}

variable "tags" {
  description = "Tags for the Lambda resources"
  type        = map(string)
  default     = {}
}
