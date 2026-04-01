variable "queue_name" {
  description = "Name of the SQS queue"
  type        = string
}

variable "visibility_timeout_seconds" {
  description = "Visibility timeout for the SQS queue in seconds"
  type        = number
  default     = 120
}

variable "message_retention_seconds" {
  description = "Message retention period for the SQS queue in seconds"
  type        = number
  default     = 345600
}

variable "receive_wait_time_seconds" {
  description = "Long polling wait time for ReceiveMessage in seconds"
  type        = number
  default     = 20
}

variable "tags" {
  description = "Tags for the SQS queue"
  type        = map(string)
  default     = {}
}
