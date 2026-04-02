variable "job_name" {
  description = "Glue job name"
  type        = string
}

variable "iam_role_name" {
  description = "IAM role name for the Glue job"
  type        = string
}

variable "bucket_arn" {
  description = "S3 bucket ARN used by the job"
  type        = string
}

variable "script_location" {
  description = "S3 script location for the Glue job"
  type        = string
}

variable "temp_dir" {
  description = "S3 TempDir location for Glue"
  type        = string
}

variable "glue_version" {
  description = "Glue version"
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker type"
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 2
}

variable "timeout" {
  description = "Glue job timeout in minutes"
  type        = number
  default     = 30
}

variable "max_retries" {
  description = "Max retries for Glue job"
  type        = number
  default     = 0
}

variable "default_arguments" {
  description = "Additional default arguments for Glue job"
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Tags for Glue resources"
  type        = map(string)
  default     = {}
}
