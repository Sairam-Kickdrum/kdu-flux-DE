variable "state_machine_name" {
  description = "Step Functions state machine name"
  type        = string
}

variable "iam_role_name" {
  description = "IAM role name for the state machine"
  type        = string
}

variable "glue_job_arns" {
  description = "Glue job ARNs the state machine can execute"
  type        = list(string)
}

variable "definition" {
  description = "Amazon States Language definition for the state machine"
  type        = string
}

variable "tags" {
  description = "Tags for Step Functions resources"
  type        = map(string)
  default     = {}
}
