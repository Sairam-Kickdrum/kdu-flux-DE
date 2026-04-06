variable "function_name" {
  type = string
}

variable "iam_role_name" {
  type = string
}

variable "runtime" {
  type    = string
  default = "python3.12"
}

variable "handler" {
  type = string
}

variable "lambda_package_file" {
  type = string
}

variable "timeout" {
  type    = number
  default = 120
}

variable "memory_size" {
  type    = number
  default = 256
}

variable "environment_variables" {
  type    = map(string)
  default = {}
}

variable "policy_json" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
