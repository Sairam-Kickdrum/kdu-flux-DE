output "job_name" {
  value = aws_glue_job.this.name
}

output "job_arn" {
  value = aws_glue_job.this.arn
}

output "role_arn" {
  value = aws_iam_role.this.arn
}
