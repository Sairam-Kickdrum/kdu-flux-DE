resource "aws_sfn_state_machine" "adscribe_raw_landing" {
  name     = var.adscribe_step_function_name
  role_arn = aws_iam_role.adscribe_step_functions.arn

  definition = jsonencode({
    Comment = "Minimal Adscribe raw landing workflow"
    StartAt = "RunAdscribeRawLandingJob"
    States = {
      RunAdscribeRawLandingJob = {
        Type     = "Task"
        Resource = "arn:${data.aws_partition.current.partition}:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.adscribe_raw_landing.name
          Arguments = {
            "--batch_id.$"      = "$.batch_id"
            "--start_date.$"    = "$.start_date"
            "--end_date.$"      = "$.end_date"
            "--presigned_url.$" = "$.presigned_url"
            "--run_id.$"        = "$.run_id"
          }
        }
        End = true
      }
    }
  })

  tags = local.common_tags
}
