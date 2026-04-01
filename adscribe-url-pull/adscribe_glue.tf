resource "aws_s3_object" "adscribe_glue_script" {
  bucket = data.aws_s3_bucket.adscribe.bucket
  key    = var.adscribe_glue_script_s3_key
  source = "${path.module}/glue_src/adscribe_raw_landing.py"
  etag   = filemd5("${path.module}/glue_src/adscribe_raw_landing.py")
}

resource "aws_glue_job" "adscribe_raw_landing" {
  name     = var.adscribe_glue_job_name
  role_arn = aws_iam_role.adscribe_glue.arn

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${data.aws_s3_bucket.adscribe.bucket}/${aws_s3_object.adscribe_glue_script.key}"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://${data.aws_s3_bucket.adscribe.bucket}/tmp/glue/${var.adscribe_glue_job_name}/"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries  = 0
  max_capacity = 0.0625
  timeout      = 30

  tags = local.common_tags
}
