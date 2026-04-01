module "bucket" {
  source = "./modules/s3_bucket"

  bucket_name   = var.client_upload_bucket_name
  force_destroy = var.force_destroy

  tags = {
    Name        = var.client_upload_bucket_name
    Project     = var.project_name
    Purpose     = "DE-Mini-Project"
  }
}