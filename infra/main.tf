module "bucket" {
  source = "./modules/s3_bucket"

  bucket_name   = var.client_upload_bucket_name
  force_destroy = var.force_destroy

  tags = {
    Name    = var.client_upload_bucket_name
    Project = var.project_name
    Purpose = "DE-Mini-Project"
  }
}

module "dynamodb" {
  source = "./modules/dynamodb"

  table_name                    = var.dynamodb_table_name
  billing_mode                  = var.dynamodb_billing_mode
  hash_key                      = var.dynamodb_hash_key
  hash_key_type                 = var.dynamodb_hash_key_type
  deletion_protection_enabled   = var.dynamodb_deletion_protection_enabled
  enable_point_in_time_recovery = var.dynamodb_enable_point_in_time_recovery

  tags = {
    Name    = var.dynamodb_table_name
    Project = var.project_name
    Purpose = "DynamoDB-Table"
  }
}
