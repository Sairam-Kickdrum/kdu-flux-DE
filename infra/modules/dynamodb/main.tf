resource "aws_dynamodb_table" "this" {
  name                        = var.table_name
  billing_mode                = var.billing_mode
  hash_key                    = var.hash_key
  deletion_protection_enabled = var.deletion_protection_enabled

  attribute {
    name = var.hash_key
    type = var.hash_key_type
  }

  point_in_time_recovery {
    enabled = var.enable_point_in_time_recovery
  }

  tags = var.tags
}
