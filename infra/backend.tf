terraform {
  backend "s3" {
    bucket         = "kdu-flux-tf-state-de"
    key            = "kduflux/terraform.tfstate"
    region         = "ap-southeast-1"
    dynamodb_table = "kdu-flux-tf-state-locks-de"
    encrypt        = true
  }
}


