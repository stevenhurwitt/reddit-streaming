terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region  = "us-east-2"
  access_key = "AKIA6BTEPFALMKQYDKMN"
  secret_key = ""
  # secret_key = alphabet "q1s8hXL2MJPicEKBKDZEIhTZs+53krSxAK2PY9" double u
}

# resource "aws_instance" "reddit_server" {
#   ami           = "ami-070650c005cce4203"
#   instance_type = "t4g.xlarge"

#   tags = {
#     Name = "reddit"
#   }
# }

# Creating a AWS secret versions for database master account (Masteraccoundb)
# resource "aws_secretsmanager_secret_version" "sversion" {
#   secret_id = aws_secretsmanager_secret.AWS_ACCESS_KEY_ID.id
#   secret_string = <<EOF
#    {
#     "aws_client_id": "{$AWS_ACCESS_KEY_ID}",
#     "aws_secret_key": "{$AWS_SECRET_ACCESS_KEY}"
#    }
# EOF
# }

# resource "aws_secretsmanager_secret_version" "ssversion" {
#   secret_id = aws_secretsmanager_secret.AWS_SECRET_ACCESS_KEY.id
#   secret_string = <<EOF
#    {
#     "aws_client_id": "{$AWS_ACCESS_KEY_ID}",
#     "aws_secret_key": "{$AWS_SECRET_ACCESS_KEY}"
#    }
# EOF
# }

# resource "aws_secretsmanager" "secret" {
#   name = "reddit-secrets-account"
#   description = "reddit secrets account"
#   generate_secret_string {
#     exclude_punctuation = true
#     exclude_lowercase = false
#     exclude_uppercase = false
#     exclude_digits = false
#     exclude_space = false
#     password_length = 20
#     require_each_included_type = true
#   }
# }
 
# Importing the AWS secrets created previously using arn.
# data "aws_secretsmanager_secret" "AWS_ACCESS_KEY_ID" {
#   arn = aws_secretsmanager_secret.AWS_ACCESS_KEY_ID.arn
# }

# locals {
#   aws_client = data.aws_secretsmanager_secret_version.AWS_ACCESS_KEY_ID.secret_string
# }

# data "aws_secretsmanager_secret" "AWS_SECRET_ACCESS_KEY" {
#   arn = aws_secretsmanager_secret.AWS_SECRET_ACCESS_KEY.arn
# }

# locals {
#   aws_secret = data.aws_secretsmanager_secret_version.AWS_SECRET_ACCESS_KEY.secret_string
# }
