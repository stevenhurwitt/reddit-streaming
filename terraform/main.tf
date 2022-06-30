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

resource "aws_instance" "app_server" {
  ami           = "ami-070650c005cce4203"
  instance_type = "t4g.xlarge"

  tags = {
    Name = "twitter"
  }
}
