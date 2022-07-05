terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.3"
    }
  }

  required_version = ">= 1.2.4"
}

provider "aws" {
  region  = "us-east-2"
}

resource "aws_instance" "twitter" {
  ami           = "ami-0ff596d41505819fd"
  instance_type = "t4g.xlarge"

  tags = {
    Name = "twitter"
  }
}

resource "aws_s3_bucket" "reddit-stevenhurwitt"{
  bucket = "reddit-stevenhurwitt"
}

resource "aws_s3_bucket" "twitter-stevenhurwitt"{
  bucket = "twitter-stevenhurwitt"
}

resource "aws_s3_bucket" "jars"{
  bucket = "jars"
}

resource "aws_s3_bucket" "AsiansGoneWild_clean"{
  bucket = "reddit-stevenhurwitt"
}

resource "aws_s3_bucket" "AsiansGoneWild"{
  bucket = "reddit-stevenhurwitt"
}

# resource "aws_s3_bucket" "technology"{
#   bucket = "technology"
# }

# resource "aws_s3_bucket" "news"{
#   bucket = "news"
# }

# resource "aws_s3_bucket" "worldnews"{
#   bucket = "worldnews"
# }

# resource "aws_s3_bucket" "programmerhumor"{
#   bucket = "programmerhumor"
# }
