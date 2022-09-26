data "aws_vpc" "std_vpc" {
    filter {
        name = "tag:Name"
        values = ["std-vpc"]
    }
}

data "aws_security_group" "gps_fg_alb_sg" {
    filter {
      name = "tag:Name"
      values = ["gps-fargate-alb-sg"]
    }
}

data "aws_security_group" "gps_rds_sg" {
    filter {
      name = "tag:Name"
      values = ["gps-rds-sg"]
    }
}

data "aws_security_group" "gps_fg_sg" {
    filter {
        name = "tag:Name"
        values = ["gps-fargate-sg"]
    }
}

data "aws_subnet_ids" "private_app_subnets" {
    vpc_id = data.aws_vpc.std_vpc.id
    filter {
      name = "tag:Name"
      values = ["private app subnet"]
    }
}

data "aws_subnet_ids" "public_app_subnets" {
    vpc_id = data.aws_vpc.std_vpc.id
    filter {
        name = "tag:Name"
        values = ["public subnet"]
    }
}

data "aws_security_group" "gps_lambda_rotate_db_secret_sg" {
    filter {
        name = "tag:Name"
        values = ["gps-lambda-rotate-db-secret"]
    }
}