# AWS Secrets Manager for storing credentials used by Glue jobs

resource "aws_secretsmanager_secret" "aws_access_key_id" {
  name                    = "AWS_ACCESS_KEY_ID"
  description             = "AWS Access Key ID for Reddit streaming Glue jobs"
  recovery_window_in_days = 0  # Immediate deletion for dev environment

  tags = {
    Name = "aws-access-key-id"
    Environment = "dev"
  }
}

resource "aws_secretsmanager_secret_version" "aws_access_key_id" {
  secret_id = aws_secretsmanager_secret.aws_access_key_id.id
  secret_string = jsonencode({
    "AWS_ACCESS_KEY_ID" = var.aws_access_key
  })
}

resource "aws_secretsmanager_secret" "aws_secret_access_key" {
  name                    = "AWS_SECRET_ACCESS_KEY"
  description             = "AWS Secret Access Key for Reddit streaming Glue jobs"
  recovery_window_in_days = 0  # Immediate deletion for dev environment

  tags = {
    Name = "aws-secret-access-key"
    Environment = "dev"
  }
}

resource "aws_secretsmanager_secret_version" "aws_secret_access_key" {
  secret_id = aws_secretsmanager_secret.aws_secret_access_key.id
  secret_string = jsonencode({
    "AWS_SECRET_ACCESS_KEY" = var.aws_secret_key
  })
}
