# glue cloudwatch logs
resource "aws_cloudwatch_log_group" "glue_output" {
  name = "/aws-glue/jobs/output"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "glue_error" {
  name = "/aws-glue/jobs/error"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "glue_logs" {
  name = "/aws-glue/jobs/logs-v2"
  retention_in_days = 30
}

# lambda logs
resource "aws_lambda_log_group" "lambda_logs" {
  name = "/aws-lambda/jobs/logs/output"
  retention_in_days = 30
}

resource "aws_lambda_error_group" "lambda_errors" {
  name = "/aws-lambda/jobs/logs/error"
  retention_in_days = 30
}

# cloudwatch event rule
resource "aws_cloudwatch_event_rule" "gps_delete_project" {
  name                = "${local.product}-delete-project"
  description         = "Runs GPS delete project stored procedure every day."
  schedule_expression = "cron(0 0- * * ? *)"
  tags = merge(local.standard_tags, map("name", "${local.product}-event-rule${local.environment}"))
}

# cloudwatch event target
resource "aws_cloudwatch_event_target" "event_delete_project" {
  rule              = aws_cloudwatch_event_rule.gps_delete_project.name
  target_id         = "lambda"
  arn               = aws_lambda_function.secret_rotation_lambda.arn
  input             = <<EOF
  {
    "Params": "[]"
  }
  EOF
}