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
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name = "/aws-lambda/jobs/logs/output"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "lambda_errors" {
  name = "/aws-lambda/jobs/logs/error"
  retention_in_days = 30
}

# cloudwatch events
resource "aws_cloudwatch_event_rule" "example_event" {
  name = "example event rule"
}

resource "aws_cloudwatch_event_target" "example_target" {
  name = "example event target"
  source_arn = aws_lambda_function.example_lambda.arn
  # target_id = 
}