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