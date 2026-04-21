# Updated Glue Job Definitions with modern configuration (Glue 4.0+)
# These replace the old glue.tf definitions with better defaults

resource "aws_glue_job" "technology_v2" {
  name             = "technology-curation-v2"
  role_arn         = aws_iam_role.glue.arn
  glue_version     = "4.0"
  worker_type      = "G.2X"
  number_of_workers = 2
  timeout          = 60
  max_retries      = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_name}/scripts/technology-curation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_logs.name
    "--enable-continuous-log-filter"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.s3_bucket_name}/spark_event_log/"
    "--TempDir"                          = "s3://${var.s3_bucket_name}/temp/"
    "--BUCKET_NAME"                      = var.s3_bucket_name
  }

  tags = {
    Name        = "technology-curation-v2"
    Environment = "dev"
  }
}

resource "aws_glue_job" "news_v2" {
  name             = "news-curation-v2"
  role_arn         = aws_iam_role.glue.arn
  glue_version     = "4.0"
  worker_type      = "G.2X"
  number_of_workers = 2
  timeout          = 60
  max_retries      = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_name}/scripts/news-curation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_logs.name
    "--enable-continuous-log-filter"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.s3_bucket_name}/spark_event_log/"
    "--TempDir"                          = "s3://${var.s3_bucket_name}/temp/"
    "--BUCKET_NAME"                      = var.s3_bucket_name
  }

  tags = {
    Name        = "news-curation-v2"
    Environment = "dev"
  }
}

resource "aws_glue_job" "programmerhumer_v2" {
  name             = "ProgrammerHumor-curation-v2"
  role_arn         = aws_iam_role.glue.arn
  glue_version     = "4.0"
  worker_type      = "G.2X"
  number_of_workers = 2
  timeout          = 60
  max_retries      = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_name}/scripts/ProgrammerHumor-curation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_logs.name
    "--enable-continuous-log-filter"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.s3_bucket_name}/spark_event_log/"
    "--TempDir"                          = "s3://${var.s3_bucket_name}/temp/"
    "--BUCKET_NAME"                      = var.s3_bucket_name
  }

  tags = {
    Name        = "ProgrammerHumor-curation-v2"
    Environment = "dev"
  }
}

resource "aws_glue_job" "worldnews_v2" {
  name             = "worldnews-curation-v2"
  role_arn         = aws_iam_role.glue.arn
  glue_version     = "4.0"
  worker_type      = "G.2X"
  number_of_workers = 2
  timeout          = 60
  max_retries      = 1

  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_name}/scripts/worldnews-curation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_logs.name
    "--enable-continuous-log-filter"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.s3_bucket_name}/spark_event_log/"
    "--TempDir"                          = "s3://${var.s3_bucket_name}/temp/"
    "--BUCKET_NAME"                      = var.s3_bucket_name
  }

  tags = {
    Name        = "worldnews-curation-v2"
    Environment = "dev"
  }
}
