# EventBridge Schedules for Glue Jobs running at midnight UTC every day
# Cron expression: 0 0 * * ? * (midnight UTC, every day)

# IAM role for EventBridge to invoke Glue
resource "aws_iam_role" "eventbridge_glue_role" {
  name = "eventbridge-glue-invocation-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "scheduler.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_glue_policy" {
  name = "eventbridge-glue-policy"
  role = aws_iam_role.eventbridge_glue_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:StartJobRun"
      ]
      Resource = [
        aws_glue_job.technology_v2.arn,
        aws_glue_job.news_v2.arn,
        aws_glue_job.programmerhumer_v2.arn,
        aws_glue_job.worldnews_v2.arn
      ]
    }]
  })
}

# Technology subreddit schedule - midnight UTC
resource "aws_scheduler_schedule" "technology_schedule" {
  name                = "technology-curation-midnight"
  description         = "Run technology curation job daily at midnight UTC"
  schedule_expression = "cron(0 0 * * ? *)"
  timezone            = "UTC"
  
  flexible_time_window {
    mode = "OFF"
  }
  
  target {
    arn      = aws_glue_job.technology_v2.arn
    role_arn = aws_iam_role.eventbridge_glue_role.arn
    
    input = jsonencode({
      Name = aws_glue_job.technology_v2.name
    })
  }
}

# News subreddit schedule - midnight UTC
resource "aws_scheduler_schedule" "news_schedule" {
  name                = "news-curation-midnight"
  description         = "Run news curation job daily at midnight UTC"
  schedule_expression = "cron(0 0 * * ? *)"
  timezone            = "UTC"
  
  flexible_time_window {
    mode = "OFF"
  }
  
  target {
    arn      = aws_glue_job.news_v2.arn
    role_arn = aws_iam_role.eventbridge_glue_role.arn
    
    input = jsonencode({
      Name = aws_glue_job.news_v2.name
    })
  }
}

# ProgrammerHumor subreddit schedule - midnight UTC
resource "aws_scheduler_schedule" "programmerhumer_schedule" {
  name                = "programmerhumer-curation-midnight"
  description         = "Run ProgrammerHumor curation job daily at midnight UTC"
  schedule_expression = "cron(0 0 * * ? *)"
  timezone            = "UTC"
  
  flexible_time_window {
    mode = "OFF"
  }
  
  target {
    arn      = aws_glue_job.programmerhumer_v2.arn
    role_arn = aws_iam_role.eventbridge_glue_role.arn
    
    input = jsonencode({
      Name = aws_glue_job.programmerhumer_v2.name
    })
  }
}

# Worldnews subreddit schedule - midnight UTC
resource "aws_scheduler_schedule" "worldnews_schedule" {
  name                = "worldnews-curation-midnight"
  description         = "Run worldnews curation job daily at midnight UTC"
  schedule_expression = "cron(0 0 * * ? *)"
  timezone            = "UTC"
  
  flexible_time_window {
    mode = "OFF"
  }
  
  target {
    arn      = aws_glue_job.worldnews_v2.arn
    role_arn = aws_iam_role.eventbridge_glue_role.arn
    
    input = jsonencode({
      Name = aws_glue_job.worldnews_v2.name
    })
  }
}
