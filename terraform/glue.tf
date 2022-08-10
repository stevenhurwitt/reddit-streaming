################### glue jobs ####################
resource "aws_glue_job" "news" {
  name     = "news-curation"
  role_arn = aws_iam_role.glue.arn
  max_retries = 0
  glue_version = "3.0"
  worker_type = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${var.s3_bucket_name}/scripts/news-curation.py"
  }

  default_arguments = {
    # ... potentially other arguments ...
    "--job-language"               = "python" 
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_logs.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--extra-jars"                  = "s3://${var.s3_bucket_name}/jars/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar,s3://${var.s3_bucket_name}/jars/com.ibm.icu_icu4j-58.2.jar,s3://${var.s3_bucket_name}/jars/hadoop-aws-3.3.1.jar,s3://${var.s3_bucket_name}/jars/io.delta_delta-core_2.12-1.0.0.jar,s3://${var.s3_bucket_name}/jars/org.abego.treelayout_org.abego.treelayout.core-1.0.3.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr-runtime-3.5.2.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr4-4.7.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr4-runtime-4.7.jar,s3://${var.s3_bucket_name}/jars/org.antlr_ST4-4.0.8.jar,s3://${var.s3_bucket_name}/jars/org.glassfish_javax.json-1.0.4.jar,s3://${var.s3_bucket_name}/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
    "--extra-py-files"              = "s3://${var.s3_bucket_name}/jars/delta.zip"
    # "--user-jars-first"             = "true"
    "--enable-spark-ui"            = "true"
    "--spark-event-logs-path"     = "s3://${var.s3_bucket_name}/spark_event_log/"
  }
}

resource "aws_glue_job" "technology" {
  name     = "technology-curation"
  role_arn = aws_iam_role.glue.arn
  max_retries = 0
  glue_version = "3.0"
  worker_type = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${var.s3_bucket_name}/scripts/technology-curation.py"
  }

  default_arguments = {
    # ... potentially other arguments ...
    "--job-language"               = "python" 
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_logs.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--extra-jars"                  = "s3://${var.s3_bucket_name}/jars/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar,s3://${var.s3_bucket_name}/jars/com.ibm.icu_icu4j-58.2.jar,s3://${var.s3_bucket_name}/jars/hadoop-aws-3.3.1.jar,s3://${var.s3_bucket_name}/jars/io.delta_delta-core_2.12-1.0.0.jar,s3://${var.s3_bucket_name}/jars/org.abego.treelayout_org.abego.treelayout.core-1.0.3.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr-runtime-3.5.2.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr4-4.7.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr4-runtime-4.7.jar,s3://${var.s3_bucket_name}/jars/org.antlr_ST4-4.0.8.jar,s3://${var.s3_bucket_name}/jars/org.glassfish_javax.json-1.0.4.jar,s3://${var.s3_bucket_name}/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
    "--extra-py-files"              = "s3://${var.s3_bucket_name}/jars/delta.zip"
    # "--user-jars-first"             = "true"
    "--enable-spark-ui"            = "true"
    "--spark-event-logs-path"     = "s3://${var.s3_bucket_name}/spark_event_log/"
  }
}

resource "aws_glue_job" "worldnews" {
  name     = "worldnews-curation"
  role_arn = aws_iam_role.glue.arn
  max_retries = 0
  glue_version = "3.0"
  worker_type = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${var.s3_bucket_name}/scripts/worldnews-curation.py"
  }

  default_arguments = {
    # ... potentially other arguments ...
    "--job-language"               = "python" 
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_logs.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--extra-jars"                  = "s3://${var.s3_bucket_name}/jars/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar,s3://${var.s3_bucket_name}/jars/com.ibm.icu_icu4j-58.2.jar,s3://${var.s3_bucket_name}/jars/hadoop-aws-3.3.1.jar,s3://${var.s3_bucket_name}/jars/io.delta_delta-core_2.12-1.0.0.jar,s3://${var.s3_bucket_name}/jars/org.abego.treelayout_org.abego.treelayout.core-1.0.3.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr-runtime-3.5.2.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr4-4.7.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr4-runtime-4.7.jar,s3://${var.s3_bucket_name}/jars/org.antlr_ST4-4.0.8.jar,s3://${var.s3_bucket_name}/jars/org.glassfish_javax.json-1.0.4.jar,s3://${var.s3_bucket_name}/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
    "--extra-py-files"              = "s3://${var.s3_bucket_name}/jars/delta.zip"
    # "--user-jars-first"             = "true"
    "--enable-spark-ui"            = "true"
    "--spark-event-logs-path"     = "s3://${var.s3_bucket_name}/spark_event_log/"
  }
}

resource "aws_glue_job" "ProgrammerHumor" {
  name     = "ProgrammerHumor-curation"
  role_arn = aws_iam_role.glue.arn
  max_retries = 0
  glue_version = "3.0"
  worker_type = "G.1X"
  number_of_workers = 2

  command {
    script_location = "s3://${var.s3_bucket_name}/scripts/ProgrammerHumor-curation.py"
  }

  default_arguments = {
    # ... potentially other arguments ...
    "--job-language"               = "python" 
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_logs.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--extra-jars"                  = "s3://${var.s3_bucket_name}/jars/com.amazonaws_aws-java-sdk-bundle-1.11.901.jar,s3://${var.s3_bucket_name}/jars/com.ibm.icu_icu4j-58.2.jar,s3://${var.s3_bucket_name}/jars/hadoop-aws-3.3.1.jar,s3://${var.s3_bucket_name}/jars/io.delta_delta-core_2.12-1.0.0.jar,s3://${var.s3_bucket_name}/jars/org.abego.treelayout_org.abego.treelayout.core-1.0.3.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr-runtime-3.5.2.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr4-4.7.jar,s3://${var.s3_bucket_name}/jars/org.antlr_antlr4-runtime-4.7.jar,s3://${var.s3_bucket_name}/jars/org.antlr_ST4-4.0.8.jar,s3://${var.s3_bucket_name}/jars/org.glassfish_javax.json-1.0.4.jar,s3://${var.s3_bucket_name}/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
    "--extra-py-files"              = "s3://${var.s3_bucket_name}/jars/delta.zip"
    # "--user-jars-first"             = "true"
    "--enable-spark-ui"            = "true"
    "--spark-event-logs-path"     = "s3://${var.s3_bucket_name}/spark_event_log/"
  }
}

################### glue schedule ####################
resource "aws_glue_trigger" "nightly_news" {
  name     = "nightly_news"
  schedule = "cron(0 5 * * ? *)"
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.news.name
  }
}

resource "aws_glue_trigger" "nightly_worldnews" {
  name     = "nightly_worldnews"
  schedule = "cron(0 5 * * ? *)"
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.worldnews.name
  }
}

resource "aws_glue_trigger" "nightly_technology" {
  name     = "nightly_technology"
  schedule = "cron(0 5 * * ? *)"
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.technology.name
  }
}

resource "aws_glue_trigger" "nightly_ProgrammerHumor" {
  name     = "nightly_ProgrammerHumor"
  schedule = "cron(0 5 * * ? *)"
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.ProgrammerHumor.name
  }
}