# Local Cron Jobs - Architecture & Design

## Overview

This setup allows you to run AWS Glue curation jobs locally using PySpark and system cron scheduling, without requiring AWS Glue infrastructure.

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      System Cron (Linux)                    │
│                                                             │
│  Midnight UTC daily                                        │
│  ┌──────────────────┐                                      │
│  │ Run news job     │                                      │
│  │ Run tech job     │                                      │
│  │ Run humor job    │                                      │
│  │ Run worldnews    │                                      │
│  └────────┬─────────┘                                      │
└───────────┼────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────┐
│              run_curation_job.py (PySpark)                 │
│                                                             │
│  1. Parse arguments                                        │
│  2. Get AWS credentials                                    │
│  3. Create Spark session                                   │
│  4. Read data from S3                                      │
│  5. Transform & clean                                      │
│  6. Write cleaned data to S3                               │
│  7. Optimize (vacuum, manifest)                            │
│  8. Trigger Athena table repair                            │
│  9. Log results                                            │
└────────────┬──────────────────┬──────────────────┬─────────┘
             │                  │                  │
             ▼                  ▼                  ▼
    ┌──────────────────┐ ┌──────────────┐ ┌──────────────────┐
    │   S3 Buckets     │ │  Athena      │ │  Local Logs      │
    │                  │ │              │ │                  │
    │ • Raw data       │ │ Table repair │ │ news.log         │
    │ • Clean data     │ │              │ │ technology.log   │
    │ • Manifests      │ │              │ │ ProgrammerHumor. │
    │ • Athena results │ │              │ │ worldnews.log    │
    └──────────────────┘ └──────────────┘ └──────────────────┘
```

## Data Flow for a Single Job

```
START (Cron triggers)
  │
  ├─→ Parse job name (e.g., "news")
  │
  ├─→ Load AWS Credentials
  │    └─→ Try: AWS Secrets Manager
  │    └─→ Fallback: Local files (aws_access_key.txt, aws_secret.txt)
  │
  ├─→ Initialize Spark Session
  │    └─→ Configure: S3A access, Delta, Hive support
  │    └─→ Download JARs (first run only)
  │
  ├─→ Read Delta Table from S3
  │    └─→ s3a://bucket-name/news/
  │
  ├─→ Transform Data
  │    ├─→ Cast timestamp columns
  │    ├─→ Add date parts (year, month, day)
  │    └─→ Deduplicate by title
  │
  ├─→ Write Clean Data to S3
  │    ├─→ Path: s3a://bucket-name/news_clean/
  │    ├─→ Format: Delta
  │    ├─→ Partition: year, month, day
  │    └─→ Mode: Overwrite with schema merge
  │
  ├─→ Optimize Delta Table
  │    ├─→ Vacuum (remove old versions)
  │    └─→ Generate symlink manifest
  │
  ├─→ Repair Athena Table
  │    └─→ Run: MSCK REPAIR TABLE reddit.news
  │
  ├─→ Log Results
  │    └─→ logs/news.log
  │
  END (Success) or ERROR (Logged)
```

## File Dependencies

```
run_curation_job.py
  │
  ├─→ Python stdlib: json, logging, os, sys, argparse
  │
  ├─→ boto3 (AWS SDK)
  │    ├─→ secretsmanager.get_secret_value()
  │    └─→ athena.start_query_execution()
  │
  ├─→ pyspark
  │    ├─→ SparkSession
  │    └─→ SQL functions (col, to_date, year, month, etc)
  │
  └─→ delta-spark
       ├─→ DeltaTable
       ├─→ Vacuum & manifest generation
       └─→ Delta format I/O
```

## Cron Job Structure

```
Crontab Entry:
┌─────────────────────────────────────────────────────────────┐
│ 0 0 * * * source /path/to/venv/bin/activate && \           │
│ cd /path/to/local_jobs && \                                │
│ python3 run_curation_job.py --job news >> logs/news.log 2>&1
└─────────────────────────────────────────────────────────────┘

Components:
├─ 0 0 * * *                    ← Cron schedule (midnight UTC)
├─ source venv/bin/activate     ← Activate Python virtual env
├─ cd /path/to/local_jobs       ← Set working directory
├─ python3 run_curation_job.py  ← Run job script
├─ --job news                   ← Job name argument
└─ >> logs/news.log 2>&1        ← Capture stdout + stderr
```

## Credential Resolution Strategy

```
get_aws_credentials()
  │
  ├─→ Try AWS Secrets Manager
  │    ├─→ Success: Return credentials
  │    └─→ Error: Fall through (log warning)
  │
  └─→ Try Local Files
       ├─→ Read: aws_access_key.txt
       ├─→ Read: aws_secret.txt
       ├─→ Success: Return credentials
       └─→ Error: Raise exception (cannot proceed)
```

## Spark Session Configuration

```
SparkSession Configuration
├─ App Name: "local-reddit-curation"
│
├─ JAR Packages:
│  ├─ spark-sql-kafka-0-10_2.12:3.5.3
│  ├─ hadoop-common:3.3.4
│  ├─ hadoop-aws:3.3.4
│  ├─ hadoop-client:3.3.4
│  ├─ delta-core_2.12:3.2.0
│  └─ postgresql:42.6.0
│
├─ S3A Configuration:
│  ├─ fs.s3a.access.key = AWS_ACCESS_KEY_ID
│  ├─ fs.s3a.secret.key = AWS_SECRET_ACCESS_KEY
│  ├─ fs.s3a.impl = S3AFileSystem
│  └─ aws.credentials.provider = SimpleAWSCredentialsProvider
│
├─ Delta Configuration:
│  ├─ sql.extensions = DeltaSparkSessionExtension
│  └─ sql.catalog.spark_catalog = DeltaCatalog
│
└─ Hive Support: Enabled
```

## Logging Architecture

```
logs/ directory
│
├─ news.log           ← News curation logs
├─ technology.log     ← Technology curation logs
├─ ProgrammerHumor.log ← Programmer Humor logs
└─ worldnews.log      ← World News logs

Log Format:
├─ Timestamp: 2026-01-26 14:00:00
├─ Logger: run_curation_job
├─ Level: INFO/ERROR
└─ Message: "Reading Delta table from s3a://bucket/news"

Examples:
  INFO - Spark session created successfully
  INFO - Successfully read 50000 records from news
  INFO - Transforming data...
  INFO - Successfully wrote cleaned data to s3a://bucket/news_clean/
  ERROR - Error processing news: [error details]
```

## Comparison: AWS Glue vs Local Cron

### AWS Glue Architecture
```
AWS EventBridge (cron schedule)
  │
  └─→ AWS Glue Job Runner
       │
       ├─→ Load job code from S3
       ├─→ Initialize Glue Context
       ├─→ Run transformation
       └─→ CloudWatch Logs
```

### Local Cron Architecture
```
Linux Cron (system scheduler)
  │
  └─→ Shell Process
       │
       └─→ Python Interpreter
            │
            ├─→ Load job from local disk
            ├─→ Initialize Spark Session
            ├─→ Run transformation
            └─→ Local filesystem logs
```

### Key Differences

| Aspect | AWS Glue | Local Cron |
|--------|----------|-----------|
| **Scheduler** | EventBridge | Linux cron |
| **Job Runner** | AWS Glue service | Local Python process |
| **Code Location** | S3 | Local filesystem |
| **Context** | GlueContext | SparkSession |
| **Logging** | CloudWatch | Local filesystem |
| **Credentials** | IAM role | Secrets Manager or local files |
| **Monitoring** | AWS Console | Log files |
| **Cost** | Per-DPU-hour | Free (your machine) |

## Performance Characteristics

### Cold Start (First Run)
```
Downloading Spark & JARs: 60-120 seconds (one-time)
Creating Spark session: 30-40 seconds
Reading data: 20-30 seconds
Transformation: 20-40 seconds (depends on data volume)
Writing data: 30-60 seconds
Athena table repair: 5-10 seconds

Total: 2-5 minutes
```

### Warm Start (Subsequent Runs)
```
Creating Spark session: 20-30 seconds
Reading data: 10-20 seconds
Transformation: 15-30 seconds
Writing data: 20-40 seconds
Athena table repair: 5-10 seconds

Total: 70-130 seconds (~1.5-2 minutes)
```

## Error Handling

```
Exception during execution
  │
  ├─→ Log error with full traceback
  │
  ├─→ Attempt Spark session cleanup
  │
  ├─→ Exit with code 1 (failure)
  │
  └─→ Cron typically retries or alerts based on exit code
```

## Security Considerations

1. **Credentials Storage**
   - Local files: Store in project root (never commit)
   - Secrets Manager: Recommended for production
   - Environment variables: Alternative option

2. **S3 Permissions**
   - Required: s3:GetObject, s3:PutObject, s3:ListBucket
   - Minimum: Read from source, write to clean bucket

3. **Athena Permissions**
   - Required: athena:StartQueryExecution
   - Output location: S3 bucket for query results

## Customization Points

```
run_curation_job.py

Customizable sections:
├─ JAR packages list → Add/remove dependencies
├─ Transformation logic → Modify data transformations
├─ S3 paths → Change bucket or directory structure
├─ Athena queries → Modify table repair logic
├─ Partitioning scheme → Change year/month/day
├─ Deduplication keys → Change from "title" to other field
└─ Spark configuration → Adjust memory, parallelism
```

## Migration Path: Glue → Local Cron

```
1. Review AWS Glue job code
   └─→ Understand: imports, context, transformations

2. Remove Glue dependencies
   ├─→ Remove: awsglue imports
   ├─→ Replace: GlueContext with SparkSession
   ├─→ Replace: Job initialization
   └─→ Replace: job.commit() (optional with standalone)

3. Add argument parsing
   └─→ Replace: getResolvedOptions with argparse

4. Add credential handling
   └─→ Remove: Glue credential handling
   └─→ Add: Manual credential retrieval

5. Test locally
   └─→ Run: python3 run_curation_job.py --job {name}

6. Schedule with cron
   └─→ Run: bash setup_cron.sh
```

## Monitoring & Observability

```
Sources of Truth:
├─ Cron logs
│  ├─ /var/log/syslog (Linux)
│  ├─ /var/log/system.log (macOS)
│  └─ journalctl (systemd)
│
├─ Application logs
│  ├─ local_jobs/logs/news.log
│  ├─ local_jobs/logs/technology.log
│  └─ [other job logs]
│
├─ Process status
│  ├─ ps aux | grep python3
│  └─ pgrep -f run_curation_job
│
└─ System status
   ├─ Free disk space (for Spark temp files)
   ├─ Memory available
   ├─ Network connectivity
   └─ S3 accessibility
```

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [AWS S3A Documentation](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)
- [Linux Cron Documentation](https://man7.org/linux/man-pages/man5/crontab.5.html)
- [boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
