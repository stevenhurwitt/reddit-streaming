# Local Cron-Based Job Scheduling

This directory contains scripts to run AWS Glue curation jobs locally using cron scheduling, without needing AWS Glue infrastructure.

## Overview

The setup includes:
- **run_curation_job.py** - Standalone PySpark script that replicates AWS Glue job logic
- **setup_local_jobs.sh** - One-time setup script to install dependencies
- **setup_cron.sh** - Configure cron jobs for automatic execution
- **remove_cron.sh** - Remove cron jobs when no longer needed

## Prerequisites

- Python 3.7+
- Java (required by Spark)
- Spark 3.1+ (installed via pip during setup)
- AWS credentials access (via Secrets Manager or local files)

## Quick Start

### 1. Initial Setup

```bash
cd /path/to/reddit-streaming/local_jobs
bash setup_local_jobs.sh
```

This will:
- Create a Python virtual environment
- Install dependencies (boto3, pyspark, delta-spark)
- Create a logs directory

### 2. Prepare AWS Credentials

The script looks for credentials in this order:
1. **AWS Secrets Manager** (if running on EC2/Lambda with IAM role)
2. **Local files** in the project root:
   - `aws_access_key.txt`
   - `aws_secret.txt`

Ensure credentials are in the project root:
```bash
cp /path/to/credentials/aws_access_key.txt /path/to/reddit-streaming/
cp /path/to/credentials/aws_secret.txt /path/to/reddit-streaming/
```

### 3. Test a Job Manually

```bash
cd /path/to/reddit-streaming/local_jobs
source venv/bin/activate
python3 run_curation_job.py --job news
```

### 4. Configure Cron Jobs

```bash
bash setup_cron.sh
```

This configures jobs to run **daily at midnight UTC** (matching AWS EventBridge schedule):
- `news` curation
- `technology` curation
- `ProgrammerHumor` curation
- `worldnews` curation

## Usage

### Manual Job Execution

```bash
source venv/bin/activate
python3 run_curation_job.py --job {job_name} [--bucket BUCKET_NAME]

# Examples
python3 run_curation_job.py --job news
python3 run_curation_job.py --job technology --bucket my-custom-bucket
```

Supported jobs:
- `news`
- `technology`
- `ProgrammerHumor`
- `worldnews`
- `aws`
- `bikinibottomtwitter`
- `blackpeopletwitter`
- `whitepeopletwitter`

### View Cron Jobs

```bash
crontab -l | grep REDDIT_CRON
```

### View Logs

```bash
# Real-time log viewing
tail -f logs/news.log
tail -f logs/technology.log

# View all logs
ls -la logs/
```

### Modify Cron Schedule

Edit `/etc/crontab` or use `crontab -e`:

```bash
crontab -e
```

Look for entries with `REDDIT_CRON` comments. Examples:

```cron
# Run at 2 AM UTC instead of midnight
0 2 * * * source /path/to/venv/bin/activate && cd /path/to/local_jobs && python3 run_curation_job.py --job news >> logs/news.log 2>&1

# Run twice daily (midnight and noon UTC)
0 0 * * * command
0 12 * * * command

# Run every 6 hours
0 */6 * * * command
```

### Remove Cron Jobs

```bash
bash remove_cron.sh
```

## How It Works

### Differences from AWS Glue

The local version removes AWS Glue-specific dependencies:
- ✓ Removes `awsglue.context.GlueContext` and `awsglue.job.Job`
- ✓ Uses standalone PySpark instead
- ✓ Maintains all data transformation logic
- ✓ Still uses Athena for table repair
- ✓ Still writes to S3 Delta tables

### Job Flow

1. **Initialize Spark Session** - Creates a PySpark session with necessary configurations
2. **Get Credentials** - Reads AWS credentials from Secrets Manager or local files
3. **Read Data** - Loads Delta table from S3
4. **Transform** - Applies data transformations (cast types, add date columns, deduplicate)
5. **Write** - Saves cleaned data back to S3 as Delta format
6. **Optimize** - Runs vacuum and generates symlink manifests
7. **Repair** - Runs MSCK REPAIR TABLE via Athena

## Troubleshooting

### Java Not Found

Spark requires Java. Install it:

```bash
# macOS
brew install openjdk@11

# Ubuntu/Debian
sudo apt-get install openjdk-11-jdk

# Amazon Linux
sudo yum install java-11-openjdk
```

### Credentials Not Found

Check that credential files exist in the project root:
```bash
ls -la aws_access_key.txt aws_secret.txt
```

Or verify AWS Secrets Manager is accessible:
```bash
aws secretsmanager get-secret-value --secret-id AWS_ACCESS_KEY_ID
```

### Cron Jobs Not Running

Check system cron logs:
```bash
# macOS
log stream --predicate 'process == "cron"'

# Linux
grep CRON /var/log/syslog
# or
journalctl -u cron
```

Verify permissions:
```bash
ls -la setup_local_jobs.sh setup_cron.sh
# Should have execute permission (x)
# If not: chmod +x *.sh
```

### Out of Memory

Adjust Spark memory settings in `run_curation_job.py`:

```python
spark = SparkSession.builder \
    .appName("local-reddit-curation") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    # ... other configs
```

### S3 Access Issues

Verify credentials have S3 permissions:
- `s3:GetObject`
- `s3:PutObject`
- `s3:ListBucket`

Test access:
```bash
aws s3 ls s3://reddit-streaming-stevenhurwitt-2/
```

## Comparison with AWS EventBridge

| Aspect | EventBridge | Local Cron |
|--------|-------------|-----------|
| **Scheduler** | AWS Managed | Linux/macOS cron |
| **Cost** | Pay per invocation | Free |
| **Dependency** | AWS Account required | None (runs locally) |
| **Reliability** | Highly available | Depends on machine uptime |
| **Monitoring** | CloudWatch | Logs in local filesystem |
| **Debugging** | CloudWatch Logs | Local log files |
| **Scaling** | Automatic | Limited to local resources |

## Combining with AWS

You can run **both** AWS EventBridge and local cron simultaneously:
- Use AWS EventBridge in production/cloud environments
- Use local cron for development or backup execution
- Monitor for duplicate runs if running at the same time

## Customization

### Add a New Job

1. Add the job name to the `choices` in `run_curation_job.py`
2. Add a cron entry in `setup_cron.sh`:

```bash
add_or_update_cron "REDDIT_CRON - my_subreddit curation" \
    "0 0 * * *" \
    "source $VENV_DIR/bin/activate && cd $SCRIPT_DIR && python3 run_curation_job.py --job my_subreddit >> $LOG_DIR/my_subreddit.log 2>&1"
```

3. Re-run `setup_cron.sh`

### Modify Job Logic

Edit the `process_subreddit()` function in `run_curation_job.py` to customize transformations.

## Support

For issues or questions:
1. Check logs in `local_jobs/logs/`
2. Test job manually: `python3 run_curation_job.py --job news`
3. Check AWS credentials are accessible
4. Verify Spark and Java installations
