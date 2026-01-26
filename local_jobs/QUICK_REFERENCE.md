# Local Cron Jobs - Quick Reference

## File Structure

```
reddit-streaming/
└── local_jobs/
    ├── README.md                      # Full documentation
    ├── requirements.txt               # Python dependencies
    ├── run_curation_job.py            # Main job script (no AWS Glue)
    ├── setup_local_jobs.sh            # Install dependencies
    ├── setup_cron.sh                  # Configure cron jobs
    ├── remove_cron.sh                 # Remove cron jobs
    ├── quick_start.sh                 # Quick setup helper
    └── logs/                          # Job logs (created after setup)
        ├── news.log
        ├── technology.log
        ├── ProgrammerHumor.log
        └── worldnews.log
```

## Setup Commands

```bash
# One-time setup
cd reddit-streaming/local_jobs
bash setup_local_jobs.sh

# Configure cron scheduling
bash setup_cron.sh

# Remove cron jobs
bash remove_cron.sh
```

## Manual Execution

```bash
cd reddit-streaming/local_jobs
source venv/bin/activate

# Run a specific job
python3 run_curation_job.py --job news

# Run with custom bucket
python3 run_curation_job.py --job news --bucket my-bucket
```

## Cron Examples

### Current Setup (Midnight UTC Daily)
```cron
0 0 * * * source /path/to/venv/bin/activate && cd /path/to/local_jobs && python3 run_curation_job.py --job {job} >> logs/{job}.log 2>&1
```

### Common Schedules
```cron
# Every day at 2 AM UTC
0 2 * * *

# Every 6 hours
0 */6 * * *

# Twice daily (2 AM and 2 PM UTC)
0 2 * * *
0 14 * * *

# Every weekday at midnight
0 0 * * 1-5

# Every Sunday at 3 AM
0 3 * * 0
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `python3: command not found` | Install Python 3.7+ |
| `Java not found` | Install Java 11+ (`brew install openjdk@11`) |
| `Credentials not found` | Copy `aws_access_key.txt` and `aws_secret.txt` to project root |
| `ModuleNotFoundError` | Run `source venv/bin/activate` |
| Cron not running | Check `crontab -l \| grep REDDIT_CRON` and cron logs |
| Out of memory | Reduce Spark memory in `run_curation_job.py` |

## Viewing Logs

```bash
# Real-time monitoring
tail -f logs/news.log

# Last 50 lines
tail -50 logs/news.log

# Search for errors
grep ERROR logs/*.log

# All logs
ls -lh logs/
```

## Monitoring Cron

```bash
# View scheduled jobs
crontab -l

# View with just our jobs
crontab -l | grep REDDIT_CRON

# Check if cron is running
ps aux | grep cron

# View cron logs (Linux)
grep CRON /var/log/syslog
journalctl -u cron

# View cron logs (macOS)
log stream --predicate 'process == "cron"'
```

## Key Differences: Glue vs Local

| Feature | AWS Glue | Local Cron |
|---------|----------|-----------|
| Execution | AWS managed | Your machine |
| Scheduling | EventBridge | Linux cron |
| Cost | Per execution | Free |
| Uptime | 99.99% | Depends on machine |
| Monitoring | CloudWatch | Local logs |
| Setup | Terraform required | Bash scripts |

## Credentials Management

### Option 1: Local Files (Development)
```bash
# Copy credentials to project root
cp ~/aws_credentials/aws_access_key.txt reddit-streaming/
cp ~/aws_credentials/aws_secret.txt reddit-streaming/
```

### Option 2: AWS Secrets Manager (Production)
- IAM role with `secretsmanager:GetSecretValue` permission
- Script automatically retrieves from Secrets Manager if available
- Falls back to local files if Secrets Manager fails

### Option 3: Environment Variables (Alternative)
Edit script to use:
```python
os.environ.get('AWS_ACCESS_KEY_ID')
os.environ.get('AWS_SECRET_ACCESS_KEY')
```

## Performance Notes

- First run may take 2-3 minutes (downloading Spark & JARs)
- Subsequent runs take 30-60 seconds (depends on data volume)
- Adjust `spark.driver.memory` and `spark.executor.memory` if needed
- Run during off-peak hours to minimize impact on machine

## Getting Help

1. Check `README.md` for detailed documentation
2. View job logs: `tail -f logs/{job}.log`
3. Test manually: `python3 run_curation_job.py --job news`
4. Check system cron logs
5. Verify credentials exist and have proper S3 permissions
