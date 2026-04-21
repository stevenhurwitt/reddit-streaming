# Getting Started - Local Cron Jobs

## ⚡ Quick Start (5 Minutes)

### Prerequisites
- [ ] Python 3.7+ installed
- [ ] Java 11+ installed
- [ ] AWS credentials available

### Installation
```bash
cd reddit-streaming/local_jobs
bash setup_local_jobs.sh
```

### Configuration
```bash
# Copy credentials to project root
cp /path/to/aws_access_key.txt ../
cp /path/to/aws_secret.txt ../

# Set up cron scheduling
bash setup_cron.sh
```

### Verify
```bash
# Check cron jobs are scheduled
crontab -l | grep REDDIT_CRON

# Check a job runs
source .venv/bin/activate
python3 run_curation_job.py --job news
```

Done! Jobs will now run automatically every day at midnight UTC.

---

## 📚 Documentation Files

Read these in order:

1. **[SETUP_GUIDE.txt](SETUP_GUIDE.txt)** ← Start here!
   - 3 simple setup steps
   - Troubleshooting tips
   - 5-minute read

2. **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)**
   - Common commands
   - Examples
   - Quick tips
   - 10-minute read

3. **[README.md](README.md)**
   - Complete documentation
   - Configuration details
   - Customization options
   - 20-minute read

4. **[ARCHITECTURE.md](ARCHITECTURE.md)**
   - System design
   - Data flow
   - Performance characteristics
   - 15-minute read

5. **[INDEX.md](INDEX.md)**
   - Documentation index
   - File guide
   - Topic lookup
   - 5-minute read

---

## 🎯 Common Tasks

### Test a Job
```bash
source venv/bin/activate
python3 run_curation_job.py --job news
```

### View Logs
```bash
tail -f logs/news.log
```

### Check Scheduled Jobs
```bash
crontab -l | grep REDDIT_CRON
```

### Modify Schedule
```bash
crontab -e
# Edit the REDIS_CRON entries
```

### Remove Cron Jobs
```bash
bash remove_cron.sh
```

---

## 📋 Setup Checklist

- [ ] Python 3.7+ installed
- [ ] Java 11+ installed
- [ ] Ran `bash setup_local_jobs.sh`
- [ ] Copied AWS credentials to project root
- [ ] Ran `bash setup_cron.sh`
- [ ] Verified: `crontab -l | grep REDDIT_CRON`
- [ ] Tested a job manually
- [ ] Checked logs: `tail -f logs/news.log`

---

## 🚨 Troubleshooting

### Java not found
```bash
# Install Java
brew install openjdk@11        # macOS
sudo apt-get install openjdk-11-jdk  # Ubuntu
```

### Credentials not found
```bash
# Check files exist in project root
ls -la ../aws_access_key.txt ../aws_secret.txt
```

### Cron not running
```bash
# Check cron is configured
crontab -l | grep REDDIT_CRON

# Check cron logs
grep CRON /var/log/syslog        # Linux
log stream --predicate 'process == "cron"'  # macOS
```

### Module errors
```bash
# Activate virtual environment
source venv/bin/activate
```

For more troubleshooting, see [README.md](README.md#troubleshooting)

---

## 📞 Need Help?

1. Check the relevant documentation file above
2. Review logs in `logs/` directory
3. See troubleshooting section in [README.md](README.md)
4. Test manually: `python3 run_curation_job.py --job news`

---

## ✅ You're Done!

Your jobs are now scheduled to run:
- **Time**: Daily at midnight UTC
- **Jobs**: news, technology, ProgrammerHumor, worldnews
- **Logs**: `local_jobs/logs/`
- **Status**: Check with `crontab -l | grep REDDIT_CRON`

Monitor your jobs:
```bash
tail -f logs/news.log
```

---

**Next Step**: Read [SETUP_GUIDE.txt](SETUP_GUIDE.txt) for the 3-step setup process.
