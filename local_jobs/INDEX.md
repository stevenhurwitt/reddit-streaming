# Local Cron Jobs - Documentation Index

## Where to Start

1. **New to this setup?** → Read [`SETUP_GUIDE.txt`](SETUP_GUIDE.txt) (3 simple steps)
2. **Already set up?** → Read [`QUICK_REFERENCE.md`](QUICK_REFERENCE.md)
3. **Need details?** → Read [`README.md`](README.md)

---

## Files Overview

### 🚀 Setup Scripts
- **`setup_local_jobs.sh`** - Install dependencies (run once)
- **`setup_cron.sh`** - Configure cron jobs (run once)
- **`remove_cron.sh`** - Remove cron jobs (if needed)
- **`quick_start.sh`** - Interactive setup helper

### 💻 Application
- **`run_curation_job.py`** - Main PySpark job script
- **`requirements.txt`** - Python package dependencies

### 📚 Documentation
- **`SETUP_GUIDE.txt`** - Quick 3-step setup guide
- **`README.md`** - Complete documentation with troubleshooting
- **`QUICK_REFERENCE.md`** - Commands, examples, and quick tips
- **`INDEX.md`** - This file

---

## Common Tasks

### Initial Setup
```bash
bash setup_local_jobs.sh
bash setup_cron.sh
```

### Test a Job
```bash
source venv/bin/activate
python3 run_curation_job.py --job news
```

### View Scheduled Jobs
```bash
crontab -l | grep REDDIT_CRON
```

### Check Logs
```bash
tail -f logs/news.log
```

### Modify Schedule
```bash
crontab -e
# Find REDDIT_CRON entries and edit
```

### Remove Cron Jobs
```bash
bash remove_cron.sh
```

---

## Documentation by Topic

| Topic | File | Section |
|-------|------|---------|
| Getting started | `SETUP_GUIDE.txt` | Entire file |
| Installation | `README.md` | Quick Start |
| Job execution | `QUICK_REFERENCE.md` | Manual Execution |
| Cron scheduling | `README.md` | Modify Cron Schedule |
| Troubleshooting | `README.md` | Troubleshooting |
| Logs | `QUICK_REFERENCE.md` | Viewing Logs |
| Performance | `README.md` | Performance Notes |
| Customization | `README.md` | Customization |

---

## Quick Commands

```bash
# Activate environment
source venv/bin/activate

# Run a job
python3 run_curation_job.py --job {job_name}

# View cron jobs
crontab -l

# Edit cron schedule
crontab -e

# View logs
tail -f logs/{job_name}.log

# Check job status
ps aux | grep python3

# Remove all reddit cron jobs
bash remove_cron.sh
```

---

## Supported Jobs

All 8 subreddit curation jobs are supported:
- `news`
- `technology`
- `ProgrammerHumor`
- `worldnews`
- `aws`
- `bikinibottomtwitter`
- `blackpeopletwitter`
- `whitepeopletwitter`

---

## Need Help?

1. **First time?** → `SETUP_GUIDE.txt`
2. **Can't remember commands?** → `QUICK_REFERENCE.md`
3. **Something's broken?** → `README.md` Troubleshooting section
4. **Want to customize?** → `README.md` Customization section

---

## File Locations

```
reddit-streaming/
├── aws_access_key.txt        ← Your credentials (needed)
├── aws_secret.txt            ← Your credentials (needed)
└── local_jobs/
    ├── INDEX.md              ← You are here
    ├── SETUP_GUIDE.txt       ← Start here
    ├── README.md             ← Full docs
    ├── QUICK_REFERENCE.md    ← Quick tips
    ├── run_curation_job.py   ← Main script
    ├── setup_local_jobs.sh
    ├── setup_cron.sh
    ├── remove_cron.sh
    ├── quick_start.sh
    ├── requirements.txt
    ├── venv/                 ← Created after setup
    └── logs/                 ← Created after setup
        ├── news.log
        ├── technology.log
        ├── ProgrammerHumor.log
        └── worldnews.log
```

---

**Last Updated:** 2026-01-26
