# PostgreSQL Duplicate Key Error - Fixed!

## The Error You Got

```
ERROR: duplicate key value violates unique constraint "news_pkey"
```

This happened because the curation job was trying to insert records that already existed in the PostgreSQL table.

## The Fix

I've updated `run_curation_job.py` to intelligently handle duplicate records. The job now has three modes:

### 1. **Skip Duplicates** (Default/Recommended) ✅

```bash
python run_curation_job.py --job news
# or explicitly
python run_curation_job.py --job news --handle-duplicates skip
```

**What it does:**
- Reads existing IDs from PostgreSQL
- Compares with new data from S3
- Only writes new/unique posts
- Safe to run multiple times

**Docker:**
```bash
./run_tests_docker.sh curation --job news

# Or with docker exec
docker exec -it reddit-spark-master python /opt/workspace/local_jobs/run_curation_job.py \
    --job news --spark-master spark://spark-master:7077
```

### 2. **Overwrite** (Use with caution)

```bash
python run_curation_job.py --job news --handle-duplicates overwrite
```

**What it does:**
- Deletes ALL existing records
- Writes fresh data from S3
- Use for re-processing or fixing data

## How It Works

### Skip Mode Process:
```
1. Count total records in S3 Delta table
2. Read existing post_ids from PostgreSQL
3. Filter out posts that already exist
4. Write only NEW posts
5. Log what was skipped
```

### Example Output:
```
Total records to write: 50000
Reading existing post_ids from PostgreSQL...
Found 48500 existing records in PostgreSQL
Records to write after deduplication: 1500
Skipping 48500 duplicate records
Successfully wrote data to PostgreSQL: reddit_schema.news
```

## Usage Examples

### For the first time running:
```bash
# Skip mode works even if table doesn't exist
python run_curation_job.py --job news
# Table will be auto-created on first write
```

### Running daily (safe to repeat):
```bash
# Same command every day - it will skip existing posts
0 2 * * * python run_curation_job.py --job news
```

### If you need to refresh all data:
```bash
# Only when you want to rebuild the table
python run_curation_job.py --job news --handle-duplicates overwrite
```

### Docker Compose:
```bash
# Daily safe run
docker exec -it reddit-spark-master python /opt/workspace/local_jobs/run_curation_job.py \
    --job news --spark-master spark://spark-master:7077

# Overwrite mode
docker exec -it reddit-spark-master python /opt/workspace/local_jobs/run_curation_job.py \
    --job news --handle-duplicates overwrite --spark-master spark://spark-master:7077
```

## What Changed in the Code

✅ `write_to_postgres()` - Now accepts `handle_duplicates` parameter
✅ `process_subreddit()` - Passes duplicate handling mode through
✅ Command-line argument `--handle-duplicates` - Control the behavior
✅ Intelligent filtering - Only writes new records in skip mode

## Reference

| Parameter | Default | Options | Notes |
|-----------|---------|---------|-------|
| `--job` | required | news, technology, worldnews, etc. | Which subreddit |
| `--bucket` | reddit-streaming-stevenhurwitt-2 | S3 bucket name | S3 location |
| `--spark-master` | None (local) | spark://host:7077 | For cluster mode |
| `--handle-duplicates` | skip | skip, overwrite | How to handle duplicates |

## Troubleshooting

### Q: "Could not read existing records (table may not exist)"
**A:** This is normal on first run. The table will be created automatically.

### Q: "No new records to write - all records already exist"
**A:** This is fine! It means your table is up-to-date. No changes needed.

### Q: Script is slow
**A:** First run is slowest. It needs to count and deduplicate. Subsequent runs are faster because there are fewer new records.

### Q: Want to reset everything?
**A:** Use overwrite mode:
```bash
python run_curation_job.py --job news --handle-duplicates overwrite
```

## For More Details

See [DUPLICATE_HANDLING_GUIDE.md](DUPLICATE_HANDLING_GUIDE.md) for comprehensive documentation.

---

## Quick Start

```bash
# Local: Run today and every day after (safe!)
python run_curation_job.py --job news

# Docker: Same thing but in container
docker exec -it reddit-spark-master python /opt/workspace/local_jobs/run_curation_job.py \
    --job news --spark-master spark://spark-master:7077

# Via helper script (easiest)
./run_tests_docker.sh curation --job news
```

That's it! No more duplicate key errors. ✅
