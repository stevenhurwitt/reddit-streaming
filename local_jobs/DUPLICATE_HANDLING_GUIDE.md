# Handling Duplicate Key Errors in PostgreSQL Curation

## The Problem

When running curation jobs multiple times, you might encounter this error:

```
ERROR: duplicate key value violates unique constraint "news_pkey"
```

This happens because:
1. Your PostgreSQL table has a PRIMARY KEY constraint on the `post_id` column
2. The curation job previously wrote data to the same table
3. Running the job again tries to write the same records again
4. PostgreSQL rejects the duplicate inserts

## The Solution

The updated `run_curation_job.py` now supports three ways to handle duplicates:

### Option 1: Skip Duplicates (Default/Recommended)
**Recommended for production use**

```bash
python run_curation_job.py --job news
# or explicitly
python run_curation_job.py --job news --handle-duplicates skip
```

**What it does:**
- Reads existing post_ids from PostgreSQL
- Compares them with new data from S3 Delta table
- Only writes posts that don't already exist
- Safe, idempotent, can run multiple times

**When to use:**
- Regular scheduled jobs that run multiple times
- You want to preserve existing data and only add new records
- Default behavior for all jobs

### Option 2: Overwrite Existing Data
**Use with caution**

```bash
python run_curation_job.py --job news --handle-duplicates overwrite
```

**What it does:**
- Deletes all existing records in the table
- Writes all records from the Delta table
- Useful for re-processing or fixing data

**When to use:**
- First-time data load
- Re-processing corrupted data
- Recalculating cleaned records
- **WARNING:** This permanently deletes existing data

## Setup Once (Create Tables First)

Before you can use the deduplication features, the PostgreSQL tables must exist. Here's how:

### Option A: Let skip mode auto-create tables

```bash
# First run - tables will be created automatically
python run_curation_job.py --job news

# Subsequent runs - will skip duplicates
python run_curation_job.py --job news
```

### Option B: Manually create tables

If you want to pre-create tables with specific schema:

```bash
docker exec -it reddit-postgres psql -U postgres -d reddit -c "
CREATE SCHEMA IF NOT EXISTS reddit_schema;

CREATE TABLE reddit_schema.news (
    post_id VARCHAR(50) PRIMARY KEY,
    title TEXT,
    author VARCHAR(255),
    score INT,
    num_comments INT,
    created_utc TIMESTAMP,
    url TEXT,
    selftext TEXT
);
"
```

## Docker Compose Usage

### Using the helper script

```bash
cd /home/steven/reddit-streaming

# Skip duplicates (safe, default)
./run_tests_docker.sh curation --job news

# Overwrite all data
docker exec -it reddit-spark-master python /opt/workspace/local_jobs/run_curation_job.py \
    --job news --handle-duplicates overwrite
```

### Direct docker exec

```bash
# Skip duplicates (default)
docker exec -it reddit-spark-master python /opt/workspace/local_jobs/run_curation_job.py \
    --job news \
    --spark-master spark://spark-master:7077

# Overwrite existing data
docker exec -it reddit-spark-master python /opt/workspace/local_jobs/run_curation_job.py \
    --job news \
    --spark-master spark://spark-master:7077 \
    --handle-duplicates overwrite
```

## How Skip Duplicates Works

### Step 1: Read existing IDs from PostgreSQL
```sql
SELECT DISTINCT post_id FROM reddit_schema.news
```
Gets all post_ids that already exist.

### Step 2: Filter DataFrame
```python
df_pg.filter(~col("post_id").isin(existing_ids))
```
Removes any records from the new data that already exist in the table.

### Step 3: Write only new records
```python
df_pg.write.mode("append").save()
```
Appends only the new records that don't exist.

### Step 4: Log deduplication
```
Found 50000 existing records in PostgreSQL
Records to write after deduplication: 2500
Skipping 47500 duplicate records
```

## Example Scenarios

### Scenario 1: Running job for the first time

```bash
# Run 1
python run_curation_job.py --job news
# Output: "Found 0 existing records"
# Writes: 50000 records

# PostgreSQL now has: 50000 records
```

### Scenario 2: Running job again the next day

```bash
# Run 2 (next day)
python run_curation_job.py --job news
# Output: "Found 50000 existing records"
# Input: 51000 new records from S3
# Writes: 1000 new records (only the new ones)

# PostgreSQL now has: 51000 records
```

### Scenario 3: Want fresh start/re-process

```bash
# Overwrite mode
python run_curation_job.py --job news --handle-duplicates overwrite
# Deletes all existing records
# Writes all records from S3

# PostgreSQL now has: Latest cleaned data
```

## Monitoring During Run

### Watch the logs

```bash
tail -f local_jobs/logs/news.log
```

Look for these messages:

**Successful skip:**
```
Found 50000 existing records in PostgreSQL
Records to write after deduplication: 1000
Skipping 49000 duplicate records
Successfully wrote data to PostgreSQL: reddit_schema.news
```

**First time write:**
```
Could not read existing records (table may not exist): 
Writing all records
Successfully wrote data to PostgreSQL: reddit_schema.news
```

**Overwrite mode:**
```
Overwrite mode: will delete all existing data in reddit_schema.news
Successfully wrote data to PostgreSQL: reddit_schema.news
```

## Troubleshooting

### Error: "Table reddit_schema.news doesn't exist"

**Solution 1:** Use skip mode on first run (recommended)
```bash
python run_curation_job.py --job news --handle-duplicates skip
# This will create the table automatically
```

**Solution 2:** Create table manually before running
```bash
docker exec -it reddit-postgres psql -U postgres -d reddit -c "
    CREATE TABLE reddit_schema.news (
        post_id VARCHAR(50) PRIMARY KEY,
        title TEXT,
        author VARCHAR(255),
        score INT,
        num_comments INT,
        created_utc TIMESTAMP,
        url TEXT,
        selftext TEXT
    );
"
```

### Error: "Could not read existing records"

This is normal on first run - the table doesn't exist yet. The job will write all records anyway.

Log message:
```
Could not read existing records (table may not exist): 
Writing all records
```

### Performance Issue: Script is slow on first run

The first run will be slow because:
1. It needs to count all records (passes through all data once)
2. It reads all existing post_ids from PostgreSQL
3. It deduplicates against all existing records

Subsequent runs are faster because there are fewer duplicates.

To speed up first run:
```bash
# Use overwrite on first load (if starting fresh)
python run_curation_job.py --job news --handle-duplicates overwrite

# Then use skip for subsequent runs
python run_curation_job.py --job news
```

## Scheduling Multiple Jobs

If you schedule jobs to run at different times:

```bash
# Daily cron job - safe to run every night
0 2 * * * python /home/steven/reddit-streaming/local_jobs/run_curation_job.py --job news

# Weekly reprocess - refresh all data
0 3 * * 0 python /home/steven/reddit-streaming/local_jobs/run_curation_job.py --job news --handle-duplicates overwrite
```

Safe because skip mode checks for duplicates each run.

## Advanced: Custom Duplicate Handling

You can modify the logic in `write_to_postgres()` function (around line 157 in `run_curation_job.py`).

### Current modes:

```python
if handle_duplicates == "skip":
    # Read existing IDs and filter them out
    existing_ids = set(...)
    df_pg = df_pg.filter(~col("post_id").isin(existing_ids))

elif handle_duplicates == "overwrite":
    # Delete all and write new data
    write_mode = "overwrite"
```

### To add custom logic:

Edit the function and add new conditions before the write:
```python
elif handle_duplicates == "my_custom_mode":
    # Your custom logic here
    df_pg = df_pg.filter(custom_condition)
```

Then use it:
```bash
python run_curation_job.py --job news --handle-duplicates my_custom_mode
```

## Summary

| Situation | Command | Result |
|-----------|---------|--------|
| First time | `--handle-duplicates skip` | Creates table, writes all data |
| Regular runs | `--handle-duplicates skip` | Skips existing, adds new |
| Re-process data | `--handle-duplicates overwrite` | Deletes all, writes fresh |
| Fixing errors | `--handle-duplicates overwrite` | Resets and rebuilds table |

**Default (recommended):** `--handle-duplicates skip` (safe, idempotent, can run anytime)

