# Trino/PrestoDb Quick Start Guide

## What is Trino?

Trino (formerly PrestoDb) is a distributed SQL query engine that allows you to query your Delta Lake tables stored in S3 using standard SQL. This setup gives you 8 tables (4 raw + 4 clean) for the subreddits: technology, ProgrammerHumor, news, and worldnews.

## Quick Start

### 1. Start Trino
```bash
cd /home/steven/reddit-streaming
docker-compose up -d trino
```

Wait about 10-15 seconds for Trino to fully initialize.

### 2. Register Delta Tables
```bash
./register_trino_tables.sh
```

This registers all 8 tables (raw and clean versions of all 4 subreddits).

### 3. Query Your Data

**Connect to Trino CLI:**
```bash
docker exec -it reddit-trino trino --catalog delta --schema reddit
```

**Run queries:**
```sql
-- See all available tables
SHOW TABLES;

-- Query technology posts
SELECT title, author, score, created_utc 
FROM technology_clean 
ORDER BY score DESC 
LIMIT 10;

-- Count posts per subreddit
SELECT 
  'technology' as subreddit, COUNT(*) as posts FROM technology_clean
UNION ALL
SELECT 'programmerhumor', COUNT(*) FROM programmerhumor_clean
UNION ALL
SELECT 'news', COUNT(*) FROM news_clean
UNION ALL
SELECT 'worldnews', COUNT(*) FROM worldnews_clean;
```

### 4. Access Web UI
Open http://localhost:8089 in your browser to view the Trino Web UI.

## Available Tables

| Table Name | Type | S3 Location |
|------------|------|-------------|
| technology_raw | Raw | s3://reddit-streaming-stevenhurwitt-new/technology |
| technology_clean | Clean | s3://reddit-streaming-stevenhurwitt-new/technology_clean |
| programmerhumor_raw | Raw | s3://reddit-streaming-stevenhurwitt-new/ProgrammerHumor |
| programmerhumor_clean | Clean | s3://reddit-streaming-stevenhurwitt-new/ProgrammerHumor_clean |
| news_raw | Raw | s3://reddit-streaming-stevenhurwitt-new/news |
| news_clean | Clean | s3://reddit-streaming-stevenhurwitt-new/news_clean |
| worldnews_raw | Raw | s3://reddit-streaming-stevenhurwitt-new/worldnews |
| worldnews_clean | Clean | s3://reddit-streaming-stevenhurwitt-new/worldnews_clean |

## Common Commands

### Check if Trino is running:
```bash
docker ps | grep trino
```

### View Trino logs:
```bash
docker logs reddit-trino
```

### Stop Trino:
```bash
docker-compose stop trino
```

### Restart Trino:
```bash
docker-compose restart trino
```

### Connect from Python:
```bash
pip install trino
```

```python
from trino.dbapi import connect

conn = connect(
    host='localhost',
    port=8089,
    catalog='delta',
    schema='reddit',
)

cursor = conn.cursor()
cursor.execute('SELECT * FROM technology_clean LIMIT 5')
for row in cursor.fetchall():
    print(row)
```

## Useful Queries

### Get schema information:
```sql
DESCRIBE technology_clean;
```

### Recent posts with high engagement:
```sql
SELECT title, author, score, num_comments, created_utc
FROM technology_clean
WHERE score > 100
ORDER BY created_utc DESC
LIMIT 20;
```

### Top authors by total score:
```sql
SELECT author, 
       COUNT(*) as post_count,
       SUM(score) as total_score,
       AVG(score) as avg_score
FROM news_clean
GROUP BY author
ORDER BY total_score DESC
LIMIT 25;
```

### Posts created in the last 24 hours:
```sql
SELECT title, author, score, created_utc
FROM worldnews_clean
WHERE created_utc > CURRENT_TIMESTAMP - INTERVAL '24' HOUR
ORDER BY score DESC;
```

### Compare raw vs clean data:
```sql
SELECT 
  (SELECT COUNT(*) FROM technology_raw) as raw_count,
  (SELECT COUNT(*) FROM technology_clean) as clean_count,
  (SELECT COUNT(*) FROM technology_raw) - (SELECT COUNT(*) FROM technology_clean) as filtered_count;
```

## Troubleshooting

### Tables not showing up:
```bash
# Re-run the registration script
./register_trino_tables.sh
```

### Connection refused:
```bash
# Check if Trino is running
docker ps | grep trino

# If not running, start it
docker-compose up -d trino

# Wait 15 seconds, then try again
```

### AWS credentials error:
Make sure `aws_access_key.txt` and `aws_secret.txt` exist in the reddit-streaming directory with valid credentials.

### Memory issues (Raspberry Pi):
The configuration is optimized for Raspberry Pi with 2GB allocated. If you experience issues, you can reduce memory in `trino/jvm.config` and `trino/config.properties`.

## More Information

See [trino/README.md](trino/README.md) for detailed documentation.
