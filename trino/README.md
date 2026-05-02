# Trino (PrestoDb) Setup for Reddit Streaming

This directory contains configuration files for Trino, an open-source distributed SQL query engine (fork of PrestoDb) that can query Delta Lake tables on S3.

## Overview

The Trino setup provides SQL query access to your Delta Lake tables stored in S3:
- **4 Raw tables**: technology_raw, programmerhumor_raw, news_raw, worldnews_raw
- **4 Clean tables**: technology_clean, programmerhumor_clean, news_clean, worldnews_clean

## Files

- `catalog/delta.properties` - Delta Lake connector configuration with S3 access
- `config.properties` - Trino coordinator configuration
- `node.properties` - Trino node settings
- `jvm.config` - JVM memory and GC settings
- `register_tables.sql` - SQL script to register all Delta tables
- `../register_trino_tables.sh` - Shell script to register tables automatically

## Starting Trino

1. **Start the Trino service:**
   ```bash
   cd /home/steven/reddit-streaming
   docker-compose up -d trino
   ```

2. **Register Delta tables:**
   ```bash
   bash register_trino_tables.sh
   ```

3. **Access Trino Web UI:**
   - Open browser to http://localhost:8089
   - View running queries and system status

## Querying Data

### Connect to Trino CLI:
```bash
docker exec -it reddit-trino trino --catalog delta --schema reddit
```

### Example Queries:

```sql
-- Show all tables
SHOW TABLES;

-- Count records in technology raw table
SELECT COUNT(*) FROM technology_raw;

-- Query recent technology posts
SELECT title, author, created_utc, score 
FROM technology_clean 
ORDER BY created_utc DESC 
LIMIT 10;

-- Compare raw vs clean record counts
SELECT 
  'technology' as subreddit,
  (SELECT COUNT(*) FROM technology_raw) as raw_count,
  (SELECT COUNT(*) FROM technology_clean) as clean_count;

-- Top authors by post count
SELECT author, COUNT(*) as post_count
FROM news_clean
GROUP BY author
ORDER BY post_count DESC
LIMIT 20;

-- Posts across all subreddits
SELECT 'technology' as subreddit, COUNT(*) as count FROM technology_clean
UNION ALL
SELECT 'programmerhumor', COUNT(*) FROM programmerhumor_clean
UNION ALL
SELECT 'news', COUNT(*) FROM news_clean
UNION ALL
SELECT 'worldnews', COUNT(*) FROM worldnews_clean;
```

## Connecting from Python

```python
from trino.dbapi import connect

conn = connect(
    host='localhost',
    port=8089,
    user='user',
    catalog='delta',
    schema='reddit',
)

cur = conn.cursor()
cur.execute('SELECT * FROM technology_clean LIMIT 10')
rows = cur.fetchall()
for row in rows:
    print(row)
```

## Connecting from R

```r
library(RJDBC)

drv <- JDBC("io.trino.jdbc.TrinoDriver",
            "/path/to/trino-jdbc.jar")
conn <- dbConnect(drv, 
                  "jdbc:trino://localhost:8089/delta/reddit",
                  "user", "")

data <- dbGetQuery(conn, "SELECT * FROM technology_clean LIMIT 10")
```

## Troubleshooting

### Check Trino logs:
```bash
docker logs reddit-trino
```

### Verify tables are registered:
```bash
docker exec -it reddit-trino trino --catalog delta --schema reddit --execute "SHOW TABLES;"
```

### Re-register a specific table:
```bash
docker exec -it reddit-trino trino --catalog delta --execute "
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'technology_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/technology'
);"
```

### Check S3 connectivity:
Ensure AWS credentials are properly mounted in the container and have read access to the S3 bucket.

## Resource Settings

Current memory allocation (for Raspberry Pi):
- Max query memory: 2GB
- Memory per node: 1GB
- JVM heap: 2GB

Adjust these in `config.properties` and `jvm.config` if running on a more powerful machine.

## Port

Trino is accessible on port **8089** (mapped from container port 8080).
