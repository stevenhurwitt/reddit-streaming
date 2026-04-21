# Reddit Streaming - PostgreSQL Curation Test Guide

This guide provides test scripts and troubleshooting steps for debugging PostgreSQL write issues in the curation pipeline.

## Quick Start

### 1. **Test PostgreSQL Connection First** (Recommended starting point)
```bash
cd local_jobs
python test_postgres_direct.py
```
This uses psycopg2 to test direct PostgreSQL connectivity without Spark complexity.

**Success indicators:**
- ✓ Connected successfully
- ✓ Can list tables in reddit_schema
- ✓ Insert test passes

**If this fails:** Your PostgreSQL server is not accessible. Fix networking/server issues before proceeding.

### 2. **Test Spark JDBC Connection**
```bash
python test_spark_jdbc.py
```
This tests if Spark can load the PostgreSQL driver and connect via JDBC.

**Success indicators:**
- ✓ PostgreSQL driver loads
- ✓ Test query returns result
- ✓ Can read existing tables

**If this fails:** JDBC driver issue - see troubleshooting section below.

### 3. **Full Integration Test**
```bash
python test_curation_postgres.py --job news
```
This is the complete test that:
- Reads news_clean Delta table from S3
- Tests PostgreSQL connectivity
- Writes sample data to PostgreSQL

**Options:**
```
--job news                      # Which subreddit (default: news)
--bucket BUCKET_NAME            # S3 bucket (default: reddit-streaming-stevenhurwitt-2)
--limit 100                     # Number of records to write (default: 100)
--db-host reddit-postgres       # PostgreSQL host (default: reddit-postgres)
--db-port 5434                  # PostgreSQL port (default: 5434)
--db-name reddit                # Database name (default: reddit)
--db-user postgres              # Database user (default: postgres)
--db-password secret!1234       # Database password
--skip-postgres-test            # Skip the connection test phase
```

**Example with custom host:**
```bash
python test_curation_postgres.py --job news --db-host localhost --db-port 5432
```

## Common Issues & Solutions

### Issue 1: PostgreSQL Connection Refused

**Error messages:**
```
Connection refused: Connection to {host}:{port} refused
psycopg2.OperationalError: could not connect to server
```

**Solutions:**
1. Check if PostgreSQL is running:
   ```bash
   # Docker
   docker ps | grep postgres
   
   # Linux/systemd
   sudo systemctl status postgresql
   ```

2. Verify PostgreSQL is listening on the correct port:
   ```bash
   # Check listening ports
   netstat -tlnp | grep postgres
   ss -tlnp | grep 5434
   ```

3. Check PostgreSQL logs for errors:
   ```bash
   # Docker
   docker logs <postgres_container_id>
   
   # Linux
   sudo tail -f /var/log/postgresql/postgresql.log
   ```

4. Verify network connectivity:
   ```bash
   # From host to PostgreSQL
   telnet reddit-postgres 5434
   # or
   nc -zv reddit-postgres 5434
   ```

### Issue 2: Authentication Failed

**Error messages:**
```
FATAL: password authentication failed for user
FATAL: Ident authentication failed
```

**Solutions:**
1. Verify credentials are correct:
   ```bash
   # Test with correct password
   psql -h reddit-postgres -U postgres -d reddit
   ```

2. Check PostgreSQL configuration (`pg_hba.conf`):
   ```bash
   # Find postgres config
   sudo find / -name pg_hba.conf
   
   # Should have entry like:
   # host    all     all     127.0.0.1/32    md5
   # host    all     all     ::1/128         md5
   ```

3. For Docker, check environment variables:
   ```bash
   docker exec <postgres_container> env | grep POSTGRES
   ```

### Issue 3: JDBC Driver Not Found

**Error messages:**
```
java.lang.ClassNotFoundException: org.postgresql.Driver
No suitable driver found for jdbc:postgresql://
```

**Solutions:**
1. Set Spark to download JDBC driver automatically:
   ```python
   .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
   ```

2. Or pre-download and add to jars:
   ```bash
   # Download JDBC driver
   cd ~/.local/share/spark/jars
   wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
   ```

3. Test JDBC driver with dedicated script:
   ```bash
   python test_spark_jdbc.py
   ```

### Issue 4: Table Not Found / Schema Issues

**Error messages:**
```
ERROR: relation "reddit_schema.news" does not exist
ERROR: schema "reddit_schema" does not exist
```

**Solutions:**
1. Create schema if missing:
   ```bash
   python test_postgres_direct.py  # This will auto-create schema
   ```

2. Create table manually from SQL:
   ```sql
   CREATE SCHEMA IF NOT EXISTS reddit_schema;
   
   CREATE TABLE reddit_schema.news (
       post_id VARCHAR(50) PRIMARY KEY,
       title TEXT,
       author VARCHAR(100),
       score INT,
       num_comments INT,
       created_utc TIMESTAMP,
       url TEXT,
       selftext TEXT
   );
   ```

3. Verify table structure matches DataFrame schema - run test script and compare:
   ```bash
   python test_curation_postgres.py --job news --limit 10
   ```

### Issue 5: S3 Connection or Delta Table Issues

**Error messages:**
```
No such file or directory: s3a://bucket/news_clean
Unable to read Delta table
```

**Solutions:**
1. Verify S3 path exists:
   ```bash
   # Check bucket contents
   aws s3 ls s3://reddit-streaming-stevenhurwitt-2/news_clean/
   ```

2. Verify AWS credentials are loaded:
   ```bash
   # Test with boto3
   python -c "import boto3; s3 = boto3.client('s3'); print(s3.list_buckets())"
   ```

3. Check AWS credentials format:
   ```bash
   cat aws_access_key.txt  # Should be just the key, no extra whitespace
   cat aws_secret.txt      # Should be just the secret, no extra whitespace
   ```

4. For Docker/remote, verify credentials files are mounted/available

### Issue 6: Memory Issues

**Error messages:**
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
ExecutorLostFailure
```

**Solutions:**
1. Increase memory allocation - edit test script:
   ```python
   .config("spark.driver.memory", "4g")      # Increase from 3g
   .config("spark.executor.memory", "4g")    # Increase from 2g
   ```

2. Reduce batch size:
   ```bash
   python test_curation_postgres.py --limit 10  # Start small
   ```

3. Check available system memory:
   ```bash
   free -h
   # or
   vmstat -h
   ```

## Debugging Steps (Systematic Approach)

Follow these steps in order:

1. **Check Java**
   ```bash
   java -version
   ```

2. **Test PostgreSQL directly**
   ```bash
   python test_postgres_direct.py --host reddit-postgres
   ```
   
3. **Test Spark JDBC**
   ```bash
   python test_spark_jdbc.py --host reddit-postgres
   ```

4. **Test with Spark + S3**
   ```bash
   python test_curation_postgres.py --job news --limit 10
   ```

5. **Test with more data**
   ```bash
   python test_curation_postgres.py --job news --limit 1000
   ```

6. **Run actual curation job**
   ```bash
   python run_curation_job.py --job news
   ```

## Environment Variable Setup

These environment variables may help with debugging:

```bash
# Enable debug logging
export SPARK_LOG_LEVEL=DEBUG

# Set Java home if needed
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# For network debugging
export PYTHONUNBUFFERED=1
```

## Common Configuration Patterns

### Local PostgreSQL (Docker Compose)
```bash
python test_curation_postgres.py \
  --db-host localhost \
  --db-port 5432 \
  --db-password your_password
```

### Remote PostgreSQL on AWS RDS
```bash
python test_curation_postgres.py \
  --db-host reddit-db-instance.amazonaws.com \
  --db-port 5432 \
  --db-user postgres_user \
  --db-password your_password
```

### Docker Network PostgreSQL
```bash
python test_curation_postgres.py \
  --db-host postgres_service_name \
  --db-port 5432
```

## Monitoring Database Growth

After successful writes, monitor table size:

```bash
# Direct connection
python -c "
import psycopg2
conn = psycopg2.connect(host='reddit-postgres', port=5434, database='reddit', user='postgres', password='secret!1234')
cursor = conn.cursor()
cursor.execute('SELECT table_name, pg_size_pretty(pg_total_relation_size(schemaname||'\"'\"'.'\"'\"'||tablename)) as size FROM pg_tables WHERE schemaname = \"reddit_schema\" ORDER BY pg_total_relation_size(schemaname||'\"'\"'.'\"'\"'||tablename) DESC;')
for row in cursor.fetchall():
    print(f'{row[0]}: {row[1]}')
cursor.close()
conn.close()
"
```

## Next Steps

Once all tests pass:

1. Run the actual curation job:
   ```bash
   python run_curation_job.py --job news
   ```

2. Monitor logs:
   ```bash
   tail -f logs/news.log
   ```

3. Verify data in PostgreSQL:
   ```bash
   python test_postgres_direct.py --user postgres
   # Check row counts in the output
   ```

4. Set up automated jobs (cron/scheduler):
   ```bash
   python setup_cron.sh
   ```

## Support

For additional help:
- Check main README.md
- Review GETTING_STARTED.md
- Check logs in `logs/` directory
- Review SERVICE_MANAGEMENT.md for infrastructure details
