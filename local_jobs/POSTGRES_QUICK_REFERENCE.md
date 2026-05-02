# PostgreSQL Curation Test Scripts - Quick Reference

## Script Overview

| Script | Purpose | When to Use |
|--------|---------|------------|
| `test_postgres_direct.py` | Test PostgreSQL connection with psycopg2 | **Start here** - Basic connectivity test |
| `test_spark_jdbc.py` | Test Spark JDBC driver and connection | Verify JDBC driver is loaded and working |
| `test_curation_postgres.py` | Full integration test (S3 → Delta → PostgreSQL) | Complete end-to-end test |
| `diagnose_postgres_write.py` | Step-by-step diagnostic tool | When you're getting specific write errors |

## Quick Start Flowchart

```
┌─────────────────────────────────────────┐
│  Start: Having PostgreSQL write issues? │
└────────────────┬────────────────────────┘
                 │
                 ▼
    ┌────────────────────────────┐
    │ test_postgres_direct.py    │  Test direct connection
    │ (psycopg2 - simplest)     │
    └────────┬───────────────────┘
             │ Success?
             ├─ Yes ──────────────┐
             │                    │
             │ No ──────────────┐ │
             │                │ │
             ▼                │ │
    ┌──────────────────────┐ │ │
    │ Fix PostgreSQL       │ │ │
    │ - Check if running   │ │ │
    │ - Verify networking  │ │ │
    │ - Check credentials  │ │ │
    └──────────────────────┘ │ │
                             │ │
                             ▼ ▼
                    ┌─────────────────────┐
                    │ test_spark_jdbc.py  │  Test JDBC driver
                    └────────┬────────────┘
                             │ Success?
                             ├─ Yes ──────────────┐
                             │                    │
                             │ No ──────────────┐ │
                             │                │ │
                             ▼                │ │
                    ┌──────────────────────┐ │ │
                    │ Fix JDBC             │ │ │
                    │ - Reinstall driver   │ │ │
                    │ - Check Spark config │ │ │
                    └──────────────────────┘ │ │
                                             │ │
                                             ▼ ▼
                        ┌──────────────────────────────────┐
                        │ test_curation_postgres.py        │ Full test
                        │ (S3 → Delta → PostgreSQL)       │
                        └────────┬─────────────────────────┘
                                 │ Success?
                                 ├─ Yes ──────────────┐
                                 │                    │
                                 │ No ──────────────┐ │
                                 │                │ │
                                 ▼                │ │
                        ┌──────────────────────┐ │ │
                        │ diagnose_postgres   │ │ │
                        │ _write.py            │ │ │
                        │ (detailed steps)     │ │ │
                        └──────────────────────┘ │ │
                                                 │ │
                                                 ▼ ▼
                                    ┌────────────────────────┐
                                    │ Success! Run:         │
                                    │ python               │
                                    │ run_curation_job.py  │
                                    │ --job news           │
                                    └────────────────────────┘
```

## Running the Tests

### Test 1: Direct PostgreSQL Connection (Recommended First)

```bash
cd local_jobs

# Basic test with defaults
python test_postgres_direct.py

# With custom host/port
python test_postgres_direct.py --host localhost --port 5432 --password your_password

# Full options
python test_postgres_direct.py \
  --host reddit-postgres \
  --port 5434 \
  --db reddit \
  --user postgres \
  --password secret!1234
```

**What it checks:**
- ✓ psycopg2 module available
- ✓ Can connect to PostgreSQL
- ✓ postgres schema exists
- ✓ Can list tables
- ✓ Insert capability

### Test 2: Spark JDBC Connection

```bash
# Basic test
python test_spark_jdbc.py

# With custom host
python test_spark_jdbc.py --host localhost --port 5432
```

**What it checks:**
- ✓ Java is installed
- ✓ Spark session can be created
- ✓ PostgreSQL JDBC driver loads
- ✓ Can execute JDBC queries
- ✓ Can read from PostgreSQL tables

### Test 3: Full Integration Test

```bash
# Test with news subreddit (default)
python test_curation_postgres.py --job news

# Test with limited records (faster)
python test_curation_postgres.py --job news --limit 100

# Test with different host
python test_curation_postgres.py --job news --db-host localhost --db-port 5432

# Skip PostgreSQL test phase
python test_curation_postgres.py --job news --skip-postgres-test

# Full options
python test_curation_postgres.py \
  --job news \
  --bucket reddit-streaming-stevenhurwitt-2 \
  --limit 100 \
  --db-host reddit-postgres \
  --db-port 5434 \
  --db-name reddit \
  --db-user postgres \
  --db-password secret!1234
```

**What it checks:**
- ✓ S3 connectivity
- ✓ Delta table can be read
- ✓ PostgreSQL connection
- ✓ Table exists with correct schema
- ✓ Can write sample data

### Test 4: Detailed Diagnostics

```bash
# Run all diagnostic tests
python diagnose_postgres_write.py --job news

# With custom configuration
python diagnose_postgres_write.py \
  --job news \
  --db-host localhost \
  --db-port 5432
```

**What it checks (step by step):**
1. Python dependencies (boto3, psycopg2, pyspark, delta)
2. Java installation
3. AWS credentials
4. S3 bucket access
5. Delta table on S3
6. Spark session creation
7. Delta table reading
8. PostgreSQL basic connection
9. Spark JDBC connection
10. Test data write

## Common Command Patterns

### For Docker-based PostgreSQL (local)
```bash
python test_postgres_direct.py \
  --host localhost \
  --port 5432

python test_curation_postgres.py --job news \
  --db-host localhost \
  --db-port 5432
```

### For Named Service (Docker Compose)
```bash
python test_postgres_direct.py \
  --host postgres_service_name \
  --port 5432

python test_curation_postgres.py --job news \
  --db-host postgres_service_name
```

### For AWS RDS
```bash
python test_postgres_direct.py \
  --host reddit-db.*.amazonaws.com \
  --port 5432 \
  --user postgres_user \
  --password your_password

python test_curation_postgres.py --job news \
  --db-host reddit-db.*.amazonaws.com \
  --db-port 5432 \
  --db-user postgres_user \
  --db-password your_password
```

### For Remote SSH Host with Port Forward
```bash
# First, create SSH tunnel
ssh -L 5432:remote-host:5432 user@jump-host

# Then run test
python test_postgres_direct.py --host localhost --port 5432
```

## Interpreting Results

### Successful Output Example
```
✓ Connected successfully!

PostgreSQL version: PostgreSQL 14.5 (Ubuntu 14.5-1.pgdg22.04+1)

Checking reddit_schema tables...
✓ reddit_schema exists

Tables in reddit_schema:
  Table: news
    - post_id: character varying (NOT NULL)
    - title: text (NULL)
    ...
    - Rows: 1250
```

### Error Output Examples

**Connection Refused**
```
✗ Connection failed: Connection to reddit-postgres:5434 refused

Troubleshooting:
1. Verify PostgreSQL is running
2. Check network connectivity: ping reddit-postgres
```

**Authentication Failed**
```
✗ Connection failed: password authentication failed for user "postgres"

Troubleshooting:
1. Verify credentials are correct
2. Check PostgreSQL configuration (pg_hba.conf)
```

**JDBC Driver Not Found**
```
✗ Spark JDBC connection failed: java.lang.ClassNotFoundException: org.postgresql.Driver

Troubleshooting:
1. JDBC driver needs to be downloaded
2. This usually happens automatically with docker
```

## Environment Variables

```bash
# Enable debug logging
export SPARK_LOG_LEVEL=DEBUG

# Set Java home if not detected automatically
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Unbuffered Python output
export PYTHONUNBUFFERED=1

# Then run the test
python test_curation_postgres.py --job news
```

## Monitoring Progress

Watch logs while tests are running:

```bash
# In another terminal
tail -f logs/curation.log
```

## After Tests Pass

Once all tests pass successfully:

```bash
# Run the actual curation job
python run_curation_job.py --job news

# Monitor the job
tail -f logs/news.log

# Verify data in PostgreSQL
python -c "
import psycopg2
conn = psycopg2.connect(host='reddit-postgres', port=5434, database='reddit', 
                        user='postgres', password='secret!1234')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM reddit_schema.news')
print(f'Total news records: {cursor.fetchone()[0]}')
cursor.close()
conn.close()
"

# Set up automated jobs (optional)
python setup_cron.sh
```

## Troubleshooting Tips

### If Java not found
```bash
# Install Java
sudo apt-get install default-jdk

# Or set JAVA_HOME
export JAVA_HOME=$(dirname $(dirname $(which javac)))
python test_spark_jdbc.py
```

### If psycopg2 not installed
```bash
# Install psycopg2
pip install psycopg2-binary

# Or use apt
sudo apt-get install python3-psycopg2
```

### If PostgreSQL server issues
```bash
# Check server status
sudo systemctl status postgresql

# View logs
sudo tail -f /var/log/postgresql/postgresql-*.log

# Or for Docker
docker logs <container_name>
docker exec <container_name> psql -U postgres -d reddit -c "SELECT version();"
```

### If Spark memory issues
Edit the test script and increase memory:
```python
.config("spark.driver.memory", "4g")      # Increase from 2g
.config("spark.executor.memory", "3g")    # Increase from 2g
```

## Script Dependencies

### Python Requirements
```
boto3>=1.26.0           # AWS SDK
pyspark>=3.1.0          # Spark
delta-spark>=3.2.0      # Delta Lake
psycopg2-binary>=2.9.0  # PostgreSQL (optional but recommended)
```

Install with:
```bash
pip install -r requirements.txt
```

### System Requirements
- Java 8 or higher
- Python 3.7+
- Network access to PostgreSQL
- Network access to S3
- Valid AWS credentials

## Getting Help

If tests are still failing:

1. **Check logs**: `tail -f logs/news.log`
2. **Run diagnose tool**: `python diagnose_postgres_write.py`
3. **Read full guide**: See `POSTGRES_TESTING_GUIDE.md`
4. **Check main docs**: See `README.md` and `SERVICE_MANAGEMENT.md`
