# Running Tests on Docker Compose Spark Cluster

This guide explains how to run the PostgreSQL curation tests on your Docker Compose Spark cluster instead of locally.

## Overview

Your Docker Compose stack includes:
- **Spark Master**: `spark-master` (port 7077 for submissions, 8085 for UI)
- **Spark Workers**: 2 workers with 4 cores and 2GB memory each
- **PostgreSQL**: `reddit-postgres` (port 5432 internally, 5434 externally)
- **Shared Workspace**: `/opt/workspace/`

## Quick Start

### Option 1: Using the Helper Script (Easiest)

```bash
cd /home/steven/reddit-streaming

# Run diagnostics
./run_tests_docker.sh diagnose

# Or test individual components
./run_tests_docker.sh test postgres    # Test PostgreSQL only
./run_tests_docker.sh test jdbc        # Test JDBC driver
./run_tests_docker.sh test full        # Full integration test

# Run actual curation job
./run_tests_docker.sh curation --job news

# Use spark-submit for distributed processing
./run_tests_docker.sh submit --job news
```

### Option 2: Direct Docker Exec Commands

```bash
# Test PostgreSQL (inside Docker network)
docker exec -it reddit-spark-master \
  python /opt/workspace/local_jobs/test_postgres_direct.py \
  --host reddit-postgres

# Full integration test
docker exec -it reddit-spark-master \
  python /opt/workspace/local_jobs/test_curation_postgres.py \
  --job news \
  --db-host reddit-postgres

# Run diagnostics
docker exec -it reddit-spark-master \
  python /opt/workspace/local_jobs/diagnose_postgres_write.py \
  --job news \
  --db-host reddit-postgres

# Run actual curation job
docker exec -it reddit-spark-master \
  python /opt/workspace/local_jobs/run_curation_job.py \
  --job news \
  --spark-master spark://spark-master:7077
```

## Docker Compose Service Architecture

```
┌─────────────────────────────────────────────────────┐
│           Docker Compose Network (redis)            │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌──────────────────────────────────────────┐     │
│  │         Spark Master Container           │     │
│  │  - Python, Spark, JDBC driver installed │     │
│  │  - Local jobs mounted at /opt/workspace │     │
│  │  - Spark UI: localhost:8085              │     │
│  └──────────────────────────────────────────┘     │
│                        │                             │
│         ┌──────────────┴──────────────┐            │
│         │                             │            │
│  ┌──────▼────────────┐      ┌────────▼─────────┐ │
│  │ Spark Worker 1    │      │ Spark Worker 2   │ │
│  │ 4 cores, 2GB      │      │ 4 cores, 2GB     │ │
│  └───────────────────┘      └──────────────────┘ │
│                                                     │
│  ┌──────────────────────────────────────────┐     │
│  │   PostgreSQL Container                   │     │
│  │   - Port 5432 (internal)                 │     │
│  │   - Port 5434 (forwarded to host)       │     │
│  │   - reddit_schema exists                 │     │
│  └──────────────────────────────────────────┘     │
│                                                     │
└─────────────────────────────────────────────────────┘
```

## Network Details

### From Inside Container (Container-to-Container)
- PostgreSQL: `reddit-postgres:5432` (DNS resolution in Docker network)
- Spark Master: `spark-master:7077`
- Shared files: `/opt/workspace/`

### From Host Machine
- PostgreSQL: `localhost:5434` (port forwarded)
- Spark HTTP UI: `localhost:8085`
- Tests must connect to `localhost:5434` NOT `5432`

## Step-by-Step Walkthrough

### 1. Start Docker Compose Services

```bash
cd /home/steven/reddit-streaming
docker-compose up -d

# Verify services are running
docker-compose ps

# Expected output:
# NAME                  STATUS
# reddit-spark-master   Up
# reddit-spark-worker-1 Up
# reddit-spark-worker-2 Up
# reddit-postgres       Up
```

### 2. Run Tests Inside Container

**Best approach:** Execute tests inside the spark-master container where all dependencies are pre-installed.

```bash
# Option A: Run full diagnostics (comprehensive)
./run_tests_docker.sh diagnose --job news

# Option B: Test individual components
./run_tests_docker.sh test postgres   # Quick connectivity test
./run_tests_docker.sh test jdbc       # Check JDBC driver loads
./run_tests_docker.sh test full       # Full pipeline test

# Option C: Manual command
docker exec -it reddit-spark-master bash -c "
    cd /opt/workspace/local_jobs
    python diagnose_postgres_write.py --job news --db-host reddit-postgres
"
```

### 3. Monitor Spark Cluster

```bash
# View Spark UI
# Open browser: http://localhost:8085

# Check cluster status
./run_tests_docker.sh monitor

# View logs
./run_tests_docker.sh logs

# Or manually
docker logs reddit-spark-master
```

### 4. Run Curation Job

Once tests pass, run the actual job:

```bash
# Option A: Local mode (single node)
./run_tests_docker.sh curation --job news

# Option B: Distributed via spark-submit
./run_tests_docker.sh submit --job news

# Option C: Manual spark-submit with custom settings
docker exec -it reddit-spark-master bash -c "
    cd /opt/workspace/local_jobs
    spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --driver-memory 2g \
        --executor-memory 2g \
        --num-executors 2 \
        run_curation_job.py --job news
"
```

## Command Reference

### Using Helper Script

```bash
# Diagnostics
./run_tests_docker.sh diagnose             # Full diagnostics
./run_tests_docker.sh diagnose --job tech  # For technology subreddit

# Testing
./run_tests_docker.sh test postgres        # Test PostgreSQL only
./run_tests_docker.sh test jdbc            # Test JDBC driver only
./run_tests_docker.sh test full            # Complete integration test
./run_tests_docker.sh test full --limit 10 # With smaller sample

# Running jobs
./run_tests_docker.sh curation             # Curation in local mode
./run_tests_docker.sh submit               # Curation via spark-submit
./run_tests_docker.sh submit-streaming     # Streaming mode (4 executors)

# Utilities
./run_tests_docker.sh shell                # Interactive bash in container
./run_tests_docker.sh monitor              # Show cluster info
./run_tests_docker.sh logs                 # Show container logs
```

### Direct Docker Exec Commands

```bash
# PostgreSQL test
docker exec -it reddit-spark-master \
  python /opt/workspace/local_jobs/test_postgres_direct.py \
  --host reddit-postgres

# Spark JDBC test
docker exec -it reddit-spark-master \
  python /opt/workspace/local_jobs/test_spark_jdbc.py \
  --host reddit-postgres

# Full integration test
docker exec -it reddit-spark-master \
  python /opt/workspace/local_jobs/test_curation_postgres.py \
  --job news \
  --db-host reddit-postgres \
  --limit 100

# Diagnostics
docker exec -it reddit-spark-master \
  python /opt/workspace/local_jobs/diagnose_postgres_write.py \
  --job news \
  --db-host reddit-postgres

# Curation job (local mode)
docker exec -it reddit-spark-master bash -c "
    cd /opt/workspace/local_jobs && \
    python run_curation_job.py --job news --spark-master spark://spark-master:7077
"

# Curation job (spark-submit)
docker exec -it reddit-spark-master bash -c "
    cd /opt/workspace/local_jobs && \
    spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 2g \
        --executor-memory 2g \
        --num-executors 2 \
        run_curation_job.py --job news
"

# Interactive shell
docker exec -it reddit-spark-master bash
```

## Common Issues & Solutions

### Issue 1: "Connection refused" to PostgreSQL

**Problem:** Container can't connect to PostgreSQL

**Solution:**
```bash
# Make sure postgres container is running
docker ps | grep reddit-postgres

# If not running, start it
docker-compose up -d reddit-postgres

# Test connectivity from host
docker exec -it reddit-postgres psql -U postgres -d reddit -c "SELECT 1"
```

### Issue 2: "JDBC driver not found"

**Problem:** PostgreSQL JDBC driver not available in Spark

**Solution:**
```bash
# Spark master should have JDBC pre-installed
# If not, check the Spark container has the jar
docker exec reddit-spark-master find /opt/spark -name "*postgresql*"

# If missing, the script will auto-download it
./run_tests_docker.sh test jdbc
```

### Issue 3: Tests hang or timeout

**Problem:** Tests running too slowly or hanging

**Solution:**
```bash
# Run with smaller dataset
./run_tests_docker.sh test full --limit 10

# Check Spark web UI for stuck jobs
# http://localhost:8085

# Check container resources
docker stats reddit-spark-master

# Increase container memory in docker-compose.yaml
# Then restart: docker-compose restart reddit-spark-master
```

### Issue 4: Port forwarding issues

**Problem:** Can't access services from host

**Solution:**
```bash
# Check Docker port forwarding
docker ps --format "table {{.Names}}\t{{.Ports}}"

# Expected output:
# reddit-spark-master  0.0.0.0:8085->8080/tcp, 0.0.0.0:7081->7077/tcp
# reddit-postgres      0.0.0.0:5434->5432/tcp

# If ports not forwarding, restart containers
docker-compose restart
```

## Performance Tuning

### Increase Spark Resources

Edit `docker-compose.yaml`:

```yaml
spark-worker-1:
  environment:
    - SPARK_WORKER_CORES=8        # Increase from 4
    - SPARK_WORKER_MEMORY=4g      # Increase from 2g
```

Then:
```bash
docker-compose restart spark-worker-1 spark-worker-2
```

### Increase PostgreSQL Resources

Edit `docker-compose.yaml`:

```yaml
reddit-postgres:
  environment:
    - POSTGRES_MAX_CONNECTIONS=200
    - POSTGRES_SHARED_BUFFERS=256MB
```

### Run spark-submit with More Executors

```bash
docker exec -it reddit-spark-master bash -c "
    cd /opt/workspace/local_jobs && \
    spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 3g \
        --executor-memory 2g \
        --num-executors 4 \
        --executor-cores 2 \
        run_curation_job.py --job news
"
```

## Monitoring & Debugging

### View Spark Web UI

```
http://localhost:8085
```

Shows:
- Running applications
- Worker status
- Memory usage
- Application details

### View Application Logs

```bash
# While job is running
docker logs -f reddit-spark-master

# Or access via Spark UI -> Application details
```

### Query PostgreSQL from Container

```bash
docker exec -it reddit-postgres psql -U postgres -d reddit -c "
    SELECT table_name, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
    FROM pg_tables 
    WHERE schemaname = 'reddit_schema'
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
"
```

### Interactive Shell in Container

```bash
# Open bash in spark-master
./run_tests_docker.sh shell

# Or manually
docker exec -it reddit-spark-master bash

# Then you can run Python commands interactively
cd /opt/workspace/local_jobs
python test_postgres_direct.py --host reddit-postgres
```

## Common Workflow

### Complete Workflow: Setup → Test → Run

```bash
# 1. Start services
docker-compose up -d
sleep 10  # Wait for services to start

# 2. Run diagnostics
./run_tests_docker.sh diagnose --job news

# 3. If diagnostics pass, run curation job
./run_tests_docker.sh curation --job news

# 4. Verify data was written
docker exec -it reddit-postgres psql -U postgres -d reddit -c "
    SELECT COUNT(*) as row_count FROM reddit_schema.news
"

# 5. Check logs
./run_tests_docker.sh logs

# 6. Clean up (if needed)
docker-compose down
```

### In-Place Development Cycle

```bash
# 1. Make changes to test_curation_postgres.py (on host)
vim local_jobs/test_curation_postgres.py

# 2. Changes are immediately available in container (mounted volume)

# 3. Test changes
./run_tests_docker.sh test full --job news

# 4. Repeat
```

## Environment Variables in Docker

These are already set in spark-master container:

```bash
# AWS credentials (from mounted files)
AWS_ACCESS_KEY_ID=/opt/workspace/aws_access_key.txt
AWS_SECRET_ACCESS_KEY=/opt/workspace/aws_secret.txt

# Spark configuration
SPARK_HOME=/opt/spark
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

If you need to set additional variables:

```bash
docker exec -it reddit-spark-master bash -c "
    export CUSTOM_VAR=value && \
    python /opt/workspace/local_jobs/test_postgres_direct.py --host reddit-postgres
"
```

## Docker Compose vs Local Mode

| Aspect | Local | Docker Compose |
|--------|-------|----------------|
| Java required | Yes | No (in container) |
| Dependencies | Manual install | Pre-installed |
| PostgreSQL | Must run separately | Part of stack |
| Spark cluster | N/A | 1 master + 2 workers |
| Network simple | Yes | Use service names |
| Scalability | Single machine | Can add workers |
| Debugging | Direct access | Via docker exec |
| Logs | Local files | Docker logs |

## Troubleshooting Summary

| Problem | Command to Fix |
|---------|----------------|
| Services not running | `docker-compose up -d` |
| Can't find files | Check `/opt/workspace/` in container |
| PostgreSQL connection fails | Test with `./run_tests_docker.sh test postgres` |
| JDBC driver missing | Run `./run_tests_docker.sh test jdbc` |
| Spark cluster issues | View `http://localhost:8085` |
| Container hung | `docker-compose restart reddit-spark-master` |
| High memory usage | Check `docker stats` and increase resources |

## Next Steps

1. **Start services:** `docker-compose up -d`
2. **Run diagnostics:** `./run_tests_docker.sh diagnose --job news`
3. **Check results** - if pass, proceed to step 4
4. **Run curation:** `./run_tests_docker.sh curation --job news`
5. **Verify data:** Query PostgreSQL for results

See [POSTGRES_QUICK_REFERENCE.md](local_jobs/POSTGRES_QUICK_REFERENCE.md) for general testing info.
