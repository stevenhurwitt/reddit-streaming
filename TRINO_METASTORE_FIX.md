# Trino Metastore Error - Troubleshooting Guide

## Problem

When trying to register Delta Lake tables in Trino, you encountered:
```
Query 20260312_200756_00012_mmasp failed: Could not read database schema from /etc/trino/catalog/metastore/.reddit.trinoSchema
```

## Root Cause

This error occurs because:
1. **File metastore needs proper directory permissions** - The `/etc/trino/catalog/metastore` directory didn't exist or wasn't writable
2. **AWS credentials weren't properly exported** - Environment variables weren't being set correctly in the container
3. **Schema must be created before registering tables** - You can't register tables  to a schema that doesn't exist

## Solutions Applied

### 1. Added Persistent Volume for Metastore Data
Updated [docker-compose.yaml](docker-compose.yaml) to include:
- A named volume `trino-metastore` for persistent storage
- Mounted to `/data/trino` in the container with proper permissions

### 2. Fixed AWS Credentials Export
Changed the container command to properly read and export AWS credentials from Docker secrets:
```bash
export AWS_ACCESS_KEY_ID=$(cat /run/secrets/aws_access_key_id | tr -d '\n' | tr -d '\r');
export AWS_SECRET_ACCESS_KEY=$(cat /run/secrets/aws_secret_access_key | tr -d '\n' | tr -d '\r');
```

### 3. Updated Metastore Path
Changed [trino/catalog/delta.properties](trino/catalog/delta.properties) to use the writable volume:
```properties
hive.metastore.catalog.dir=/data/trino/metastore
```

## How to Fix

### Step 1: Ensure Trino is Running
```bash
docker ps | grep trino
```

You should see it as "healthy" or at least "starting".

### Step 2: Wait for Trino to Fully Start
```bash
# Check if Trino has finished starting
curl -s http://localhost:8089/v1/info | grep '"starting":false'
```

### Step 3: Create the Reddit Schema
Run the setup script:
```bash
./setup_trino_schema.sh
```

This script will:
- Wait for Trino to be fully ready
- Create the `reddit` schema with S3 location
- Verify the schema was created

### Step 4: Register Tables
After the schema is created, register your Delta tables:
```bash
./register_trino_tables.sh
```

OR register manually:
```bash
docker exec -i reddit-trino trino --catalog delta --schema reddit 
```

Then run:
```sql
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'technology_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/technology'
);
```

## Verification

### Check Schema Exists
```bash
docker exec -i reddit-trino trino --catalog delta --execute "SHOW SCHEMAS;"
```

Should show `reddit` in the list.

### Check Tables (after registration)
```bash
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "SHOW TABLES;"
```

### Query Data
```bash
docker exec -it reddit-trino trino --catalog delta --schema reddit
```

Then:
```sql
SELECT * FROM technology_raw LIMIT 5;
```

## Common Issues

### Issue: "Could not read database schema"
- **Cause:** Schema doesn't exist yet
- **Fix:** Run `./setup_trino_schema.sh` first

### Issue: Queries hang or timeout
- **Cause:** AWS credentials not loaded, can't access S3
- **Fix:** Verify credentials are exported:
  ```bash
  docker exec reddit-trino bash -c "echo \$AWS_ACCESS_KEY_ID | head -c 10"
  ```
  Should show the first 10 characters (not empty)

### Issue: "HIVE_METASTORE_ERROR"
- **Cause:** Metastore directory not writable or doesn't exist
- **Fix:** Recreate container:
  ```bash
  docker-compose stop trino && docker-compose rm -f trino && docker-compose up -d trino
  ```

### Issue: Container keeps restarting
- **Cause:** Configuration error
- **Fix:** Check logs:
  ```bash
  docker logs reddit-trino 2>&1 | tail -50
  ```

## Files Modified

1. [docker-compose.yaml](docker-compose.yaml) - Added volume and fixed AWS credential export
2. [trino/catalog/delta.properties](trino/catalog/delta.properties) - Updated metastore path
3. [setup_trino_schema.sh](setup_trino_schema.sh) - New helper script for schema creation

## Testing the Setup

Run this quick test:
```bash
# 1. Check Trino is healthy
docker ps | grep trino

# 2. Check Trino API
curl -s http://localhost:8089/v1/info | grep '"state":"ACTIVE"'

# 3. Create schema
./setup_trino_schema.sh

# 4. Register a test table
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'technology_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/technology'
);"

# 5. Verify
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "SHOW TABLES;"
```

If all steps succeed, your Trino setup is working correctly!
