# Trino Debugging Summary

## Issues Fixed

### 1. **JVM Configuration Error** (FIXED)
**Error:**
```
Unrecognized VM option 'UseBiasedLocking'
Error: Could not create the Java Virtual Machine.
```

**Cause:** The `-XX:-UseBiasedLocking` JVM option was deprecated in Java 15 and removed in Java 17+. Latest Trino uses Java 25.

**Fix:** Removed the line from [trino/jvm.config](trino/jvm.config):
```diff
- -XX:-UseBiasedLocking
```

---

### 2. **Defunct Configuration Properties** (FIXED)
**Error:**
```
1) Defunct property 'query.max-total-memory-per-node' cannot be configured.
2) Configuration property 'discovery-server.enabled' was not used
```

**Cause:** These properties are no longer supported in recent Trino versions.

**Fix:** Updated [trino/config.properties](trino/config.properties):
```diff
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080

# Query settings
query.max-memory=2GB
query.max-memory-per-node=1GB
- query.max-total-memory-per-node=1GB
- discovery-server.enabled=true
```

---

### 3. **S3 Configuration Properties** (FIXED)
**Error:**
```
1) Configuration property 'hive.s3.aws-access-key' was not used
2) Configuration property 'hive.s3.aws-secret-key' was not used
3) Configuration property 'hive.s3.endpoint' was not used
4) Configuration property 'hive.s3.path-style-access' was not used
```

**Cause:** S3 property names changed in newer Trino versions with Delta Lake connector.

**Fix:** Updated [trino/catalog/delta.properties](trino/catalog/delta.properties):
```diff
connector.name=delta_lake
hive.metastore=file
hive.metastore.catalog.dir=/etc/trino/catalog/metastore

# S3 Configuration
- hive.s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
- hive.s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
- hive.s3.endpoint=s3.us-east-1.amazonaws.com
- hive.s3.path-style-access=false
+ fs.native-s3.enabled=true 
+ s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
+ s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
+ s3.region=us-east-1

# Delta Lake specific settings
delta.enable-non-concurrent-writes=true
delta.register-table-procedure.enabled=true
```

---

## Current Status

✅ **Trino is now running successfully!**

```json
{
    "state": "ACTIVE",
    "starting": false,
    "uptime": "1.55m",
    "coordinator": true
}
```

- **Container:** reddit-trino (running)
- **Web UI:** http://localhost:8089
- **Port:** 8089
- **Catalog:** delta (Delta Lake on S3)

---

## Next Steps

### 1. Register Your Delta Tables
```bash
./register_trino_tables.sh
```

This will register all 8 tables:
- technology_raw / technology_clean
- programmerhumor_raw / programmerhumor_clean
- news_raw / news_clean
- worldnews_raw / worldnews_clean

### 2. Test Queries
```bash
# Connect to Trino CLI
docker exec -it reddit-trino trino --catalog delta --schema reddit

# Or use the Python example
python3 example_trino_query.py
```

### 3. Web Interface
Open http://localhost:8089 in your browser to access the Trino Web UI.

---

## Troubleshooting Commands

```bash
# Check container status
docker ps | grep trino

# View logs
docker logs reddit-trino 2>&1 | tail -50

# Check if API is responding
curl http://localhost:8089/v1/info

# Restart Trino
docker-compose restart trino

# Run test script
./test_trino.sh
```

---

## Configuration Files Modified

1. `/home/steven/reddit-streaming/trino/jvm.config` - Removed deprecated JVM option
2. `/home/steven/reddit-streaming/trino/config.properties` - Removed deprecated properties
3. `/home/steven/reddit-streaming/trino/catalog/delta.properties` - Updated S3 configuration

All changes are backward compatible and follow Trino v479 best practices.
