# Streaming Service Scripts - Quick Reference

This guide explains how to use the updated streaming scripts that support both Spark and Polars modes.

## Starting Services

### Start with Spark (default)
```bash
./start_streaming.sh
# or explicitly
./start_streaming.sh spark
```

### Start with Polars (multi-subreddit)
```bash
./start_streaming.sh polars
```

## Stopping Services

### Stop all services (Spark and Polars)
```bash
./stop_streaming.sh
# or explicitly
./stop_streaming.sh all
```

### Stop only Spark services
```bash
./stop_streaming.sh spark
```

### Stop only Polars services
```bash
./stop_streaming.sh polars
```

## Checking Status

### Check all services
```bash
./check_status.sh
# or explicitly
./check_status.sh all
```

### Check only Spark services
```bash
./check_status.sh spark
```

### Check only Polars services
```bash
./check_status.sh polars
```

## Log Files

### Producer (common to both modes)
```bash
docker exec reddit-jupyterlab tail -f /tmp/producer.log
```

### Spark Streaming
```bash
docker exec reddit-jupyterlab tail -f /tmp/streaming.log
```

### Polars Streaming
```bash
docker exec reddit-jupyterlab tail -f /tmp/streaming_polars.log
```

## Service Runner Scripts

### Spark Mode
- Script: `/opt/workspace/redditStreaming/run_services.sh`
- Starts: `reddit_producer.py` + `reddit_streaming.py`
- PID files: `/tmp/producer.pid`, `/tmp/streaming.pid`

### Polars Mode
- Script: `/opt/workspace/redditStreaming/run_services_polars.sh`
- Starts: `reddit_producer.py` + `run_multi_polars.py`
- PID files: `/tmp/producer.pid`, `/tmp/streaming_polars.pid`

## Architecture Differences

### Spark Mode
- Uses PySpark with Kafka streaming
- Distributed processing across Spark cluster
- Higher resource usage (~2-4GB RAM)
- True streaming with micro-batches
- Single subreddit per script instance

### Polars Mode
- Uses Polars with Kafka consumer (confluent-kafka)
- Single process with multiprocessing for multiple subreddits
- Lower resource usage (~500MB-1GB RAM)
- Batch processing (configurable interval)
- Multiple subreddits in one script

## Common Workflows

### Switch from Spark to Polars
```bash
# Stop Spark
./stop_streaming.sh spark

# Start Polars
./start_streaming.sh polars

# Check status
./check_status.sh polars
```

### Run both simultaneously (not recommended for production)
```bash
# Start Spark
./start_streaming.sh spark

# Start Polars
./start_streaming.sh polars

# Check both
./check_status.sh all

# Stop both
./stop_streaming.sh all
```

### Restart services
```bash
# For Spark
./stop_streaming.sh spark
./start_streaming.sh spark

# For Polars
./stop_streaming.sh polars
./start_streaming.sh polars
```

## Troubleshooting

### No processes running
```bash
# Check if container is running
docker ps | grep jupyterlab

# Check for errors in logs
docker logs reddit-jupyterlab

# Verify Kafka is running
docker ps | grep kafka
```

### Services won't stop
```bash
# Forcefully stop all Python processes
docker exec reddit-jupyterlab pkill -9 python

# Or restart the container
docker-compose restart jupyterlab
```

### Log files not created
```bash
# Check if script is running
./check_status.sh

# Check run_services log
docker exec reddit-jupyterlab cat /tmp/run_services.log
# or for Polars
docker exec reddit-jupyterlab cat /tmp/run_services_polars.log
```

### Polars dependencies missing
```bash
# Install dependencies inside container
docker exec reddit-jupyterlab pip install -r /opt/workspace/redditStreaming/src/reddit/requirements_polars.txt

# Or rebuild container with dependencies
# (add to jupyterlab.Dockerfile)
```

## Configuration

Both modes use the same configuration files:
- `creds.json` - AWS credentials, Reddit API credentials
- `config.yaml` - Subreddit list, Kafka/Spark hosts, settings

### Polars-specific config options

Add to `config.yaml`:
```yaml
# Processing interval in seconds (default: 180)
processing_interval: 180

# Batch size for Kafka consumption (default: 100)
batch_size: 100
```

## Performance Comparison

| Metric | Spark | Polars |
|--------|-------|--------|
| Startup Time | 30-60s | 1-2s |
| Memory | 2-4 GB | 500MB-1GB |
| CPU Cores | 4+ | 1 per process |
| Throughput | Very High | Moderate-High |
| Best For | Millions/hour | Thousands-100k/hour |
| Multi-subreddit | Multiple instances | One instance |

## Recommendations

### Use Spark when:
- Processing very high volumes (millions of posts/hour)
- Need distributed processing
- Have Spark infrastructure already
- Require complex stream processing

### Use Polars when:
- Processing moderate volumes (thousands-hundreds of thousands/hour)
- Want simple deployment
- Resource-constrained environment
- Multiple subreddits from single process
- Faster development/testing cycles

## Quick Commands Reference

```bash
# Start Polars streaming
./start_streaming.sh polars

# Check Polars status
./check_status.sh polars

# View Polars logs
docker exec reddit-jupyterlab tail -f /tmp/streaming_polars.log

# Stop Polars streaming
./stop_streaming.sh polars

# Restart everything
./stop_streaming.sh all && ./start_streaming.sh polars
```
