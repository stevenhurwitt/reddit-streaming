# Reddit Streaming Services - Management Guide

## Quick Start

### Start both services:
```bash
./start_streaming.sh
```

### Check status:
```bash
./check_status.sh
```

### Stop services:
```bash
./stop_streaming.sh
```

## View Logs

### Producer logs:
```bash
docker-compose exec jupyterlab tail -f /tmp/producer.log
```

### Streaming logs:
```bash
docker-compose exec jupyterlab tail -f /tmp/streaming.log
```

### All services:
```bash
docker-compose logs -f
```

### Spark Master UI:
Open browser: http://localhost:8085

## Service Management

### Manual start (if scripts don't work):
```bash
# Inside the container
docker-compose exec jupyterlab bash
cd /opt/workspace/redditStreaming/src/reddit

# Start producer
python reddit_producer.py > /tmp/producer.log 2>&1 &

# Start streaming  
python reddit_streaming.py > /tmp/streaming.log 2>&1 &
```

### Kill processes:
```bash
docker-compose exec jupyterlab pkill -f reddit_producer
docker-compose exec jupyterlab pkill -f reddit_streaming
```

### Clean up zombie processes:
```bash
docker-compose restart jupyterlab
```

## Files Created

- `start_streaming.sh` - Start both services in background
- `stop_streaming.sh` - Stop all reddit streaming services
- `check_status.sh` - Check if services are running
- `redditStreaming/run_services.sh` - Service runner with auto-restart

## Troubleshooting

### Services not staying running:
1. Check logs for errors
2. Ensure Kafka is running: `docker-compose ps kafka`
3. Ensure Spark is running: `docker-compose ps spark-master`
4. Restart containers: `docker-compose restart`

### No data in S3:
1. Check AWS credentials in creds.json
2. Verify bucket name in config.yaml
3. Check streaming logs for write errors
4. Ensure streams are processing: look for "Batch" messages in logs

### Version conflicts:
- PySpark version must match Spark cluster version (currently 3.5.3)
- Run: `docker-compose exec jupyterlab pip list | grep pyspark`
