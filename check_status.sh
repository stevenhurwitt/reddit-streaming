#!/bin/bash

echo "=== Process Status ==="
docker-compose exec jupyterlab bash -c "ps aux | grep -E 'reddit_producer.py|reddit_streaming.py' | grep -v grep" || echo "No processes running"

echo ""
echo "=== Recent Producer Log ==="
docker-compose exec jupyterlab tail -30 /tmp/producer.log 2>/dev/null || echo "No producer log"

echo ""
echo "=== Recent Streaming Log ==="
docker-compose exec jupyterlab tail -30 /tmp/streaming.log 2>/dev/null || echo "No streaming log"

echo ""
echo "=== Spark Master Status ==="
docker-compose logs --tail 5 spark-master | grep -E "Registered|Removing" || echo "No recent Spark activity"
