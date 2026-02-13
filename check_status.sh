#!/bin/bash

MODE=${1:-all}

echo "=== Process Status ==="
if [ "$MODE" = "all" ] || [ "$MODE" = "spark" ]; then
    echo "Spark processes:"
    docker-compose exec jupyterlab bash -c "ps aux | grep -E 'reddit_streaming.py|run_services.sh' | grep -v grep" || echo "  No Spark streaming processes running"
fi

if [ "$MODE" = "all" ] || [ "$MODE" = "polars" ]; then
    echo "Polars processes:"
    docker-compose exec jupyterlab bash -c "ps aux | grep -E 'run_multi_polars.py|reddit_streaming_polars.py|run_services_polars.sh' | grep -v grep" || echo "  No Polars streaming processes running"
fi

if [ "$MODE" = "all" ]; then
    echo "Producer processes:"
    docker-compose exec jupyterlab bash -c "ps aux | grep -E 'reddit_producer.py' | grep -v grep" || echo "  No producer processes running"
fi

echo ""
echo "=== Recent Producer Log ==="
docker-compose exec jupyterlab tail -50 /tmp/producer.log 2>/dev/null || echo "No producer log"

if [ "$MODE" = "all" ] || [ "$MODE" = "spark" ]; then
    echo ""
    echo "=== Recent Spark Streaming Log ==="
    docker-compose exec jupyterlab tail -50 /tmp/streaming.log 2>/dev/null || echo "No Spark streaming log"
fi

if [ "$MODE" = "all" ] || [ "$MODE" = "polars" ]; then
    echo ""
    echo "=== Recent Polars Streaming Log ==="
    docker-compose exec jupyterlab tail -50 /tmp/streaming_polars.log 2>/dev/null || echo "No Polars streaming log"
fi

if [ "$MODE" = "all" ] || [ "$MODE" = "spark" ]; then
    echo ""
    echo "=== Spark Master Status ==="
    docker-compose logs --tail 5 spark-master 2>/dev/null | grep -E "Registered|Removing" || echo "No recent Spark activity"
fi

echo ""
echo "Usage: $0 [all|spark|polars]"
