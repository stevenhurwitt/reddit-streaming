#!/bin/bash

# Default to stopping all unless specific mode specified
MODE=${1:-all}

if [ "$MODE" = "polars" ]; then
    echo "Stopping reddit streaming services (Polars only)..."
    STOP_SPARK=false
    STOP_POLARS=true
elif [ "$MODE" = "spark" ]; then
    echo "Stopping reddit streaming services (Spark only)..."
    STOP_SPARK=true
    STOP_POLARS=false
elif [ "$MODE" = "all" ]; then
    echo "Stopping all reddit streaming services (Spark and Polars)..."
    STOP_SPARK=true
    STOP_POLARS=true
else
    echo "Error: Invalid mode '$MODE'. Use 'spark', 'polars', or 'all'"
    echo "Usage: $0 [spark|polars|all]"
    echo ""
    echo "Examples:"
    echo "  $0           # Stop all services (default)"
    echo "  $0 all       # Stop all services"
    echo "  $0 spark     # Stop only Spark services"
    echo "  $0 polars    # Stop only Polars services"
    exit 1
fi

# Kill run_services.sh processes first
if [ "$STOP_SPARK" = true ]; then
    echo "Killing Spark run_services processes..."
    docker-compose exec -T jupyterlab pkill -9 -f run_services.sh || true
fi

if [ "$STOP_POLARS" = true ]; then
    echo "Killing Polars run_services processes..."
    docker-compose exec -T jupyterlab pkill -9 -f run_services_polars.sh || true
fi

# Kill all producer processes
echo "Killing producer processes..."
docker-compose exec -T jupyterlab pkill -9 -f reddit_producer.py || true

# Kill streaming processes based on mode
if [ "$STOP_SPARK" = true ]; then
    echo "Killing Spark streaming processes..."
    docker-compose exec -T jupyterlab pkill -9 -f reddit_streaming.py || true
fi

if [ "$STOP_POLARS" = true ]; then
    echo "Killing Polars streaming processes..."
    docker-compose exec -T jupyterlab pkill -9 -f run_multi_polars.py || true
    docker-compose exec -T jupyterlab pkill -9 -f reddit_streaming_polars.py || true
fi

# Clean up zombie processes
echo "Cleaning up zombie processes..."
docker-compose exec -T jupyterlab pkill -9 -f python || true

# Wait for processes to terminate
sleep 2

# Verify cleanup
echo ""
echo "Checking remaining processes..."
if [ "$MODE" = "all" ]; then
    docker-compose exec -T jupyterlab ps aux | grep -E "reddit_producer|reddit_streaming|run_services|run_multi_polars" | grep -v grep || echo "All streaming services stopped successfully!"
elif [ "$MODE" = "spark" ]; then
    docker-compose exec -T jupyterlab ps aux | grep -E "reddit_producer|reddit_streaming.py|run_services.sh" | grep -v grep || echo "Spark streaming services stopped successfully!"
elif [ "$MODE" = "polars" ]; then
    docker-compose exec -T jupyterlab ps aux | grep -E "reddit_producer|run_multi_polars|reddit_streaming_polars|run_services_polars" | grep -v grep || echo "Polars streaming services stopped successfully!"
fi

# Clean up PID files
if [ "$STOP_SPARK" = true ]; then
    docker-compose exec -T jupyterlab rm -f /tmp/streaming.pid || true
fi

if [ "$STOP_POLARS" = true ]; then
    docker-compose exec -T jupyterlab rm -f /tmp/streaming_polars.pid || true
fi

# Always clean up producer PID in all mode
if [ "$MODE" = "all" ]; then
    docker-compose exec -T jupyterlab rm -f /tmp/producer.pid || true
fi

echo ""
echo "Services stopped and cleaned up!"
