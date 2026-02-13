#!/bin/bash

# Default to Spark unless 'polars' is specified
MODE=${1:-spark}

if [ "$MODE" = "polars" ]; then
    echo "Starting Reddit Streaming Services (Polars Mode)..."
    echo ""
    SERVICE_SCRIPT="run_services_polars.sh"
    LOG_FILE="/tmp/streaming_polars.log"
elif [ "$MODE" = "spark" ]; then
    echo "Starting Reddit Streaming Services (Spark Mode)..."
    echo ""
    SERVICE_SCRIPT="run_services.sh"
    LOG_FILE="/tmp/streaming.log"
else
    echo "Error: Invalid mode '$MODE'. Use 'spark' or 'polars'"
    echo "Usage: $0 [spark|polars]"
    echo ""
    echo "Examples:"
    echo "  $0           # Start with Spark (default)"
    echo "  $0 spark     # Start with Spark"
    echo "  $0 polars    # Start with Polars"
    exit 1
fi

# Run the service script in detached mode
docker exec -d reddit-jupyterlab bash /opt/workspace/redditStreaming/$SERVICE_SCRIPT

echo "Services starting in background..."
echo ""

# Wait for log files to be created
echo "Waiting for log files to be created..."
max_wait=30
elapsed=0
while [ $elapsed -lt $max_wait ]; do
    if docker exec reddit-jupyterlab test -f /tmp/producer.log && docker exec reddit-jupyterlab test -f $LOG_FILE; then
        echo "Log files created successfully!"
        break
    fi
    sleep 1
    elapsed=$((elapsed + 1))
done

if [ $elapsed -ge $max_wait ]; then
    echo "WARNING: Log files not created after ${max_wait}s."
    echo "Checking for errors..."
    echo ""
    
    # Check run_services log
    if docker exec reddit-jupyterlab test -f /tmp/run_services.log; then
        echo "=== Run Services Log ===" 
        docker exec reddit-jupyterlab cat /tmp/run_services.log
        echo ""
    fi
    
    # Check running processes
    echo "=== Running Processes ===" 
    docker exec reddit-jupyterlab ps aux | grep -E "reddit|python" | grep -v grep || echo "No reddit processes found"
    echo ""
    
    echo "Services may have failed to start. Common issues:"
    echo "  - Kafka may not be running or accessible"
    echo "  - Python dependencies may be missing"
    echo "  - Credentials or config files may be invalid"
    echo ""
    echo "Try viewing container logs directly:"
    echo "  docker logs reddit-jupyterlab"
fi

echo ""
echo "Check status:"
echo "  ./check_status.sh"
echo ""
echo "View logs:"
echo "  docker exec reddit-jupyterlab tail -f /tmp/producer.log"
echo "  docker exec reddit-jupyterlab tail -f $LOG_FILE"
echo ""
echo "Stop services:"
if [ "$MODE" = "polars" ]; then
    echo "  ./stop_streaming.sh polars"
else
    echo "  ./stop_streaming.sh"
fi
