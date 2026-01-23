#!/bin/bash

echo "Starting Reddit Streaming Services..."
echo ""

# Run the service script in detached mode
docker exec -d reddit-jupyterlab bash /opt/workspace/redditStreaming/run_services.sh

echo "Services starting in background..."
echo ""

# Wait for log files to be created
echo "Waiting for log files to be created..."
max_wait=30
elapsed=0
while [ $elapsed -lt $max_wait ]; do
    if docker exec reddit-jupyterlab test -f /tmp/producer.log && docker exec reddit-jupyterlab test -f /tmp/streaming.log; then
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
echo "  docker exec reddit-jupyterlab tail -f /tmp/streaming.log"
echo ""
echo "Stop services:"
echo "  ./stop_streaming.sh"
