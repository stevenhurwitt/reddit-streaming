#!/bin/bash

echo "Stopping reddit streaming services..."

# Kill all producer processes
echo "Killing producer processes..."
docker-compose exec -T jupyterlab pkill -9 -f reddit_producer.py

# Kill all streaming processes
echo "Killing streaming processes..."
docker-compose exec -T jupyterlab pkill -9 -f reddit_streaming.py

# Clean up zombie processes
echo "Cleaning up zombie processes..."
docker-compose exec -T jupyterlab pkill -9 -f python || true

# Wait for processes to terminate
sleep 2

# Verify cleanup
echo ""
echo "Checking remaining processes..."
docker-compose exec -T jupyterlab ps aux | grep -E "reddit_producer|reddit_streaming" | grep -v grep || echo "All streaming services stopped successfully!"

# Clean up PID files
docker-compose exec -T jupyterlab rm -f /tmp/producer.pid /tmp/streaming.pid

echo ""
echo "Services stopped and cleaned up!"
