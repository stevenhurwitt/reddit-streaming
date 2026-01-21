#!/bin/bash
# Service runner script to be executed inside the jupyterlab container

cd /opt/workspace/redditStreaming/src/reddit

# Start reddit_producer in background
echo "Starting reddit_producer..."
python -u reddit_producer.py > /tmp/producer.log 2>&1 &
PRODUCER_PID=$!
echo "Producer started with PID: $PRODUCER_PID"

# Wait for producer to initialize
sleep 10

# Start reddit_streaming in background
echo "Starting reddit_streaming..."
python reddit_streaming.py > /tmp/streaming.log 2>&1 &
STREAMING_PID=$!
echo "Streaming started with PID: $STREAMING_PID"

# Save PIDs
echo $PRODUCER_PID > /tmp/producer.pid
echo $STREAMING_PID > /tmp/streaming.pid

echo "Both services started!"
echo "Producer PID: $PRODUCER_PID (saved to /tmp/producer.pid)"
echo "Streaming PID: $STREAMING_PID (saved to /tmp/streaming.pid)"
echo ""
echo "Monitor logs with:"
echo "  tail -f /tmp/producer.log"
echo "  tail -f /tmp/streaming.log"

# Keep script running to monitor processes
while true; do
    if ! kill -0 $PRODUCER_PID 2>/dev/null; then
        echo "ERROR: Producer process died! Restarting..."
        python -u reddit_producer.py > /tmp/producer.log 2>&1 &
        PRODUCER_PID=$!
        echo $PRODUCER_PID > /tmp/producer.pid
    fi
    
    if ! kill -0 $STREAMING_PID 2>/dev/null; then
        echo "ERROR: Streaming process died! Restarting..."
        python reddit_streaming.py > /tmp/streaming.log 2>&1 &
        STREAMING_PID=$!
        echo $STREAMING_PID > /tmp/streaming.pid
    fi
    
    sleep 30
done
