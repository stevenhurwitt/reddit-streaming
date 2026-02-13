#!/bin/bash
# Polars service runner script to be executed inside the jupyterlab container

cd /opt/workspace/redditStreaming/src/reddit

# Activate virtual environment if it exists
if [ -f /opt/workspace/.venv/bin/activate ]; then
    echo "Activating virtual environment..."
    source /opt/workspace/.venv/bin/activate
    echo "Using Python: $(which python)"
else
    echo "WARNING: Virtual environment not found at /opt/workspace/.venv"
    echo "Using system Python: $(which python)"
fi

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
max_attempts=5
attempt=0
kafka_ready=false

while [ $attempt -lt $max_attempts ]; do
    if timeout 3 python -c "from kafka import KafkaProducer; KafkaProducer(bootstrap_servers=['kafka:9092'], api_version_auto_timeout_ms=3000, connections_max_idle_ms=3000)" 2>/dev/null; then
        echo "Kafka is ready!"
        kafka_ready=true
        break
    fi
    attempt=$((attempt + 1))
    if [ $attempt -lt $max_attempts ]; then
        echo "Waiting for Kafka... (attempt $attempt/$max_attempts)"
        sleep 1
    fi
done

if [ "$kafka_ready" = false ]; then
    echo "WARNING: Kafka may not be ready after $max_attempts attempts. Services may not function properly."
    echo "Proceeding anyway to start log files..."
fi

# Log setup info
echo "Polars service setup log:" > /tmp/run_services_polars.log
echo "Timestamp: $(date)" >> /tmp/run_services_polars.log
echo "Working directory: $(pwd)" >> /tmp/run_services_polars.log
echo "Python version: $(python --version 2>&1)" >> /tmp/run_services_polars.log
echo "Kafka ready: $kafka_ready" >> /tmp/run_services_polars.log
echo "" >> /tmp/run_services_polars.log

# Start reddit_producer in background
echo "Starting reddit_producer..."
python -u reddit_producer.py >> /tmp/producer.log 2>&1 &
PRODUCER_PID=$!
echo "Producer started with PID: $PRODUCER_PID"
echo "Producer started with PID: $PRODUCER_PID at $(date)" >> /tmp/run_services_polars.log

# Wait for producer to initialize
sleep 10

# Start Polars multi-subreddit streaming in background
echo "Starting Polars multi-subreddit streaming..."
python -u run_multi_polars.py >> /tmp/streaming_polars.log 2>&1 &
STREAMING_PID=$!
echo "Polars streaming started with PID: $STREAMING_PID"
echo "Polars streaming started with PID: $STREAMING_PID at $(date)" >> /tmp/run_services_polars.log

# Save PIDs
echo $PRODUCER_PID > /tmp/producer.pid
echo $STREAMING_PID > /tmp/streaming_polars.pid

echo "Both services started!"
echo "Producer PID: $PRODUCER_PID (saved to /tmp/producer.pid)"
echo "Polars Streaming PID: $STREAMING_PID (saved to /tmp/streaming_polars.pid)"
echo ""
echo "Monitor logs with:"
echo "  tail -f /tmp/producer.log"
echo "  tail -f /tmp/streaming_polars.log"

# Keep script running to monitor processes
while true; do
    if ! kill -0 $PRODUCER_PID 2>/dev/null; then
        echo "ERROR: Producer process died! Restarting..."
        python -u reddit_producer.py >> /tmp/producer.log 2>&1 &
        PRODUCER_PID=$!
        echo $PRODUCER_PID > /tmp/producer.pid
    fi
    
    if ! kill -0 $STREAMING_PID 2>/dev/null; then
        echo "ERROR: Polars streaming process died! Restarting..."
        python -u run_multi_polars.py >> /tmp/streaming_polars.log 2>&1 &
        STREAMING_PID=$!
        echo $STREAMING_PID > /tmp/streaming_polars.pid
    fi
    
    sleep 30
done
