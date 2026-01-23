#!/bin/bash

echo "Starting Reddit Streaming Services..."
echo ""

# Run the service script in detached mode
docker-compose exec -d jupyterlab bash /opt/workspace/redditStreaming/run_services.sh

echo "Services starting in background..."
echo ""
echo "Wait 15 seconds for initialization, then check status:"
echo "  ./check_status.sh"
echo ""
echo "View logs:"
echo "  docker-compose exec jupyterlab tail -f /tmp/producer.log"
echo "  docker-compose exec jupyterlab tail -f /tmp/streaming.log"
echo ""
echo "Stop services:"
echo "  ./stop_streaming.sh"
