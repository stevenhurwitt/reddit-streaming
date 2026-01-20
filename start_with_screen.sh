#!/bin/bash

# Start reddit_producer in a screen session
echo "Starting reddit_producer in screen session 'producer'..."
docker-compose exec -T -d jupyterlab bash -c "screen -dmS producer bash -c 'cd /opt/workspace/redditStreaming/src/reddit && python reddit_producer.py'"

sleep 5

# Start reddit_streaming in a screen session
echo "Starting reddit_streaming in screen session 'streaming'..."
docker-compose exec -T -d jupyterlab bash -c "screen -dmS streaming bash -c 'cd /opt/workspace/redditStreaming/src/reddit && python reddit_streaming.py'"

echo "Both services started in screen sessions!"
echo ""
echo "To view sessions:"
echo "  docker-compose exec jupyterlab screen -ls"
echo ""
echo "To attach to a session:"
echo "  docker-compose exec jupyterlab screen -r producer"
echo "  docker-compose exec jupyterlab screen -r streaming"
echo ""
echo "To detach from a session: Press Ctrl+A then D"
