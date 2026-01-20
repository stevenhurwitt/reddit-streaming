#!/bin/bash

echo "Stopping reddit streaming services..."
docker-compose exec -T jupyterlab pkill -f reddit_producer.py
docker-compose exec -T jupyterlab pkill -f reddit_streaming.py
echo "Services stopped!"
