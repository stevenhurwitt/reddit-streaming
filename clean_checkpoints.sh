#!/bin/bash

echo "Cleaning checkpoint directories..."

# Remove all checkpoint directories
echo "Removing local checkpoints..."
docker-compose exec -T jupyterlab rm -rf /opt/workspace/checkpoints/* 2>/dev/null || true

# Remove event logs
echo "Removing event logs..."
docker-compose exec -T jupyterlab rm -rf /opt/workspace/events/* 2>/dev/null || true

# Remove temp directories
echo "Removing temp directories..."
docker-compose exec -T jupyterlab rm -rf /opt/workspace/tmp/driver/* 2>/dev/null || true
docker-compose exec -T jupyterlab rm -rf /opt/workspace/tmp/executor/* 2>/dev/null || true
docker-compose exec -T jupyterlab rm -rf /opt/workspace/tmp/blocks/* 2>/dev/null || true

echo "Checkpoint directories cleaned!"
echo ""
echo "You can now restart streaming with: ./start_streaming.sh"
