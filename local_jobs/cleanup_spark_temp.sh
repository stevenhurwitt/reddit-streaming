#!/bin/bash
# cleanup_spark_temp.sh - Manual cleanup script for Spark temporary data
# Use this script to manually clean up temp data from worker containers

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOG_DIR="$SCRIPT_DIR/logs"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
CLEANUP_LOG="$LOG_DIR/manual_cleanup.log"

echo "[$TIMESTAMP] Starting manual Spark temp cleanup..." | tee -a "$CLEANUP_LOG"

# Function to clean up a worker container
cleanup_worker() {
    local worker_name="$1"
    echo "[$TIMESTAMP] Cleaning up $worker_name..." | tee -a "$CLEANUP_LOG"
    
    # Get size before cleanup
    local size_before=$(docker exec "$worker_name" bash -c "cd /opt/workspace && du -sh tmp 2>/dev/null | cut -f1" 2>/dev/null || echo "unknown")
    
    # Perform cleanup
    docker exec "$worker_name" bash -c "cd /opt/workspace/tmp && rm -rf spark-* driver/* blocks/*" 2>/dev/null || {
        echo "[$TIMESTAMP] Warning: Could not clean up $worker_name" | tee -a "$CLEANUP_LOG"
        return 1
    }
    
    # Get size after cleanup
    local size_after=$(docker exec "$worker_name" bash -c "cd /opt/workspace && du -sh tmp 2>/dev/null | cut -f1" 2>/dev/null || echo "unknown")
    
    echo "[$TIMESTAMP] $worker_name cleanup complete: $size_before -> $size_after" | tee -a "$CLEANUP_LOG"
}

# Check if containers are running
echo "[$TIMESTAMP] Checking container status..." | tee -a "$CLEANUP_LOG"

if ! docker ps -q --filter "name=reddit-spark-worker-1" | grep -q .; then
    echo "[$TIMESTAMP] Warning: reddit-spark-worker-1 is not running" | tee -a "$CLEANUP_LOG"
else
    cleanup_worker "reddit-spark-worker-1"
fi

if ! docker ps -q --filter "name=reddit-spark-worker-2" | grep -q .; then
    echo "[$TIMESTAMP] Warning: reddit-spark-worker-2 is not running" | tee -a "$CLEANUP_LOG"
else
    cleanup_worker "reddit-spark-worker-2"
fi

# Optional: Clean up event logs older than 7 days
echo "[$TIMESTAMP] Cleaning up old event logs..." | tee -a "$CLEANUP_LOG"
if docker ps -q --filter "name=reddit-spark-master" | grep -q .; then
    docker exec reddit-spark-master bash -c "find /opt/workspace/events -name '*.inprogress' -mtime +7 -delete && find /opt/workspace/events -name '*.lz4' -mtime +7 -delete" 2>/dev/null || true
    echo "[$TIMESTAMP] Event log cleanup complete" | tee -a "$CLEANUP_LOG"
fi

# Show final container sizes
echo "[$TIMESTAMP] Final container sizes:" | tee -a "$CLEANUP_LOG"
docker ps -s | grep spark-worker | tee -a "$CLEANUP_LOG"

echo "[$TIMESTAMP] Manual cleanup complete!" | tee -a "$CLEANUP_LOG"
echo ""
echo "To see disk usage inside containers:"
echo "  docker exec reddit-spark-worker-1 bash -c 'cd /opt/workspace && du -sh *'"
echo "  docker exec reddit-spark-worker-2 bash -c 'cd /opt/workspace && du -sh *'"
echo ""
echo "Cleanup log: $CLEANUP_LOG"