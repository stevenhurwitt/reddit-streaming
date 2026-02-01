#!/bin/bash
# kill_stuck_jobs.sh - Kill stuck Spark jobs and clean up resources

echo "🔍 Checking for stuck Spark jobs..."

# Function to kill all running Spark applications
kill_spark_applications() {
    echo "📡 Fetching running Spark applications..."
    
    # Get running applications from Spark Master API
    RUNNING_APPS=$(curl -s http://localhost:8085/api/v1/applications | jq -r '.[] | select(.attempts[0].completed == false) | .id' 2>/dev/null)
    
    if [ -z "$RUNNING_APPS" ]; then
        echo "✅ No running Spark applications found"
        return 0
    fi
    
    echo "🎯 Found running applications:"
    echo "$RUNNING_APPS"
    
    # Kill each running application
    for app_id in $RUNNING_APPS; do
        echo "💀 Killing Spark application: $app_id"
        curl -X POST "http://localhost:8085/api/v1/applications/$app_id/kill" 2>/dev/null
        sleep 2
    done
}

# Function to kill curation job processes
kill_curation_processes() {
    echo "🔫 Killing curation job processes in containers..."
    
    # Kill processes in spark-master container
    docker exec reddit-spark-master pkill -f "run_curation_job.py" 2>/dev/null || true
    docker exec reddit-spark-master pkill -f "pyspark" 2>/dev/null || true
    
    # Kill processes in jupyterlab container (if any)
    docker exec reddit-jupyterlab pkill -f "run_curation_job.py" 2>/dev/null || true
    docker exec reddit-jupyterlab pkill -f "pyspark" 2>/dev/null || true
    
    echo "✅ Killed curation processes"
}

# Function to restart Spark workers if they're stuck
restart_spark_workers() {
    echo "🔄 Restarting Spark workers..."
    
    # Restart spark workers to clear any stuck states
    docker restart reddit-spark-worker-1 2>/dev/null || true
    docker restart reddit-spark-worker-2 2>/dev/null || true
    
    # Wait for workers to come back up
    sleep 10
    
    echo "✅ Spark workers restarted"
}

# Function to check Spark cluster health
check_spark_health() {
    echo "🏥 Checking Spark cluster health..."
    
    # Check if master is responsive
    if curl -s http://localhost:8085 > /dev/null; then
        echo "✅ Spark Master is responsive"
    else
        echo "❌ Spark Master is not responsive"
        return 1
    fi
    
    # Check worker status
    WORKER_COUNT=$(curl -s http://localhost:8085/api/v1/applications | jq -r 'length' 2>/dev/null || echo "0")
    echo "📊 Active applications: $WORKER_COUNT"
    
    return 0
}

# Function to clean up temporary files and logs
cleanup_temp_files() {
    echo "🧹 Cleaning up temporary files..."
    
    # Clean up spark temporary directories in containers
    docker exec reddit-spark-master bash -c "rm -rf /tmp/spark-*" 2>/dev/null || true
    docker exec reddit-jupyterlab bash -c "rm -rf /tmp/spark-*" 2>/dev/null || true
    
    # Clean up old log files (keep last 7 days)
    find /home/steven/reddit-streaming/local_jobs/logs -name "*.log" -mtime +7 -delete 2>/dev/null || true
    
    echo "✅ Cleaned up temporary files"
}

# Main execution
main() {
    echo "🚀 Starting cleanup process..."
    
    # Kill stuck Spark applications
    kill_spark_applications
    
    # Kill curation processes
    kill_curation_processes
    
    # Restart workers if needed
    restart_spark_workers
    
    # Clean up temporary files
    cleanup_temp_files
    
    # Check health
    if check_spark_health; then
        echo "🎉 Cleanup completed successfully!"
        echo "💡 Spark cluster is ready for new jobs"
        return 0
    else
        echo "⚠️  Cluster health check failed - may need manual intervention"
        return 1
    fi
}

# Install jq if not available (for JSON parsing)
install_jq_if_needed() {
    if ! command -v jq &> /dev/null; then
        echo "📦 Installing jq for JSON parsing..."
        sudo apt-get update && sudo apt-get install -y jq
    fi
}

# Run with optional force flag
case "${1:-}" in
    "--force"|"-f")
        echo "⚡ Force mode: Will restart entire Spark cluster if needed"
        install_jq_if_needed
        main
        if [ $? -ne 0 ]; then
            echo "🔄 Force restarting entire Spark cluster..."
            docker restart reddit-spark-master reddit-spark-worker-1 reddit-spark-worker-2
            sleep 30
            check_spark_health
        fi
        ;;
    "--help"|"-h")
        echo "Usage: $0 [--force|-f] [--help|-h]"
        echo ""
        echo "Options:"
        echo "  --force, -f    Force restart entire cluster if cleanup fails"
        echo "  --help, -h     Show this help message"
        ;;
    *)
        install_jq_if_needed
        main
        ;;
esac