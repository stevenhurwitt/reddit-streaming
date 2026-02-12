#!/bin/bash
# kill_stuck_jobs.sh - Kill stuck local curation jobs

echo "🔍 Checking for stuck curation jobs..."

# Function to kill local curation job processes
kill_curation_processes() {
    echo "🔫 Killing local curation job processes..."
    
    # Kill any running Polars curation jobs
    pkill -f "run_curation_job_polars.py" 2>/dev/null || true
    
    # Kill any old Spark curation jobs (if still running)
    pkill -f "run_curation_job.py" 2>/dev/null || true
    
    # Also kill in containers if they're still being used
    docker exec reddit-spark-master pkill -f "run_curation_job" 2>/dev/null || true
    docker exec reddit-jupyterlab pkill -f "run_curation_job" 2>/dev/null || true
    
    echo "✅ Killed curation processes"
}

# Function to clean up temporary files and logs
cleanup_temp_files() {
    echo "🧹 Cleaning up temporary files..."
    
    # Clean up old log files (keep last 7 days)
    find /home/steven/reddit-streaming/local_jobs/logs -name "*.log" -mtime +7 -delete 2>/dev/null || true
    
    # Clean up any Python cache files
    find /home/steven/reddit-streaming/local_jobs -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    find /home/steven/reddit-streaming/local_jobs -name "*.pyc" -delete 2>/dev/null || true
    
    echo "✅ Cleaned up temporary files"
}

# Main execution
main() {
    echo "🚀 Starting cleanup process..."
    
    # Kill stuck curation processes
    kill_curation_processes
    
    # Clean up temporary files
    cleanup_temp_files
    
    echo "🎉 Cleanup completed successfully!"
    return 0
}

# Run with optional help flag
case "${1:-}" in
    "--help"|"-h")
        echo "Usage: $0 [--help|-h]"
        echo ""
        echo "Kills stuck local curation job processes and cleans up temporary files."
        echo ""
        echo "Options:"
        echo "  --help, -h     Show this help message"
        ;;
    *)
        main
        ;;
esac