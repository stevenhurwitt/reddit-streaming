#!/bin/bash
# setup_cron.sh - Configure cron jobs for automated curation execution
# This script sets up cron schedules to run jobs on Docker Spark cluster

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOG_DIR="$SCRIPT_DIR/logs"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Temporary crontab file
TEMP_CRON="/tmp/reddit_cron_temp_$$"

# Get current crontab or create empty one
crontab -l > "$TEMP_CRON" 2>/dev/null || true

# Function to add or update a cron job
add_or_update_cron() {
    local comment="$1"
    local schedule="$2"
    local command="$3"
    
    # Remove existing entry if present (both comment and actual job lines)
    # Use grep -F for fixed string matching to avoid regex issues
    grep -F -v "$comment" "$TEMP_CRON" > "$TEMP_CRON.tmp" 2>/dev/null || true
    mv "$TEMP_CRON.tmp" "$TEMP_CRON"
    
    # Also remove any lines that contain the actual command
    grep -F -v "$command" "$TEMP_CRON" > "$TEMP_CRON.tmp" 2>/dev/null || true
    mv "$TEMP_CRON.tmp" "$TEMP_CRON"
    
    # Add new entry
    echo "# $comment" >> "$TEMP_CRON"
    echo "$schedule $command" >> "$TEMP_CRON"
}

echo "Configuring cron jobs..."
echo ""

# Clear existing reddit cron entries
grep -v "REDDIT_CRON" "$TEMP_CRON" > "$TEMP_CRON.tmp" 2>/dev/null || true
mv "$TEMP_CRON.tmp" "$TEMP_CRON"

# Stop streaming before curation jobs - 11:55 PM UTC
add_or_update_cron "REDDIT_CRON - stop streaming" \
    "55 23 * * *" \
    "cd /home/steven/reddit-streaming && ./stop_streaming.sh >> $LOG_DIR/stop_streaming.log 2>&1"

# Kill any stuck jobs and clean up resources - 11:57 PM UTC
add_or_update_cron "REDDIT_CRON - cleanup stuck jobs" \
    "57 23 * * *" \
    "cd /home/steven/reddit-streaming/local_jobs && ./kill_stuck_jobs.sh >> $LOG_DIR/cleanup.log 2>&1"

# News curation - midnight UTC (with timeout, using Polars)
add_or_update_cron "REDDIT_CRON - news curation" \
    "0 0 * * *" \
    "cd /home/steven/reddit-streaming/local_jobs && /home/steven/reddit-streaming/.venv/bin/python run_curation_job_polars.py --job news --handle-duplicates skip >> $LOG_DIR/news.log 2>&1"

# Technology curation - 12:30 AM UTC (using Polars)
add_or_update_cron "REDDIT_CRON - technology curation" \
    "30 0 * * *" \
    "cd /home/steven/reddit-streaming/local_jobs && /home/steven/reddit-streaming/.venv/bin/python run_curation_job_polars.py --job technology --handle-duplicates skip >> $LOG_DIR/technology.log 2>&1"

# ProgrammerHumor curation - 1:00 AM UTC (using Polars)
add_or_update_cron "REDDIT_CRON - ProgrammerHumor curation" \
    "0 1 * * *" \
    "cd /home/steven/reddit-streaming/local_jobs && /home/steven/reddit-streaming/.venv/bin/python run_curation_job_polars.py --job ProgrammerHumor --handle-duplicates skip >> $LOG_DIR/ProgrammerHumor.log 2>&1"

# Worldnews curation - 1:30 AM UTC (using Polars)
add_or_update_cron "REDDIT_CRON - worldnews curation" \
    "30 1 * * *" \
    "cd /home/steven/reddit-streaming/local_jobs && /home/steven/reddit-streaming/.venv/bin/python run_curation_job_polars.py --job worldnews --handle-duplicates skip >> $LOG_DIR/worldnews.log 2>&1"

# Backup Postgres database - 2:05 AM UTC
add_or_update_cron "REDDIT_CRON - backup postgres" \
    "5 2 * * *" \
    "cd /home/steven/reddit-streaming && ./backup_postgres.sh >> $LOG_DIR/backup_postgres.log 2>&1"

# Start streaming after curation jobs - 2:15 AM UTC
add_or_update_cron "REDDIT_CRON - start streaming" \
    "15 2 * * *" \
    "cd /home/steven/reddit-streaming && ./start_streaming.sh >> $LOG_DIR/start_streaming.log 2>&1"

# Install the crontab
crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo "✓ Cron jobs configured successfully!"
echo ""
echo "Scheduled jobs (UTC times):"
echo "  • 11:55 PM - Stop streaming"
echo "  • 11:57 PM - Clean up stuck jobs"
echo "  • 12:00 AM - News curation (Polars - local)"
echo "  • 12:30 AM - Technology curation (Polars - local)"
echo "  • 01:00 AM - ProgrammerHumor curation (Polars - local)"
echo "  • 01:30 AM - Worldnews curation (Polars - local)"
echo "  • 02:05 AM - Backup PostgreSQL database"
echo "  • 02:15 AM - Start streaming"
echo ""
echo "Logs location: $LOG_DIR/"
echo ""
echo "View current cron jobs:"
echo "  crontab -l | grep REDDIT_CRON"
echo ""
echo "View logs:"
echo "  tail -f $LOG_DIR/cleanup.log"
echo "  tail -f $LOG_DIR/news.log"
echo "  tail -f $LOG_DIR/technology.log"
echo "  tail -f $LOG_DIR/ProgrammerHumor.log"
echo "  tail -f $LOG_DIR/worldnews.log"
echo "  tail -f $LOG_DIR/backup_postgres.log"
echo "  tail -f $LOG_DIR/stop_streaming.log"
echo "  tail -f $LOG_DIR/start_streaming.log"
echo ""
echo "To remove cron jobs:"
echo "  bash $(dirname "$0")/remove_cron.sh"
