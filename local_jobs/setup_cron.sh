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

# Stop streaming - 11:55 PM (23:55)
add_or_update_cron "REDDIT_CRON - stop streaming" \
    "55 23 * * *" \
    "cd /home/steven/reddit-streaming && ./stop_streaming.sh >> $LOG_DIR/stop_streaming.log 2>&1"

# News curation - midnight (00:00)
add_or_update_cron "REDDIT_CRON - news curation" \
    "0 0 * * *" \
    "cd /home/steven/reddit-streaming/local_jobs && /home/steven/reddit-streaming/.venv/bin/python run_curation_job_polars.py --job news --handle-duplicates skip >> $LOG_DIR/news.log 2>&1"

# Technology curation - 12:10 AM (00:10)
add_or_update_cron "REDDIT_CRON - technology curation" \
    "10 0 * * *" \
    "cd /home/steven/reddit-streaming/local_jobs && /home/steven/reddit-streaming/.venv/bin/python run_curation_job_polars.py --job technology --handle-duplicates skip >> $LOG_DIR/technology.log 2>&1"

# ProgrammerHumor curation - 12:20 AM (00:20)
add_or_update_cron "REDDIT_CRON - programmerhumor curation" \
    "20 0 * * *" \
    "cd /home/steven/reddit-streaming/local_jobs && /home/steven/reddit-streaming/.venv/bin/python run_curation_job_polars.py --job ProgrammerHumor --handle-duplicates skip >> $LOG_DIR/ProgrammerHumor.log 2>&1"

# Worldnews curation - 12:30 AM (00:30)
add_or_update_cron "REDDIT_CRON - worldnews curation" \
    "30 0 * * *" \
    "cd /home/steven/reddit-streaming/local_jobs && /home/steven/reddit-streaming/.venv/bin/python run_curation_job_polars.py --job worldnews --handle-duplicates skip >> $LOG_DIR/worldnews.log 2>&1"

# Backup Postgres database - 12:40 AM (00:40)
add_or_update_cron "REDDIT_CRON - backup postgres" \
    "40 0 * * *" \
    "cd /home/steven/reddit-streaming && ./backup_postgres.sh >> $LOG_DIR/backup_postgres.log 2>&1"

# Start streaming (Polars) - 12:50 AM (00:50)
add_or_update_cron "REDDIT_CRON - start streaming polars" \
    "50 0 * * *" \
    "cd /home/steven/reddit-streaming && ./start_streaming.sh polars >> $LOG_DIR/start_streaming.log 2>&1"

# Install the crontab
crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo "✓ Cron jobs configured successfully!"
echo ""
echo "Scheduled jobs (Local time):"
echo "  • 11:55 PM - Stop streaming"
echo "  • 12:00 AM - News curation"
echo "  • 12:10 AM - Technology curation"
echo "  • 12:20 AM - ProgrammerHumor curation"
echo "  • 12:30 AM - Worldnews curation"
echo "  • 12:40 AM - Backup PostgreSQL database"
echo "  • 12:50 AM - Start streaming (Polars)"
echo ""
echo "Logs location: $LOG_DIR/"
echo ""
echo "View current cron jobs:"
echo "  crontab -l | grep REDDIT_CRON"
echo ""
echo "View logs:"
echo "  tail -f $LOG_DIR/stop_streaming.log"
echo "  tail -f $LOG_DIR/news.log"
echo "  tail -f $LOG_DIR/technology.log"
echo "  tail -f $LOG_DIR/ProgrammerHumor.log"
echo "  tail -f $LOG_DIR/worldnews.log"
echo "  tail -f $LOG_DIR/backup_postgres.log"
echo "  tail -f $LOG_DIR/start_streaming.log"
echo ""
echo "To remove cron jobs:"
echo "  bash $(dirname "$0")/remove_cron.sh"
