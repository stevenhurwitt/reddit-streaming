#!/bin/bash
# setup_cron.sh - Configure cron jobs for automated curation execution
# This script sets up cron schedules matching the AWS EventBridge configuration

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
VENV_DIR="$SCRIPT_DIR/.venv"
LOG_DIR="$SCRIPT_DIR/logs"

# Verify setup is complete
if [ ! -d "$VENV_DIR" ]; then
    echo "ERROR: Virtual environment not found. Run setup_local_jobs.sh first"
    exit 1
fi

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
    
    # Remove existing entry if present
    grep -v "$comment" "$TEMP_CRON" > "$TEMP_CRON.tmp" 2>/dev/null || true
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

# News curation - midnight UTC every day
add_or_update_cron "REDDIT_CRON - news curation" \
    "0 0 * * *" \
    "source $VENV_DIR/bin/activate && cd $SCRIPT_DIR && python3 run_curation_job.py --job news >> $LOG_DIR/news.log 2>&1"

# Technology curation - midnight UTC every day
add_or_update_cron "REDDIT_CRON - technology curation" \
    "0 0 * * *" \
    "source $VENV_DIR/bin/activate && cd $SCRIPT_DIR && python3 run_curation_job.py --job technology >> $LOG_DIR/technology.log 2>&1"

# ProgrammerHumor curation - midnight UTC every day
add_or_update_cron "REDDIT_CRON - ProgrammerHumor curation" \
    "0 0 * * *" \
    "source $VENV_DIR/bin/activate && cd $SCRIPT_DIR && python3 run_curation_job.py --job ProgrammerHumor >> $LOG_DIR/ProgrammerHumor.log 2>&1"

# Worldnews curation - midnight UTC every day
add_or_update_cron "REDDIT_CRON - worldnews curation" \
    "0 0 * * *" \
    "source $VENV_DIR/bin/activate && cd $SCRIPT_DIR && python3 run_curation_job.py --job worldnews >> $LOG_DIR/worldnews.log 2>&1"

# Install the crontab
crontab "$TEMP_CRON"
rm "$TEMP_CRON"

echo "✓ Cron jobs configured successfully!"
echo ""
echo "Scheduled jobs (midnight UTC daily):"
echo "  • news"
echo "  • technology"
echo "  • ProgrammerHumor"
echo "  • worldnews"
echo ""
echo "Logs location: $LOG_DIR/"
echo ""
echo "View current cron jobs:"
echo "  crontab -l | grep REDDIT_CRON"
echo ""
echo "View logs:"
echo "  tail -f $LOG_DIR/news.log"
echo "  tail -f $LOG_DIR/technology.log"
echo "  tail -f $LOG_DIR/ProgrammerHumor.log"
echo "  tail -f $LOG_DIR/worldnews.log"
echo ""
echo "To remove cron jobs:"
echo "  bash $(dirname "$0")/remove_cron.sh"
