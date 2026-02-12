#!/bin/bash
# remove_cron.sh - Remove cron jobs configured for local reddit curation

TEMP_CRON="/tmp/reddit_cron_temp_$$"

# Get current crontab
crontab -l > "$TEMP_CRON" 2>/dev/null || true

# Remove reddit cron entries by looking for various patterns
if grep -q -E "(REDDIT_CRON|reddit-streaming|run_curation_job.*\.py)" "$TEMP_CRON"; then
    # Remove lines with REDDIT_CRON comments
    grep -v "REDDIT_CRON" "$TEMP_CRON" > "$TEMP_CRON.tmp" 2>/dev/null || true
    mv "$TEMP_CRON.tmp" "$TEMP_CRON"
    
    # Remove lines containing reddit-streaming paths  
    grep -v "reddit-streaming" "$TEMP_CRON" > "$TEMP_CRON.tmp" 2>/dev/null || true
    mv "$TEMP_CRON.tmp" "$TEMP_CRON"
    
    # Remove lines containing run_curation_job.py or run_curation_job_polars.py
    grep -v "run_curation_job.*\.py" "$TEMP_CRON" > "$TEMP_CRON.tmp" 2>/dev/null || true
    mv "$TEMP_CRON.tmp" "$TEMP_CRON"
    
    # Install the cleaned crontab
    crontab "$TEMP_CRON"
    echo "✓ Cron jobs removed successfully"
else
    echo "No reddit-related cron jobs found in crontab"
fi

rm -f "$TEMP_CRON" "$TEMP_CRON.tmp"

echo ""
echo "To verify removal:"
echo "  crontab -l"
