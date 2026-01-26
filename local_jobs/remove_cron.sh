#!/bin/bash
# remove_cron.sh - Remove cron jobs configured for local reddit curation

TEMP_CRON="/tmp/reddit_cron_temp_$$"

# Get current crontab
crontab -l > "$TEMP_CRON" 2>/dev/null || true

# Remove reddit cron entries
if grep -q "REDDIT_CRON" "$TEMP_CRON"; then
    grep -v "REDDIT_CRON" "$TEMP_CRON" > "$TEMP_CRON.tmp"
    mv "$TEMP_CRON.tmp" "$TEMP_CRON"
    crontab "$TEMP_CRON"
    echo "✓ Cron jobs removed successfully"
else
    echo "No REDDIT_CRON jobs found in crontab"
fi

rm -f "$TEMP_CRON" "$TEMP_CRON.tmp"

echo ""
echo "To verify removal:"
echo "  crontab -l | grep REDDIT_CRON"
