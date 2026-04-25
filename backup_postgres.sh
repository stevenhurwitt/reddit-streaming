#!/bin/bash
# backup_postgres.sh - Backup reddit PostgreSQL database
# Uses Grandfather-Father-Son (GFS) rotation:
#   Son:         daily backups, kept for 7 days
#   Father:      weekly backups (Sunday), kept for 4 weeks
#   Grandfather: monthly backups (1st of month), kept for 12 months

set -e

# Configuration
POSTGRES_CONTAINER="reddit-postgres"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="secret!1234"
POSTGRES_DB="reddit"
BACKUP_DIR="${BACKUP_DIR:-./backups}"
MIRROR_DIR="/mnt/samsung/reddit-streaming/backups"

# GFS tier directories
DAILY_DIR="${BACKUP_DIR}/daily"
WEEKLY_DIR="${BACKUP_DIR}/weekly"
MONTHLY_DIR="${BACKUP_DIR}/monthly"

# Mirror directories on external drive
MIRROR_DAILY_DIR="${MIRROR_DIR}/daily"
MIRROR_WEEKLY_DIR="${MIRROR_DIR}/weekly"
MIRROR_MONTHLY_DIR="${MIRROR_DIR}/monthly"

# Retention limits
DAILY_KEEP=7    # days
WEEKLY_KEEP=28  # days (4 weeks)
MONTHLY_KEEP=365 # days (12 months)

# Generate filename with current date
TIMESTAMP=$(date +%Y%m%d)
DAY_OF_WEEK=$(date +%u)   # 1=Monday … 7=Sunday
DAY_OF_MONTH=$(date +%d)  # 01–31
BACKUP_FILE="reddit_postgres_${TIMESTAMP}.sql.gz"

# Create GFS tier directories (local + mirror)
mkdir -p "$DAILY_DIR" "$WEEKLY_DIR" "$MONTHLY_DIR"
if [ -d "$MIRROR_DIR" ] || mkdir -p "$MIRROR_DIR" 2>/dev/null; then
    mkdir -p "$MIRROR_DAILY_DIR" "$MIRROR_WEEKLY_DIR" "$MIRROR_MONTHLY_DIR"
    MIRROR_AVAILABLE=true
else
    echo "WARNING: Mirror destination '$MIRROR_DIR' is not available. Skipping mirror."
    MIRROR_AVAILABLE=false
fi

echo "Starting PostgreSQL backup (GFS rotation)..."
echo "Database: $POSTGRES_DB"
echo "Timestamp: $TIMESTAMP"
echo ""

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}$"; then
    echo "ERROR: PostgreSQL container '${POSTGRES_CONTAINER}' is not running!"
    echo "Start it with: docker-compose up -d reddit-postgres"
    exit 1
fi

# --- Step 1: Create today's daily backup ---
DAILY_PATH="${DAILY_DIR}/${BACKUP_FILE}"
echo "Creating daily backup: $DAILY_PATH"
docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
    pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" | gzip > "$DAILY_PATH"

if [ ! -f "$DAILY_PATH" ]; then
    echo "✗ Backup failed!"
    exit 1
fi

BACKUP_SIZE=$(du -h "$DAILY_PATH" | cut -f1)
echo "✓ Daily backup created (${BACKUP_SIZE})"

# Copy daily backup to mirror
if [ "$MIRROR_AVAILABLE" = true ]; then
    sudo cp "$DAILY_PATH" "${MIRROR_DAILY_DIR}/${BACKUP_FILE}"
    echo "✓ Daily backup mirrored to $MIRROR_DAILY_DIR"
fi

# --- Step 2: Promote to weekly if today is Sunday ---
if [ "$DAY_OF_WEEK" -eq 7 ]; then
    cp "$DAILY_PATH" "${WEEKLY_DIR}/${BACKUP_FILE}"
    echo "✓ Weekly backup saved (Sunday)"
    if [ "$MIRROR_AVAILABLE" = true ]; then
        sudo cp "$DAILY_PATH" "${MIRROR_WEEKLY_DIR}/${BACKUP_FILE}"
        echo "✓ Weekly backup mirrored to $MIRROR_WEEKLY_DIR"
    fi
fi

# --- Step 3: Promote to monthly if today is the 1st ---
if [ "$DAY_OF_MONTH" -eq "01" ]; then
    cp "$DAILY_PATH" "${MONTHLY_DIR}/${BACKUP_FILE}"
    echo "✓ Monthly backup saved (1st of month)"
    if [ "$MIRROR_AVAILABLE" = true ]; then
        sudo cp "$DAILY_PATH" "${MIRROR_MONTHLY_DIR}/${BACKUP_FILE}"
        echo "✓ Monthly backup mirrored to $MIRROR_MONTHLY_DIR"
    fi
fi

# --- Step 4: Purge old backups per retention policy ---
echo ""
echo "Purging old backups..."

purge_old() {
    local dir="$1"
    local keep_days="$2"
    local count
    count=$(find "$dir" -name "*.sql.gz" -mtime +"$keep_days" | wc -l)
    if [ "$count" -gt 0 ]; then
        find "$dir" -name "*.sql.gz" -mtime +"$keep_days" -delete
        echo "  Removed $count file(s) older than ${keep_days} days from $(basename "$dir")/"
    else
        echo "  Nothing to purge in $(basename "$dir")/"
    fi
}

purge_old "$DAILY_DIR"   "$DAILY_KEEP"
purge_old "$WEEKLY_DIR"  "$WEEKLY_KEEP"
purge_old "$MONTHLY_DIR" "$MONTHLY_KEEP"

# Purge mirror with same retention policy
if [ "$MIRROR_AVAILABLE" = true ]; then
    echo ""
    echo "Purging old mirror backups..."
    purge_old "$MIRROR_DAILY_DIR"   "$DAILY_KEEP"
    purge_old "$MIRROR_WEEKLY_DIR"  "$WEEKLY_KEEP"
    purge_old "$MIRROR_MONTHLY_DIR" "$MONTHLY_KEEP"
fi

# --- Summary ---
echo ""
echo "Backup summary:"
printf "  %-12s %s files\n" "daily:"   "$(ls "$DAILY_DIR"/*.sql.gz   2>/dev/null | wc -l)"
printf "  %-12s %s files\n" "weekly:"  "$(ls "$WEEKLY_DIR"/*.sql.gz  2>/dev/null | wc -l)"
printf "  %-12s %s files\n" "monthly:" "$(ls "$MONTHLY_DIR"/*.sql.gz 2>/dev/null | wc -l)"
echo ""
TOTAL_SIZE=$(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1)
echo "Total backup directory size: $TOTAL_SIZE"

echo ""
echo "To restore the latest daily backup:"
echo "  gunzip < ${DAILY_DIR}/${BACKUP_FILE} | docker exec -i -e PGPASSWORD='$POSTGRES_PASSWORD' $POSTGRES_CONTAINER psql -U $POSTGRES_USER $POSTGRES_DB"
