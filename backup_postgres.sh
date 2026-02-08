#!/bin/bash
# backup_postgres.sh - Backup reddit PostgreSQL database
# Creates a compressed backup with timestamp in filename

set -e

# Configuration
POSTGRES_CONTAINER="reddit-postgres"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="secret!1234"
POSTGRES_DB="reddit"
BACKUP_DIR="${BACKUP_DIR:-./backups}"

# Generate filename with current date
TIMESTAMP=$(date +%Y%m%d)
BACKUP_FILE="reddit_postgres_${TIMESTAMP}.sql.gz"
BACKUP_PATH="${BACKUP_DIR}/${BACKUP_FILE}"

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

echo "Starting PostgreSQL backup..."
echo "Database: $POSTGRES_DB"
echo "Backup file: $BACKUP_PATH"
echo ""

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}$"; then
    echo "ERROR: PostgreSQL container '${POSTGRES_CONTAINER}' is not running!"
    echo "Start it with: docker-compose up -d reddit-postgres"
    exit 1
fi

# Perform backup using docker exec and pg_dump
# Pipe directly to gzip for compression
echo "Creating backup..."
docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
    pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB" | gzip > "$BACKUP_PATH"

# Check if backup was successful
if [ $? -eq 0 ] && [ -f "$BACKUP_PATH" ]; then
    BACKUP_SIZE=$(du -h "$BACKUP_PATH" | cut -f1)
    echo ""
    echo "✓ Backup completed successfully!"
    echo "  File: $BACKUP_PATH"
    echo "  Size: $BACKUP_SIZE"
    echo ""
    
    # Show existing backups
    echo "Existing backups in $BACKUP_DIR:"
    ls -lh "$BACKUP_DIR"/reddit_postgres_*.sql.gz 2>/dev/null || echo "  (no previous backups found)"
    echo ""
    
    # Calculate total backup size
    TOTAL_SIZE=$(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1)
    echo "Total backup directory size: $TOTAL_SIZE"
else
    echo ""
    echo "✗ Backup failed!"
    exit 1
fi

echo ""
echo "To restore this backup:"
echo "  gunzip < $BACKUP_PATH | docker exec -i -e PGPASSWORD='$POSTGRES_PASSWORD' $POSTGRES_CONTAINER psql -U $POSTGRES_USER $POSTGRES_DB"
