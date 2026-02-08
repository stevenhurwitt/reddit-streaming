#!/bin/bash
# restore_postgres.sh - Restore reddit PostgreSQL database from backup
# Restores a compressed backup file to the database

set -e

# Configuration
POSTGRES_CONTAINER="reddit-postgres"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="secret!1234"
POSTGRES_DB="reddit"
BACKUP_DIR="${BACKUP_DIR:-./backups}"

# Function to list available backups
list_backups() {
    echo "Available backups in $BACKUP_DIR:"
    echo ""
    if ls "$BACKUP_DIR"/reddit_postgres_*.sql.gz 1> /dev/null 2>&1; then
        local count=1
        for backup in "$BACKUP_DIR"/reddit_postgres_*.sql.gz; do
            local size=$(du -h "$backup" | cut -f1)
            local date=$(stat -c %y "$backup" 2>/dev/null || stat -f %Sm "$backup" 2>/dev/null)
            printf "  %d) %s (%s) - %s\n" "$count" "$(basename "$backup")" "$size" "$date"
            ((count++))
        done
    else
        echo "  No backups found in $BACKUP_DIR/"
        exit 1
    fi
    echo ""
}

# Function to confirm action
confirm_restore() {
    local backup_file="$1"
    echo ""
    echo "⚠️  WARNING: This will REPLACE all data in the '$POSTGRES_DB' database!"
    echo "   Backup file: $backup_file"
    echo ""
    read -p "Are you sure you want to continue? (yes/no): " confirmation
    
    if [ "$confirmation" != "yes" ]; then
        echo "Restore cancelled."
        exit 0
    fi
}

# Show usage
usage() {
    echo "Usage: $0 [BACKUP_FILE]"
    echo ""
    echo "Restore a PostgreSQL database backup."
    echo ""
    echo "Options:"
    echo "  BACKUP_FILE    Path to backup file (if not provided, will show list)"
    echo ""
    echo "Examples:"
    echo "  $0                                          # Interactive selection"
    echo "  $0 backups/reddit_postgres_20260208.sql.gz  # Restore specific file"
    echo ""
}

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}$"; then
    echo "ERROR: PostgreSQL container '${POSTGRES_CONTAINER}' is not running!"
    echo "Start it with: docker-compose up -d reddit-postgres"
    exit 1
fi

# If backup file is provided as argument
if [ $# -eq 1 ]; then
    BACKUP_FILE="$1"
    
    # Check if file exists
    if [ ! -f "$BACKUP_FILE" ]; then
        echo "ERROR: Backup file not found: $BACKUP_FILE"
        echo ""
        list_backups
        exit 1
    fi
    
    confirm_restore "$BACKUP_FILE"
    
    echo "Restoring database from backup..."
    echo "This may take a few minutes..."
    echo ""
    
    # Drop and recreate database to ensure clean restore
    echo "Step 1/3: Dropping existing database..."
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -c "DROP DATABASE IF EXISTS $POSTGRES_DB;"
    
    echo "Step 2/3: Creating fresh database..."
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -c "CREATE DATABASE $POSTGRES_DB;"
    
    echo "Step 3/3: Restoring data from backup..."
    gunzip < "$BACKUP_FILE" | docker exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" \
        "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" "$POSTGRES_DB"
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "✓ Database restored successfully from: $BACKUP_FILE"
        
        # Show table counts
        echo ""
        echo "Verifying restore..."
        docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
            psql -U "$POSTGRES_USER" "$POSTGRES_DB" -c "\
            SELECT 'news' as table, COUNT(*) as rows FROM curated_news_posts UNION ALL \
            SELECT 'technology', COUNT(*) FROM curated_technology_posts UNION ALL \
            SELECT 'ProgrammerHumor', COUNT(*) FROM curated_programmerhumor_posts UNION ALL \
            SELECT 'worldnews', COUNT(*) FROM curated_worldnews_posts \
            ORDER BY table;" 2>/dev/null || echo "Tables may not exist yet."
    else
        echo ""
        echo "✗ Restore failed!"
        exit 1
    fi

# Interactive mode - list and select backup
else
    list_backups
    
    # Build array of backup files
    backups=()
    for backup in "$BACKUP_DIR"/reddit_postgres_*.sql.gz; do
        backups+=("$backup")
    done
    
    # Prompt for selection
    read -p "Select backup number (or 'q' to quit): " selection
    
    if [ "$selection" = "q" ] || [ "$selection" = "Q" ]; then
        echo "Cancelled."
        exit 0
    fi
    
    # Validate selection
    if ! [[ "$selection" =~ ^[0-9]+$ ]] || [ "$selection" -lt 1 ] || [ "$selection" -gt "${#backups[@]}" ]; then
        echo "ERROR: Invalid selection"
        exit 1
    fi
    
    # Get selected backup (array is 0-indexed)
    BACKUP_FILE="${backups[$((selection-1))]}"
    
    confirm_restore "$BACKUP_FILE"
    
    echo "Restoring database from backup..."
    echo "This may take a few minutes..."
    echo ""
    
    # Drop and recreate database to ensure clean restore
    echo "Step 1/3: Dropping existing database..."
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -c "DROP DATABASE IF EXISTS $POSTGRES_DB;"
    
    echo "Step 2/3: Creating fresh database..."
    docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
        psql -U "$POSTGRES_USER" -c "CREATE DATABASE $POSTGRES_DB;"
    
    echo "Step 3/3: Restoring data from backup..."
    gunzip < "$BACKUP_FILE" | docker exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" \
        "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" "$POSTGRES_DB"
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "✓ Database restored successfully from: $(basename "$BACKUP_FILE")"
        
        # Show table counts
        echo ""
        echo "Verifying restore..."
        docker exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
            psql -U "$POSTGRES_USER" "$POSTGRES_DB" -c "\
            SELECT 'news' as table, COUNT(*) as rows FROM curated_news_posts UNION ALL \
            SELECT 'technology', COUNT(*) FROM curated_technology_posts UNION ALL \
            SELECT 'ProgrammerHumor', COUNT(*) FROM curated_programmerhumor_posts UNION ALL \
            SELECT 'worldnews', COUNT(*) FROM curated_worldnews_posts \
            ORDER BY table;" 2>/dev/null || echo "Tables may not exist yet."
    else
        echo ""
        echo "✗ Restore failed!"
        exit 1
    fi
fi
