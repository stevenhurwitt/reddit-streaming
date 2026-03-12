#!/bin/bash
# test_trino.sh - Test Trino connectivity and query Delta tables

echo "Testing Trino setup..."
echo ""

# Check if Trino container is running
if ! docker ps | grep -q reddit-trino; then
    echo "❌ Trino container is not running!"
    echo "   Start it with: docker-compose up -d trino"
    exit 1
fi

echo "✓ Trino container is running"

# Wait for Trino to be ready
echo "Checking Trino health..."
RETRY=0
MAX_RETRY=30
until curl -s http://localhost:8089/v1/info > /dev/null 2>&1; do
    RETRY=$((RETRY+1))
    if [ $RETRY -ge $MAX_RETRY ]; then
        echo "❌ Trino failed to start after ${MAX_RETRY} attempts"
        exit 1
    fi
    echo "   Waiting for Trino... ($RETRY/$MAX_RETRY)"
    sleep 2
done

echo "✓ Trino is healthy and responding"
echo ""

# Test catalog connectivity
echo "Testing Delta catalog..."
if docker exec reddit-trino trino --catalog delta --execute "SHOW SCHEMAS;" > /dev/null 2>&1; then
    echo "✓ Delta catalog is accessible"
else
    echo "❌ Delta catalog failed - check S3 credentials"
    exit 1
fi

# Check if reddit schema exists
echo "Checking reddit schema..."
SCHEMAS=$(docker exec reddit-trino trino --catalog delta --execute "SHOW SCHEMAS;")
if echo "$SCHEMAS" | grep -q "reddit"; then
    echo "✓ Reddit schema exists"
    
    # Show tables
    echo ""
    echo "Available tables:"
    docker exec reddit-trino trino --catalog delta --schema reddit --execute "SHOW TABLES;" 2>/dev/null | grep -E "technology|programmerhumor|news|worldnews" || echo "   No tables registered yet"
    
    # Try a simple query if tables exist
    echo ""
    echo "Testing query on technology_clean..."
    if docker exec reddit-trino trino --catalog delta --schema reddit --execute "SELECT COUNT(*) as count FROM technology_clean LIMIT 1;" 2>/dev/null; then
        echo "✓ Successfully queried technology_clean table"
    else
        echo "⚠ Table exists but query failed - tables may need to be registered"
        echo "   Run: ./register_trino_tables.sh"
    fi
else
    echo "⚠ Reddit schema does not exist - tables need to be registered"
    echo "   Run: ./register_trino_tables.sh"
fi

echo ""
echo "Trino Web UI: http://localhost:8089"
echo "Connect CLI: docker exec -it reddit-trino trino --catalog delta --schema reddit"
