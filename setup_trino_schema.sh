#!/bin/bash
# setup_trino_schema.sh - Create schema and register tables in Trino
# This script waits for Trino to be fully ready before attempting operations

TRINO_HOST="localhost"
TRINO_PORT="8089"
MAX_WAIT=120  # Maximum wait time in seconds

echo "===== Trino Schema Setup ====="
echo ""

# Function to check if Trino is ready
check_trino_ready() {
    local response=$(curl -s http://${TRINO_HOST}:${TRINO_PORT}/v1/info 2>/dev/null)
    if [ $? -ne 0 ]; then
        return 1
    fi
    
    local starting=$(echo "$response" | grep -o '"starting":[^,]*' | cut -d':' -f2)
    if [ "$starting" == "false" ]; then
        return 0
    else
        return 1
    fi
}

# Wait for Trino to be ready
echo "Waiting for Trino to be ready..."
SECONDS=0
while ! check_trino_ready; do
    if [ $SECONDS -ge $MAX_WAIT ]; then
        echo "❌ Trino did not become ready within ${MAX_WAIT} seconds"
        echo "Check logs with: docker logs reddit-trino"
        exit 1
    fi
    printf "."
    sleep 3
done

echo ""
echo "✓ Trino is ready!"
echo ""

# Create schema
echo "Creating 'reddit' schema..."
docker exec -i reddit-trino trino --catalog delta --execute "CREATE SCHEMA IF NOT EXISTS reddit WITH (location = 's3a://reddit-streaming-stevenhurwitt-new/');" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ Schema 'reddit' created successfully"
else
    echo "⚠ Schema creation had issues, but may already exist"
fi

echo ""
echo "Listing schemas..."
docker exec -i reddit-trino trino --catalog delta --execute "SHOW SCHEMAS;"

echo ""
echo "✓ Schema setup complete!"
echo ""
echo "Next steps:"
echo "  1. Register tables with: ./register_trino_tables.sh"
echo "  2. Or register manually via Trino CLI"
echo ""
