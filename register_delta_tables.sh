#!/bin/bash
# register_delta_tables.sh - Register existing Delta Lake tables from S3 in Trino

TRINO_HOST="localhost"
TRINO_PORT="8089"
MAX_WAIT=120

echo "===== Registering Existing Delta Lake Tables ====="
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
    echo "✓ Schema created"
else
    echo "⚠ Schema may already exist"
fi

sleep 2

# Register tables using system.register_table procedure
echo ""
echo "Registering tables from S3..."

# Technology Raw
echo "  • technology_raw..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CALL system.register_table(
  schema_name => 'reddit',
  table_name => 'technology_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/technology'
);" 2>&1

# Technology Clean
echo "  • technology_clean..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CALL system.register_table(
  schema_name => 'reddit',
  table_name => 'technology_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/technology_clean'
);" 2>&1

# ProgrammerHumor Raw
echo "  • programmerhumor_raw..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CALL system.register_table(
  schema_name => 'reddit',
  table_name => 'programmerhumor_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/ProgrammerHumor'
);" 2>&1

# ProgrammerHumor Clean
echo "  • programmerhumor_clean..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CALL system.register_table(
  schema_name => 'reddit',
  table_name => 'programmerhumor_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/ProgrammerHumor_clean'
);" 2>&1

# News Raw
echo "  • news_raw..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CALL system.register_table(
  schema_name => 'reddit',
  table_name => 'news_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/news'
);" 2>&1

# News Clean
echo "  • news_clean..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CALL system.register_table(
  schema_name => 'reddit',
  table_name => 'news_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/news_clean'
);" 2>&1

# WorldNews Raw
echo "  • worldnews_raw..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CALL system.register_table(
  schema_name => 'reddit',
  table_name => 'worldnews_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/worldnews'
);" 2>&1

# WorldNews Clean
echo "  • worldnews_clean..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CALL system.register_table(
  schema_name => 'reddit',
  table_name => 'worldnews_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-new/worldnews_clean'
);" 2>&1

echo ""
echo "✓ All tables registered!"
echo ""
echo "Listing tables:"
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "SHOW TABLES;" 2>&1

echo ""
echo "Testing a query on technology_raw:"
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "SELECT COUNT(*) as total_posts FROM technology_raw;" 2>&1

echo ""
echo "✓ Setup complete!"
echo ""
echo "Access Trino CLI:"
echo "  docker exec -it reddit-trino trino --catalog delta --schema reddit"
echo ""
echo "Example queries:"
echo "  SELECT * FROM technology_clean LIMIT 5;"
echo "  SELECT COUNT(*) FROM news_raw;"
echo ""
