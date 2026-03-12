#!/bin/bash
# create_trino_tables.sh - Create Delta Lake tables in Trino using CREATE TABLE

TRINO_HOST="localhost"
TRINO_PORT="8089"
MAX_WAIT=120

echo "===== Creating Trino Delta Lake Tables ====="
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
docker exec -i reddit-trino trino --catalog delta --execute "CREATE SCHEMA IF NOT EXISTS reddit;" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ Schema created"
else
    echo "⚠ Schema may already exist"
fi

sleep 2

# Create tables pointing to S3 Delta Lake locations
echo ""
echo "Creating tables..."

# Technology Raw
echo "  • technology_raw..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CREATE TABLE IF NOT EXISTS technology_raw (
  author VARCHAR,
  created_utc TIMESTAMP(3) WITH TIME ZONE,
  domain VARCHAR,
  id VARCHAR,
  num_comments BIGINT,
  score BIGINT,
  selftext VARCHAR,
  subreddit VARCHAR,
  title VARCHAR,
  url VARCHAR
)
WITH (
  location = 's3a://reddit-streaming-stevenhurwitt-2/technology'
);" 2>&1

# Technology Clean
echo "  • technology_clean..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CREATE TABLE IF NOT EXISTS technology_clean (
  author VARCHAR,
  created_utc TIMESTAMP(3) WITH TIME ZONE,
  domain VARCHAR,
  id VARCHAR,
  num_comments BIGINT,
  score BIGINT,
  selftext VARCHAR,
  subreddit VARCHAR,
  title VARCHAR,
  url VARCHAR
)
WITH (
  location = 's3a://reddit-streaming-stevenhurwitt-2/technology_clean'
);" 2>&1

# ProgrammerHumor Raw
echo "  • programmerhumor_raw..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CREATE TABLE IF NOT EXISTS programmerhumor_raw (
  author VARCHAR,
  created_utc TIMESTAMP(3) WITH TIME ZONE,
  domain VARCHAR,
  id VARCHAR,
  num_comments BIGINT,
  score BIGINT,
  selftext VARCHAR,
  subreddit VARCHAR,
  title VARCHAR,
  url VARCHAR
)
WITH (
  location = 's3a://reddit-streaming-stevenhurwitt-2/ProgrammerHumor'
);" 2>&1

# ProgrammerHumor Clean
echo "  • programmerhumor_clean..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CREATE TABLE IF NOT EXISTS programmerhumor_clean (
  author VARCHAR,
  created_utc TIMESTAMP(3) WITH TIME ZONE,
  domain VARCHAR,
  id VARCHAR,
  num_comments BIGINT,
  score BIGINT,
  selftext VARCHAR,
  subreddit VARCHAR,
  title VARCHAR,
  url VARCHAR
)
WITH (
  location = 's3a://reddit-streaming-stevenhurwitt-2/ProgrammerHumor_clean'
);" 2>&1

# News Raw
echo "  • news_raw..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CREATE TABLE IF NOT EXISTS news_raw (
  author VARCHAR,
  created_utc TIMESTAMP(3) WITH TIME ZONE,
  domain VARCHAR,
  id VARCHAR,
  num_comments BIGINT,
  score BIGINT,
  selftext VARCHAR,
  subreddit VARCHAR,
  title VARCHAR,
  url VARCHAR
)
WITH (
  location = 's3a://reddit-streaming-stevenhurwitt-2/news'
);" 2>&1

# News Clean
echo "  • news_clean..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CREATE TABLE IF NOT EXISTS news_clean (
  author VARCHAR,
  created_utc TIMESTAMP(3) WITH TIME ZONE,
  domain VARCHAR,
  id VARCHAR,
  num_comments BIGINT,
  score BIGINT,
  selftext VARCHAR,
  subreddit VARCHAR,
  title VARCHAR,
  url VARCHAR
)
WITH (
  location = 's3a://reddit-streaming-stevenhurwitt-2/news_clean'
);" 2>&1

# WorldNews Raw
echo "  • worldnews_raw..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CREATE TABLE IF NOT EXISTS worldnews_raw (
  author VARCHAR,
  created_utc TIMESTAMP(3) WITH TIME ZONE,
  domain VARCHAR,
  id VARCHAR,
  num_comments BIGINT,
  score BIGINT,
  selftext VARCHAR,
  subreddit VARCHAR,
  title VARCHAR,
  url VARCHAR
)
WITH (
  location = 's3a://reddit-streaming-stevenhurwitt-2/worldnews'
);" 2>&1

# WorldNews Clean
echo "  • worldnews_clean..."
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CREATE TABLE IF NOT EXISTS worldnews_clean (
  author VARCHAR,
  created_utc TIMESTAMP(3) WITH TIME ZONE,
  domain VARCHAR,
  id VARCHAR,
  num_comments BIGINT,
  score BIGINT,
  selftext VARCHAR,
  subreddit VARCHAR,
  title VARCHAR,
  url VARCHAR
)
WITH (
  location = 's3a://reddit-streaming-stevenhurwitt-2/worldnews_clean'
);" 2>&1

echo ""
echo "✓ All tables created!"
echo ""
echo "Listing tables:"
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "SHOW TABLES;"

echo ""
echo "You can now query your data:"
echo "  docker exec -it reddit-trino trino --catalog delta --schema reddit"
echo ""
