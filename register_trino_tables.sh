#!/bin/bash
# register_trino_tables.sh - Register Delta Lake tables in Trino

TRINO_HOST="localhost"
TRINO_PORT="8089"

echo "Waiting for Trino to start..."
sleep 10

# Check if Trino is ready
until curl -s http://${TRINO_HOST}:${TRINO_PORT}/v1/info > /dev/null 2>&1; do
  echo "Waiting for Trino to be available..."
  sleep 5
done

echo "Trino is ready! Registering tables..."

# Use docker exec to run the SQL script inside the Trino container
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "
CREATE SCHEMA IF NOT EXISTS reddit;
"

# Register each table
echo "Registering technology_raw..."
docker exec -i reddit-trino trino --catalog delta --execute "
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'technology_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/technology'
);
"

echo "Registering programmerhumor_raw..."
docker exec -i reddit-trino trino --catalog delta --execute "
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'programmerhumor_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/ProgrammerHumor'
);
"

echo "Registering news_raw..."
docker exec -i reddit-trino trino --catalog delta --execute "
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'news_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/news'
);
"

echo "Registering worldnews_raw..."
docker exec -i reddit-trino trino --catalog delta --execute "
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'worldnews_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/worldnews'
);
"

echo "Registering technology_clean..."
docker exec -i reddit-trino trino --catalog delta --execute "
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'technology_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/technology_clean'
);
"

echo "Registering programmerhumor_clean..."
docker exec -i reddit-trino trino --catalog delta --execute "
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'programmerhumor_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/ProgrammerHumor_clean'
);
"

echo "Registering news_clean..."
docker exec -i reddit-trino trino --catalog delta --execute "
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'news_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/news_clean'
);
"

echo "Registering worldnews_clean..."
docker exec -i reddit-trino trino --catalog delta --execute "
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'worldnews_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/worldnews_clean'
);
"

echo ""
echo "✓ All tables registered successfully!"
echo ""
echo "Available tables:"
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "SHOW TABLES;"

echo ""
echo "Access Trino at: http://localhost:8089"
echo "Connect to Trino CLI: docker exec -it reddit-trino trino --catalog delta --schema reddit"
