-- Register Delta Lake tables from S3
-- This script registers all raw and clean tables for the four subreddits

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS delta.reddit;

-- Register raw tables
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'technology_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/technology'
);

CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'programmerhumor_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/ProgrammerHumor'
);

CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'news_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/news'
);

CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'worldnews_raw',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/worldnews'
);

-- Register clean tables
CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'technology_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/technology_clean'
);

CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'programmerhumor_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/ProgrammerHumor_clean'
);

CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'news_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/news_clean'
);

CALL delta.system.register_table(
  schema_name => 'reddit',
  table_name => 'worldnews_clean',
  table_location => 's3a://reddit-streaming-stevenhurwitt-2/worldnews_clean'
);

-- Show all registered tables
SHOW TABLES FROM delta.reddit;
