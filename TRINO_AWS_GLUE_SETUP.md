# Trino with AWS Glue Metastore - Final Solution

## Problem Solved

The initial file-based metastore configuration caused errors:
- `No factory for location: /data/trino/metastore`
- `hive.metastore.uri: must not be null`

## Solution: AWS Glue Data Catalog

Instead of using a file-based or separate Hive Metastore service, we're using **AWS Glue Data Catalog** as the metastore. This is the recommended approach for cloud-based Delta Lake deployments.

### Why AWS Glue?

✅ **No additional infrastructure** - Uses AWS managed service  
✅ **Persistent metadata** - Survives container restarts  
✅ **Serverless** - No metastore service to manage  
✅ **AWS integrated** - Works seamlessly with S3 credentials  
✅ **Free tier** - First 1 million requests/month are free

## Configuration

### 1. Updated [trino/catalog/delta.properties](trino/catalog/delta.properties):
```properties
connector.name=delta_lake
hive.metastore=glue
hive.metastore.glue.region=us-east-1

# S3 Configuration  
fs.native-s3.enabled=true
s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
s3.region=us-east-1

# Delta Lake specific settings
delta.enable-non-concurrent-writes=true
```

### 2. AWS Credentials
Your existing AWS credentials in `aws_access_key.txt` and `aws_secret.txt` are used for both:
- S3 access (reading Delta Lake tables)
- AWS Glue Data Catalog access (metadata storage)

## Setup Instructions

### 1. Start Trino
```bash
docker-compose up -d trino
```

### 2. Create Tables
Run the table creation script:
```bash
./create_trino_tables.sh
```

This will:
- Wait for Trino to be fully ready
- Create the `reddit` schema in AWS Glue
- Create 8 tables (4 raw + 4 clean) pointing to your S3 Delta Lake locations
- Store metadata in AWS Glue Data Catalog

### 3. Verify
```bash
# List schemas
docker exec -i reddit-trino trino --catalog delta --execute "SHOW SCHEMAS;"

# List tables
docker exec -i reddit-trino trino --catalog delta --schema reddit --execute "SHOW TABLES;"

# Query data
docker exec -it reddit-trino trino --catalog delta --schema reddit
```

## Tables Created

All tables are stored in AWS Glue and point to S3 Delta Lake locations:

| Table Name | S3 Location |
|------------|-------------|
| technology_raw | s3a://reddit-streaming-stevenhurwitt-new/technology |
| technology_clean | s3a://reddit-streaming-stevenhurwitt-new/technology_clean |
| programmerhumor_raw | s3a://reddit-streaming-stevenhurwitt-new/ProgrammerHumor |
| programmerhumor_clean | s3a://reddit-streaming-stevenhurwitt-new/ProgrammerHumor_clean |
| news_raw | s3a://reddit-streaming-stevenhurwitt-new/news |
| news_clean | s3a://reddit-streaming-stevenhurwitt-new/news_clean |
| worldnews_raw | s3a://reddit-streaming-stevenhurwitt-new/worldnews |
| worldnews_clean | s3a://reddit-streaming-stevenhurwitt-new/worldnews_clean |

## Querying Data

### Connect to Trino CLI
```bash
docker exec -it reddit-trino trino --catalog delta --schema reddit
```

### Example Queries
```sql
-- Count posts
SELECT COUNT(*) FROM technology_clean;

-- Top posts by score
SELECT title, author, score, created_utc 
FROM news_clean 
ORDER BY score DESC 
LIMIT 10;

-- Posts per subreddit
SELECT 
  'technology' as subreddit, COUNT(*) as posts FROM technology_clean
UNION ALL
SELECT 'programmerhumor', COUNT(*) FROM programmerhumor_clean
UNION ALL
SELECT 'news', COUNT(*) FROM news_clean
UNION ALL
SELECT 'worldnews', COUNT(*) FROM worldnews_clean;
```

## Advantages Over Other Approaches

### vs. File Metastore
- ❌ File metastore: Not supported in newer Trino/Delta Lake versions
- ✅ Glue: Fully supported and recommended

### vs. Separate Hive Metastore Service  
- ❌ Hive service: Additional container, more complexity, more memory
- ✅ Glue: Serverless, managed by AWS

### vs. No Metastore
- ❌ No metastore: Can't list schemas/tables easily
- ✅ Glue: Persistent catalog, can browse in AWS Console

## AWS Glue Console

You can view your metadata in the AWS Glue Console:
1. Go to https://console.aws.amazon.com/glue/
2. Navigate to "Data Catalog" → "Databases"
3. Find the `reddit` database
4. View tables and their schemas

## Persistence

- **Metadata**: Stored in AWS Glue Data Catalog (persists across Trino restarts)
- **Data**: Stored in S3 Delta Lake tables (unchanged)
- **Query history**: Available in Trino Web UI at http://localhost:8089

## Troubleshooting

### "Access Denied" errors
Check AWS credentials have Glue permissions:
```json
{
  "Effect": "Allow",
  "Action": [
    "glue:GetDatabase",
    "glue:GetDatabases",
    "glue:GetTable",
    "glue:GetTables",
    "glue:CreateDatabase",
    "glue:CreateTable",
    "glue:UpdateTable"
  ],
  "Resource": "*"
}
```

### Tables not showing
Re-run the table creation script:
```bash
./create_trino_tables.sh
```

### Trino not starting
Check logs:
```bash
docker logs reddit-trino 2>&1 | tail -50
```

## Cost

AWS Glue Data Catalog pricing:
- **Storage**: First 1 million objects stored free per month
- **Requests**: First 1 million requests free per month
- **Your usage**: ~8 tables = negligible cost (likely $0)

See: https://aws.amazon.com/glue/pricing/

## Files

- [trino/catalog/delta.properties](trino/catalog/delta.properties) - Delta Lake connector with Glue metastore
- [create_trino_tables.sh](create_trino_tables.sh) - Script to create schema and tables
- [docker-compose.yaml](docker-compose.yaml) - Trino service configuration

## Summary

✅ **Error Fixed**: No more "No factory for location" or "must not be null" errors  
✅ **Persistent Metadata**: Tables survive Trino restarts  
✅ **Simple Setup**: Just one command to create tables  
✅ **Production Ready**: Uses AWS managed service  
✅ **Cost Effective**: Free tier covers typical usage  

Your Trino setup is now ready to query Delta Lake tables on S3! 🎉
