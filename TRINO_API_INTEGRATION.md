# Trino Backend API Integration

This document explains the dual-source architecture for the Reddit API, which supports querying both **Postgres (clean data)** and **Trino (raw data from Delta Lake)**.

## Overview

The Reddit API now supports two data sources:

1. **Postgres (Clean)**: Processed and cleaned data stored in PostgreSQL database
2. **Trino (Raw)**: Raw data from Delta Lake tables in S3, with API-layer deduplication

Users can toggle between sources via the frontend UI or by passing a `source` parameter to the API.

## Architecture

### Backend Components

```
reddit-api/
├── main.py              # FastAPI application with dual-source support
├── db/
│   ├── __init__.py      # Module exports
│   ├── adapters.py      # DataAdapter abstraction and implementations
│   └── utils.py         # Deduplication and pagination utilities
└── requirements.txt     # Dependencies (includes trino==0.328.0)
```

### Data Flow

```
User Request → API Endpoint → Adapter Factory → [PostgresAdapter | TrinoAdapter]
                                                       ↓                ↓
                                                   Postgres DB     Trino Query
                                                       ↓                ↓
                                                   Clean Data      Raw Data
                                                                       ↓
                                                                 Deduplication
                                                                       ↓
                                                   ← Unified Response ←
```

## API Usage

### Get Posts with Source Selection

**Endpoint**: `GET /api/posts`

**Parameters**:
- `source` (string, optional): Data source to query. Default: `"postgres"`
  - `"postgres"`: Clean data from PostgreSQL
  - `"trino"`: Raw data from Trino Delta Lake tables
- `limit` (int, 1-100): Number of posts per page. Default: 25
- `offset` (int): Number of posts to skip. Default: 0
- `subreddit` (string, optional): Filter by subreddit
- `search` (string, optional): Search in title or author
- `sort` (string): Sort column. Options: `created_utc`, `score`, `num_comments`. Default: `created_utc`
- `order` (string): Sort order. Options: `asc`, `desc`. Default: `desc`

**Example Requests**:

```bash
# Get clean data from Postgres (default)
curl "http://localhost:8001/api/posts?limit=10"

# Get raw data from Trino
curl "http://localhost:8001/api/posts?source=trino&limit=10"

# Filter by subreddit with Trino source
curl "http://localhost:8001/api/posts?source=trino&subreddit=technology&limit=20"

# Search posts with Trino source
curl "http://localhost:8001/api/posts?source=trino&search=python&sort=score&order=desc"
```

**Response Format**:

```json
{
  "total": 1523,
  "limit": 25,
  "offset": 0,
  "source": "trino",
  "duplicate_count": 47,
  "posts": [
    {
      "post_id": "abc123",
      "title": "Example Post Title",
      "author": "username",
      "score": 1234,
      "num_comments": 56,
      "created_utc": "2026-05-17T12:34:56",
      "url": "https://...",
      "selftext": "Post content...",
      "subreddit": "technology"
    }
  ]
}
```

**Response Fields**:
- `total`: Total number of posts (after deduplication for Trino)
- `limit`: Requested page size
- `offset`: Current offset
- `source`: Data source used (`"postgres"` or `"trino"`)
- `duplicate_count`: Number of duplicates removed (only for Trino source, `null` for Postgres)
- `posts`: Array of post objects

### List Available Sources

**Endpoint**: `GET /api/sources`

```bash
curl "http://localhost:8001/api/sources"
```

**Response**:

```json
{
  "sources": [
    {
      "id": "postgres",
      "name": "Postgres (Clean)",
      "description": "Cleaned and processed data from Postgres database"
    },
    {
      "id": "trino",
      "name": "Trino (Raw)",
      "description": "Raw data from Delta Lake tables with deduplication"
    }
  ]
}
```

### Health Check

**Endpoint**: `GET /health`

```bash
curl "http://localhost:8001/health"
```

**Response**:

```json
{
  "status": "ok",
  "postgres": {
    "status": "healthy"
  },
  "trino": {
    "status": "healthy"
  }
}
```

## Deduplication Algorithm

For the Trino source, raw data may contain duplicate posts. The API implements deduplication at the application layer:

### Strategy: Highest Score

1. Fetch posts from Trino (up to 10,000 to avoid memory issues)
2. Group posts by `post_id`
3. For each group, keep the post with the **highest score**
4. If scores are equal, the last one encountered is kept
5. Apply sorting and pagination to deduplicated results
6. Return both posts and duplicate count

### Implementation

See `reddit-api/db/utils.py`:

```python
def deduplicate_posts(posts, keep_strategy="highest_score"):
    """
    Deduplicate posts by post_id.
    Returns: (deduplicated_posts, duplicate_count)
    """
    post_dict = {}
    for post in posts:
        post_id = post.get("post_id")
        if post_id not in post_dict:
            post_dict[post_id] = post
        else:
            existing_score = post_dict[post_id].get("score", 0) or 0
            new_score = post.get("score", 0) or 0
            if new_score > existing_score:
                post_dict[post_id] = post
    
    deduplicated = list(post_dict.values())
    duplicate_count = len(posts) - len(deduplicated)
    return deduplicated, duplicate_count
```

### Alternative Strategies

The deduplication function supports different strategies via the `keep_strategy` parameter:
- `"highest_score"`: Keep post with highest score (default)
- `"most_recent"`: Keep most recent post based on `created_utc`

## Trino Configuration

### Tables

The Trino source queries these raw Delta Lake tables:
- `reddit.technology_raw` → s3://reddit-streaming-stevenhurwitt-2/technology
- `reddit.programmerhumor_raw` → s3://reddit-streaming-stevenhurwitt-2/ProgrammerHumor
- `reddit.news_raw` → s3://reddit-streaming-stevenhurwitt-2/news
- `reddit.worldnews_raw` → s3://reddit-streaming-stevenhurwitt-2/worldnews

### Query Pattern

The API generates a UNION ALL query:

```sql
SELECT post_id, title, author, score, num_comments, created_utc, url, selftext, 'technology' AS subreddit 
FROM technology_raw
UNION ALL
SELECT post_id, title, author, score, num_comments, created_utc, url, selftext, 'programmerhumor' AS subreddit 
FROM programmerhumor_raw
UNION ALL
SELECT post_id, title, author, score, num_comments, created_utc, url, selftext, 'news' AS subreddit 
FROM news_raw
UNION ALL
SELECT post_id, title, author, score, num_comments, created_utc, url, selftext, 'worldnews' AS subreddit 
FROM worldnews_raw
WHERE <filters>
LIMIT 10000
```

### Connection Configuration

Environment variables (set in `docker-compose.yaml`):
- `TRINO_HOST`: Hostname of Trino service (default: `trino`)
- `TRINO_PORT`: Port number (default: `8080`)
- `TRINO_CATALOG`: Catalog name (default: `delta`)
- `TRINO_SCHEMA`: Schema name (default: `reddit`)

## Frontend Integration

### Source Selector

The frontend includes a dropdown in the Filters component to select the data source:

```jsx
<select value={filters.source} onChange={(e) => update('source', e.target.value)}>
  <option value="postgres">📊 Postgres (Clean)</option>
  <option value="trino">🗄️ Trino (Raw)</option>
</select>
```

The selector is visually distinct with color coding:
- **Postgres**: Blue background (`#d1ecf1`)
- **Trino**: Yellow background (`#fff3cd`)

### Metadata Display

When using Trino source, the Feed component displays:
- Total post count (after deduplication)
- Number of duplicates removed
- Source badge indicating "Raw" vs "Clean"

Example:
```
Showing 1–25 of 1,523 posts (47 duplicates removed) [🗄️ Raw]
```

### Pagination Reset

Changing the source automatically resets pagination to page 0 to avoid confusion.

## Deployment

### Docker Compose

The `docker-compose.yaml` file configures the API service with access to both databases:

```yaml
reddit-api:
  environment:
    - POSTGRES_HOST=reddit-postgres
    - POSTGRES_PORT=5432
    - TRINO_HOST=trino
    - TRINO_PORT=8080
    - TRINO_CATALOG=delta
    - TRINO_SCHEMA=reddit
  depends_on:
    - reddit-postgres
    - trino
```

### Rebuild and Restart

After code changes:

```bash
# Rebuild API container
docker-compose build reddit-api

# Restart services
docker-compose up -d reddit-api reddit-frontend
```

### Verify Trino Tables

Ensure Trino tables are registered:

```bash
./register_trino_tables.sh
```

Check tables are accessible:

```bash
docker exec -it reddit-trino trino --catalog delta --schema reddit --execute "SHOW TABLES"
```

## Performance Considerations

### Trino Query Limits

- The API limits Trino queries to **10,000 rows** before deduplication
- This prevents memory issues while allowing most queries to work
- For very large result sets, consider adding server-side pagination to Trino queries

### Caching

Currently, no caching is implemented. For production use, consider:
- Redis caching for frequently accessed queries
- Query result caching with TTL
- CDN for static API responses

### Connection Pooling

- Postgres uses a connection pool (1-10 connections)
- Trino creates new connections per request (stateless)
- For high-traffic scenarios, consider adding Trino connection pooling

## Troubleshooting

### "Invalid source" Error

**Error**: `400 Bad Request: Invalid source: xyz`

**Solution**: Ensure `source` parameter is either `"postgres"` or `"trino"`

### Trino Connection Failed

**Error**: `500 Internal Server Error: Error fetching posts: ...`

**Check**:
1. Is Trino running? `docker ps | grep trino`
2. Are tables registered? `./register_trino_tables.sh`
3. Check Trino logs: `docker logs reddit-trino`
4. Verify network: `docker exec reddit-api ping trino`

### No Posts Returned from Trino

**Possible causes**:
1. Tables are empty (check with example_trino_query.py)
2. Filters are too restrictive
3. S3 credentials are invalid

### High Duplicate Count

This is expected for raw data. Duplicates occur due to:
- Streaming ingestion writing same post multiple times
- Data consistency during processing
- Time-based partitioning creating copies

The deduplication algorithm handles this automatically.

## Future Enhancements

1. **Query optimization**: Implement server-side DISTINCT in Trino queries
2. **Incremental loading**: Add cursor-based pagination for better performance
3. **Analytics**: Track source usage and duplicate rates
4. **Caching**: Add Redis cache for popular queries
5. **Hybrid mode**: Combine clean and raw data sources
6. **Real-time updates**: WebSocket support for live data streaming

## References

- [Trino Documentation](https://trino.io/docs/current/)
- [Delta Lake Connector](https://trino.io/docs/current/connector/delta-lake.html)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Repository README](../README.md)
- [Trino Quickstart Guide](../TRINO_QUICKSTART.md)
