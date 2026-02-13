# Reddit Streaming with Polars

This document describes the Polars-based implementation of Reddit streaming as an alternative to the Spark-based version.

## Overview

The `reddit_streaming_polars.py` file replicates the Kafka streaming functionality using Polars instead of PySpark, writing to S3 Delta tables using the `deltalake` Python library.

## Key Differences from Spark Version

### Architecture
- **Spark Version**: True streaming with micro-batches, distributed processing
- **Polars Version**: Polling-based batch processing, single-process (can be scaled with multiprocessing)

### Dependencies
- **Spark**: Requires JVM, Spark cluster, multiple JAR dependencies
- **Polars**: Pure Python, lightweight dependencies (confluent-kafka, deltalake, polars)

### Performance Characteristics
- **Spark**: Better for very high throughput, distributed processing across workers
- **Polars**: Faster for moderate throughput, lower resource overhead, simpler deployment

### Delta Lake Implementation
- **Spark**: Uses `delta-spark` with native integration
- **Polars**: Uses `deltalake` (Delta Lake Rust implementation) with Polars integration

## Installation

Install required dependencies:

```bash
pip install -r requirements_polars.txt
```

Or manually:

```bash
pip install polars deltalake confluent-kafka s3fs boto3 PyYAML
```

## Configuration

Uses the same `config.yaml` and `creds.json` files as the Spark version.

### Additional Config Options

You can add these options to `config.yaml`:

```yaml
# Processing interval in seconds (default: 180)
processing_interval: 180

# Number of messages to consume per batch (default: 100)
batch_size: 100
```

## Usage

Run the streaming script:

```bash
python reddit_streaming_polars.py
```

Or specify a subreddit via environment variable:

```bash
export subreddit=technology
python reddit_streaming_polars.py
```

## Features

### 1. Kafka Consumer
- Consumes messages from Kafka topics (`reddit_{subreddit}`)
- Configurable batch size and timeout
- Auto-commit with configurable intervals
- Handles connection failures gracefully

### 2. Schema Validation
- Maintains exact schema compatibility with Spark version
- All 107 fields from Reddit API supported
- Automatic null-filling for missing fields

### 3. Console Output
- Displays batch statistics and sample records
- Shows: subreddit, title, score, created_utc
- Timestamp conversion for readability

### 4. Delta Lake Writing
- Appends to existing Delta tables or creates new ones
- Uses S3 for storage (configurable bucket)
- Automatic schema evolution support
- Fallback to local parquet on S3 failures

### 5. Error Handling
- Graceful handling of Kafka connection issues
- Failed batches saved to local checkpoints
- Keyboard interrupt support for clean shutdown

## Scaling

### Single Subreddit
The current implementation streams one subreddit per process.

### Multiple Subreddits
For multiple subreddits, use one of these approaches:

#### Option 1: Separate Processes
```bash
# Terminal 1
export subreddit=technology
python reddit_streaming_polars.py

# Terminal 2
export subreddit=worldnews
python reddit_streaming_polars.py
```

#### Option 2: Process Pool (modify main function)
```python
from multiprocessing import Process

def main():
    creds, config = read_files()
    subreddit_list = config.get("subreddit", ["technology"])
    
    processes = []
    for subreddit in subreddit_list:
        p = Process(target=stream_subreddit, 
                   args=(subreddit, config["kafka_host"], creds))
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()
```

## Monitoring

### Checkpoints
Local checkpoints stored in: `/opt/workspace/checkpoints/polars_{subreddit}/`

### Failed Batches
Failed writes backed up to: `/opt/workspace/checkpoints/polars_{subreddit}_failed/`

### Console Logs
- Batch size and processing time
- Delta table write confirmations
- Error messages with timestamps

## Data Output

### S3 Delta Table Structure
```
s3://reddit-streaming-stevenhurwitt-2/{subreddit}/
├── _delta_log/
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   └── ...
└── part-*.parquet
```

### Reading Data Back

Using Polars:
```python
from deltalake import DeltaTable
import polars as pl

storage_options = {
    "AWS_ACCESS_KEY_ID": "...",
    "AWS_SECRET_ACCESS_KEY": "...",
    "AWS_REGION": "us-east-2"
}

dt = DeltaTable("s3://reddit-streaming-stevenhurwitt-2/technology", 
                storage_options=storage_options)
df = pl.from_arrow(dt.to_pyarrow_table())
print(df)
```

Using DuckDB:
```python
import duckdb

conn = duckdb.connect()
conn.execute("""
    INSTALL delta;
    LOAD delta;
    
    SELECT * FROM delta_scan('s3://reddit-streaming-stevenhurwitt-2/technology')
    LIMIT 10;
""")
```

## Performance Tuning

### Batch Size
- **Small batches (10-50)**: Lower latency, more frequent writes
- **Large batches (100-500)**: Better throughput, less S3 API calls

### Processing Interval
- **Short interval (60s)**: Near real-time processing
- **Long interval (300s)**: Reduced overhead, better for high volume

### Memory Usage
Polars is memory-efficient, but for very large batches:
- Monitor with: `df.estimated_size("mb")`
- Adjust batch_size if memory issues occur

## Troubleshooting

### Kafka Connection Issues
```
Error: Kafka error: Local: Broker transport failure
```
**Solution**: Check Kafka host in config.yaml, verify Kafka is running

### S3 Write Failures
```
Error writing to Delta table: Access Denied
```
**Solution**: Verify AWS credentials in creds.json, check S3 bucket permissions

### No Messages Received
```
No new messages for technology at 2026-02-13 10:30:00
```
**Solution**: Check Kafka producer is running, verify topic name, check offset position

### Schema Mismatch
If Reddit API changes, update schema in `get_polars_schema()` function.

## Comparison with Spark Version

| Feature | Spark Version | Polars Version |
|---------|--------------|----------------|
| Startup Time | ~30-60s | ~1-2s |
| Memory Usage | ~2-4GB | ~500MB-1GB |
| CPU Cores | 4+ (distributed) | 1 (single process) |
| Dependencies | JVM, Spark JARs | Python only |
| Throughput | Very High | Moderate-High |
| Latency | Low (streaming) | Medium (polling) |
| Setup Complexity | High | Low |
| Operational Complexity | High | Low |

## When to Use Which Version

### Use Spark Version When:
- Processing millions of records per hour
- Need distributed processing across cluster
- Already have Spark infrastructure
- Require exactly-once semantics
- Need complex stream joins/aggregations

### Use Polars Version When:
- Processing thousands to hundreds-of-thousands of records per hour
- Want simple deployment without JVM/cluster
- Prefer Python-native tooling
- Need faster development iteration
- Running on resource-constrained environments

## Future Enhancements

Potential improvements for the Polars version:

1. **Async Processing**: Use `asyncio` for concurrent Kafka consumption
2. **Metrics**: Add Prometheus metrics for monitoring
3. **Partitioning**: Implement time-based partitioning for Delta tables
4. **Compaction**: Add periodic Delta table optimization
5. **Schema Evolution**: Automatic schema migration on changes
6. **Data Quality**: Add validation and deduplication logic
7. **PostgreSQL Support**: Add optional JDBC-like writes to PostgreSQL
