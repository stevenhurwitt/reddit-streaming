import os
import json
import yaml
import polars as pl
from deltalake import DeltaTable
from datetime import datetime

# Function to find files in the workspace
def find_file(filename, search_paths):
    """Search for a file in specified paths and parent directories"""
    # Check specific paths first
    for path in search_paths:
        if os.path.exists(path):
            return path
    
    # Walk up from CWD
    curr = os.getcwd()
    while True:
        f = os.path.join(curr, filename)
        if os.path.exists(f):
            return f
        parent = os.path.dirname(curr)
        if parent == curr:
            break
        curr = parent
    return None

def read_config_creds():
    """Read credentials and configuration files"""
    print(f"Current working directory: {os.getcwd()}")
    
    # Creds paths to search
    creds_paths = [
        "creds.json",
        "redditStreaming/creds.json",
        "/home/steven/reddit-streaming/creds.json",
        "/opt/workspace/creds.json",
    ]
    
    creds_file = find_file("creds.json", creds_paths)
    if not creds_file:
         raise FileNotFoundError("Could not find creds.json")
    
    print(f"Found credentials at: {creds_file}")
    with open(creds_file, "r") as f:
        creds = json.load(f)

    # Config paths to search
    config_paths = [
        "config.yaml",
        "redditStreaming/config.yaml",
        "/home/steven/reddit-streaming/config.yaml",
        "/opt/workspace/config.yaml"
    ]
    
    config_file = find_file("config.yaml", config_paths)
    if not config_file:
         raise FileNotFoundError("Could not find config.yaml")
         
    print(f"Found config at: {config_file}")
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
        
    return creds, config

# Load configuration and credentials
creds, config = read_config_creds()

# Extract values
aws_client = creds.get("aws_client")
aws_secret = creds.get("aws_secret")
subreddits = config.get("subreddit", [])
bucket_name = "reddit-streaming-stevenhurwitt-2" 

print(f"Subreddits to process: {subreddits}")
print(f"S3 Bucket: {bucket_name}")

# Configure AWS credentials for Delta Lake S3 access
storage_options = {
    "AWS_ACCESS_KEY_ID": aws_client,
    "AWS_SECRET_ACCESS_KEY": aws_secret,
    "AWS_REGION": "us-east-2"  # Adjust if your bucket is in a different region
}

print("AWS credentials configured for Delta Lake.")

# Read cleaned Delta tables for each subreddit
dfs = {}

for sub in subreddits:
    path = f"s3://{bucket_name}/{sub}_clean"
    print(f"Reading from: {path}")
    try:
        # Read Delta table using deltalake and convert to Polars
        dt = DeltaTable(path, storage_options=storage_options)
        df = dt.to_pyarrow_table()
        df = pl.from_arrow(df)
        dfs[sub] = df
        count = len(df)
        print(f"✓ Successfully read {sub}_clean: {count:,} records")
    except Exception as e:
        print(f"✗ Error reading {sub}_clean: {str(e)}")

print(f"\nTotal tables loaded: {len(dfs)}")

# Union all dataframes and sort by recency
if dfs:
    # Concatenate all dataframes
    all_dfs = list(dfs.values())
    combined_df = pl.concat(all_dfs)
    
    # Sort by created_utc (most recent first)
    sorted_df = combined_df.sort("created_utc", descending=True)
    
    # Add human-readable timestamp
    display_df = sorted_df.with_columns(
        pl.from_epoch("created_utc", time_unit="s").alias("created_time")
    )
    
    total_count = len(sorted_df)
    print(f"Total records across all subreddits: {total_count:,}")
    print(f"\nMost recent posts across all subreddits:")
    
    # Display relevant columns
    print(display_df.select([
            "subreddit", 
            "created_time", 
            "title", 
            "score", 
            "author",
            "num_comments"

        ]).head(50)    
    )
    
else:
    print("No data available from any subreddit.")

with pl.Config(tbl_rows=20):
    print(display_df.group_by("subreddit", "date").agg([
        pl.len().alias("post_count")]).sort("date", descending=True).head(20))