import os
import json
import yaml
import polars as pl
from deltalake import DeltaTable
from datetime import datetime

# Function to read credentials and config
def find_file(filename, search_paths):
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
    print(f"Current working directory: {os.getcwd()}")
    
    # Creds paths to search
    creds_paths = [
        "creds.json",
        "redditStreaming/creds.json",
        "redditStreaming/src/reddit/creds.json",
        "/home/steven/reddit-streaming/creds.json",
        "/home/steven/reddit-streaming/redditStreaming/creds.json",
        "/home/steven/reddit-streaming/redditStreaming/src/reddit/creds.json",
        "/opt/workspace/creds.json",
        "/opt/workspace/redditStreaming/creds.json"
    ]
    
    creds_file = find_file("creds.json", creds_paths)
    if not creds_file:
         raise FileNotFoundError("Could not find creds.json in search paths or parent directories.")
    
    print(f"Found credentials at: {creds_file}")
    with open(creds_file, "r") as f:
        creds = json.load(f)

    # Config paths to search
    config_paths = [
        "config.yaml",
        "redditStreaming/config.yaml",
        "redditStreaming/src/reddit/config.yaml",
        "/home/steven/reddit-streaming/config.yaml",
        "/home/steven/reddit-streaming/redditStreaming/config.yaml",
        "/home/steven/reddit-streaming/redditStreaming/src/reddit/config.yaml",
        "/opt/workspace/redditStreaming/config.yaml"
    ]
    
    config_file = find_file("config.yaml", config_paths)
    if not config_file:
         print("Warning: Could not find config.yaml, using defaults or trying alternate locations.")
         # Fallback search if needed, but for now we raise error if strictly required
         # In the original code config was critical for bucket/spark host, but here we need subreddits.
         pass
         
    if config_file:
        print(f"Found config at: {config_file}")
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
    else:
        # Fallback config if file missing
        config = {"subreddit": ["technology", "ProgrammerHumor", "news", "worldnews"]}
        print("Using default configuration for subreddits.")
        
    return creds, config

creds, config = read_config_creds()

# Extract values
aws_client = creds.get("aws_client")
aws_secret = creds.get("aws_secret")
subreddits = config.get("subreddit", ["technology", "ProgrammerHumor", "news", "worldnews"])
bucket_name = "reddit-streaming-stevenhurwitt-2" 

print(f"Subreddits to process: {subreddits}")

# Set up AWS credentials for Delta Lake
storage_options = {
    "AWS_ACCESS_KEY_ID": aws_client,
    "AWS_SECRET_ACCESS_KEY": aws_secret,
    "AWS_REGION": "us-east-2"  # Adjust if your bucket is in a different region
}

# Read Delta tables for each subreddit
dfs = []

for sub in subreddits:
    path = f"s3://{bucket_name}/{sub}"
    print(f"Attempting to read from: {path}")
    try:
        # Read Delta table using deltalake
        dt = DeltaTable(path, storage_options=storage_options)
        df = dt.to_pyarrow_table()
        
        # Convert to Polars DataFrame
        pl_df = pl.from_arrow(df)
        dfs.append(pl_df)
        print(f"Successfully read data for {sub}. Count: {len(pl_df)}")
    except Exception as e:
        print(f"Error reading {sub} (might not exist yet): {str(e)}")

# Union all dataframes and get top 100 sorted by created_utc
if dfs:
    # Concatenate all dataframes (union)
    full_df = pl.concat(dfs)
    
    # Sort by created_utc (most recent first) and take top 100
    sorted_df = full_df.sort("created_utc", descending=True).head(100)
    
    # Add a human-readable timestamp column
    display_df = sorted_df.with_columns([
        pl.from_epoch("created_utc", time_unit="s").alias("created_time")
    ])
    
    # Select relevant columns for display
    columns_to_show = ["subreddit", "created_time", "title", "score", "author"]
    # Only select columns that exist
    available_cols = [col for col in columns_to_show if col in display_df.columns]
    
    print(f"Total records in union: {len(full_df)}")
    print(f"\nTop 100 most recent posts:")
    print(display_df.select(available_cols))
else:
    print("No data available.")
