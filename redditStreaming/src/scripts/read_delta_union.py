import os
import json
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from functools import reduce
from pyspark.sql import DataFrame

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
# Initialize Spark Session with Delta and S3 support
# Using the same jars as in the main application
extra_jar_list = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-common:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-client:3.3.1,io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.5.0"

spark = SparkSession.builder \
    .appName("RedditDeltaRead") \
    .master("local[*]") \
    .config("spark.jars.packages", extra_jar_list) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", aws_client) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session created.")

# Read Delta tables for each subreddit
dfs = []

for sub in subreddits:
    path = f"s3a://{bucket_name}/{sub}"
    print(f"Attempting to read from: {path}")
    try:
        # Read Delta table
        df = spark.read.format("delta").load(path)
        dfs.append(df)
        print(f"Successfully read data for {sub}. Count: {df.count()}")
    except Exception as e:
        print(f"Error reading {sub} (might not exist yet): {str(e)}")

# Union all dataframes and sort
if dfs:
    # Union all dataframes found
    # takes 10 min
    full_df = reduce(DataFrame.unionAll, dfs)
    
    # Sort by created_utc (most recent first)
    # created_utc is likely a float timestamp
    sorted_df = full_df.orderBy(col("created_utc").desc())
    
    # Show result (selecting a few relevant columns)
    # takes 10 min
    display_df = sorted_df.withColumn("created_time", from_unixtime("created_utc")) \
                          .select("subreddit", "created_time", "title", "score", "author")
    
    print(f"Total records: {sorted_df.count()}")
    display_df.show(100, truncate=False)
else:
    print("No data available.")
