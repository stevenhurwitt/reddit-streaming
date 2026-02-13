"""
Reddit Streaming with Polars and Delta Lake

This module replicates the Spark streaming functionality using Polars for processing
and writing to S3 Delta tables.
"""

import polars as pl
import json
import os
import sys
import yaml
import time
import pprint
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException
from deltalake import DeltaTable, write_deltalake
import s3fs
from datetime import datetime

pp = pprint.PrettyPrinter(indent=1)


def read_files():
    """
    Initializes configuration using config.yaml and creds.json files.
    
    Returns:
        tuple: (creds dict, config dict)
    """
    base = os.getcwd()
    creds_path = os.path.join(base, "creds.json")

    try:
        with open(creds_path, "r") as f:
            creds = json.load(f)
    except FileNotFoundError:
        try:
            with open("/opt/workspace/redditStreaming/creds.json", "r") as f:
                creds = json.load(f)
        except FileNotFoundError:
            print("Failed to find creds.json.")
            sys.exit(1)

    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
    except:
        try:
            with open("/opt/workspace/redditStreaming/src/reddit/config.yaml", "r") as f:
                config = yaml.safe_load(f)
        except:
            print("Failed to find config.yaml, exiting now.")
            sys.exit(1)

    return creds, config


def get_polars_schema():
    """
    Define the Polars schema matching the Spark schema.
    
    Returns:
        dict: Polars schema with field names and types
    """
    schema = {
        "approved_at_utc": pl.Float32,
        "subreddit": pl.Utf8,
        "selftext": pl.Utf8,
        "author_fullname": pl.Utf8,
        "saved": pl.Boolean,
        "mod_reason_title": pl.Utf8,
        "gilded": pl.Int32,
        "clicked": pl.Boolean,
        "title": pl.Utf8,
        "subreddit_name_prefixed": pl.Utf8,
        "hidden": pl.Boolean,
        "pwls": pl.Int32,
        "link_flair_css_class": pl.Utf8,
        "downs": pl.Int32,
        "thumbnail_height": pl.Int32,
        "top_awarded_type": pl.Utf8,
        "hide_score": pl.Boolean,
        "name": pl.Utf8,
        "quarantine": pl.Boolean,
        "link_flair_text_color": pl.Utf8,
        "upvote_ratio": pl.Float32,
        "author_flair_background_color": pl.Utf8,
        "ups": pl.Int32,
        "total_awards_received": pl.Int32,
        "thumbnail_width": pl.Int32,
        "author_flair_template_id": pl.Utf8,
        "is_original_content": pl.Boolean,
        "secure_media": pl.Utf8,
        "is_reddit_media_domain": pl.Boolean,
        "is_meta": pl.Boolean,
        "category": pl.Utf8,
        "link_flair_text": pl.Utf8,
        "can_mod_post": pl.Boolean,
        "score": pl.Int32,
        "approved_by": pl.Utf8,
        "is_created_from_ads_ui": pl.Boolean,
        "author_premium": pl.Boolean,
        "thumbnail": pl.Utf8,
        "edited": pl.Boolean,
        "author_flair_css_class": pl.Utf8,
        "post_hint": pl.Utf8,
        "content_categories": pl.Utf8,
        "is_self": pl.Boolean,
        "subreddit_type": pl.Utf8,
        "created": pl.Float32,
        "link_flair_type": pl.Utf8,
        "wls": pl.Int32,
        "removed_by_category": pl.Utf8,
        "banned_by": pl.Utf8,
        "author_flair_type": pl.Utf8,
        "domain": pl.Utf8,
        "allow_live_comments": pl.Boolean,
        "selftext_html": pl.Utf8,
        "likes": pl.Int32,
        "suggested_sort": pl.Utf8,
        "banned_at_utc": pl.Float32,
        "url_overridden_by_dest": pl.Utf8,
        "view_count": pl.Int32,
        "archived": pl.Boolean,
        "no_follow": pl.Boolean,
        "is_crosspostable": pl.Boolean,
        "pinned": pl.Boolean,
        "over_18": pl.Boolean,
        "media_only": pl.Boolean,
        "link_flair_template_id": pl.Utf8,
        "can_gild": pl.Boolean,
        "spoiler": pl.Boolean,
        "locked": pl.Boolean,
        "author_flair_text": pl.Utf8,
        "visited": pl.Boolean,
        "removed_by": pl.Utf8,
        "mod_note": pl.Utf8,
        "distinguished": pl.Utf8,
        "subreddit_id": pl.Utf8,
        "author_is_blocked": pl.Boolean,
        "mod_reason_by": pl.Utf8,
        "num_reports": pl.Int32,
        "removal_reason": pl.Utf8,
        "link_flair_background_color": pl.Utf8,
        "id": pl.Utf8,
        "is_robot_indexable": pl.Boolean,
        "report_reasons": pl.Utf8,
        "author": pl.Utf8,
        "discussion_type": pl.Utf8,
        "num_comments": pl.Int32,
        "send_replies": pl.Boolean,
        "whitelist_status": pl.Utf8,
        "contest_mode": pl.Boolean,
        "author_patreon_flair": pl.Boolean,
        "author_flair_text_color": pl.Utf8,
        "permalink": pl.Utf8,
        "parent_whitelist_status": pl.Utf8,
        "stickied": pl.Boolean,
        "url": pl.Utf8,
        "subreddit_subscribers": pl.Int32,
        "created_utc": pl.Float32,
        "num_crossposts": pl.Int32,
        "media": pl.Utf8,
        "is_video": pl.Boolean,
    }
    
    return schema


def create_kafka_consumer(kafka_host: str, subreddit: str, group_id: Optional[str] = None) -> Consumer:
    """
    Create a Kafka consumer for the specified subreddit.
    
    Args:
        kafka_host: Kafka broker hostname
        subreddit: Subreddit name
        group_id: Consumer group ID (optional)
    
    Returns:
        Consumer: Configured Kafka consumer
    """
    if group_id is None:
        group_id = f"polars_reddit_{subreddit}"
    
    conf = {
        'bootstrap.servers': f'{kafka_host}:9092',
        'group.id': group_id,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'session.timeout.ms': 45000,
        'heartbeat.interval.ms': 10000,
        'max.poll.interval.ms': 300000,
    }
    
    consumer = Consumer(conf)
    topic = f"reddit_{subreddit}"
    consumer.subscribe([topic])
    
    print(f"Kafka consumer created for topic: {topic}")
    return consumer


def parse_message(msg_value: bytes, schema: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a Kafka message and validate against schema.
    
    Args:
        msg_value: Raw message bytes
        schema: Expected schema
    
    Returns:
        Parsed message dict or None if parsing fails
    """
    try:
        data = json.loads(msg_value.decode('utf-8'))
        
        # Ensure all schema fields are present (fill with None if missing)
        parsed_data = {}
        for field_name in schema.keys():
            parsed_data[field_name] = data.get(field_name, None)
        
        return parsed_data
    except json.JSONDecodeError as e:
        print(f"Failed to parse message: {e}")
        return None
    except Exception as e:
        print(f"Error processing message: {e}")
        return None


def consume_batch(consumer: Consumer, schema: Dict[str, Any], 
                  batch_size: int = 100, timeout: float = 30.0) -> pl.DataFrame:
    """
    Consume a batch of messages from Kafka and return as Polars DataFrame.
    
    Args:
        consumer: Kafka consumer
        schema: Polars schema
        batch_size: Maximum number of messages to consume
        timeout: Maximum time to wait for messages (seconds)
    
    Returns:
        Polars DataFrame with consumed messages
    """
    messages = []
    start_time = time.time()
    
    while len(messages) < batch_size and (time.time() - start_time) < timeout:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Kafka error: {msg.error()}")
                continue
        
        parsed = parse_message(msg.value(), schema)
        if parsed:
            messages.append(parsed)
    
    if messages:
        # Create DataFrame from messages
        df = pl.DataFrame(messages, schema=schema)
        return df
    else:
        # Return empty DataFrame with schema
        return pl.DataFrame(schema=schema)


def write_to_console(df: pl.DataFrame, subreddit: str):
    """
    Write a subset of the DataFrame to console for monitoring.
    
    Args:
        df: Polars DataFrame
        subreddit: Subreddit name
    """
    if df.height > 0:
        # Convert created_utc to timestamp and select subset
        console_df = df.select([
            pl.col("subreddit"),
            pl.col("title"),
            pl.col("score"),
            pl.from_epoch("created_utc", time_unit="s").alias("created_utc")
        ])
        
        print(f"\n=== {subreddit} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        print(console_df.head(10))
        print(f"Batch size: {df.height} records\n")


def write_to_delta(df: pl.DataFrame, subreddit: str, creds: Dict[str, str]):
    """
    Write DataFrame to S3 Delta table.
    
    Args:
        df: Polars DataFrame
        subreddit: Subreddit name
        creds: AWS credentials
    """
    if df.height == 0:
        return
    
    bucket = "reddit-streaming-stevenhurwitt-2"
    table_path = f"s3://{bucket}/{subreddit}"
    
    # Set up S3 storage options
    storage_options = {
        "AWS_ACCESS_KEY_ID": creds["aws_client"],
        "AWS_SECRET_ACCESS_KEY": creds["aws_secret"],
        "AWS_REGION": "us-east-2",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
    
    try:
        # Check if Delta table exists
        try:
            dt = DeltaTable(table_path, storage_options=storage_options)
            # Append to existing table
            write_deltalake(
                table_path,
                df,
                mode="append",
                storage_options=storage_options
            )
            print(f"Appended {df.height} records to Delta table: {table_path}")
        except Exception:
            # Create new Delta table
            write_deltalake(
                table_path,
                df,
                mode="overwrite",
                storage_options=storage_options
            )
            print(f"Created new Delta table with {df.height} records: {table_path}")
    
    except Exception as e:
        print(f"Error writing to Delta table: {e}")
        # Fallback: save to local checkpoint
        checkpoint_dir = f"/opt/workspace/checkpoints/polars_{subreddit}_failed"
        os.makedirs(checkpoint_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{checkpoint_dir}/batch_{timestamp}.parquet"
        df.write_parquet(backup_path)
        print(f"Saved failed batch to: {backup_path}")


def stream_subreddit(subreddit: str, kafka_host: str, creds: Dict[str, str], 
                     batch_size: int = 100, processing_interval: int = 180):
    """
    Continuously stream data from Kafka for a specific subreddit and write to Delta.
    
    Args:
        subreddit: Subreddit name
        kafka_host: Kafka broker hostname
        creds: AWS credentials
        batch_size: Number of messages per batch
        processing_interval: Time between processing batches (seconds)
    """
    schema = get_polars_schema()
    consumer = create_kafka_consumer(kafka_host, subreddit)
    
    print(f"Starting streaming for subreddit: {subreddit}")
    print(f"Batch size: {batch_size}, Processing interval: {processing_interval}s")
    
    # Create checkpoint directory
    checkpoint_dir = f"/opt/workspace/checkpoints/polars_{subreddit}"
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    try:
        while True:
            start_time = time.time()
            
            # Consume batch
            df = consume_batch(consumer, schema, batch_size=batch_size, 
                             timeout=min(processing_interval, 30.0))
            
            # Process and write
            if df.height > 0:
                write_to_console(df, subreddit)
                write_to_delta(df, subreddit, creds)
            else:
                print(f"No new messages for {subreddit} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Wait for next processing interval
            elapsed = time.time() - start_time
            sleep_time = max(0, processing_interval - elapsed)
            if sleep_time > 0:
                print(f"Waiting {sleep_time:.1f}s until next batch...")
                time.sleep(sleep_time)
    
    except KeyboardInterrupt:
        print(f"\nStopping streaming for {subreddit}")
    except Exception as e:
        print(f"Error in streaming loop for {subreddit}: {e}")
    finally:
        consumer.close()
        print(f"Consumer closed for {subreddit}")


def main():
    """
    Initialize streaming for configured subreddits.
    """
    try:
        creds, config = read_files()
        
        try:
            subreddit = os.environ.get("subreddit")
        except:
            subreddit = None
        
        kafka_host = config.get("kafka_host", "localhost")
        subreddit_list = config.get("subreddit", [subreddit] if subreddit else ["technology"])
        debug = config.get("debug", False)
        
        print("Read creds & config.")
        
        if debug:
            print("CONFIG: ")
            pp.pprint(config)
        
        # For multiple subreddits, we could use multiprocessing, but for simplicity
        # we'll stream them sequentially or just the first one
        # In a production setup, you'd use multiprocessing.Process for each subreddit
        
        if len(subreddit_list) > 1:
            print(f"Warning: Multiple subreddits configured. Streaming only first: {subreddit_list[0]}")
            print("To stream multiple subreddits simultaneously, run separate instances.")
        
        subreddit_to_stream = subreddit_list[0]
        
        # Get processing interval from config or use default
        processing_interval = config.get("processing_interval", 180)
        batch_size = config.get("batch_size", 100)
        
        stream_subreddit(
            subreddit=subreddit_to_stream,
            kafka_host=kafka_host,
            creds=creds,
            batch_size=batch_size,
            processing_interval=processing_interval
        )
    
    except Exception as e:
        print(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        print("Starting Polars-based Reddit streaming...")
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
