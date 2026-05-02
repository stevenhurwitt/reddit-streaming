#!/usr/bin/env python3
"""
Local wrapper script to run curation jobs using Polars instead of PySpark.
Significantly faster than the Spark version and no Java dependencies.
Implements batch processing to handle large datasets without memory issues.

Usage: python run_curation_job_polars.py --job news [--bucket BUCKET_NAME] [--batch-size 10000]
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Generator, Set

import boto3
import polars as pl
import s3fs
from deltalake import DeltaTable, write_deltalake
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_aws_credentials():
    """Retrieve AWS credentials from Secrets Manager or local files."""
    try:
        # Try Secrets Manager first (if running on AWS)
        secretmanager_client = boto3.client("secretsmanager", region_name="us-east-1")
        aws_client = json.loads(
            secretmanager_client.get_secret_value(SecretId="AWS_ACCESS_KEY_ID")["SecretString"]
        )["AWS_ACCESS_KEY_ID"]
        aws_secret = json.loads(
            secretmanager_client.get_secret_value(SecretId="AWS_SECRET_ACCESS_KEY")["SecretString"]
        )["AWS_SECRET_ACCESS_KEY"]
        logger.info("Retrieved AWS credentials from Secrets Manager")
    except Exception as e:
        logger.warning(f"Failed to retrieve from Secrets Manager: {e}")
        # Fall back to local credential files
        # Try current directory first, then parent directory (for Docker)
        credential_paths = [
            ("aws_access_key.txt", "aws_secret.txt"),
            ("../aws_access_key.txt", "../aws_secret.txt"),
            ("/opt/workspace/aws_access_key.txt", "/opt/workspace/aws_secret.txt")
        ]
        
        for key_path, secret_path in credential_paths:
            try:
                with open(key_path) as f:
                    aws_client = f.read().strip()
                with open(secret_path) as f:
                    aws_secret = f.read().strip()
                logger.info(f"Retrieved AWS credentials from local files: {key_path}")
                return aws_client, aws_secret
            except Exception:
                continue
        
        logger.error("Failed to retrieve credentials from any location")
        raise FileNotFoundError("AWS credentials not found")

    return aws_client, aws_secret


def read_delta_from_s3_batches(bucket, subreddit, aws_access_key, aws_secret_key, 
                               batch_size: int = 10000) -> Generator[pl.DataFrame, None, None]:
    """Read Delta table from S3 in batches using Polars to minimize memory usage.
    
    Args:
        bucket: S3 bucket name
        subreddit: Subreddit name (used as path in bucket)
        aws_access_key: AWS access key
        aws_secret_key: AWS secret key
        batch_size: Number of records per batch
    
    Yields:
        Polars DataFrames of size ~batch_size
    """
    try:
        logger.info(f"Reading Delta table from s3://{bucket}/{subreddit} in batches of {batch_size}")
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key,
            "AWS_REGION": "us-east-2"
        }
        
        table_uri = f"s3://{bucket}/{subreddit}"
        dt = DeltaTable(table_uri, storage_options=storage_options)
        
        # Convert Delta table to Polars DataFrame via PyArrow
        arrow_table = dt.to_pyarrow_table()
        df = pl.from_arrow(arrow_table)
        
        # Get total record count
        total_records = len(df)
        logger.info(f"Total records to process: {total_records}")
        
        # Read and yield in batches
        offset = 0
        batch_num = 0
        while offset < total_records:
            batch_num += 1
            df_batch = df.slice(offset, batch_size)
            
            if len(df_batch) > 0:
                logger.info(f"Batch {batch_num}: Read {len(df_batch)} records (offset: {offset})")
                yield df_batch
            
            offset += batch_size
        
        logger.info(f"Completed reading all {batch_num} batches")
        
    except Exception as e:
        logger.error(f"Error reading Delta table: {str(e)}", exc_info=True)
        raise


def transform_data_batch(df: pl.DataFrame, seen_titles: Set[str]) -> tuple[pl.DataFrame, Set[str]]:
    """Transform and clean a batch of data using Polars.
    
    Args:
        df: Polars DataFrame (single batch)
        seen_titles: Set of titles already seen in previous batches
    
    Returns:
        Tuple of (transformed DataFrame, updated set of seen titles)
    """
    try:
        logger.info(f"Transforming batch of {len(df)} records...")
        
        df = df.with_columns([
            pl.from_epoch("approved_at_utc", time_unit="s").alias("approved_at_utc") if "approved_at_utc" in df.columns else pl.lit(None).alias("approved_at_utc"),
            pl.from_epoch("banned_at_utc", time_unit="s").alias("banned_at_utc") if "banned_at_utc" in df.columns else pl.lit(None).alias("banned_at_utc"),
            pl.from_epoch("created_utc", time_unit="s").alias("created_utc"),
            pl.from_epoch("created", time_unit="s").alias("created") if "created" in df.columns else pl.lit(None).alias("created"),
        ])
        
        df = df.with_columns([
            pl.col("created_utc").cast(pl.Date).alias("date"),
        ]).with_columns([
            pl.col("date").dt.year().alias("year"),
            pl.col("date").dt.month().alias("month"),
            pl.col("date").dt.day().alias("day"),
        ])
        
        # Deduplicate within this batch and against seen titles from previous batches
        before_count = len(df)
        
        # Remove duplicates within this batch
        df = df.unique(subset=["title"])
        
        # Remove titles seen in previous batches
        titles_in_batch = set(df["title"].to_list())
        new_titles = titles_in_batch - seen_titles
        df = df.filter(pl.col("title").is_in(new_titles))
        
        after_count = len(df)
        duplicates_removed = before_count - after_count
        
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicates from batch (within batch + cross-batch)")
        
        # Update seen titles for next batch
        seen_titles.update(new_titles)
        
        logger.info(f"Batch transformed: {after_count} records after deduplication")
        return df, seen_titles
        
    except Exception as e:
        logger.error(f"Error transforming data batch: {str(e)}", exc_info=True)
        raise


def write_delta_batch_to_s3(df_batch: pl.DataFrame, bucket, subreddit, aws_access_key, 
                             aws_secret_key, is_first_batch: bool = False):
    """Write a batch of cleaned data to S3 as Delta table.
    
    Args:
        df_batch: Polars DataFrame (single batch)
        bucket: S3 bucket name
        subreddit: Subreddit name
        aws_access_key: AWS access key
        aws_secret_key: AWS secret key
        is_first_batch: If True, use 'overwrite' mode; otherwise 'append'
    """
    try:
        filepath = f"s3://{bucket}/{subreddit}_clean/"
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key,
            "AWS_REGION": "us-east-2"
        }
        
        arrow_table = df_batch.to_arrow()
        
        mode = "overwrite" if is_first_batch else "append"
        
        write_deltalake(
            filepath,
            arrow_table,
            mode=mode,
            storage_options=storage_options,
            partition_by=["year", "month", "day"],
            schema_mode="overwrite" if is_first_batch else "merge"
        )
        
        logger.info(f"Wrote batch ({mode} mode) to {filepath}")
        
    except Exception as e:
        logger.error(f"Error writing batch to S3: {str(e)}", exc_info=True)
        raise


def get_existing_post_ids(subreddit, db_host="localhost", db_port=5434, db_name="reddit",
                          db_user="postgres", db_password="secret!1234", db_schema="reddit_schema"):
    """Fetch existing post IDs from PostgreSQL efficiently.
    
    Returns:
        Set of existing post IDs, or empty set if table doesn't exist
    """
    try:
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        
        table_name = subreddit.lower().replace(" ", "_")
        
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT DISTINCT post_id FROM {db_schema}.{table_name}")
                )
                existing_ids = set(row[0] for row in result)
            
            logger.info(f"Found {len(existing_ids)} existing records in PostgreSQL")
            return existing_ids
            
        except Exception as e:
            logger.warning(f"Could not read existing records (table may not exist): {e}")
            return set()
            
    except Exception as e:
        logger.error(f"Error fetching existing post IDs: {str(e)}", exc_info=True)
        raise


def write_to_postgres_batch(df_batch: pl.DataFrame, subreddit, existing_ids: Set[str],
                            db_host="localhost", db_port=5434, db_name="reddit",
                            db_user="postgres", db_password="secret!1234", db_schema="reddit_schema",
                            handle_duplicates="skip"):
    """Write a batch of cleaned data to PostgreSQL.
    
    Args:
        df_batch: Polars DataFrame (single batch)
        subreddit: Subreddit name
        existing_ids: Set of post IDs already in the database
        db_host: PostgreSQL host
        db_port: PostgreSQL port
        db_name: Database name
        db_user: Database user
        db_password: Database password
        db_schema: Schema name
        handle_duplicates: How to handle duplicates ("skip" or "overwrite")
    
    Returns:
        Tuple of (records_written, records_skipped)
    """
    try:
        table_name = subreddit.lower().replace(" ", "_")
        
        columns_to_write = ["id", "title", "author", "score", "num_comments",
                           "created_utc", "url", "selftext"]
        
        available_columns = [col for col in columns_to_write if col in df_batch.columns]
        df_pg = df_batch.select(available_columns)
        
        if "id" in df_pg.columns:
            df_pg = df_pg.rename({"id": "post_id"})
        
        total_records = len(df_pg)
        
        if handle_duplicates == "skip":
            df_pg = df_pg.filter(~pl.col("post_id").is_in(existing_ids))
            new_records = len(df_pg)
            
            if new_records == 0:
                logger.info(f"Batch: All {total_records} records already exist, skipping")
                return 0, total_records
            
            logger.info(f"Batch: Writing {new_records}/{total_records} new records to PostgreSQL")
            
            # Update existing_ids with new records
            for post_id in df_pg["post_id"].to_list():
                existing_ids.add(post_id)
        else:
            logger.info(f"Batch: Writing {total_records} records (overwrite mode) to PostgreSQL")
        
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        if_table_exists = "replace" if handle_duplicates == "overwrite" else "append"
        
        qualified_table_name = f"{db_schema}.{table_name}"
        df_pg.write_database(
            table_name=qualified_table_name,
            connection=connection_string,
            if_table_exists=if_table_exists,
            engine="sqlalchemy"
        )
        
        return new_records if handle_duplicates == "skip" else total_records, \
               (total_records - new_records) if handle_duplicates == "skip" else 0
        
    except Exception as e:
        logger.error(f"Error writing batch to PostgreSQL: {str(e)}", exc_info=True)
        raise


def vacuum_delta_table(bucket, subreddit, aws_access_key, aws_secret_key):
    """Vacuum old files from Delta table (keep 7 days of history)."""
    try:
        filepath = f"s3://{bucket}/{subreddit}_clean/"
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key,
            "AWS_REGION": "us-east-2"
        }
        
        logger.info("Running vacuum on Delta table...")
        dt = DeltaTable(filepath, storage_options=storage_options)
        dt.vacuum(retention_hours=168)  # 7 days
        logger.info("Vacuum complete")
        
    except Exception as e:
        logger.error(f"Error vacuuming Delta table: {str(e)}", exc_info=True)
        raise


def process_subreddit(subreddit, bucket, aws_access_key, aws_secret_key, batch_size=10000,
                      handle_duplicates="skip"):
    """Process a subreddit curation job using Polars with batch processing.
    
    Args:
        subreddit: Subreddit name to process
        bucket: S3 bucket name
        aws_access_key: AWS access key
        aws_secret_key: AWS secret key
        batch_size: Number of records per batch
        handle_duplicates: How to handle duplicate records in PostgreSQL
            - "skip": Only write new records (default, recommended)
            - "overwrite": Replace all existing data
    """
    try:
        logger.info(f"Starting curation for subreddit: {subreddit}")
        logger.info(f"Batch size: {batch_size}")
        
        # Fetch existing post IDs from PostgreSQL once
        existing_ids = get_existing_post_ids(subreddit)
        
        # Track titles for cross-batch deduplication
        seen_titles = set()
        
        # Metrics
        total_records_read = 0
        total_records_written_s3 = 0
        total_records_written_pg = 0
        total_duplicates_skipped_pg = 0
        batch_num = 0
        first_batch = True
        
        # Process in batches
        for df_batch in read_delta_from_s3_batches(bucket, subreddit, aws_access_key, aws_secret_key, batch_size):
            batch_num += 1
            total_records_read += len(df_batch)
            
            if len(df_batch) == 0:
                continue
            
            # Transform batch
            df_batch, seen_titles = transform_data_batch(df_batch, seen_titles)
            
            if len(df_batch) == 0:
                logger.info(f"Batch {batch_num}: All records were duplicates, skipping")
                continue
            
            total_records_written_s3 += len(df_batch)
            
            # Write to Delta on S3
            write_delta_batch_to_s3(df_batch, bucket, subreddit, aws_access_key, aws_secret_key, 
                                     is_first_batch=first_batch)
            first_batch = False
            
            # Write to PostgreSQL
            written, skipped = write_to_postgres_batch(df_batch, subreddit, existing_ids,
                                                        handle_duplicates=handle_duplicates)
            total_records_written_pg += written
            total_duplicates_skipped_pg += skipped
        
        # Vacuum Delta table after all batches
        if batch_num > 0:
            vacuum_delta_table(bucket, subreddit, aws_access_key, aws_secret_key)
        
        # Log summary
        logger.info("=" * 60)
        logger.info("Curation Job Summary")
        logger.info("=" * 60)
        logger.info(f"Total records read:              {total_records_read}")
        logger.info(f"Total records written to S3:     {total_records_written_s3}")
        logger.info(f"Total records written to PG:     {total_records_written_pg}")
        logger.info(f"Total duplicates skipped (PG):   {total_duplicates_skipped_pg}")
        logger.info(f"Total batches processed:         {batch_num}")
        logger.info("=" * 60)
        
        logger.info(f"Curation for {subreddit} completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing {subreddit}: {str(e)}", exc_info=True)
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Run reddit curation jobs with Polars (much faster than Spark!)"
    )
    parser.add_argument(
        "--job",
        required=True,
        choices=["news", "technology", "ProgrammerHumor", "worldnews", "aws",
                 "bikinibottomtwitter", "blackpeopletwitter", "whitepeopletwitter"],
        help="Curation job to run"
    )
    parser.add_argument(
        "--bucket",
        default="reddit-streaming-stevenhurwitt-2",
        help="S3 bucket name (default: reddit-streaming-stevenhurwitt-2)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Records per batch (default: 10000). Adjust based on available memory."
    )
    parser.add_argument(
        "--handle-duplicates",
        default="skip",
        choices=["skip", "overwrite"],
        help="How to handle duplicate records when writing to PostgreSQL (default: skip)"
    )

    args = parser.parse_args()

    try:
        logger.info(f"Starting Polars curation job for: {args.job}")
        logger.info("Using Polars with batch processing - no Java/Spark required! 🚀")
        
        # Get credentials
        aws_access_key, aws_secret_key = get_aws_credentials()
        
        # Process subreddit
        process_subreddit(
            args.job, 
            args.bucket, 
            aws_access_key, 
            aws_secret_key,
            batch_size=args.batch_size,
            handle_duplicates=args.handle_duplicates
        )
        
        logger.info("Job completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
