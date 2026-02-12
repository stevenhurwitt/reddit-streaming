#!/usr/bin/env python3
"""
Local wrapper script to run curation jobs using Polars instead of PySpark.
Significantly faster than the Spark version and no Java dependencies.

Usage: python run_curation_job_polars.py --job news [--bucket BUCKET_NAME]
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

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


def read_delta_from_s3(bucket, subreddit, aws_access_key, aws_secret_key):
    """Read Delta table from S3 using Polars.
    
    Args:
        bucket: S3 bucket name
        subreddit: Subreddit name (used as path in bucket)
        aws_access_key: AWS access key
        aws_secret_key: AWS secret key
    
    Returns:
        Polars DataFrame
    """
    try:
        logger.info(f"Reading Delta table from s3://{bucket}/{subreddit}")
        
        # Set up S3 credentials for deltalake
        storage_options = {
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key,
            "AWS_REGION": "us-east-2"
        }
        
        # Read Delta table
        table_uri = f"s3://{bucket}/{subreddit}"
        dt = DeltaTable(table_uri, storage_options=storage_options)
        df = dt.to_pyarrow_table()
        
        # Convert to Polars
        df_polars = pl.from_arrow(df)
        
        record_count = len(df_polars)
        logger.info(f"Successfully read {record_count} records from {subreddit}")
        
        return df_polars
        
    except Exception as e:
        logger.error(f"Error reading Delta table: {str(e)}", exc_info=True)
        raise


def transform_data(df):
    """Transform and clean data using Polars.
    
    Args:
        df: Polars DataFrame
    
    Returns:
        Transformed Polars DataFrame
    """
    try:
        logger.info("Transforming data...")
        
        # Convert columns to appropriate types and add date columns
        # Note: created_utc is stored as Unix timestamp (seconds since epoch)
        df = df.with_columns([
            # Convert Unix timestamps to datetime
            pl.from_epoch("approved_at_utc", time_unit="s").alias("approved_at_utc") if "approved_at_utc" in df.columns else pl.lit(None).alias("approved_at_utc"),
            pl.from_epoch("banned_at_utc", time_unit="s").alias("banned_at_utc") if "banned_at_utc" in df.columns else pl.lit(None).alias("banned_at_utc"),
            pl.from_epoch("created_utc", time_unit="s").alias("created_utc"),
            pl.from_epoch("created", time_unit="s").alias("created") if "created" in df.columns else pl.lit(None).alias("created"),
        ])
        
        # Add date partitioning columns
        df = df.with_columns([
            pl.col("created_utc").cast(pl.Date).alias("date"),
        ]).with_columns([
            pl.col("date").dt.year().alias("year"),
            pl.col("date").dt.month().alias("month"),
            pl.col("date").dt.day().alias("day"),
        ])
        
        # Drop duplicates based on title
        original_count = len(df)
        df = df.unique(subset=["title"])
        dedup_count = len(df)
        
        if original_count != dedup_count:
            logger.info(f"Removed {original_count - dedup_count} duplicate titles")
        
        logger.info(f"Transformed {len(df)} records")
        return df
        
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}", exc_info=True)
        raise


def write_delta_to_s3(df, bucket, subreddit, aws_access_key, aws_secret_key):
    """Write cleaned DataFrame to S3 as Delta table.
    
    Args:
        df: Polars DataFrame
        bucket: S3 bucket name
        subreddit: Subreddit name
        aws_access_key: AWS access key
        aws_secret_key: AWS secret key
    """
    try:
        filepath = f"s3://{bucket}/{subreddit}_clean/"
        logger.info(f"Writing cleaned data to {filepath}")
        
        # Set up S3 credentials
        storage_options = {
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key,
            "AWS_REGION": "us-east-2"
        }
        
        # Convert to PyArrow for Delta Lake write
        arrow_table = df.to_arrow()
        
        # Write as Delta table with partitioning
        write_deltalake(
            filepath,
            arrow_table,
            mode="overwrite",
            storage_options=storage_options,
            partition_by=["year", "month", "day"],
            schema_mode="overwrite"
        )
        
        logger.info(f"Successfully wrote cleaned data to {filepath}")
        
        # Vacuum old files (keep 7 days of history)
        logger.info("Running vacuum on Delta table...")
        dt = DeltaTable(filepath, storage_options=storage_options)
        dt.vacuum(retention_hours=168)  # 7 days
        logger.info("Vacuum complete")
        
    except Exception as e:
        logger.error(f"Error writing to S3: {str(e)}", exc_info=True)
        raise


def write_to_postgres(df, subreddit, db_host="localhost", db_port=5434, db_name="reddit", 
                      db_user="postgres", db_password="secret!1234", db_schema="reddit_schema",
                      handle_duplicates="skip"):
    """Write cleaned dataframe to PostgreSQL database using Polars.
    
    Args:
        df: Polars DataFrame to write
        subreddit: Subreddit name
        db_host: PostgreSQL host
        db_port: PostgreSQL port
        db_name: Database name
        db_user: Database user
        db_password: Database password
        db_schema: Schema name
        handle_duplicates: How to handle duplicates
            - "skip": Only write records that don't exist (default, safe)
            - "overwrite": Delete all existing data and write new data
    """
    try:
        # Convert subreddit name to table name (replace spaces and special chars)
        table_name = subreddit.lower().replace(" ", "_")
        
        logger.info(f"Writing cleaned data to PostgreSQL: {db_schema}.{table_name}")
        logger.info(f"Duplicate handling mode: {handle_duplicates}")
        
        # Select relevant columns for PostgreSQL
        columns_to_write = ["id", "title", "author", "score", "num_comments", 
                           "created_utc", "url", "selftext"]
        
        # Filter to only columns that exist in the dataframe
        available_columns = [col for col in columns_to_write if col in df.columns]
        df_pg = df.select(available_columns)
        
        # Rename 'id' to 'post_id' for consistency with table schema
        if "id" in df_pg.columns:
            df_pg = df_pg.rename({"id": "post_id"})
        
        total_records = len(df_pg)
        logger.info(f"Total records to write: {total_records}")
        
        # Create database connection
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        
        # Handle duplicates based on mode
        if handle_duplicates == "skip":
            # Read existing IDs from PostgreSQL
            logger.info("Reading existing post_ids from PostgreSQL...")
            try:
                with engine.connect() as conn:
                    result = conn.execute(
                        text(f"SELECT DISTINCT post_id FROM {db_schema}.{table_name}")
                    )
                    existing_ids = set(row[0] for row in result)
                
                logger.info(f"Found {len(existing_ids)} existing records in PostgreSQL")
                
                # Filter out duplicates
                df_pg = df_pg.filter(~pl.col("post_id").is_in(existing_ids))
                new_records = len(df_pg)
                
                logger.info(f"Records to write after deduplication: {new_records}")
                logger.info(f"Skipping {total_records - new_records} duplicate records")
                
                if new_records == 0:
                    logger.warning(f"No new records to write - all {total_records} records already exist")
                    return
                    
            except Exception as e:
                # Table might not exist yet
                logger.warning(f"Could not read existing records (table may not exist): {e}")
                logger.warning("Writing all records")
        
        elif handle_duplicates == "overwrite":
            logger.info(f"Overwrite mode: will delete all existing data in {db_schema}.{table_name}")
        
        # Write to PostgreSQL
        if handle_duplicates == "overwrite":
            if_table_exists = "replace"
        else:
            if_table_exists = "append"
        
        # Write using Polars' native PostgreSQL support
        # Use fully qualified table name to ensure correct schema
        qualified_table_name = f"{db_schema}.{table_name}"
        df_pg.write_database(
            table_name=qualified_table_name,
            connection=connection_string,
            if_table_exists=if_table_exists,
            engine="sqlalchemy"
        )
        
        logger.info(f"Successfully wrote data to PostgreSQL: {db_schema}.{table_name}")
        
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}", exc_info=True)
        raise


def process_subreddit(subreddit, bucket, aws_access_key, aws_secret_key, handle_duplicates="skip"):
    """Process a subreddit curation job using Polars.
    
    Args:
        subreddit: Subreddit name to process
        bucket: S3 bucket name
        aws_access_key: AWS access key
        aws_secret_key: AWS secret key
        handle_duplicates: How to handle duplicate records in PostgreSQL
            - "skip": Only write new records (default, recommended)
            - "overwrite": Replace all existing data
    """
    try:
        logger.info(f"Starting curation for subreddit: {subreddit}")
        
        # Read Delta table from S3
        df = read_delta_from_s3(bucket, subreddit, aws_access_key, aws_secret_key)
        
        if len(df) == 0:
            logger.warning(f"No records found for {subreddit}")
            return
        
        # Transform and clean data
        df = transform_data(df)
        
        # Write cleaned data to Delta on S3
        write_delta_to_s3(df, bucket, subreddit, aws_access_key, aws_secret_key)
        
        # Write cleaned data to PostgreSQL
        write_to_postgres(df, subreddit, handle_duplicates=handle_duplicates)
        
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
        "--handle-duplicates",
        default="skip",
        choices=["skip", "overwrite"],
        help="How to handle duplicate records when writing to PostgreSQL (default: skip)"
    )

    args = parser.parse_args()

    try:
        logger.info(f"Starting Polars curation job for: {args.job}")
        logger.info("Using Polars - no Java/Spark required! 🚀")
        
        # Get credentials
        aws_access_key, aws_secret_key = get_aws_credentials()
        
        # Process subreddit
        process_subreddit(
            args.job, 
            args.bucket, 
            aws_access_key, 
            aws_secret_key,
            handle_duplicates=args.handle_duplicates
        )
        
        logger.info("Job completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
