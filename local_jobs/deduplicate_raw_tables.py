#!/usr/bin/env python3
"""
Deduplicate raw Delta Lake tables in S3 using Polars.

This script removes duplicate records from the four raw subreddit tables
by keeping the most recent record (by created_utc) for each unique post_id.
Overwrites the original tables with deduplicated data.

Usage:
    python deduplicate_raw_tables.py [--bucket BUCKET_NAME] [--subreddit SUBREDDIT] [--dry-run]
    
Examples:
    # Deduplicate all four tables
    python deduplicate_raw_tables.py
    
    # Deduplicate only news table
    python deduplicate_raw_tables.py --subreddit news
    
    # Test without writing (dry run)
    python deduplicate_raw_tables.py --dry-run
"""

import argparse
import gc
import json
import logging
import os
import sys
from datetime import datetime
from typing import Optional

import polars as pl
from deltalake import DeltaTable, write_deltalake

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_aws_credentials():
    """Retrieve AWS credentials from local files."""
    # Try current directory first, then parent directory (for Docker)
    credential_paths = [
        ("aws_access_key.txt", "aws_secret.txt"),
        ("../aws_access_key.txt", "../aws_secret.txt"),
        ("/opt/workspace/aws_access_key.txt", "/opt/workspace/aws_secret.txt"),
        ("/home/steven/reddit-streaming/aws_access_key.txt", 
         "/home/steven/reddit-streaming/aws_secret.txt")
    ]
    
    for key_path, secret_path in credential_paths:
        try:
            with open(key_path) as f:
                aws_client = f.read().strip()
            with open(secret_path) as f:
                aws_secret = f.read().strip()
            logger.info(f"Retrieved AWS credentials from: {key_path}")
            return aws_client, aws_secret
        except Exception:
            continue
    
    logger.error("Failed to retrieve credentials from any location")
    raise FileNotFoundError("AWS credentials not found in standard locations")


def deduplicate_subreddit(subreddit: str, bucket: str, storage_options: dict, 
                          dry_run: bool = False) -> dict:
    """
    Deduplicate a single subreddit table.
    
    Args:
        subreddit: Subreddit name (e.g., 'news', 'technology')
        bucket: S3 bucket name
        storage_options: AWS credentials for S3 access
        dry_run: If True, only report statistics without writing
        
    Returns:
        dict: Statistics about the deduplication
    """
    path = f"s3://{bucket}/{subreddit}"
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing: {subreddit}")
    logger.info(f"Path: {path}")
    logger.info(f"{'='*60}")
    
    stats = {
        "subreddit": subreddit,
        "success": False,
        "before_count": 0,
        "after_count": 0,
        "duplicates_removed": 0,
        "error": None
    }
    
    try:
        # Read Delta table
        logger.info("Reading Delta table from S3...")
        dt = DeltaTable(path, storage_options=storage_options)
        df = pl.from_arrow(dt.to_pyarrow_table())
        
        stats["before_count"] = df.height
        logger.info(f"Records before deduplication: {stats['before_count']:,}")
        
        # Check if 'id' column exists
        if "id" not in df.columns:
            logger.error(f"Column 'id' not found in table. Available columns: {df.columns}")
            stats["error"] = "Missing 'id' column"
            return stats
        
        # Check if 'created_utc' column exists
        if "created_utc" not in df.columns:
            logger.warning("Column 'created_utc' not found. Will deduplicate without sorting.")
            # Deduplicate by 'id' only, keeping first occurrence
            df_deduped = df.unique(subset=["id"], keep="first")
        else:
            # Deduplicate by 'id', keeping the most recent by 'created_utc'
            logger.info("Deduplicating by 'id', keeping most recent by 'created_utc'...")
            df_deduped = df.sort("created_utc", descending=True) \
                           .unique(subset=["id"], keep="first")
        
        stats["after_count"] = df_deduped.height
        stats["duplicates_removed"] = stats["before_count"] - stats["after_count"]
        
        logger.info(f"Records after deduplication: {stats['after_count']:,}")
        logger.info(f"Duplicates removed: {stats['duplicates_removed']:,}")
        
        if stats["duplicates_removed"] == 0:
            logger.info("✓ No duplicates found - table is already clean!")
            stats["success"] = True
            return stats
        
        # Calculate percentage
        pct_removed = (stats["duplicates_removed"] / stats["before_count"]) * 100
        logger.info(f"Percentage removed: {pct_removed:.2f}%")
        
        if dry_run:
            logger.info("DRY RUN - Skipping write operation")
            stats["success"] = True
            return stats
        
        # Write back to S3 (overwrite mode)
        logger.info("Writing deduplicated data back to S3...")
        write_deltalake(
            path,
            df_deduped.to_arrow(),
            mode="overwrite",
            storage_options=storage_options,
            overwrite_schema=False
        )
        
        logger.info("✓ Successfully wrote deduplicated data")
        
        # Verify write
        dt_verify = DeltaTable(path, storage_options=storage_options)
        df_verify = pl.from_arrow(dt_verify.to_pyarrow_table())
        verify_count = df_verify.height
        
        if verify_count == stats["after_count"]:
            logger.info(f"✓ Verification successful: {verify_count:,} records in table")
            stats["success"] = True
        else:
            logger.warning(f"⚠ Verification mismatch: Expected {stats['after_count']:,}, found {verify_count:,}")
            stats["error"] = "Verification count mismatch"
        
        # Clean up verification dataframes
        del dt_verify, df_verify
        
    except Exception as e:
        logger.error(f"✗ Error processing {subreddit}: {str(e)}")
        stats["error"] = str(e)
    
    finally:
        # Explicit memory cleanup to prevent accumulation across subreddits
        logger.info("Clearing memory...")
        if 'df' in locals():
            del df
        if 'df_deduped' in locals():
            del df_deduped
        if 'dt' in locals():
            del dt
        gc.collect()
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Deduplicate raw Delta Lake tables in S3 using Polars'
    )
    parser.add_argument(
        '--bucket',
        default='reddit-streaming-stevenhurwitt-2',
        help='S3 bucket name (default: reddit-streaming-stevenhurwitt-new)'
    )
    parser.add_argument(
        '--subreddit',
        help='Specific subreddit to deduplicate (default: all four tables)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Report statistics without writing changes'
    )
    
    args = parser.parse_args()
    
    # Start timer
    start_time = datetime.now()
    logger.info(f"Starting deduplication process at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if args.dry_run:
        logger.info("⚠ DRY RUN MODE - No data will be modified")
    
    # Get AWS credentials
    try:
        aws_client, aws_secret = get_aws_credentials()
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)
    
    # Set up storage options for Delta Lake
    storage_options = {
        "AWS_ACCESS_KEY_ID": aws_client,
        "AWS_SECRET_ACCESS_KEY": aws_secret,
        "AWS_REGION": "us-east-2"
    }
    
    # Determine which subreddits to process
    if args.subreddit:
        subreddits = [args.subreddit]
        logger.info(f"Processing single subreddit: {args.subreddit}")
    else:
        subreddits = ["technology", "ProgrammerHumor", "news", "worldnews"]
        logger.info(f"Processing all subreddits: {', '.join(subreddits)}")
    
    # Process each subreddit
    results = []
    for subreddit in subreddits:
        result = deduplicate_subreddit(
            subreddit=subreddit,
            bucket=args.bucket,
            storage_options=storage_options,
            dry_run=args.dry_run
        )
        results.append(result)
    
    # Print summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")
    
    total_before = 0
    total_after = 0
    total_removed = 0
    success_count = 0
    
    for result in results:
        status = "✓ SUCCESS" if result["success"] else "✗ FAILED"
        logger.info(f"\n{result['subreddit']}: {status}")
        logger.info(f"  Before:           {result['before_count']:>12,}")
        logger.info(f"  After:            {result['after_count']:>12,}")
        logger.info(f"  Duplicates:       {result['duplicates_removed']:>12,}")
        if result["error"]:
            logger.info(f"  Error: {result['error']}")
        
        total_before += result["before_count"]
        total_after += result["after_count"]
        total_removed += result["duplicates_removed"]
        if result["success"]:
            success_count += 1
    
    logger.info(f"\n{'='*60}")
    logger.info(f"TOTAL:")
    logger.info(f"  Before:           {total_before:>12,}")
    logger.info(f"  After:            {total_after:>12,}")
    logger.info(f"  Duplicates:       {total_removed:>12,}")
    logger.info(f"  Success rate:     {success_count}/{len(results)}")
    logger.info(f"  Duration:         {duration:.2f} seconds")
    logger.info(f"{'='*60}")
    
    if args.dry_run:
        logger.info("\n⚠ DRY RUN - No changes were written to S3")
        logger.info("Run without --dry-run to apply changes")
    
    # Exit with appropriate code
    if success_count == len(results):
        logger.info("\n✓ All tables processed successfully!")
        sys.exit(0)
    else:
        logger.warning(f"\n⚠ {len(results) - success_count} table(s) failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
