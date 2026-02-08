#!/usr/bin/env python3
"""
Local wrapper script to run curation jobs as standalone PySpark jobs.
Removes AWS Glue dependencies and can be scheduled with cron.

Usage: python run_curation_job.py --job news [--bucket BUCKET_NAME]
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime

import boto3
from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql.session import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_java():
    """Check if Java is installed and properly configured."""
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"Java found: {result.stderr.strip().split(chr(10))[0]}")
            return True
    except FileNotFoundError:
        pass
    
    logger.error("ERROR: Java is not installed or not in PATH")
    logger.error("PySpark requires Java to run. Please install Java:")
    logger.error("  Ubuntu/Debian: sudo apt-get install default-jre default-jdk")
    logger.error("  macOS: brew install openjdk")
    logger.error("  Or download from: https://www.oracle.com/java/technologies/downloads/")
    sys.exit(1)


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


def create_spark_session(aws_access_key, aws_secret_key, spark_master=None):
    """Create a Spark session with necessary configurations."""
    # Ensure JAVA_HOME is set (only for local mode)
    if not spark_master and 'JAVA_HOME' not in os.environ:
        logger.warning("JAVA_HOME not set, attempting to find Java...")
        # Try to find Java automatically
        java_paths = [
            '/usr/lib/jvm/default-java',
            '/usr/lib/jvm/java-11-openjdk-amd64',
            '/usr/lib/jvm/java-17-openjdk-amd64',
            '/usr/lib/jvm/java-17-openjdk-arm64',
            '/opt/homebrew/opt/openjdk',  # macOS
        ]
        for java_path in java_paths:
            if os.path.exists(java_path):
                os.environ['JAVA_HOME'] = java_path
                logger.info(f"Set JAVA_HOME to: {java_path}")
                break
    
    if spark_master:
        logger.info(f"Connecting to Spark cluster at: {spark_master}")
    else:
        logger.info(f"Using JAVA_HOME: {os.environ.get('JAVA_HOME', 'NOT SET')}")
    
    extra_jar_list = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
        "org.apache.hadoop:hadoop-common:3.3.4,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.hadoop:hadoop-client:3.3.4,"
        "io.delta:delta-spark_2.12:3.1.0,"
        "org.postgresql:postgresql:42.6.0"
    )

    try:
        builder = SparkSession.builder \
            .appName("local-reddit-curation") \
            .config("spark.jars.packages", extra_jar_list) \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        
        if spark_master:
            # Connect to cluster
            builder = builder.master(spark_master)
        else:
            # Local mode
            builder = builder.master("local[*]") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.driver.maxResultSize", "0")
        
        spark = builder.getOrCreate()

        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}", exc_info=True)
        logger.error("\nTroubleshooting tips:")
        logger.error("1. Verify Java is installed: java -version")
        logger.error("2. Set JAVA_HOME: export JAVA_HOME=/path/to/java")
        logger.error("3. Try with increased verbosity: SPARK_LOG_LEVEL=DEBUG python3 run_curation_job.py --job news")
        if spark_master:
            logger.error(f"4. Verify Spark cluster is running: {spark_master}")
        raise


def write_to_postgres(spark, df, subreddit, db_host="reddit-postgres", db_port=5432, db_name="reddit", 
                      db_user="postgres", db_password="secret!1234", db_schema="reddit_schema",
                      handle_duplicates="skip"):
    """Write cleaned dataframe to PostgreSQL database.
    
    Args:
        spark: Spark session
        df: Spark dataframe to write
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
            - "fail": Raise error if duplicates found (old behavior)
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
            df_pg = df_pg.withColumnRenamed("id", "post_id")
        
        total_records = df_pg.count()
        logger.info(f"Total records to write: {total_records}")
        
        # Handle duplicates based on mode
        if handle_duplicates == "skip":
            # Read existing IDs from PostgreSQL
            logger.info("Reading existing post_ids from PostgreSQL...")
            try:
                existing_ids_df = spark.read \
                    .format("jdbc") \
                    .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?sslmode=disable") \
                    .option("query", f"SELECT DISTINCT post_id FROM {db_schema}.{table_name}") \
                    .option("user", db_user) \
                    .option("password", db_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()
                
                existing_ids = set(row.post_id for row in existing_ids_df.collect())
                logger.info(f"Found {len(existing_ids)} existing records in PostgreSQL")
                
                # Filter out duplicates
                df_pg = df_pg.filter(~col("post_id").isin(existing_ids))
                new_records = df_pg.count()
                
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
        
        elif handle_duplicates == "fail":
            logger.info("Fail mode: will fail if any duplicates are encountered")
        
        # Write to PostgreSQL
        write_mode = "overwrite" if handle_duplicates == "overwrite" else "append"
        
        df_pg.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?sslmode=disable") \
            .option("dbtable", f"{db_schema}.{table_name}") \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode(write_mode) \
            .save()
        
        logger.info(f"Successfully wrote data to PostgreSQL: {db_schema}.{table_name}")
        
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}", exc_info=True)
        raise


def process_subreddit(spark, subreddit, bucket, handle_duplicates="skip"):
    """Process a subreddit curation job.
    
    Args:
        spark: Spark session
        subreddit: Subreddit name to process
        bucket: S3 bucket name
        handle_duplicates: How to handle duplicate records in PostgreSQL
            - "skip": Only write new records (default, recommended)
            - "overwrite": Replace all existing data
    """
    try:
        logger.info(f"Starting curation for subreddit: {subreddit}")
        
        # Read Delta table from S3
        logger.info(f"Reading Delta table from s3a://{bucket}/{subreddit}")
        df = spark.read.format("delta").load(f"s3a://{bucket}/{subreddit}")
        record_count = df.count()
        logger.info(f"Successfully read {record_count} records from {subreddit}")
        
        if record_count == 0:
            logger.warning(f"No records found for {subreddit}")
            return
        
        # Transform and clean data
        logger.info("Transforming data...")
        df = df.withColumn("approved_at_utc", col("approved_at_utc").cast("timestamp")) \
                .withColumn("banned_at_utc", col("banned_at_utc").cast("timestamp")) \
                .withColumn("created_utc", col("created_utc").cast("timestamp")) \
                .withColumn("created", col("created").cast("timestamp")) \
                .withColumn("date", to_date(col("created_utc"), "MM-dd-yyyy")) \
                .withColumn("year", year(col("date"))) \
                .withColumn("month", month(col("date"))) \
                .withColumn("day", dayofmonth(col("date"))) \
                .dropDuplicates(subset=["title"])
        
        # Write cleaned data to Delta
        filepath = f"s3a://{bucket}/{subreddit}_clean/"
        logger.info(f"Writing cleaned data to {filepath}")
        df.write.format("delta") \
            .partitionBy("year", "month", "day") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .save(filepath)
        logger.info(f"Successfully wrote cleaned data to {filepath}")
        
        # Write cleaned data to PostgreSQL
        write_to_postgres(spark, df, subreddit, handle_duplicates=handle_duplicates)
        
        # Vacuum and generate manifests
        logger.info("Running vacuum and generating symlink manifests...")
        delta_table = DeltaTable.forPath(spark, filepath)
        delta_table.vacuum(168)  # Retain 7 days of historical versions
        delta_table.generate("symlink_format_manifest")
        logger.info("Vacuum and manifest generation complete")
        
        # Run MSCK REPAIR TABLE in Athena
        # logger.info(f"Running MSCK REPAIR TABLE for reddit.{subreddit}")
        # athena = boto3.client("athena", region_name="us-east-1")
        # response = athena.start_query_execution(
        #     QueryString=f"MSCK REPAIR TABLE reddit.{subreddit}",
        #     ResultConfiguration={
        #         "OutputLocation": f"s3://{bucket}/_athena_results"
        #     }
        # )
        # logger.info(f"MSCK REPAIR TABLE query submitted to Athena (QueryExecutionId: {response['QueryExecutionId']})")
        
        logger.info(f"Curation for {subreddit} completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing {subreddit}: {str(e)}", exc_info=True)
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Run reddit curation jobs with Spark (local or cluster mode)"
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
        "--spark-master",
        default=None,
        help="Spark master URL (e.g., spark://spark-master:7077 for Docker cluster). If not provided, runs in local mode."
    )
    parser.add_argument(
        "--handle-duplicates",
        default="skip",
        choices=["skip", "overwrite"],
        help="How to handle duplicate records when writing to PostgreSQL (default: skip)"
    )

    args = parser.parse_args()

    try:
        logger.info(f"Starting curation job for: {args.job}")
        
        # Check Java installation only for local mode
        if not args.spark_master:
            check_java()
        
        # Get credentials
        aws_access_key, aws_secret_key = get_aws_credentials()
        
        # Create Spark session
        spark = create_spark_session(aws_access_key, aws_secret_key, spark_master=args.spark_master)
        
        # Process subreddit
        process_subreddit(spark, args.job, args.bucket, handle_duplicates=args.handle_duplicates)
        
        logger.info("Job completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
