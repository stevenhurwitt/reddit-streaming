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
        try:
            with open("aws_access_key.txt") as f:
                aws_client = f.read().strip()
            with open("aws_secret.txt") as f:
                aws_secret = f.read().strip()
            logger.info("Retrieved AWS credentials from local files")
        except Exception as e2:
            logger.error(f"Failed to retrieve credentials from local files: {e2}")
            raise

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
        "io.delta:delta-core_2.12:3.2.0,"
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
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        if spark_master:
            # Connect to cluster
            builder = builder.master(spark_master)
        else:
            # Local mode
            builder = builder.master("local[*]") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.driver.maxResultSize", "0")
        
        spark = builder.enableHiveSupport().getOrCreate()

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


def process_subreddit(spark, subreddit, bucket):
    """Process a subreddit curation job."""
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
        
        # Vacuum and generate manifests
        logger.info("Running vacuum and generating symlink manifests...")
        delta_table = DeltaTable.forPath(spark, filepath)
        delta_table.vacuum(168)  # Retain 7 days of historical versions
        delta_table.generate("symlink_format_manifest")
        logger.info("Vacuum and manifest generation complete")
        
        # Run MSCK REPAIR TABLE in Athena
        logger.info(f"Running MSCK REPAIR TABLE for reddit.{subreddit}")
        athena = boto3.client("athena", region_name="us-east-1")
        response = athena.start_query_execution(
            QueryString=f"MSCK REPAIR TABLE reddit.{subreddit}",
            ResultConfiguration={
                "OutputLocation": f"s3://{bucket}/_athena_results"
            }
        )
        logger.info(f"MSCK REPAIR TABLE query submitted to Athena (QueryExecutionId: {response['QueryExecutionId']})")
        
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
        process_subreddit(spark, args.job, args.bucket)
        
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
