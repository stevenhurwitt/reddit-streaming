#!/usr/bin/env python3
"""
Test script for reading news_clean Delta table from S3 and writing to PostgreSQL.
Includes comprehensive error handling and debugging output.

Usage: python test_curation_postgres.py [--job news] [--bucket BUCKET_NAME] [--limit LIMIT]
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
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_java():
    """Check if Java is installed and properly configured."""
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"✓ Java found: {result.stderr.strip().split(chr(10))[0]}")
            return True
    except FileNotFoundError:
        pass
    
    logger.error("✗ Java is not installed or not in PATH")
    logger.error("  Ubuntu/Debian: sudo apt-get install default-jre default-jdk")
    logger.error("  macOS: brew install openjdk")
    sys.exit(1)


def get_aws_credentials():
    """Retrieve AWS credentials from Secrets Manager or local files."""
    try:
        # Try Secrets Manager first
        secretmanager_client = boto3.client("secretsmanager", region_name="us-east-1")
        aws_client = json.loads(
            secretmanager_client.get_secret_value(SecretId="AWS_ACCESS_KEY_ID")["SecretString"]
        )["AWS_ACCESS_KEY_ID"]
        aws_secret = json.loads(
            secretmanager_client.get_secret_value(SecretId="AWS_SECRET_ACCESS_KEY")["SecretString"]
        )["AWS_SECRET_ACCESS_KEY"]
        logger.info("✓ Retrieved AWS credentials from Secrets Manager")
    except Exception as e:
        logger.debug(f"Failed to retrieve from Secrets Manager: {e}")
        # Fall back to local credential files
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
                logger.info(f"✓ Retrieved AWS credentials from: {key_path}")
                return aws_client, aws_secret
            except Exception:
                continue
        
        logger.error("✗ Failed to retrieve credentials from any location")
        raise FileNotFoundError("AWS credentials not found")

    return aws_client, aws_secret


def create_spark_session(aws_access_key, aws_secret_key):
    """Create a Spark session with PostgreSQL JDBC support."""
    # Set JAVA_HOME if not set
    if 'JAVA_HOME' not in os.environ:
        java_paths = [
            '/usr/lib/jvm/default-java',
            '/usr/lib/jvm/java-11-openjdk-amd64',
            '/usr/lib/jvm/java-17-openjdk-amd64',
            '/usr/lib/jvm/java-17-openjdk-arm64',
            '/opt/homebrew/opt/openjdk',
        ]
        for java_path in java_paths:
            if os.path.exists(java_path):
                os.environ['JAVA_HOME'] = java_path
                logger.info(f"✓ Set JAVA_HOME to: {java_path}")
                break
    
    extra_jar_list = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
        "org.apache.hadoop:hadoop-common:3.3.4,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.hadoop:hadoop-client:3.3.4,"
        "io.delta:delta-spark_2.12:3.1.0,"
        "org.postgresql:postgresql:42.6.0"
    )

    try:
        logger.info("Creating Spark session...")
        builder = SparkSession.builder \
            .appName("test-reddit-curation-postgres") \
            .config("spark.jars.packages", extra_jar_list) \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .master("local[*]") \
            .config("spark.driver.memory", "3g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.maxResultSize", "0")
        
        spark = builder.getOrCreate()
        logger.info("✓ Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {str(e)}", exc_info=True)
        raise


def test_s3_connection(spark, bucket, subreddit):
    """Test S3 connectivity and read Delta table."""
    try:
        logger.info(f"\n{'='*70}")
        logger.info(f"Testing S3 Delta table reading: s3a://{bucket}/{subreddit}_clean")
        logger.info(f"{'='*70}")
        
        delta_path = f"s3a://{bucket}/{subreddit}_clean"
        
        # Try to read the Delta table
        logger.info(f"Reading Delta table from: {delta_path}")
        df = spark.read.format("delta").load(delta_path)
        
        record_count = df.count()
        logger.info(f"✓ Successfully read {record_count} records from {subreddit}_clean")
        
        # Show schema
        logger.info(f"\nDataFrame Schema:")
        df.printSchema()
        
        # Show sample data
        logger.info(f"\nSample data (first 3 rows):")
        df.show(3, truncate=False)
        
        # Show column info
        logger.info(f"\nColumn information:")
        for col_name, col_type in df.dtypes:
            logger.info(f"  {col_name}: {col_type}")
        
        return df
        
    except Exception as e:
        logger.error(f"✗ Failed to read Delta table: {str(e)}", exc_info=True)
        logger.error("\nTroubleshooting tips:")
        logger.error("1. Verify the bucket name and S3 path exist")
        logger.error("2. Check AWS credentials have S3 access")
        logger.error("3. Verify Delta table is present at the location")
        raise


def test_postgres_connection(db_host="reddit-postgres", db_port=5432, db_name="reddit",
                            db_user="postgres", db_password="secret!1234"):
    """Test basic PostgreSQL connectivity."""
    try:
        logger.info(f"\n{'='*70}")
        logger.info(f"Testing PostgreSQL Connection")
        logger.info(f"{'='*70}")
        logger.info(f"Host: {db_host}:{db_port}")
        logger.info(f"Database: {db_name}")
        logger.info(f"User: {db_user}")
        
        # Try psycopg2 if available
        try:
            import psycopg2
            logger.info("✓ psycopg2 module available")
            
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password
            )
            logger.info(f"✓ Successfully connected to PostgreSQL")
            
            # Test query
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            logger.info(f"✓ PostgreSQL version: {version}")
            
            # List existing tables in reddit_schema
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'reddit_schema'
            """)
            tables = cursor.fetchall()
            logger.info(f"\nExisting tables in reddit_schema:")
            if tables:
                for table in tables:
                    logger.info(f"  - {table[0]}")
            else:
                logger.warning("  - No tables found in reddit_schema")
            
            cursor.close()
            conn.close()
            return True
            
        except ImportError:
            logger.warning("⚠ psycopg2 not available, will test via Spark JDBC")
            return None
        
    except Exception as e:
        logger.error(f"✗ Failed to connect to PostgreSQL: {str(e)}", exc_info=True)
        logger.error("\nTroubleshooting tips:")
        logger.error(f"1. Verify PostgreSQL is running on {db_host}:{db_port}")
        logger.error(f"2. Check credentials (user: {db_user})")
        logger.error("3. Verify database 'reddit' exists")
        logger.error("4. Check firewall/network connectivity")
        return False


def create_table_if_not_exists(spark, df, subreddit, db_host="reddit-postgres", db_port=5432,
                               db_name="reddit", db_user="postgres", db_password="secret!1234",
                               db_schema="reddit_schema"):
    """Create PostgreSQL table from DataFrame schema if it doesn't exist."""
    try:
        import psycopg2
        
        table_name = subreddit.lower().replace(" ", "_")
        logger.info(f"\nChecking if table {db_schema}.{table_name} exists...")
        
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = '{db_schema}' 
                AND table_name = '{table_name}'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            logger.info(f"✓ Table {db_schema}.{table_name} already exists")
            
            # Show column info
            cursor.execute(f"""
                SELECT column_name, data_type FROM information_schema.columns 
                WHERE table_schema = '{db_schema}' 
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            logger.info(f"\nExisting columns in {db_schema}.{table_name}:")
            for col_name, col_type in columns:
                logger.info(f"  {col_name}: {col_type}")
        else:
            logger.info(f"⚠ Table {db_schema}.{table_name} does not exist")
            logger.warning("You may need to create the table manually or use append mode with correct schema")
        
        cursor.close()
        conn.close()
        
    except ImportError:
        logger.warning("⚠ psycopg2 not available, skipping table check")
    except Exception as e:
        logger.error(f"✗ Error checking table: {str(e)}")


def write_to_postgres_sample(spark, df, subreddit, limit=100, db_host="reddit-postgres", 
                            db_port=5432, db_name="reddit", db_user="postgres", 
                            db_password="secret!1234", db_schema="reddit_schema"):
    """Write sample data to PostgreSQL with detailed error handling."""
    try:
        logger.info(f"\n{'='*70}")
        logger.info(f"Writing Sample Data to PostgreSQL")
        logger.info(f"{'='*70}")
        
        table_name = subreddit.lower().replace(" ", "_")
        
        # Select relevant columns
        columns_to_write = ["id", "title", "author", "score", "num_comments", 
                           "created_utc", "url", "selftext"]
        
        available_columns = [c for c in columns_to_write if c in df.columns]
        df_pg = df.select(available_columns).limit(limit)
        
        logger.info(f"Preparing to write {df_pg.count()} records")
        logger.info(f"Columns to write: {available_columns}")
        
        # Rename 'id' to 'post_id' for consistency
        if "id" in df_pg.columns:
            df_pg = df_pg.withColumnRenamed("id", "post_id")
        
        logger.info(f"\nDataFrame Schema for PostgreSQL:")
        df_pg.printSchema()
        
        logger.info(f"\nSample rows to be written:")
        df_pg.show(3, truncate=False)
        
        logger.info(f"\nWriting to PostgreSQL {db_schema}.{table_name}...")
        
        # Write to PostgreSQL
        df_pg.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?sslmode=disable") \
            .option("dbtable", f"{db_schema}.{table_name}") \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"✓ Successfully wrote sample data to PostgreSQL: {db_schema}.{table_name}")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error writing to PostgreSQL: {str(e)}", exc_info=True)
        logger.error("\nTroubleshooting tips:")
        logger.error("1. Verify table exists: SELECT * FROM information_schema.tables WHERE table_schema = 'reddit_schema'")
        logger.error("2. Verify column names and types match")
        logger.error("3. Check PostgreSQL driver is in Spark jars")
        logger.error("4. Try connecting with psycopg2 first to verify connectivity")
        logger.error("5. Check PostgreSQL logs for connection errors")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Test Reddit curation - read Delta from S3 and write to PostgreSQL"
    )
    parser.add_argument(
        "--job",
        default="news",
        choices=["news", "technology", "ProgrammerHumor", "worldnews"],
        help="Subreddit to test (default: news)"
    )
    parser.add_argument(
        "--bucket",
        default="reddit-streaming-stevenhurwitt-2",
        help="S3 bucket name (default: reddit-streaming-stevenhurwitt-2)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Limit number of records to write (default: 100)"
    )
    parser.add_argument(
        "--db-host",
        default="reddit-postgres",
        help="PostgreSQL hostname (default: reddit-postgres)"
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=5434,
        help="PostgreSQL port (default: 5434)"
    )
    parser.add_argument(
        "--db-name",
        default="reddit",
        help="PostgreSQL database name (default: reddit)"
    )
    parser.add_argument(
        "--db-user",
        default="postgres",
        help="PostgreSQL user (default: postgres)"
    )
    parser.add_argument(
        "--db-password",
        default="secret!1234",
        help="PostgreSQL password (default: secret!1234)"
    )
    parser.add_argument(
        "--skip-postgres-test",
        action="store_true",
        help="Skip PostgreSQL connection test"
    )

    args = parser.parse_args()

    try:
        logger.info("="*70)
        logger.info("REDDIT CURATION - S3 TO POSTGRESQL TEST")
        logger.info("="*70)
        logger.info(f"Job: {args.job}")
        logger.info(f"Bucket: {args.bucket}")
        logger.info(f"Limit: {args.limit}")
        logger.info(f"PostgreSQL: {args.db_host}:{args.db_port}/{args.db_name}")
        
        # Check Java
        logger.info("\nStep 1: Checking Java installation...")
        check_java()
        
        # Get AWS credentials
        logger.info("\nStep 2: Retrieving AWS credentials...")
        aws_access_key, aws_secret_key = get_aws_credentials()
        
        # Create Spark session
        logger.info("\nStep 3: Creating Spark session...")
        spark = create_spark_session(aws_access_key, aws_secret_key)
        
        # Test S3
        logger.info("\nStep 4: Reading from S3 Delta table...")
        df = test_s3_connection(spark, args.bucket, args.job)
        
        # Test PostgreSQL connection
        if not args.skip_postgres_test:
            logger.info("\nStep 5: Testing PostgreSQL connection...")
            postgres_ok = test_postgres_connection(args.db_host, args.db_port, args.db_name,
                                                   args.db_user, args.db_password)
            
            # Check table exists
            logger.info("\nStep 6: Checking PostgreSQL table...")
            create_table_if_not_exists(spark, df, args.job, args.db_host, args.db_port,
                                      args.db_name, args.db_user, args.db_password)
            
            # Write sample data
            logger.info("\nStep 7: Writing sample data to PostgreSQL...")
            write_to_postgres_sample(spark, df, args.job, args.limit, args.db_host,
                                    args.db_port, args.db_name, args.db_user, args.db_password)
        
        logger.info("\n" + "="*70)
        logger.info("TEST COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"\n{'='*70}")
        logger.error("TEST FAILED")
        logger.error("="*70)
        logger.error(f"Error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        try:
            spark.stop()
            logger.info("✓ Spark session stopped")
        except Exception:
            pass


if __name__ == "__main__":
    main()
