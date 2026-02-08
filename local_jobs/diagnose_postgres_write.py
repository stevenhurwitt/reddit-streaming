#!/usr/bin/env python3
"""
Diagnostic script to identify exactly which part of the PostgreSQL write is failing.
Run this when you're getting write errors to pinpoint the issue.

Usage: python diagnose_postgres_write.py --job news
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_1_check_dependencies():
    """Test 1: Check if all required Python packages are available."""
    logger.info("\n" + "="*70)
    logger.info("TEST 1: Checking Python Dependencies")
    logger.info("="*70)
    
    dependencies = {
        'boto3': 'AWS SDK',
        'psycopg2': 'PostgreSQL direct connection',
        'pyspark': 'Spark',
        'delta': 'Delta Lake',
    }
    
    results = {}
    for package, description in dependencies.items():
        try:
            __import__(package)
            logger.info(f"✓ {package:15} - {description}")
            results[package] = True
        except ImportError as e:
            logger.error(f"✗ {package:15} - {description}")
            logger.error(f"  Error: {e}")
            logger.error(f"  Install with: pip install {package}")
            results[package] = False
    
    return all(results.values())


def test_2_check_java():
    """Test 2: Check Java installation."""
    logger.info("\n" + "="*70)
    logger.info("TEST 2: Checking Java")
    logger.info("="*70)
    
    import subprocess
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            java_version = result.stderr.strip().split('\n')[0]
            logger.info(f"✓ Java found: {java_version}")
            return True
    except Exception as e:
        logger.error(f"✗ Java check failed: {e}")
        logger.error("  Install with: sudo apt-get install default-jdk")
    
    return False


def test_3_check_aws_credentials():
    """Test 3: Check AWS credentials."""
    logger.info("\n" + "="*70)
    logger.info("TEST 3: Checking AWS Credentials")
    logger.info("="*70)
    
    import boto3
    
    try:
        # Try to load credentials
        session = boto3.Session()
        credentials = session.get_credentials()
        
        if credentials:
            logger.info("✓ AWS credentials found")
            logger.info(f"  Access Key ID: {credentials.access_key[:10]}...")
            
            # Try to verify credentials work
            try:
                s3_client = boto3.client('s3')
                response = s3_client.list_buckets()
                logger.info(f"✓ AWS credentials are valid")
                logger.info(f"  Found {len(response['Buckets'])} S3 buckets")
                return True
            except Exception as e:
                logger.error(f"✗ AWS credentials not valid: {e}")
                return False
        else:
            logger.error("✗ AWS credentials not found")
            logger.error("  Create aws_access_key.txt and aws_secret.txt")
            return False
            
    except Exception as e:
        logger.error(f"✗ Error checking credentials: {e}")
        return False


def test_4_check_s3_bucket(bucket="reddit-streaming-stevenhurwitt-2", subreddit="news"):
    """Test 4: Check S3 bucket and Delta table."""
    logger.info("\n" + "="*70)
    logger.info("TEST 4: Checking S3 Bucket and Delta Table")
    logger.info("="*70)
    
    import boto3
    
    s3_client = boto3.client('s3')
    
    try:
        # Check if bucket exists
        logger.info(f"Checking if bucket exists: {bucket}")
        try:
            s3_client.head_bucket(Bucket=bucket)
            logger.info(f"✓ Bucket '{bucket}' exists")
        except Exception as e:
            logger.error(f"✗ Bucket '{bucket}' not found: {e}")
            return False
        
        # Check for Delta table
        logger.info(f"Checking for Delta table: {subreddit}_clean")
        prefix = f"{subreddit}_clean/"
        
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10)
        
        if 'Contents' in response:
            logger.info(f"✓ Found {len(response['Contents'])} objects in {subreddit}_clean/")
            
            # Look for _delta_log
            has_delta_log = any('_delta_log' in obj['Key'] for obj in response['Contents'])
            if has_delta_log:
                logger.info("✓ Delta table metadata found (_delta_log)")
                return True
            else:
                logger.warning("⚠ No _delta_log directory found - may not be a valid Delta table")
                return False
        else:
            logger.error(f"✗ No objects found at {subreddit}_clean/")
            return False
            
    except Exception as e:
        logger.error(f"✗ Error checking S3: {e}", exc_info=True)
        return False


def test_5_spark_session():
    """Test 5: Create Spark session."""
    logger.info("\n" + "="*70)
    logger.info("TEST 5: Creating Spark Session")
    logger.info("="*70)
    
    from pyspark.sql.session import SparkSession
    
    try:
        logger.info("Initializing Spark session...")
        
        builder = SparkSession.builder \
            .appName("diagnose-postgres") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g")
        
        spark = builder.getOrCreate()
        logger.info("✓ Spark session created successfully")
        logger.info(f"  Spark version: {spark.version}")
        logger.info(f"  Python version: {spark.sparkContext.pythonVer}")
        
        return spark
        
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {e}", exc_info=True)
        return None


def test_6_read_delta_table(spark, bucket="reddit-streaming-stevenhurwitt-2", subreddit="news"):
    """Test 6: Read Delta table from S3."""
    logger.info("\n" + "="*70)
    logger.info("TEST 6: Reading Delta Table from S3")
    logger.info("="*70)
    
    if not spark:
        logger.error("✗ No Spark session available")
        return None
    
    try:
        delta_path = f"s3a://{bucket}/{subreddit}_clean"
        logger.info(f"Reading from: {delta_path}")
        
        df = spark.read.format("delta").load(delta_path)
        count = df.count()
        
        logger.info(f"✓ Successfully read Delta table")
        logger.info(f"  Records: {count}")
        logger.info(f"  Columns: {len(df.columns)}")
        
        logger.info(f"\nDataFrame Schema:")
        df.printSchema()
        
        return df
        
    except Exception as e:
        logger.error(f"✗ Failed to read Delta table: {e}", exc_info=True)
        return None


def test_7_postgresql_basic(db_host="reddit-postgres", db_port=5434, db_name="reddit",
                           db_user="postgres", db_password="secret!1234"):
    """Test 7: Basic PostgreSQL connection with psycopg2."""
    logger.info("\n" + "="*70)
    logger.info("TEST 7: PostgreSQL Basic Connection")
    logger.info("="*70)
    
    try:
        import psycopg2
        
        logger.info(f"Connecting to: {db_host}:{db_port}/{db_name}")
        
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
            connect_timeout=10
        )
        
        logger.info("✓ PostgreSQL connection successful")
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logger.info(f"  PostgreSQL version: {version.split(',')[0]}")
        
        # Check schema
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = 'reddit_schema'
            )
        """)
        schema_exists = cursor.fetchone()[0]
        
        if schema_exists:
            logger.info("✓ reddit_schema exists")
        else:
            logger.warning("⚠ reddit_schema does not exist")
            logger.info("  Creating reddit_schema...")
            cursor.execute("CREATE SCHEMA reddit_schema;")
            conn.commit()
            logger.info("✓ reddit_schema created")
        
        cursor.close()
        conn.close()
        return True
        
    except ImportError:
        logger.warning("⚠ psycopg2 not available (optional for this test)")
        return None
    except Exception as e:
        logger.error(f"✗ PostgreSQL connection failed: {e}", exc_info=True)
        return False


def test_8_spark_jdbc(spark, db_host="reddit-postgres", db_port=5434, db_name="reddit",
                      db_user="postgres", db_password="secret!1234"):
    """Test 8: Spark JDBC connectivity."""
    logger.info("\n" + "="*70)
    logger.info("TEST 8: Spark JDBC Connection")
    logger.info("="*70)
    
    if not spark:
        logger.error("✗ No Spark session available")
        return False
    
    try:
        logger.info(f"Testing Spark JDBC to: {db_host}:{db_port}/{db_name}")
        
        # Try simple query
        df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?sslmode=disable") \
            .option("query", "SELECT 1 as test") \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        result = df.collect()
        logger.info("✓ Spark JDBC connection successful")
        logger.info(f"  Test query result: {result[0]['test']}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Spark JDBC connection failed: {e}", exc_info=True)
        logger.error("\nPossible causes:")
        logger.error("1. PostgreSQL server not running or not accessible")
        logger.error("2. PostgreSQL JDBC driver not loaded")
        logger.error("3. Network connectivity issue")
        logger.error("4. Invalid credentials")
        return False


def test_9_create_test_table(spark, db_host="reddit-postgres", db_port=5434,
                            db_name="reddit", db_user="postgres", db_password="secret!1234"):
    """Test 9: Create test table and write sample data."""
    logger.info("\n" + "="*70)
    logger.info("TEST 9: Writing Test Data")
    logger.info("="*70)
    
    if not spark:
        logger.error("✗ No Spark session available")
        return False
    
    try:
        logger.info("Creating test DataFrame...")
        
        # Create simple test DataFrame
        test_data = [
            ("1", "Test Post 1", "author1", 100, 10, "2024-01-01 00:00:00", "http://test1.com", "Self text 1"),
            ("2", "Test Post 2", "author2", 200, 20, "2024-01-02 00:00:00", "http://test2.com", "Self text 2"),
        ]
        
        df_test = spark.createDataFrame(
            test_data,
            ["post_id", "title", "author", "score", "num_comments", "created_utc", "url", "selftext"]
        )
        
        logger.info("✓ Test DataFrame created")
        logger.info(f"  Rows: {df_test.count()}")
        df_test.printSchema()
        
        logger.info("\nAttempting to write test data to PostgreSQL...")
        
        df_test.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?sslmode=disable") \
            .option("dbtable", "reddit_schema.test_write") \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        logger.info("✓ Successfully wrote test data to PostgeSQL!")
        logger.info("  Table: reddit_schema.test_write")
        logger.info("  Rows written: 2")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to write test data: {e}", exc_info=True)
        logger.error("\nPossible causes:")
        logger.error("1. Table reddit_schema.test_write doesn't exist")
        logger.error("2. Column types don't match")
        logger.error("3. Permission denied")
        logger.error("4. Disk space issue on PostgreSQL server")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Diagnostic tool for PostgreSQL write issues"
    )
    parser.add_argument("--job", default="news", help="Subreddit to test")
    parser.add_argument("--bucket", default="reddit-streaming-stevenhurwitt-2", help="S3 bucket")
    parser.add_argument("--db-host", default="reddit-postgres", help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=5434, help="PostgreSQL port")
    parser.add_argument("--db-name", default="reddit", help="Database name")
    parser.add_argument("--db-user", default="postgres", help="Database user")
    parser.add_argument("--db-password", default="secret!1234", help="Database password")
    
    args = parser.parse_args()
    
    logger.info("\n" + "="*70)
    logger.info("POSTGRESQL WRITE DIAGNOSTIC TOOL")
    logger.info("="*70)
    logger.info(f"Test Configuration:")
    logger.info(f"  Subreddit: {args.job}")
    logger.info(f"  S3 Bucket: {args.bucket}")
    logger.info(f"  PostgreSQL: {args.db_host}:{args.db_port}/{args.db_name}")
    
    results = {}
    spark = None
    df = None
    
    try:
        # Run tests sequentially
        results['dependencies'] = test_1_check_dependencies()
        results['java'] = test_2_check_java()
        results['aws_creds'] = test_3_check_aws_credentials()
        results['s3_bucket'] = test_4_check_s3_bucket(args.bucket, args.job)
        
        spark = test_5_spark_session()
        results['spark'] = spark is not None
        
        if spark:
            df = test_6_read_delta_table(spark, args.bucket, args.job)
            results['delta_read'] = df is not None
        
        results['postgres_basic'] = test_7_postgresql_basic(args.db_host, args.db_port,
                                                            args.db_name, args.db_user, args.db_password)
        
        if spark:
            results['spark_jdbc'] = test_8_spark_jdbc(spark, args.db_host, args.db_port,
                                                      args.db_name, args.db_user, args.db_password)
            results['test_write'] = test_9_create_test_table(spark, args.db_host, args.db_port,
                                                             args.db_name, args.db_user, args.db_password)
        
    except Exception as e:
        logger.error(f"\nFatal error during diagnostics: {e}", exc_info=True)
    
    finally:
        if spark:
            try:
                spark.stop()
            except:
                pass
    
    # Print summary
    logger.info("\n" + "="*70)
    logger.info("DIAGNOSTIC SUMMARY")
    logger.info("="*70)
    
    test_names = {
        'dependencies': 'Python Dependencies',
        'java': 'Java Installation',
        'aws_creds': 'AWS Credentials',
        's3_bucket': 'S3 Bucket Access',
        'spark': 'Spark Session',
        'delta_read': 'Delta Table Reading',
        'postgres_basic': 'PostgreSQL Connection',
        'spark_jdbc': 'Spark JDBC',
        'test_write': 'Test Data Write',
    }
    
    passed = 0
    failed = 0
    skipped = 0
    
    for test_key, test_name in test_names.items():
        if test_key not in results:
            continue
            
        result = results[test_key]
        
        if result is None:
            status = "⊘ SKIPPED"
            skipped += 1
        elif result:
            status = "✓ PASSED"
            passed += 1
        else:
            status = "✗ FAILED"
            failed += 1
        
        logger.info(f"{status:12} - {test_name}")
    
    logger.info(f"\nTotal: {passed} passed, {failed} failed, {skipped} skipped")
    
    if failed == 0:
        logger.info("\n✓ All tests passed! You can run the full curation job.")
        sys.exit(0)
    else:
        logger.error(f"\n✗ {failed} test(s) failed. See details above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
