#!/usr/bin/env python3
"""
Test Spark JDBC connectivity to PostgreSQL and diagnose driver issues.
Run this to verify Spark can load the PostgreSQL driver and connect.

Usage: python test_spark_jdbc.py [--host HOST] [--port PORT]
"""

import logging
import os
import subprocess
import sys
from pyspark.sql.session import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_java():
    """Check Java installation."""
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("✓ Java is installed")
            return True
    except FileNotFoundError:
        pass
    
    logger.error("✗ Java not found. Install with:")
    logger.error("  Ubuntu: sudo apt-get install default-jdk")
    logger.error("  macOS: brew install openjdk")
    return False


def create_spark_session_with_postgres():
    """Create Spark session with PostgreSQL driver."""
    logger.info("Creating Spark session with PostgreSQL JDBC driver...")
    
    # Set JAVA_HOME if needed
    if 'JAVA_HOME' not in os.environ:
        java_paths = [
            '/usr/lib/jvm/default-java',
            '/usr/lib/jvm/java-11-openjdk-amd64',
            '/usr/lib/jvm/java-17-openjdk-amd64',
        ]
        for path in java_paths:
            if os.path.exists(path):
                os.environ['JAVA_HOME'] = path
                logger.info(f"✓ Set JAVA_HOME to {path}")
                break
    
    try:
        spark = SparkSession.builder \
            .appName("test-spark-jdbc") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        
        logger.info("✓ Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {e}", exc_info=True)
        return None


def test_spark_postgresql_connection(db_host="reddit-postgres", db_port=5434,
                                     db_name="reddit", db_user="postgres",
                                     db_password="secret!1234"):
    """Test Spark JDBC connection to PostgreSQL."""
    
    logger.info("\n" + "="*70)
    logger.info("SPARK JDBC POSTGRESQL TEST")
    logger.info("="*70)
    
    spark = create_spark_session_with_postgres()
    if not spark:
        return False
    
    try:
        logger.info(f"\nAttempting to read from PostgreSQL...")
        logger.info(f"  Host: {db_host}:{db_port}")
        logger.info(f"  Database: {db_name}")
        logger.info(f"  User: {db_user}\n")
        
        # Try to read a simple query
        df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?sslmode=disable") \
            .option("query", "SELECT 1 as test") \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        result = df.collect()
        logger.info("✓ JDBC connection successful!")
        logger.info(f"  Test query returned: {result[0]['test']}\n")
        
        # Try to read from an actual table if available
        logger.info("Attempting to read existing tables...")
        tables_df = spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?sslmode=disable") \
            .option("query", """
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'reddit_schema'
                LIMIT 5
            """) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        tables = [row['table_name'] for row in tables_df.collect()]
        if tables:
            logger.info(f"✓ Found {len(tables)} tables in reddit_schema:")
            for table in tables:
                logger.info(f"  - {table}")
        else:
            logger.warning("⚠ No tables found in reddit_schema")
        
        logger.info("\n✓ ALL JDBC TESTS PASSED")
        return True
        
    except Exception as e:
        logger.error(f"\n✗ JDBC connection failed: {e}", exc_info=True)
        logger.error("\nTroubleshooting:")
        logger.error("1. Verify PostgreSQL is running and accessible")
        logger.error("2. Check network connectivity to the PostgreSQL host")
        logger.error("3. Verify credentials are correct")
        logger.error("4. Check PostgreSQL server logs")
        logger.error("5. Try connecting with psycopg2 first:")
        logger.error("   python test_postgres_direct.py")
        return False
        
    finally:
        try:
            spark.stop()
            logger.info("\n✓ Spark session closed")
        except Exception:
            pass


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Test Spark JDBC connection to PostgreSQL"
    )
    parser.add_argument("--host", default="reddit-postgres", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5434, help="PostgreSQL port")
    parser.add_argument("--db", default="reddit", help="Database name")
    parser.add_argument("--user", default="postgres", help="Database user")
    parser.add_argument("--password", default="secret!1234", help="Database password")
    
    args = parser.parse_args()
    
    logger.info("="*70)
    logger.info("SPARK POSTGRESQL CONNECTION TEST")
    logger.info("="*70 + "\n")
    
    if not check_java():
        sys.exit(1)
    
    success = test_spark_postgresql_connection(args.host, args.port, args.db,
                                              args.user, args.password)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
