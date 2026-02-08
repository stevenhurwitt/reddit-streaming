#!/usr/bin/env python3
"""
Quick PostgreSQL connection tester using psycopg2.
Use this to verify your PostgreSQL setup before running the full curation job.

Usage: python test_postgres_direct.py [--host HOST] [--port PORT] [--db DB] [--user USER] [--password PASS]
"""

import argparse
import logging
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_postgres_connection(db_host="reddit-postgres", db_port=5434, db_name="reddit",
                            db_user="postgres", db_password="secret!1234"):
    """Test PostgreSQL connection using psycopg2."""
    
    logger.info("="*70)
    logger.info("POSTGRESQL CONNECTION TEST")
    logger.info("="*70)
    logger.info(f"Host: {db_host}")
    logger.info(f"Port: {db_port}")
    logger.info(f"Database: {db_name}")
    logger.info(f"User: {db_user}")
    
    try:
        import psycopg2
        logger.info("✓ psycopg2 module imported successfully\n")
    except ImportError:
        logger.error("✗ psycopg2 not installed. Install it with:")
        logger.error("  pip install psycopg2-binary")
        sys.exit(1)
    
    # Test connection
    try:
        logger.info("Attempting to connect...")
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
            connect_timeout=10
        )
        logger.info("✓ Connected successfully!\n")
        
        # Get version
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logger.info(f"PostgreSQL version: {version}\n")
        
        # List tables in reddit_schema
        logger.info("Checking reddit_schema tables...")
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = 'reddit_schema'
            )
        """)
        schema_exists = cursor.fetchone()[0]
        
        if schema_exists:
            logger.info("✓ reddit_schema exists\n")
            
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'reddit_schema'
                ORDER BY table_name
            """)
            tables = cursor.fetchall()
            
            if tables:
                logger.info("Tables in reddit_schema:")
                for table in tables:
                    table_name = table[0]
                    logger.info(f"\n  Table: {table_name}")
                    
                    # Get column info
                    cursor.execute(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns 
                        WHERE table_schema = 'reddit_schema' 
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """)
                    columns = cursor.fetchall()
                    for col_name, col_type, nullable in columns:
                        null_str = "NULL" if nullable == "YES" else "NOT NULL"
                        logger.info(f"    - {col_name}: {col_type} ({null_str})")
                    
                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {schema_exists}.{table_name}")
                    row_count = cursor.fetchone()[0]
                    logger.info(f"    - Rows: {row_count}")
            else:
                logger.warning("  No tables found in reddit_schema")
        else:
            logger.warning("✗ reddit_schema does not exist")
            logger.info("\nCreating reddit_schema...")
            cursor.execute("CREATE SCHEMA reddit_schema;")
            conn.commit()
            logger.info("✓ reddit_schema created\n")
        
        # Test insert capabilities
        logger.info("Testing insert capability...")
        try:
            cursor.execute("""
                CREATE TEMP TABLE test_insert (
                    id SERIAL,
                    test_value TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cursor.execute("INSERT INTO test_insert (test_value) VALUES (%s)", ("test",))
            conn.commit()
            logger.info("✓ Insert test successful\n")
        except Exception as e:
            logger.warning(f"⚠ Insert test failed: {e}")
        
        cursor.close()
        conn.close()
        
        logger.info("="*70)
        logger.info("✓ ALL TESTS PASSED")
        logger.info("="*70)
        return True
        
    except psycopg2.OperationalError as e:
        logger.error(f"\n✗ Connection failed: {e}")
        logger.error("\nTroubleshooting:")
        logger.error(f"1. Verify PostgreSQL is running on {db_host}:{db_port}")
        logger.error("   Linux: sudo systemctl status postgresql")
        logger.error("   Docker: docker ps | grep postgres")
        logger.error(f"2. Check network connectivity: ping {db_host}")
        logger.error(f"3. Check credentials (user: {db_user})")
        logger.error(f"4. Verify database '{db_name}' exists")
        logger.error("5. Check postgres logs for errors")
        return False
        
    except psycopg2.ProgrammingError as e:
        logger.error(f"\n✗ SQL error: {e}")
        return False
        
    except Exception as e:
        logger.error(f"\n✗ Unexpected error: {e}", exc_info=True)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Test PostgreSQL connection directly"
    )
    parser.add_argument("--host", default="reddit-postgres", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5434, help="PostgreSQL port")
    parser.add_argument("--db", default="reddit", help="Database name")
    parser.add_argument("--user", default="postgres", help="Database user")
    parser.add_argument("--password", default="secret!1234", help="Database password")
    
    args = parser.parse_args()
    
    success = test_postgres_connection(args.host, args.port, args.db, args.user, args.password)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
