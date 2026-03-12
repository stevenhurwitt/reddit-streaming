#!/usr/bin/env python3
"""
Example script for querying Delta Lake tables via Trino
Make sure to install: pip install trino
"""

from trino.dbapi import connect
import sys

def main():
    # Connect to Trino
    print("Connecting to Trino...")
    try:
        conn = connect(
            host='localhost',
            port=8089,
            catalog='delta',
            schema='reddit',
        )
        cursor = conn.cursor()
        print("✓ Connected to Trino\n")
    except Exception as e:
        print(f"❌ Failed to connect to Trino: {e}")
        print("\nMake sure Trino is running: docker-compose up -d trino")
        sys.exit(1)
    
    # Show all tables
    print("Available tables:")
    print("-" * 50)
    try:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        for table in tables:
            print(f"  • {table[0]}")
        print()
    except Exception as e:
        print(f"❌ Error listing tables: {e}")
        print("Run ./register_trino_tables.sh to register tables\n")
        sys.exit(1)
    
    # Example queries
    queries = {
        "Top 5 Technology Posts by Score": """
            SELECT title, author, score, created_utc
            FROM technology_clean
            ORDER BY score DESC
            LIMIT 5
        """,
        
        "Post Count by Subreddit": """
            SELECT 
                'technology' as subreddit, COUNT(*) as posts FROM technology_clean
            UNION ALL
            SELECT 'programmerhumor', COUNT(*) FROM programmerhumor_clean
            UNION ALL
            SELECT 'news', COUNT(*) FROM news_clean
            UNION ALL
            SELECT 'worldnews', COUNT(*) FROM worldnews_clean
        """,
        
        "Top 5 Authors in News by Total Score": """
            SELECT author, 
                   COUNT(*) as post_count,
                   SUM(score) as total_score,
                   ROUND(AVG(score), 2) as avg_score
            FROM news_clean
            GROUP BY author
            ORDER BY total_score DESC
            LIMIT 5
        """
    }
    
    # Run each query
    for title, query in queries.items():
        print(f"\n{title}:")
        print("=" * 50)
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Print column headers
            if cursor.description:
                headers = [desc[0] for desc in cursor.description]
                print(" | ".join(headers))
                print("-" * 50)
            
            # Print rows
            for row in rows:
                print(" | ".join(str(val) for val in row))
                
        except Exception as e:
            print(f"❌ Error executing query: {e}")
    
    print("\n" + "=" * 50)
    print("✓ All example queries completed")
    print("\nTrino Web UI: http://localhost:8089")
    print("Connect CLI: docker exec -it reddit-trino trino --catalog delta --schema reddit")

if __name__ == "__main__":
    main()
