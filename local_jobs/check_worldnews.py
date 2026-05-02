#!/usr/bin/env python3
"""Quick script to check worldnews records in PostgreSQL."""
import psycopg2
from datetime import datetime

conn = psycopg2.connect(
    host='localhost',
    port=5434,
    database='reddit',
    user='postgres',
    password='secret!1234'
)

cursor = conn.cursor()

# Check total count
cursor.execute("""
    SELECT COUNT(*) as total_count
    FROM curated_posts 
    WHERE subreddit = 'worldnews'
""")
total = cursor.fetchone()[0]
print(f'Total worldnews records: {total}')

# Get latest records by created_utc
cursor.execute("""
    SELECT id, title, created_utc, score, num_comments
    FROM curated_posts 
    WHERE subreddit = 'worldnews'
    ORDER BY created_utc DESC
    LIMIT 10
""")

print('\nLatest 10 worldnews posts by created_utc:')
print('-' * 100)
for row in cursor.fetchall():
    post_id, title, created_utc, score, num_comments = row
    print(f'{created_utc} | {post_id[:10]} | Score: {score} | Comments: {num_comments} | {title[:60]}')

# Get the timestamp range
cursor.execute("""
    SELECT 
        MIN(created_utc) as oldest,
        MAX(created_utc) as newest,
        COUNT(*) as count
    FROM curated_posts 
    WHERE subreddit = 'worldnews'
""")
stats = cursor.fetchone()
print(f'\nData range:')
print(f'Oldest: {stats[0]}')
print(f'Newest: {stats[1]}')
print(f'Count: {stats[2]}')

# Check when the most recent record was inserted
cursor.execute("""
    SELECT id, title, created_utc
    FROM curated_posts 
    WHERE subreddit = 'worldnews'
    ORDER BY created_utc DESC
    LIMIT 1
""")
latest = cursor.fetchone()
if latest:
    print(f'\nMost recent post:')
    print(f'ID: {latest[0]}')
    print(f'Title: {latest[1]}')
    print(f'Created UTC: {latest[2]}')
    print(f'Created UTC (readable): {datetime.fromtimestamp(int(latest[2]))}')

cursor.close()
conn.close()
