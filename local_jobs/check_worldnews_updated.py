#!/usr/bin/env python3
"""Check worldnews records in the worldnews table."""
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
cursor.execute("SELECT COUNT(*) FROM worldnews")
total = cursor.fetchone()[0]
print(f'Total worldnews records: {total}')

# Get latest records by created_utc
cursor.execute("""
    SELECT post_id, title, created_utc, score, num_comments, retrieved_at
    FROM worldnews
    ORDER BY created_utc DESC
    LIMIT 15
""")

print('\nLatest 15 worldnews posts by created_utc:')
print('-' * 120)
for row in cursor.fetchall():
    post_id, title, created_utc, score, num_comments, retrieved_at = row
    created_dt = datetime.fromtimestamp(int(created_utc)) if created_utc else None
    print(f'{created_dt} | {post_id[:10]}... | Score: {score:4} | Comments: {num_comments:3} | {title[:50]}')
    if retrieved_at:
        print(f'  Retrieved at: {retrieved_at}')

# Get the timestamp range
cursor.execute("""
    SELECT 
        MIN(created_utc) as oldest,
        MAX(created_utc) as newest,
        COUNT(*) as count
    FROM worldnews
""")
stats = cursor.fetchone()
oldest_dt = datetime.fromtimestamp(int(stats[0])) if stats[0] else None
newest_dt = datetime.fromtimestamp(int(stats[1])) if stats[1] else None
print(f'\nData range:')
print(f'Oldest post: {oldest_dt} (UTC timestamp: {stats[0]})')
print(f'Newest post: {newest_dt} (UTC timestamp: {stats[1]})')
print(f'Total count: {stats[2]}')

# Check if there's a retrieved_at column
cursor.execute("""
    SELECT post_id, retrieved_at
    FROM worldnews
    ORDER BY retrieved_at DESC
    LIMIT 10
""")
print('\nLatest records by retrieved_at:')
for row in cursor.fetchall():
    print(f'  {row[1]} - {row[0]}')

cursor.close()
conn.close()
