#!/usr/bin/env python3
"""Check worldnews records with proper timestamp handling."""
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
print()

# Get latest records with retrieved_at
cursor.execute("""
    SELECT post_id, title, created_utc, retrieved_at
    FROM worldnews
    ORDER BY retrieved_at DESC NULLS LAST
    LIMIT 15
""")

print('Latest 15 worldnews posts ordered by retrieved_at:')
print('-' * 120)
for row in cursor.fetchall():
    post_id, title, created_utc, retrieved_at = row
    print(f'{post_id[:10]}... | Retrieved: {retrieved_at}')
    print(f'  Title: {title[:80]}')
    print(f'  Created UTC raw: {created_utc}')
    print()

# Check the min/max retrieved_at
cursor.execute("""
    SELECT 
        MIN(retrieved_at) as oldest_retrieval,
        MAX(retrieved_at) as newest_retrieval,
        COUNT(*) as count
    FROM worldnews
    WHERE retrieved_at IS NOT NULL
""")
stats = cursor.fetchone()
print(f'\nRetrieval time range:')
print(f'Oldest retrieval: {stats[0]}')
print(f'Newest retrieval: {stats[1]}')
print(f'Count with retrieval time: {stats[2]}')

cursor.close()
conn.close()
