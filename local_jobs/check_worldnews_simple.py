#!/usr/bin/env python3
"""Check worldnews records."""
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

# Get actual column names
cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'worldnews'
    ORDER BY ordinal_position
""")
columns = [c[0] for c in cursor.fetchall()]
print(f'Actual columns: {columns}')
print()

# Check total count
cursor.execute("SELECT COUNT(*) FROM worldnews")
total = cursor.fetchone()[0]
print(f'Total worldnews records: {total}')
print()

# Get latest records - just select all columns
cursor.execute("""
    SELECT *
    FROM worldnews
    ORDER BY created_utc DESC
    LIMIT 10
""")

print('Latest 10 worldnews posts by created_utc:')
print('-' * 120)
rows = cursor.fetchall()
for row in rows:
    # Assuming first few columns are: post_id, title, author, score, num_comments, created_utc...
    print(f'Row data: {row[:6]}...')  # Print first 6 fields
    if len(row) > 5 and row[5]:  # created_utc might be at index 5
        try:
            created_dt = datetime.fromtimestamp(int(row[5]))
            print(f'  Created: {created_dt}')
        except:
            pass
    print()

cursor.close()
conn.close()
