#!/usr/bin/env python3
"""List all tables in the PostgreSQL database."""
import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5434,
    database='reddit',
    user='postgres',
    password='secret!1234'
)

cursor = conn.cursor()

# List all tables
cursor.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    ORDER BY table_name
""")

print('Tables in the database:')
print('-' * 50)
tables = cursor.fetchall()
for table in tables:
    print(f'  {table[0]}')
    
    # Get row count for each table
    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
    count = cursor.fetchone()[0]
    print(f'    Rows: {count}')
    
    # Get columns
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table[0]}'
        ORDER BY ordinal_position
    """)
    columns = cursor.fetchall()
    print(f'    Columns: {", ".join([c[0] for c in columns])}')
    print()

cursor.close()
conn.close()
