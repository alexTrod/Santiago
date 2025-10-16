import psycopg2
import os

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'Gulf'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'santiago'),
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': os.getenv('DB_PORT', '5432')
}

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

print(f"Connected to: {DB_CONFIG['dbname']} on {DB_CONFIG['host']}")
print("\n--- Current search_path ---")
cursor.execute("SHOW search_path;")
print(cursor.fetchone())

print("\n--- All schemas ---")
cursor.execute("SELECT schema_name FROM information_schema.schemata;")
for row in cursor.fetchall():
    print(row[0])

print("\n--- Tables named 'markets' in any schema ---")
cursor.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE schema_name = 'public';")
results = cursor.fetchall()
if results:
    for row in results:
        print(f"  {row[0]}.{row[1]}")
else:
    print("  NO 'markets' table found!")

print("\n--- All tables in public schema ---")
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
for row in cursor.fetchall():
    print(f"  public.{row[0]}")

cursor.close()
conn.close()

