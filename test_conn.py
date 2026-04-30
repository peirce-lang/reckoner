import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2

conn = psycopg2.connect(os.environ['DATABASE_URL'], options='-c search_path=legal,public')
cur = conn.cursor()

# Check what user we are
cur.execute("SELECT current_user, current_database(), current_schema()")
print("user/db/schema:", cur.fetchone())

# Try information_schema
cur.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'legal' 
    AND table_name = 'snf_what'
""")
print("columns:", cur.fetchall())

# Try direct table access
cur.execute("SELECT COUNT(*) FROM legal.snf_what")
print("count:", cur.fetchone())

conn.close()