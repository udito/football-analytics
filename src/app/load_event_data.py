import os
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load .env file with DATABASE_URL
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Load and normalize one match file
with open("data/open-data/data/events/7478.json") as f:
    raw = json.load(f)

df = pd.json_normalize(raw, sep='_')

# Extract useful columns
df["x"] = df["location"].apply(lambda loc: loc[0] if isinstance(loc, list) else None)
df["y"] = df["location"].apply(lambda loc: loc[1] if isinstance(loc, list) else None)

# Select relevant columns and drop rows with missing data
rows = df[["id", "type_name", "player_name", "x", "y", "timestamp"]].dropna().values.tolist()

# Connect to PostgreSQL
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Create table
cursor.execute("""
CREATE TABLE IF NOT EXISTS match_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT,
    player TEXT,
    x FLOAT,
    y FLOAT,
    timestamp TEXT
);
""")
conn.commit()

# Insert data
insert_query = """
INSERT INTO match_events (event_id, event_type, player, x, y, timestamp)
VALUES %s
ON CONFLICT (event_id) DO NOTHING;
"""
execute_values(cursor, insert_query, rows)
conn.commit()

# Print summary
cursor.execute("SELECT COUNT(*) FROM match_events;")
print("✅ Inserted rows:", cursor.fetchone()[0])

# Clean up
cursor.close()
conn.close()
