#!/usr/bin/env python3
"""
Generate the schema of the listings.db database and save it to the data folder.
"""

import sqlite3
from pathlib import Path

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"
SCHEMA_PATH = ROOT / "data" / "listings_schema.sql"

def generate_database_schema():
    """
    Connects to the database, retrieves its schema, and saves it to a file.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # Query the sqlite_master table to get the schema
        c.execute("SELECT sql FROM sqlite_master WHERE type='table';")
        schema_sql = c.fetchall()

        if not schema_sql:
            print(f"❌ No tables found in {DB_PATH}.")
            return

        # Write the schema to the file
        with open(SCHEMA_PATH, 'w', encoding='utf-8') as f:
            for table_schema in schema_sql:
                if table_schema[0]: # Ensure the SQL is not None
                    f.write(f"{table_schema[0]};\\n\\n")

        print(f"✅ Database schema saved to: {SCHEMA_PATH}")

    except FileNotFoundError:
        print(f"❌ Error: Database file not found at {DB_PATH}")
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    generate_database_schema()
