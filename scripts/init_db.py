import sqlite3
import os

DB_PATH = "data/listings.db"

def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        address TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        price INTEGER,
        beds INTEGER,
        baths INTEGER,
        sqft INTEGER,
        estimated_rent REAL,
        rent_yield REAL,
        url TEXT UNIQUE,
        source TEXT DEFAULT 'compass',
        imported_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    conn.close()
    print("✅ Database initialized at", DB_PATH)

if __name__ == "__main__":
    init_db()
