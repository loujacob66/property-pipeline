import sqlite3
from pathlib import Path

db_path = Path("data/listings.db")
db_path.parent.mkdir(parents=True, exist_ok=True)

with sqlite3.connect(db_path) as conn:
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS listings")
    cursor.execute("""
        CREATE TABLE listings (
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
            imported_at TEXT DEFAULT CURRENT_TIMESTAMP,
            from_collection BOOLEAN DEFAULT NULL
        )
    """)
    print("✅ listings.db initialized with full schema.")