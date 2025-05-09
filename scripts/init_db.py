import sqlite3
import os
from pathlib import Path

def init_db():
    # Define path relative to this script file
    script_dir = Path(__file__).resolve().parent
    root_dir = script_dir.parent
    db_path = root_dir / 'data' / 'listings.db'

    # Ensure the 'data/' directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Connect and create table first
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL UNIQUE,
            city TEXT,
            state TEXT,
            zip TEXT,
            price INTEGER,
            beds INTEGER,
            baths INTEGER,
            sqft INTEGER,
            price_per_sqft INTEGER,
            url TEXT NOT NULL,
            from_collection INTEGER DEFAULT 0,
            source TEXT,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            estimated_rent INTEGER,
            rent_yield REAL,
            mls_number TEXT,
            mls_type TEXT,
            tax_information TEXT,
            days_on_compass INTEGER,
            last_updated DATE,
            favorite INTEGER DEFAULT 0,
            status TEXT,
            walk_score INTEGER,
            transit_score INTEGER,
            bike_score INTEGER,
            walkscore_shorturl TEXT,
            compass_shorturl TEXT,
            estimated_monthly_cashflow REAL
        )
    ''')
    conn.commit()
    conn.close()

    # AFTER connection and table creation, check if file now exists
    if db_path.is_file():
        print(f"✅ Database created successfully: {db_path}")
    else:
        print(f"❌ Error: database was not created.")

if __name__ == "__main__":
    init_db()
