import sqlite3
import os

def init_db():
    db_folder = 'data'
    db_filename = os.path.join(db_folder, 'listings.db')

    os.makedirs(db_folder, exist_ok=True)  # Ensure 'data/' folder exists

    # Connect and create table first
    conn = sqlite3.connect(db_filename)
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
            favorite INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

    # AFTER connection and table creation, check if file now exists
    if os.path.isfile(db_filename):
        print(f"✅ Database created successfully: {db_filename}")
    else:
        print(f"❌ Error: database was not created.")

if __name__ == "__main__":
    init_db()
