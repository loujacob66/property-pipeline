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
            id INTEGER PRIMARY KEY,
            address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            price INTEGER,
            beds REAL,
            baths REAL,
            sqft INTEGER,
            url TEXT,
            collection INTEGER,
            source TEXT,
            imported_at TEXT,
            estimated_rent REAL,
            rent_yield REAL,
            mls_number TEXT,
            mls_type TEXT,
            tax_info TEXT
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
