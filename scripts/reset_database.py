#!/usr/bin/env python3
"""
Reset and recreate the property listings database with a complete schema.
This script will:
1. Back up your existing database
2. Create a fresh database with the correct schema including tax_information and mls_type
3. Migrate your existing data to the new schema

Run this from the project root directory.
"""

import sqlite3
import os
import shutil
from datetime import datetime

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "listings.db")
BACKUP_PATH = os.path.join(DATA_DIR, f"listings_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")

def backup_database():
    """Create a backup of the current database."""
    if os.path.exists(DB_PATH):
        shutil.copy2(DB_PATH, BACKUP_PATH)
        print(f"✅ Created backup at {BACKUP_PATH}")
    else:
        print("ℹ️ No existing database found to backup")

def migrate_data(old_db_path, new_db_path):
    """Migrate data from old database to new one."""
    try:
        # Connect to both databases
        old_conn = sqlite3.connect(old_db_path)
        new_conn = sqlite3.connect(new_db_path)
        
        # Get the data from the old database
        cursor = old_conn.cursor()
        cursor.execute("SELECT * FROM listings")
        rows = cursor.fetchall()
        
        # Get column names from the old database
        cursor.execute("PRAGMA table_info(listings)")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        # Check which columns exist in the old database
        has_tax_info = "tax_information" in column_names
        has_mls_type = "mls_type" in column_names
        
        # Insert data into the new database
        if rows:
            # Prepare the insert statement
            placeholders = ", ".join(["?"] * (len(column_names) + (0 if has_tax_info else 1) + (0 if has_mls_type else 1)))
            columns = ", ".join(column_names)
            
            # Add new columns if they don't exist
            if not has_tax_info:
                columns += ", tax_information"
            if not has_mls_type:
                columns += ", mls_type"
            
            # Create insert statement
            insert_sql = f"INSERT INTO listings ({columns}) VALUES ({placeholders})"
            
            # Insert each row
            new_cursor = new_conn.cursor()
            for row in rows:
                # Add NULL values for new columns if they don't exist
                values = list(row)
                if not has_tax_info:
                    values.append(None)
                if not has_mls_type:
                    values.append(None)
                
                new_cursor.execute(insert_sql, values)
                
            new_conn.commit()
            print(f"✅ Migrated {len(rows)} records to the new database")
        else:
            print("ℹ️ No records found to migrate")
        
        # Close connections
        old_conn.close()
        new_conn.close()
        
        return True
    except Exception as e:
        print(f"❌ Error during migration: {e}")
        return False

def recreate_database():
    """Create a fresh database with the correct schema."""
    try:
        # Back up the existing database
        backup_database()
        
        # Create a temporary path for the new database
        temp_db_path = DB_PATH + ".new"
        
        # Create the new database
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        
        # Create listings table with all columns
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            price REAL,
            beds REAL,
            baths REAL,
            sqft INTEGER,
            estimated_rent REAL,
            rent_yield REAL,
            url TEXT UNIQUE,
            source TEXT DEFAULT 'compass',
            imported_at TEXT DEFAULT CURRENT_TIMESTAMP,
            from_collection BOOLEAN DEFAULT NULL,
            tax_information TEXT,
            mls_type TEXT
        )
        ''')
        
        # Create indices
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_listings_url ON listings(url);
        ''')
        
        conn.commit()
        conn.close()
        
        # Migrate data from the old database to the new one
        if os.path.exists(DB_PATH):
            success = migrate_data(BACKUP_PATH, temp_db_path)
            if success:
                # Replace the old database with the new one
                if os.path.exists(DB_PATH):
                    os.remove(DB_PATH)
                os.rename(temp_db_path, DB_PATH)
                print(f"✅ Successfully recreated database at {DB_PATH}")
            else:
                # Keep the new database for inspection but don't replace the old one
                print(f"⚠️ Migration had errors. New database saved at {temp_db_path} for inspection.")
        else:
            # No old database to migrate from
            os.rename(temp_db_path, DB_PATH)
            print(f"✅ Created fresh database at {DB_PATH}")
            
    except Exception as e:
        print(f"❌ Error recreating database: {e}")

if __name__ == "__main__":
    print("🔄 Starting database reset process...")
    recreate_database()
    print("✅ Database reset complete")
