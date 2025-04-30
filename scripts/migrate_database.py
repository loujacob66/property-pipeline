#!/usr/bin/env python3
"""
Migrate the database to add UNIQUE constraint on address
"""

import sqlite3
import os
from pathlib import Path
from backup_database import backup_database
from cleanup_duplicates import cleanup_duplicates

def migrate_database():
    """Migrate the database to add UNIQUE constraint on address"""
    # First create a backup
    backup_path = backup_database()
    if not backup_path:
        print("❌ Cannot proceed without backup")
        return False
        
    db_path = Path(__file__).parent.parent / "data" / "listings.db"
    conn = sqlite3.connect(db_path)
    
    try:
        c = conn.cursor()
        
        # First clean up any duplicates
        print("\nCleaning up duplicates...")
        cleanup_duplicates()
        
        # Get the current table structure
        c.execute("PRAGMA table_info(listings)")
        columns = c.fetchall()
        column_names = [col[1] for col in columns]
        
        # Create a temporary table with the new schema
        print("\nCreating temporary table with new schema...")
        create_table_sql = '''
            CREATE TABLE IF NOT EXISTS listings_new (
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
                status TEXT
            )
        '''
        c.execute(create_table_sql)
        
        # Copy data from old table to new table
        print("\nCopying data to new table...")
        # Get all column names that exist in both tables
        c.execute("PRAGMA table_info(listings_new)")
        new_columns = c.fetchall()
        new_column_names = [col[1] for col in new_columns]
        
        common_columns = [col for col in column_names if col in new_column_names]
        columns_str = ", ".join(common_columns)
        
        c.execute(f'''
            INSERT INTO listings_new ({columns_str})
            SELECT {columns_str} FROM listings
        ''')
        
        # Drop old table and rename new table
        print("\nReplacing old table with new table...")
        c.execute("DROP TABLE listings")
        c.execute("ALTER TABLE listings_new RENAME TO listings")
        
        conn.commit()
        print("\n✅ Migration completed successfully")
        return True
        
    except Exception as e:
        print(f"\n❌ Error during migration: {str(e)}")
        print(f"Backup is available at: {backup_path}")
        return False
        
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_database() 