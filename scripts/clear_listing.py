# !/usr/bin/env python3
"""
Script to clear a specific listing from the database.
This will allow the listing to be re-imported from Gmail if the email still exists.
"""

import sqlite3
import sys
from pathlib import Path

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"

def clear_listing(address=None, listing_id=None):
    """Clear a specific listing from the database"""
    print(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        
        if address:
            # Delete by address
            c.execute("DELETE FROM listings WHERE address = ?", (address,))
            print(f"Deleted listing with address: {address}")
        elif listing_id:
            # Delete by ID
            c.execute("DELETE FROM listings WHERE id = ?", (listing_id,))
            print(f"Deleted listing with ID: {listing_id}")
        else:
            print("Error: Must provide either address or listing_id")
            return
            
        conn.commit()
        print("✅ Database update completed")
        
    except Exception as e:
        print(f"❌ Error updating database: {str(e)}")
        conn.rollback()
        
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/clear_listing.py <address or id>")
        sys.exit(1)
        
    arg = sys.argv[1]
    if arg.isdigit():
        clear_listing(listing_id=int(arg))
    else:
        clear_listing(address=arg) 