#!/usr/bin/env python3
"""
Import Compass listings to database
"""

import sqlite3
from pathlib import Path

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"

def import_listing_to_db(listing):
    """Import a single listing to the database"""
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        
        # Check if listing with this address already exists
        c.execute("SELECT id FROM listings WHERE address = ?", (listing['address'],))
        existing = c.fetchone()
        
        if existing:
            # Update existing listing
            print(f"Updating existing listing: {listing['address']}")
            c.execute('''
                UPDATE listings 
                SET price = ?, beds = ?, baths = ?, sqft = ?, 
                    price_per_sqft = ?, url = ?, source = ?,
                    imported_at = CURRENT_TIMESTAMP
                WHERE address = ?
            ''', (
                listing['price'], listing['beds'], listing['baths'],
                listing['sqft'], listing['price_per_sqft'], listing['url'],
                listing['source'], listing['address']
            ))
        else:
            # Insert new listing
            print(f"Inserting new listing: {listing['address']}")
            c.execute('''
                INSERT INTO listings (
                    address, city, state, zip, price, beds, baths,
                    sqft, price_per_sqft, url, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                listing['address'], listing['city'], listing['state'],
                listing['zip'], listing['price'], listing['beds'],
                listing['baths'], listing['sqft'], listing['price_per_sqft'],
                listing['url'], listing['source']
            ))
        
        conn.commit()
        return True
        
    except sqlite3.IntegrityError as e:
        print(f"❌ Error importing listing {listing['address']}: {str(e)}")
        return False
        
    finally:
        conn.close() 