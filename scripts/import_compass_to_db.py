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
            update_fields = {
                'price': listing['price'],
                'beds': listing['beds'],
                'baths': listing['baths'],
                'sqft': listing['sqft'],
                'price_per_sqft': listing['price_per_sqft'],
                'url': listing['url'],
                'source': listing['source'],
                'imported_at': 'CURRENT_TIMESTAMP'
            }
            
            # Add new fields if they exist
            if 'walkscore_shorturl' in listing:
                update_fields['walkscore_shorturl'] = listing['walkscore_shorturl']
            if 'compass_shorturl' in listing:
                update_fields['compass_shorturl'] = listing['compass_shorturl']
            
            set_clause = ", ".join(f"{key} = ?" for key in update_fields.keys())
            values = list(update_fields.values()) + [listing['address']]
            
            c.execute(f'''
                UPDATE listings 
                SET {set_clause}
                WHERE address = ?
            ''', values)
        else:
            # Insert new listing
            print(f"Inserting new listing: {listing['address']}")
            insert_fields = {
                'address': listing['address'],
                'city': listing['city'],
                'state': listing['state'],
                'zip': listing['zip'],
                'price': listing['price'],
                'beds': listing['beds'],
                'baths': listing['baths'],
                'sqft': listing['sqft'],
                'price_per_sqft': listing['price_per_sqft'],
                'url': listing['url'],
                'source': listing['source']
            }
            
            # Add new fields if they exist
            if 'walkscore_shorturl' in listing:
                insert_fields['walkscore_shorturl'] = listing['walkscore_shorturl']
            if 'compass_shorturl' in listing:
                insert_fields['compass_shorturl'] = listing['compass_shorturl']
            
            columns = ", ".join(insert_fields.keys())
            placeholders = ", ".join(["?"] * len(insert_fields))
            values = list(insert_fields.values())
            
            c.execute(f'''
                INSERT INTO listings ({columns})
                VALUES ({placeholders})
            ''', values)
        
        conn.commit()
        return True
        
    except sqlite3.IntegrityError as e:
        print(f"❌ Error importing listing {listing['address']}: {str(e)}")
        return False
        
    finally:
        conn.close() 