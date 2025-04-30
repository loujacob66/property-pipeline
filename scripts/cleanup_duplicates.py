#!/usr/bin/env python3
"""
Clean up duplicate listings in the database
"""

import sqlite3
import os
from pathlib import Path

def cleanup_duplicates():
    """Remove duplicate listings, keeping the most recently updated one"""
    db_path = Path(__file__).parent.parent / "data" / "listings.db"
    conn = sqlite3.connect(db_path)
    try:
        c = conn.cursor()
        
        # Find duplicates
        c.execute("""
            SELECT address, COUNT(*) as count, GROUP_CONCAT(id) as ids
            FROM listings
            GROUP BY address
            HAVING count > 1
        """)
        
        duplicates = c.fetchall()
        if not duplicates:
            print("✅ No duplicates found")
            return
            
        print(f"Found {len(duplicates)} addresses with duplicates")
        
        for address, count, ids in duplicates:
            ids = [int(id) for id in ids.split(',')]
            print(f"\nAddress: {address}")
            print(f"Duplicate IDs: {ids}")
            
            # Get the most recently updated listing
            c.execute("""
                SELECT id, last_updated
                FROM listings
                WHERE id IN ({})
                ORDER BY last_updated DESC NULLS LAST, id DESC
                LIMIT 1
            """.format(','.join('?' * len(ids))), ids)
            
            keep_id = c.fetchone()[0]
            print(f"Keeping ID: {keep_id}")
            
            # Delete other duplicates
            delete_ids = [id for id in ids if id != keep_id]
            c.execute(f"""
                DELETE FROM listings
                WHERE id IN ({','.join('?' * len(delete_ids))})
            """, delete_ids)
            
            print(f"Deleted IDs: {delete_ids}")
        
        conn.commit()
        print("\n✅ Duplicate cleanup completed")
        
    finally:
        conn.close()

if __name__ == "__main__":
    cleanup_duplicates() 