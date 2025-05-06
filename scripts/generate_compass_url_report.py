#!/usr/bin/env python3
"""
Generate a report of Compass URLs for all listings in the database
"""

import sqlite3
from pathlib import Path

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"

def generate_compass_url_report():
    """Generate a report of Compass URLs for all listings"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('''
        SELECT address, url
        FROM listings
        ORDER BY imported_at DESC
    ''')

    rows = c.fetchall()

    if not rows:
        print("No listings found.")
        return

    # Write to file with full URLs
    with open("compass_urls.txt", "w") as f:
        for row in rows:
            address = row[0] if row[0] else ""
            url = row[1] if row[1] else ""
            f.write(f"{address} | {url}\n")

    print("URLs written to compass_urls.txt")
    conn.close()

if __name__ == "__main__":
    generate_compass_url_report() 