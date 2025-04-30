#!/usr/bin/env python3
"""
Generate a report of all listings in the database
"""

import sqlite3
from pathlib import Path

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"

def generate_report():
    """Generate a report of all listings"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('''
        SELECT address, city, price, beds, baths, sqft, price_per_sqft, 
               estimated_rent, rent_yield, mls_number, mls_type, tax_information,
               days_on_compass, status, last_updated, favorite
        FROM listings
        ORDER BY imported_at DESC
    ''')

    rows = c.fetchall()

    if not rows:
        print("No listings found.")
        return

    # Define headers
    headers = [
        "Address", "City", "Price", "Beds", "Baths", "Sqft",
        "Price/Sqft", "Est. Rent", "Yield", "MLS#", "MLS Type", "Tax Info",
        "Days on Comp", "Status", "Updated", "Fav"
    ]

    # Calculate column widths
    col_widths = [len(header) for header in headers]
    for row in rows:
        for idx, value in enumerate(row):
            if value is None:
                value = ""
            elif isinstance(value, float):
                value = f"{value:,.2f}" if idx not in [8] else f"{value:.2%}"  # Special formatting for rent_yield
            elif isinstance(value, int):
                if idx == 15:  # Favorite field
                    value = "★" if value else ""
                else:
                    value = f"{value:,}"
            else:
                value = str(value)
            col_widths[idx] = max(col_widths[idx], len(value))

    # Print header
    header_row = " | ".join(header.ljust(col_widths[i]) for i, header in enumerate(headers))
    print(header_row)
    print("-" * len(header_row))

    # Print data rows
    for row in rows:
        formatted_row = []
        for idx, value in enumerate(row):
            if value is None:
                value = ""
            elif isinstance(value, float):
                value = f"{value:,.2f}" if idx not in [8] else f"{value:.2%}"  # Special formatting for rent_yield
            elif isinstance(value, int):
                if idx == 15:  # Favorite field
                    value = "★" if value else ""
                else:
                    value = f"{value:,}"
            else:
                value = str(value)
            formatted_row.append(value.ljust(col_widths[idx]))
        print(" | ".join(formatted_row))

    conn.close()

if __name__ == "__main__":
    generate_report()
