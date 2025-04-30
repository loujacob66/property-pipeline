#!/usr/bin/env python3
"""
One-time fix script to update tax_information formatting in the database
"""

import sqlite3
import os

def fix_tax_format():
    db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'listings.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all rows with tax_information that needs fixing
    cursor.execute("SELECT id, tax_information FROM listings WHERE tax_information IS NOT NULL")
    rows = cursor.fetchall()
    
    fixed_count = 0
    for row_id, tax_info in rows:
        if tax_info and not tax_info.startswith('$'):
            try:
                # Convert to integer if it's not already formatted
                amount = int(str(tax_info).replace('$', '').replace(',', ''))
                # Format as currency
                formatted_tax = "${:,}".format(amount)
                
                # Update the database
                cursor.execute(
                    "UPDATE listings SET tax_information = ? WHERE id = ?",
                    (formatted_tax, row_id)
                )
                fixed_count += 1
                print(f"Fixed ID {row_id}: {tax_info} -> {formatted_tax}")
            except (ValueError, TypeError):
                print(f"Skipping ID {row_id}: Invalid tax_information format: {tax_info}")
    
    conn.commit()
    print(f"\nFixed {fixed_count} entries in the database.")
    conn.close()

if __name__ == "__main__":
    fix_tax_format() 