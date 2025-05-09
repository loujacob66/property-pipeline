#!/usr/bin/env python3
"""
Generate a report of listings with their estimated monthly cashflow.
"""

import sqlite3
from pathlib import Path

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"

def format_currency(value):
    """Format a number as currency, or return 'N/A' if None."""
    if value is None:
        return "N/A"
    try:
        return f"${float(value):,.2f}"
    except (ValueError, TypeError):
        return str(value) # Fallback if not a number

def generate_cashflow_report():
    """Generate a cashflow report of listings"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        c.execute('''
            SELECT address, city, price, estimated_rent, estimated_monthly_cashflow
            FROM listings
            ORDER BY estimated_monthly_cashflow DESC NULLS LAST, address ASC
        ''')
        rows = c.fetchall()
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        conn.close()
        return

    if not rows:
        print("No listings found or no cashflow data available.")
        conn.close()
        return

    # Define headers
    headers = [
        "Address", "City", "Purchase Price", "Est. Rent", "Est. Monthly Cashflow"
    ]

    # Prepare data for width calculation and printing (and apply formatting)
    formatted_rows_data = []
    for row in rows:
        formatted_row = (
            str(row[0] if row[0] is not None else "N/A"),  # Address
            str(row[1] if row[1] is not None else "N/A"),  # City
            format_currency(row[2]),  # Purchase Price
            format_currency(row[3]),  # Est. Rent
            format_currency(row[4])   # Est. Monthly Cashflow
        )
        formatted_rows_data.append(formatted_row)

    # Calculate column widths
    col_widths = [len(header) for header in headers]
    for formatted_row in formatted_rows_data:
        for idx, value_str in enumerate(formatted_row):
            col_widths[idx] = max(col_widths[idx], len(value_str))

    # Print header
    header_line = " | ".join(header.ljust(col_widths[i]) for i, header in enumerate(headers))
    print(header_line)
    print("-" * len(header_line))

    # Print data rows
    for formatted_row in formatted_rows_data:
        row_line = " | ".join(value_str.ljust(col_widths[i]) for i, value_str in enumerate(formatted_row))
        print(row_line)

    conn.close()

if __name__ == "__main__":
    generate_cashflow_report() 