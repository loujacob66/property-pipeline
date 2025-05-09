#!/usr/bin/env python3
"""
Generate a report of quarterly rental history for all listings.
"""

import sqlite3
from pathlib import Path
from datetime import datetime

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"

def generate_rental_history_report():
    """
    Generates a report of quarterly rental history for all listings.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # SQL query to get listing details and rental history
        c.execute('''
            SELECT
                l.address,
                l.city,
                r.date,
                r.rent
            FROM listings l
            JOIN rental_history r ON l.id = r.listing_id
            ORDER BY l.address, r.date
        ''')

        rows = c.fetchall()

        if not rows:
            print("No rental history found.")
            return

        # Process data to group by listing and quarter, and collect all unique quarters
        report_data = {}
        all_quarters = set()
        for address, city, date_str, rent in rows:
            year_quarter = f"{date_str[:4]}Q{(int(date_str[5:7]) - 1) // 3 + 1}"
            all_quarters.add(year_quarter)
            if (address, city) not in report_data:
                report_data[(address, city)] = {}
            if year_quarter not in report_data[(address, city)]:
                report_data[(address, city)][year_quarter] = []
            report_data[(address, city)][year_quarter].append(rent)

        sorted_quarters = sorted(list(all_quarters))

        # Calculate average rent per quarter and format for printing
        formatted_report = []
        header = ["Address", "City"] + sorted_quarters
        formatted_report.append(header)
        formatted_report.append(['-' * len(h) for h in header]) # Separator

        for (address, city), quarters_data in report_data.items():
            row_data = [address, city]
            for quarter in sorted_quarters:
                if quarter in quarters_data:
                    avg_rent = sum(quarters_data[quarter]) // len(quarters_data[quarter])
                    row_data.append(f"${avg_rent:,}")
                else:
                    row_data.append("") # Placeholder for missing data
            formatted_report.append(row_data)

        # Calculate column widths
        # Ensure all rows have the same length before zipping
        max_cols = max(len(row) for row in formatted_report) if formatted_report else 0
        # Pad rows with empty strings to ensure they all have max_cols
        padded_report = [row + [""] * (max_cols - len(row)) for row in formatted_report]
        col_widths = [max(len(str(item)) for item in col) for col in zip(*padded_report)]

        # Print formatted report
        for row in padded_report:
            print(" | ".join(str(item).ljust(col_widths[i]) for i, item in enumerate(row)))

    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    generate_rental_history_report() 