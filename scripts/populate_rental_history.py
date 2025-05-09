#!/usr/bin/env python3
"""
Populate the rental_history table with data from zori_latest.csv
"""

import sqlite3
import csv
from pathlib import Path
from datetime import datetime, date, timedelta

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"
ZORI_CSV_PATH = ROOT / "data" / "zori_latest.csv"

def populate_rental_history():
    """
    Reads zori_latest.csv and populates the rental_history table.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # Get the date 5 years ago
        five_years_ago = date.today() - timedelta(days=5*365) # Approximation

        with open(ZORI_CSV_PATH, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader) # Skip header row

            # Find the index of the RegionName (zip code) column
            try:
                zip_code_col_idx = header.index('RegionName')
            except ValueError:
                print("❌ Error: 'RegionName' column not found in CSV header.")
                return
                
            # Find the indices of date columns that are within the last 5 years
            date_col_indices = []
            for i, col_name in enumerate(header):
                try:
                    # Attempt to parse the column name as a date
                    col_date = datetime.strptime(col_name, '%Y-%m-%d').date()
                    if col_date >= five_years_ago:
                        date_col_indices.append(i)
                except ValueError:
                    # Not a date column, ignore
                    pass

            if not date_col_indices:
                print("❌ No date columns found in the last 5 years.")
                return

            print(f"Found {len(date_col_indices)} date columns within the last 5 years.")

            # Prepare SQL statement for inserting into rental_history
            insert_sql = "INSERT OR IGNORE INTO rental_history (listing_id, date, rent) VALUES (?, ?, ?)"
            
            # Prepare SQL statement for fetching listing_id by zip code
            select_listing_sql = "SELECT id FROM listings WHERE zip = ?"

            for row in reader:
                if len(row) > zip_code_col_idx:
                    zip_code = row[zip_code_col_idx]

                    # Find listing_ids for this zip code
                    c.execute(select_listing_sql, (zip_code,))
                    listing_ids = [r[0] for r in c.fetchall()]

                    if not listing_ids:
                        # print(f"No listings found for zip code: {zip_code}. Skipping row.")
                        continue

                    for date_col_idx in date_col_indices:
                        if len(row) > date_col_idx:
                            date_str = header[date_col_idx]
                            rent_value_str = row[date_col_idx]
                            
                            if rent_value_str:
                                try:
                                    rent_value = int(float(rent_value_str)) # ZORI data might be float
                                    
                                    # Insert data for each matching listing_id
                                    for listing_id in listing_ids:
                                        c.execute(insert_sql, (listing_id, date_str, rent_value))
                                except ValueError:
                                    # print(f"Skipping invalid rent value for zip code {zip_code} on {date_str}: {rent_value_str}")
                                    pass # Skip rows with invalid rent values

        conn.commit()
        print("✅ Rental history population complete.")

    except FileNotFoundError:
        print(f"❌ Error: CSV file not found at {ZORI_CSV_PATH}")
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    populate_rental_history() 