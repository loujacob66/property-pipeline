#!/usr/bin/env python3
"""
Manage blacklisted addresses by adding all "Expired" and "Closed" listings from the listings table.

Usage:
    python blacklist_address_expired.py [--dry-run]
"""

import sqlite3
import argparse
from pathlib import Path
import sys
from datetime import datetime

# Define project root and database path using pathlib for robustness
try:
    SCRIPT_DIR = Path(__file__).resolve().parent
    ROOT = SCRIPT_DIR.parent
    DB_PATH = ROOT / "data" / "listings.db"
except NameError:
    # Fallback for environments where __file__ might not be defined reliably
    ROOT = Path(".").resolve() # Assume running from project root
    DB_PATH = ROOT / "data" / "listings.db"
    print(f"Warning: Could not determine script directory. Assuming CWD is project root: {ROOT}")
    print(f"Database path set to: {DB_PATH}")

def create_blacklist_table(conn):
    """Ensure the address_blacklist table exists."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS address_blacklist (
            address TEXT PRIMARY KEY NOT NULL,
            reason TEXT,
            blacklisted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    print("Ensured address_blacklist table exists.")

def get_reason_for_status(status):
    """Get the appropriate reason message for a given status."""
    reasons = {
        'Expired': "Listing status was 'Expired'",
        'Closed': "Listing status was 'Closed'"
    }
    return reasons.get(status, f"Listing status was '{status}'")

def process_inactive_listings(dry_run=False):
    """Find expired and closed listings, blacklist them, and remove from listings table."""
    print(f"--- Operation: Blacklist Inactive Listings ---")
    if dry_run:
        print("--- Mode: Dry Run (No changes will be made) ---")

    conn = None
    try:
        print(f"Connecting to database: {DB_PATH}")
        if not DB_PATH.parent.exists():
            print(f"Error: Data directory {DB_PATH.parent} does not exist.")
            return
        if not DB_PATH.exists() and not dry_run:
            print(f"Error: Database file {DB_PATH} does not exist.")
            if not dry_run:
                return
            else:
                print("Database doesn't exist, simulating actions.")
                conn = None # Simulate no connection for dry run
        elif DB_PATH.exists():
            conn = sqlite3.connect(DB_PATH)
            # Ensure blacklist table exists if we have a connection
            create_blacklist_table(conn)
        else: # DB doesn't exist and it's a dry run
            conn = None

        cursor = conn.cursor() if conn else None

        if cursor:
            # Find expired and closed listings
            print("Searching for inactive listings in the 'listings' table...")
            cursor.execute("SELECT id, address, status FROM listings WHERE status IN ('Expired', 'Closed')")
            inactive_listings = cursor.fetchall()
            print(f"Found {len(inactive_listings)} inactive listing(s).")

            if not inactive_listings:
                print("No inactive listings found to process.")
                return

            for listing_id, address, status in inactive_listings:
                address_lower = address.lower()
                reason = get_reason_for_status(status)

                # Check if already blacklisted
                cursor.execute("SELECT 1 FROM address_blacklist WHERE LOWER(address) = ?", (address_lower,))
                blacklisted = cursor.fetchone() is not None

                print(f"Processing address: '{address}' (Status: {status}, Currently blacklisted: {blacklisted})")

                if not blacklisted:
                    # Add to blacklist
                    if dry_run:
                        print(f"[Dry Run] Would add '{address}' to address_blacklist with reason: '{reason}'.")
                    else:
                        print(f"Adding '{address}' to address_blacklist...")
                        cursor.execute("INSERT OR IGNORE INTO address_blacklist (address, reason, blacklisted_at) VALUES (?, ?, ?)",
                                     (address, reason, datetime.now()))
                        conn.commit()
                        print("✅ Successfully added to blacklist.")
                else:
                    print(f"ℹ️ Address '{address}' is already in the blacklist.")

                # Remove from listings table
                if dry_run:
                    print(f"[Dry Run] Would remove '{address}' from listings table.")
                else:
                    print(f"Removing '{address}' from listings table...")
                    cursor.execute("DELETE FROM listings WHERE id = ?", (listing_id,))
                    conn.commit()
                    print("✅ Successfully removed from listings.")

        else: # conn is None (dry run and DB didn't exist)
            print("Database connection failed, could not search for inactive listings.")

    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Blacklist inactive addresses (Expired or Closed).")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without modifying the database.")

    args = parser.parse_args()

    process_inactive_listings(
        dry_run=args.dry_run
    )
    print("--- Operation Complete ---") 