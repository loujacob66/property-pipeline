#!/usr/bin/env python3
"""
Manage blacklisted addresses to prevent them from being added/updated in the listings database.

Usage:
    python blacklist_address.py --address "123 Main St" [--reason "Duplicate listing"] [--dry-run]
    python blacklist_address.py --address "123 Main St" --remove [--dry-run]
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

def manage_blacklist(address, reason=None, remove=False, dry_run=False):
    """Add or remove an address from the blacklist and the listings table."""
    if not address:
        print("Error: --address is required.")
        return

    print(f"--- Operation: {'Remove' if remove else 'Add'} Address --- Address: '{address}'")
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
             # Allow dry run even if DB doesn't exist
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

        # Use LOWER() for case-insensitive comparison
        address_lower = address.lower()

        # Check current status
        blacklisted = False
        in_listings = False

        if cursor:
            cursor.execute("SELECT 1 FROM address_blacklist WHERE LOWER(address) = ?", (address_lower,))
            blacklisted = cursor.fetchone() is not None

            cursor.execute("SELECT 1 FROM listings WHERE LOWER(address) = ?", (address_lower,))
            in_listings = cursor.fetchone() is not None

        print(f"Current status: Blacklisted={blacklisted}, In Listings={in_listings}")

        if remove:
            # --- Remove from Blacklist ---
            if blacklisted:
                if dry_run:
                    print(f"[Dry Run] Would remove '{address}' from address_blacklist.")
                else:
                    if cursor:
                        print(f"Removing '{address}' from address_blacklist...")
                        cursor.execute("DELETE FROM address_blacklist WHERE LOWER(address) = ?", (address_lower,))
                        conn.commit()
                        print("✅ Successfully removed from blacklist.")
                    else:
                         print("Error: Cannot remove from blacklist, DB connection failed.")
            else:
                print(f"ℹ️ Address '{address}' is not currently in the blacklist.")
        else:
            # --- Add to Blacklist (and remove from listings) ---
            if blacklisted:
                print(f"ℹ️ Address '{address}' is already in the blacklist.")
                # Optionally update reason if provided? For now, just report.
            else:
                if dry_run:
                    print(f"[Dry Run] Would add '{address}' to address_blacklist.")
                    if reason:
                        print(f"  Reason: {reason}")
                else:
                     if cursor:
                        print(f"Adding '{address}' to address_blacklist...")
                        cursor.execute("INSERT OR IGNORE INTO address_blacklist (address, reason, blacklisted_at) VALUES (?, ?, ?)",
                                     (address, reason, datetime.now()))
                        conn.commit()
                        print("✅ Successfully added to blacklist.")
                     else:
                         print("Error: Cannot add to blacklist, DB connection failed.")


            # Also remove from listings table if it exists there
            if in_listings:
                if dry_run:
                    print(f"[Dry Run] Would remove '{address}' from listings table.")
                else:
                    if cursor:
                        print(f"Removing '{address}' from listings table...")
                        cursor.execute("DELETE FROM listings WHERE LOWER(address) = ?", (address_lower,))
                        conn.commit()
                        print("✅ Successfully removed from listings.")
                    else:
                        print("Error: Cannot remove from listings, DB connection failed.")
            elif not blacklisted: # Only print if it wasn't already blacklisted
                 print(f"ℹ️ Address '{address}' was not found in the listings table.")


    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage blacklisted addresses.")
    parser.add_argument("--address", required=True, help="The street address to manage.")
    parser.add_argument("--reason", help="Reason for blacklisting (used when adding).")
    parser.add_argument("--remove", action="store_true", help="Remove the address from the blacklist.")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without modifying the database.")

    args = parser.parse_args()

    manage_blacklist(
        address=args.address,
        reason=args.reason,
        remove=args.remove,
        dry_run=args.dry_run
    )
    print("--- Operation Complete ---") 