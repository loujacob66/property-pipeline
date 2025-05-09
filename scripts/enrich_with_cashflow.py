#!/usr/bin/env python3
"""
Enriches listings in the database with calculated estimated monthly cashflow
based on default financial parameters from a configuration file.

Usage:
    python scripts/enrich_with_cashflow.py [--config-path <PATH>] [--db-path <PATH>] \
                                           [--limit <N>] [--dry-run] [--force-update] \
                                           [--address "<FULL_ADDRESS>"]
"""

import argparse
import sqlite3
import json
from pathlib import Path
import sys

# Ensure the parent directory of 'scripts' is in sys.path to allow imports from cashflow_analyzer
# This makes the script more portable when run from different locations.
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from scripts.cashflow_analyzer import calculate_financial_components, load_config, parse_tax_amount # parse_tax_amount might not be directly needed if calculate_financial_components handles it all

# Constants (relative to this script's location if ROOT_DIR is used, or absolute if defined directly)
DEFAULT_DB_PATH = ROOT_DIR / "data" / "listings.db"
DEFAULT_CONFIG_PATH = ROOT_DIR / "config" / "cashflow_config.json"

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Enrich listings with estimated monthly cashflow.")
    parser.add_argument(
        "--config-path",
        type=str,
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to the JSON configuration file (default: {DEFAULT_CONFIG_PATH})."
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to the SQLite database file (default: {DEFAULT_DB_PATH})."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without making any changes to the database."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of listings to process."
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Recalculate and update cashflow even for listings already populated."
    )
    parser.add_argument(
        "--address",
        type=str,
        default=None,
        help="Process a specific listing by its full address."
    )
    return parser.parse_args()

def fetch_listings_for_enrichment(conn, limit=None, force_update=False, specific_address=None):
    """Fetches listings that need cashflow enrichment."""
    cursor = conn.cursor()
    query = "SELECT id, address, price, tax_information, estimated_rent, estimated_monthly_cashflow FROM listings"
    conditions = []
    params = []

    if specific_address:
        conditions.append("address = ?")
        params.append(specific_address)
    elif not force_update:
        conditions.append("estimated_monthly_cashflow IS NULL")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    cursor.execute(query, params)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def update_listing_cashflow(conn, listing_id, calculated_cashflow_value):
    """Updates the estimated_monthly_cashflow for a given listing ID."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE listings SET estimated_monthly_cashflow = ? WHERE id = ?",
            (calculated_cashflow_value, listing_id)
        )
        # conn.commit() # Commit should be done once after the loop for efficiency
        return True
    except sqlite3.Error as e:
        print(f"DB Error updating listing ID {listing_id}: {e}")
        return False

def main():
    args = parse_arguments()

    print(f"--- Starting Cashflow Enrichment --- ({'DRY RUN' if args.dry_run else 'LIVE RUN'})")
    print(f"Using DB: {args.db_path}")
    print(f"Using Config: {args.config_path}")

    config_defaults = load_config(args.config_path)
    if not config_defaults:
        print(f"Error: Configuration file '{args.config_path}' is empty or could not be loaded. Cannot proceed.")
        return

    # Validate essential financial parameters from config
    required_config_keys = ["down_payment", "rate", "insurance", "misc_monthly", "loan_term"]
    missing_keys = [key for key in required_config_keys if key not in config_defaults or config_defaults[key] is None]
    if missing_keys:
        print(f"Error: Missing required financial parameters in config file '{args.config_path}': {', '.join(missing_keys)}")
        return
    
    print("Using default financial parameters from config:")
    for key in required_config_keys:
        print(f"  {key}: {config_defaults[key]}")
    print("-------------------------------------")

    conn = sqlite3.connect(args.db_path)
    listings_to_process = fetch_listings_for_enrichment(conn, args.limit, args.force_update, args.address)

    if not listings_to_process:
        print("No listings found matching the criteria for cashflow enrichment.")
        conn.close()
        return

    print(f"Found {len(listings_to_process)} listings to process...")
    updated_count = 0
    processed_count = 0

    for listing in listings_to_process:
        processed_count += 1
        print(f"\nProcessing ({processed_count}/{len(listings_to_process)}): Listing ID {listing['id']} (Address: {listing['address']})")
        
        if listing['price'] is None or listing['price'] <= 0:
            print(f"  Skipping: Invalid or missing purchase price ('{listing['price']}').")
            continue

        financials = calculate_financial_components(
            purchase_price=listing['price'],
            tax_info_raw=listing['tax_information'],
            estimated_monthly_rent=listing['estimated_rent'], # Can be None, handled by calc function
            down_payment_input_dollars=config_defaults["down_payment"],
            annual_interest_rate_percent=config_defaults["rate"],
            loan_term_years=config_defaults["loan_term"],
            annual_insurance_cost=config_defaults["insurance"],
            misc_monthly_cost=config_defaults["misc_monthly"]
        )

        if financials:
            calculated_cashflow = financials['net_monthly_cashflow']
            current_cashflow = listing['estimated_monthly_cashflow']
            print(f"  Current Stored Cashflow: {f'${current_cashflow:,.2f}' if current_cashflow is not None else 'N/A'}")
            print(f"  Calculated Net Monthly Cashflow: ${calculated_cashflow:,.2f}")

            if args.dry_run:
                print(f"  DRY RUN: Would update listing ID {listing['id']} with cashflow: ${calculated_cashflow:,.2f}")
            else:
                if update_listing_cashflow(conn, listing['id'], calculated_cashflow):
                    print(f"  SUCCESS: Updated listing ID {listing['id']} with cashflow: ${calculated_cashflow:,.2f}")
                    updated_count += 1
                else:
                    print(f"  FAILED: Could not update listing ID {listing['id']}.")
        else:
            print(f"  Skipping: Could not calculate financial components for listing ID {listing['id']}.")

    if not args.dry_run and updated_count > 0:
        conn.commit()
        print(f"\nCommitted {updated_count} updates to the database.")
    elif not args.dry_run:
        print("\nNo updates were made to the database.")

    conn.close()
    print(f"--- Cashflow Enrichment Complete --- Summary ---")
    print(f"Listings Processed: {processed_count}")
    print(f"Listings Updated (Live Run) / Would Update (Dry Run): {updated_count}")
    print("---------------------------------------------")

if __name__ == "__main__":
    main() 