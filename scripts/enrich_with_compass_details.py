#!/usr/bin/env python3
"""
Compass Listing Enricher

This script fetches additional details for property listings from Compass.com
and updates the database with the enriched information. Uses Playwright's
persistent context for authentication.

Usage:
    python enrich_with_compass_details.py [--headless] [--limit LIMIT]

Options:
    --headless           Run browser in headless mode (default: False)
    --limit LIMIT        Limit the number of listings to process (default: all)
"""

import os
# import sys # No longer needed for sys.path manipulation here
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Redundant path manipulation

import sqlite3
import time
import argparse
import json
import traceback
import re
from playwright.sync_api import sync_playwright
import random
from pathlib import Path
from datetime import datetime
# from lib.compass_utils import authenticate_compass, extract_listing_details # Not used, assuming logic is inline or different
from urllib.parse import urlparse, parse_qs

# Define project root and database path
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / 'data' / 'listings.db'
AUTH_STORAGE_PATH = ROOT / ".auth" / "compass"

def fetch_listings_needing_enrichment(query):
    """
    Fetches listings from the database that need enrichment.
    
    Args:
        query (str): SQL query to fetch listings
        
    Returns:
        list: List of tuples containing (id, url) for listings needing enrichment
    """
    # db_filename = os.path.join(ROOT, 'data', 'listings.db') # Use global DB_PATH
    print(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        print(f"Executing query: {query}")
        c.execute(query)
        results = c.fetchall()
        print(f"Found {len(results)} listings needing enrichment")
        return results
    finally:
        conn.close()

def store_listing_details(listing_id, details):
    """
    Stores the extracted listing details in the database.
    
    Args:
        listing_id (int): The ID of the listing to update
        details (dict): Dictionary containing the listing details
    """
    # db_filename = os.path.join(ROOT, 'data', 'listings.db') # Use global DB_PATH
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        
        # Get existing column names from the listings table
        c.execute("PRAGMA table_info(listings)")
        valid_columns = {row[1] for row in c.fetchall()}
        
        # Filter details to only include existing columns
        valid_fields = {k: v for k, v in details.items() if k in valid_columns and v is not None}
        
        if valid_fields:
            set_clause = ", ".join(f"{key} = ?" for key in valid_fields.keys())
            values = list(valid_fields.values()) + [listing_id]
            
            print(f"   Updating fields: {', '.join(valid_fields.keys())}")
            c.execute(
                f"UPDATE listings SET {set_clause} WHERE id = ?",
                values
            )
            conn.commit()
            
            # Log key fields
            if details.get("mls_number"):
                print(f"   MLS#: {details['mls_number']}")
            if details.get("tax_information"):
                print(f"   Taxes: {details['tax_information']}")
            if details.get("year_built"):
                print(f"   Year Built: {details['year_built']}")
    finally:
        conn.close()

def get_direct_listing_url(workspace_url):
    """Convert a workspace URL to a direct listing URL"""
    try:
        # Extract the csr parameter
        parsed_url = urlparse(workspace_url)
        query_params = parse_qs(parsed_url.query)
        csr = query_params.get('csr', [''])[0]
        
        if not csr:
            print("⚠️ No csr parameter found in URL")
            return None
            
        # Extract listing ID from csr path
        match = re.search(r'/listing/(\d+)', csr)
        if not match:
            print("⚠️ Could not find listing ID in csr path")
            return None
            
        listing_id = match.group(1)
        
        # Return the workspace URL since direct URLs are not accessible
        return workspace_url
        
    except Exception as e:
        print(f"❌ Error converting URL: {str(e)}")
        return None

def clean_mls_type(mls_type):
    """Convert MLS type to simplified format"""
    if mls_type == "Residential-Detached":
        return "Detached"
    elif mls_type == "Residential-Attached":
        return "Attached"
    return mls_type

def clean_tax_information(tax_info):
    """Extract and format tax information as currency string."""
    if not tax_info or tax_info == "-":
        return None
    # Extract the number after $ and before / or end of string
    match = re.search(r'\$([\d,]+)', tax_info)
    if match:
        # Get the number and remove commas
        amount = int(match.group(1).replace(',', ''))
        # Format as currency string with single $
        return "${:,}".format(amount)
    return None

def fix_existing_mls_types():
    """Fix existing MLS type values in the database"""
    # db_filename = os.path.join(os.path.dirname(__file__), '..', 'data', 'listings.db') # Use global DB_PATH
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get all listings with MLS types that need cleaning
    c.execute("SELECT id, mls_type FROM listings WHERE mls_type LIKE 'Residential-%'")
    listings = c.fetchall()
    
    if listings:
        print(f"🔧 Found {len(listings)} listings with MLS types that need cleaning")
        for listing_id, mls_type in listings:
            cleaned_type = clean_mls_type(mls_type)
            c.execute("UPDATE listings SET mls_type = ? WHERE id = ?", (cleaned_type, listing_id))
            print(f"✅ Fixed listing ID {listing_id}: {mls_type} -> {cleaned_type}")
        conn.commit()
    else:
        print("✅ No MLS types need cleaning")
    
    conn.close()

def enrich_listings_with_compass(max_listings=None):
    # First fix existing MLS types
    fix_existing_mls_types()
    
    # db_filename = os.path.join(os.path.dirname(__file__), '..', 'data', 'listings.db') # Use global DB_PATH
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT id, url FROM listings WHERE mls_number IS NULL OR tax_information IS NULL OR mls_type IS NULL")
    listings = c.fetchall()

    if not listings:
        print("✅ No listings need enrichment. Database is up to date.")
        conn.close()
        return

    if max_listings:
        listings = listings[:max_listings]
        print(f"🔎 Found {len(listings)} listing(s) needing enrichment (limited to {max_listings}).")
    else:
        print(f"🔎 Found {len(listings)} listing(s) needing enrichment.")

    with sync_playwright() as p:
        # Set up persistent context with saved authentication
        # ROOT = Path(__file__).parent.parent # Defined globally
        # AUTH_STORAGE_PATH = ROOT / ".auth" / "compass" # Defined globally
        AUTH_STORAGE_PATH.mkdir(parents=True, exist_ok=True)

        print("🌐 Launching browser with saved authentication...")
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(AUTH_STORAGE_PATH),
            headless=False
        )

        page = context.pages[0]  # use the first (blank) page
        
        # Check if we need to authenticate
        page.goto("https://www.compass.com/")
        if "login" in page.url:
            print("⚠️ Not authenticated. Please log in in the browser window...")
            page.wait_for_url("https://www.compass.com/**", timeout=0)  # Wait indefinitely for successful login
            print("✅ Authentication successful!")
        else:
            print("✅ Using saved authentication")

        for listing_id, url in listings:
            try:
                print(f"➡️ Visiting listing ID {listing_id}")
                page.goto(url)
                time.sleep(3)  # wait for page load

                # Switch to the iframe containing the listing details
                iframe = page.frame_locator("iframe[title='Listing page']").first

                mls_number = None
                tax_info = None
                mls_type = None

                try:
                    mls_number = iframe.locator("tr:has(th:has-text('MLS')) td").first.inner_text()
                except Exception:
                    print("⚠️ MLS number not found on page.")

                try:
                    # Wait for the Taxes row to appear (up to 5 seconds)
                    iframe.locator("tr.keyDetails-text:has(th:has-text('Taxes')) td").first.wait_for(timeout=5000)
                    raw_tax_info = iframe.locator("tr.keyDetails-text:has(th:has-text('Taxes')) td").first.inner_text()
                    tax_info = clean_tax_information(raw_tax_info)
                except Exception:
                    print("⚠️ Tax info not found on page.")

                try:
                    # Wait for the MLS Type row to appear (up to 5 seconds)
                    iframe.locator("tr.keyDetails-text:has(th:has-text('MLS Type')) td").first.wait_for(timeout=5000)
                    raw_mls_type = iframe.locator("tr.keyDetails-text:has(th:has-text('MLS Type')) td").first.inner_text()
                    mls_type = clean_mls_type(raw_mls_type)
                except Exception:
                    print("⚠️ MLS Type not found on page.")

                if mls_number or tax_info or mls_type:
                    c.execute('''
                        UPDATE listings
                        SET mls_number = COALESCE(?, mls_number),
                            tax_information = COALESCE(?, tax_information),
                            mls_type = COALESCE(?, mls_type)
                        WHERE id = ?
                    ''', (mls_number, tax_info, mls_type, listing_id))
                    conn.commit()
                    print(f"✅ Updated listing ID {listing_id}: MLS#={mls_number}, Tax=${tax_info}, MLS Type={mls_type}")
                else:
                    print(f"⚠️ No updates found for listing ID {listing_id}")

            except Exception as e:
                print(f"❌ Error processing listing ID {listing_id}: {e}")

        context.close()
    conn.close()
    print("🏁 Enrichment process completed.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-listings', type=int, help='Maximum number of listings to process')
    args = parser.parse_args()
    
    enrich_listings_with_compass(max_listings=args.max_listings)

if __name__ == "__main__":
    main()