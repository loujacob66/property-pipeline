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
import os # Ensure os is imported
import sys # Ensure sys is imported for path manipulation
# Add the parent directory of 'lib' to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.compass_utils import authenticate_compass # Added import

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
# from lib.compass_utils import extract_listing_details # Not used, assuming logic is inline or different
from urllib.parse import urlparse, parse_qs

# Define project root and database path
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / 'data' / 'listings.db'
# AUTH_STORAGE_PATH is managed by authenticate_compass

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

def track_listing_changes(listing_id, field_name, old_value, new_value, source="compass-enrichment"):
    """
    Track changes to listing fields in the listing_changes table.
    
    Args:
        listing_id (int): The ID of the listing
        field_name (str): Name of the field that changed
        old_value: Previous value
        new_value: New value
        source (str): Source of the change
    """
    if old_value != new_value:
        conn = sqlite3.connect(DB_PATH)
        try:
            c = conn.cursor()
            c.execute("""
                INSERT INTO listing_changes 
                (listing_id, field_name, old_value, new_value, source)
                VALUES (?, ?, ?, ?, ?)
            """, (listing_id, field_name, str(old_value), str(new_value), source))
            conn.commit()
            print(f"📝 Recorded change in {field_name}: {old_value} → {new_value}")
        finally:
            conn.close()

def store_listing_details(listing_id, details):
    """
    Stores the extracted listing details in the database and tracks changes.
    
    Args:
        listing_id (int): The ID of the listing to update
        details (dict): Dictionary containing the listing details
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        
        # Get existing values
        c.execute("SELECT * FROM listings WHERE id = ?", (listing_id,))
        existing = c.fetchone()
        if existing:
            # Get column names
            columns = [description[0] for description in c.description]
            existing_dict = dict(zip(columns, existing))
            
            # Get existing column names from the listings table
            c.execute("PRAGMA table_info(listings)")
            valid_columns = {row[1] for row in c.fetchall()}
            
            # Filter details to only include existing columns
            valid_fields = {k: v for k, v in details.items() if k in valid_columns and v is not None}
            
            if valid_fields:
                # Track changes for each field
                for field, new_value in valid_fields.items():
                    old_value = existing_dict.get(field)
                    if old_value != new_value:
                        track_listing_changes(listing_id, field, old_value, new_value)
                
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

def enrich_listings_with_compass(max_listings=None, headless=False): # Added headless parameter
    # First fix existing MLS types
    fix_existing_mls_types()
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Modified query to include price and status fields
    c.execute("""
        SELECT id, url FROM listings 
        WHERE mls_number IS NULL 
           OR tax_information IS NULL 
           OR mls_type IS NULL
           OR price IS NULL
           OR status IS NULL
    """)
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
        try:
            print("🔐 Authenticating with Compass...")
            page, context = authenticate_compass(p, headless=headless)
            print("✅ Authentication successful.")
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            print("   Please try running in headed mode (without --headless) to resolve authentication issues.")
            conn.close() # Close connection before returning
            return # Exit if authentication fails

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
                price = None
                status = None

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

                try:
                    # Wait for the price to appear (up to 5 seconds)
                    price_element = iframe.locator("div[data-testid='price']").first
                    if price_element:
                        price_text = price_element.inner_text()
                        m = re.search(r'\$([\d,]+)', price_text)
                        if m:
                            price = int(m.group(1).replace(',', ''))
                except Exception:
                    print("⚠️ Price not found on page.")

                try:
                    # Wait for the status to appear (up to 5 seconds)
                    status_element = iframe.locator("div[data-testid='status']").first
                    if status_element:
                        status = status_element.inner_text()
                except Exception:
                    print("⚠️ Status not found on page.")

                if mls_number or tax_info or mls_type or price or status:
                    details = {
                        'mls_number': mls_number,
                        'tax_information': tax_info,
                        'mls_type': mls_type,
                        'price': price,
                        'status': status
                    }
                    store_listing_details(listing_id, details)
                    print(f"✅ Updated listing ID {listing_id}")
                else:
                    print(f"⚠️ No updates found for listing ID {listing_id}")

            except Exception as e:
                print(f"❌ Error processing listing ID {listing_id}: {e}")

        context.close()
    conn.close()
    print("🏁 Enrichment process completed.")

def main():
    parser = argparse.ArgumentParser(description="Enrich Compass listings with details.") # Added description
    parser.add_argument('--max-listings', type=int, help='Maximum number of listings to process')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode') # Added headless argument
    args = parser.parse_args()
    
    enrich_listings_with_compass(max_listings=args.max_listings, headless=args.headless) # Pass headless argument

if __name__ == "__main__":
    main()