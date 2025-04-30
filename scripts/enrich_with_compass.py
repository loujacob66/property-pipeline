#!/usr/bin/env python3
"""
Compass Listing Enricher

This script fetches additional details for property listings from Compass.com
and updates the database with the enriched information. Uses Playwright's
storage_state functionality to manage authentication.

Usage:
    python enrich_with_compass_details.py [--headless] [--limit LIMIT]

Options:
    --headless           Run browser in headless mode (default: False)
    --limit LIMIT        Limit the number of listings to process (default: all)
"""

import sqlite3
import time
import os
import argparse
import sys
import json
import traceback

# Add project root to path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

# Import the authenticate_compass function
from lib.compass_utils import authenticate_compass, extract_listing_details

def enrich_listings_with_compass(headless=False, limit=None):
    """
    Enrich property listings with details from Compass.com.
    
    Args:
        headless (bool): Whether to run browser in headless mode
        limit (int, optional): Limit the number of listings to process
    """
    db_filename = os.path.join(ROOT, 'data', 'listings.db')
    conn = sqlite3.connect(db_filename)
    
    # Enable column name access by name
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Check if additional columns exist, if not add them
    c.execute("PRAGMA table_info(listings)")
    columns = {row['name'] for row in c.fetchall()}
    
    # Add new columns if they don't exist
    new_columns = [
        "year_built", "lot_size", "hoa_fee", "parking", "heating", "cooling",
        "style", "construction", "days_on_market", "status",
        "agent_name", "agent_phone", "agent_email",
        "schools_json", "price_history_json"
    ]
    
    for column in new_columns:
        if column not in columns:
            print(f"🔧 Adding new column: {column}")
            c.execute(f"ALTER TABLE listings ADD COLUMN {column} TEXT")
    
    # Fetch listings that need enrichment
    query = """
        SELECT id, url FROM listings 
        WHERE url LIKE '%compass.com%' AND 
        (mls_number IS NULL OR tax_info IS NULL OR mls_type IS NULL OR year_built IS NULL)
    """
    
    if limit:
        query += f" LIMIT {limit}"
        
    c.execute(query)
    listings = c.fetchall()

    if not listings:
        print("✅ No listings need enrichment. Database is up to date.")
        conn.close()
        return

    print(f"🔎 Found {len(listings)} listing(s) needing enrichment.")

    page = None
    context = None
    playwright = None
    
    try:
        # Use the authentication utility which handles state management
        page, context, playwright = authenticate_compass(headless=headless)
        
        for listing in listings:
            listing_id = listing['id']
            url = listing['url']
            
            try:
                print(f"➡️ Visiting listing ID {listing_id}: {url}")
                page.goto(url, timeout=30000)  # Increased timeout for listing pages
                time.sleep(3)  # wait for dynamic content to load

                # Check if iframe exists
                iframe_count = page.locator("iframe[title='Listing page']").count()
                if iframe_count == 0:
                    print(f"⚠️ No iframe found for listing ID {listing_id}. Skipping.")
                    continue

                # Switch to the iframe containing the listing details
                iframe = page.frame_locator("iframe[title='Listing page']").first
                
                # Extract all details using our utility function
                details = extract_listing_details(iframe)
                
                # Convert list/dict fields to JSON for storage
                if details["schools"]:
                    details["schools_json"] = json.dumps(details["schools"])
                else:
                    details["schools_json"] = None
                    
                if details["price_history"]:
                    details["price_history_json"] = json.dumps(details["price_history"])
                else:
                    details["price_history_json"] = None
                    
                # Remove the original list fields before database update
                del details["schools"]
                del details["price_history"]
                
                # Prepare SQL update statement dynamically based on available fields
                valid_fields = {k: v for k, v in details.items() if v is not None}
                
                if valid_fields:
                    # Construct the SQL update clause
                    set_clause = ", ".join(f"{key} = ?" for key in valid_fields.keys())
                    values = list(valid_fields.values()) + [listing_id]  # Add listing_id for WHERE clause
                    
                    # Execute update
                    c.execute(
                        f"UPDATE listings SET {set_clause} WHERE id = ?",
                        values
                    )
                    conn.commit()
                    
                    # Log the update
                    print(f"✅ Updated listing ID {listing_id} with {len(valid_fields)} fields")
                    
                    # Show some key fields
                    if details["mls_number"]:
                        print(f"   MLS#: {details['mls_number']}")
                    if details["tax_info"]:
                        print(f"   Taxes: {details['tax_info']}")
                    if details["year_built"]:
                        print(f"   Year Built: {details['year_built']}")
                else:
                    print(f"⚠️ No updates found for listing ID {listing_id}")

            except Exception as e:
                print(f"❌ Error processing listing ID {listing_id}: {e}")
                traceback.print_exc()
                # Continue with next listing instead of stopping

    except Exception as e:
        print(f"❌ Error during Compass enrichment: {e}")
        traceback.print_exc()
    finally:
        # Clean up resources
        if context:
            try:
                context.close()
            except:
                pass
        if playwright:
            try:
                playwright.stop()
            except:
                pass
        conn.close()
        
    print("🏁 Enrichment process completed.")

def main():
    parser = argparse.ArgumentParser(description="Enrich property listings with details from Compass.com")
    parser.add_argument("--headless", action="store_true", default=False, 
                        help="Run browser in headless mode")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit the number of listings to process")
    args = parser.parse_args()
    
    enrich_listings_with_compass(headless=args.headless, limit=args.limit)

if __name__ == "__main__":
    main()
