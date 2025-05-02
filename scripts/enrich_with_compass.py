#!/usr/bin/env python3
"""
Compass Listing Enricher with JSON Output

This script fetches additional details for property listings from Compass.com
and saves the enriched information to a JSON file for review before database updates.
Uses Playwright's persistent context for authentication.

Usage:
    python enrich_compass_to_json_new.py [--headless] [--limit LIMIT] [--output OUTPUT] [--update-db] [--address ADDRESS]

Options:
    --headless           Run browser in headless mode (default: False)
    --limit LIMIT        Limit the number of listings to process (default: all)
    --output OUTPUT      Output JSON file (default: enriched_listings_{timestamp}.json)
    --update-db          Update the database with enriched data (default: False)
    --address ADDRESS    Process a specific address
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"
AUTH_STORAGE_PATH = ROOT / ".auth" / "compass"
OUTPUT_DIR = ROOT / "data" / "enriched"

def setup_directories():
    """Ensure all necessary directories exist"""
    AUTH_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def fetch_listings_needing_enrichment(max_listings=None, specific_address=None):
    """Fetch listings that need enrichment"""
    print(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        
        if specific_address:
            # Query for a specific address
            query = """
                SELECT id, url, address, price, city, state, zip, sqft 
                FROM listings 
                WHERE address = ?
            """
            print(f"Executing query for specific address: {specific_address}")
            c.execute(query, (specific_address,))
        else:
            # Query for listings missing any of our target fields
            query = """
                SELECT id, url, address, price, city, state, zip, sqft 
                FROM listings 
                WHERE mls_number IS NULL 
                   OR mls_type IS NULL 
                   OR tax_information IS NULL
                   OR days_on_compass IS NULL 
                   OR favorite IS NULL 
                   OR last_updated IS NULL
                   OR status IS NULL
            """
            if max_listings:
                query += f" LIMIT {max_listings}"
                
            print(f"Executing query: {query}")
            c.execute(query)
            
        results = c.fetchall()
        
        # Convert to list of dictionaries
        columns = [column[0] for column in c.description]
        listings = [dict(zip(columns, row)) for row in results]
        
        print(f"Found {len(listings)} listings needing enrichment")
        return listings
    finally:
        conn.close()

def process_workspace_url(page, url):
    """Process a workspace URL to get to the listing page"""
    try:
        # First check if we have a listing ID in the URL
        match = re.search(r'/listing/(\d+)', url)
        if match:
            listing_id = match.group(1)
            # Construct the public listing URL
            public_url = f"https://www.compass.com/listing/{listing_id}"
            print(f"📝 Redirecting to public listing URL: {public_url}")
            page.goto(public_url)
            page.wait_for_load_state("networkidle")
            return
            
        # Wait for the page to be fully loaded
        page.wait_for_load_state("networkidle", timeout=30000)
        time.sleep(2)  # Give extra time for dynamic content
        
        # Try to find and click the "View Listing" button
        view_listing_selectors = [
            "button:has-text('View Listing')",
            "a:has-text('View Listing')",
            "button:has-text('View Full Listing')",
            "a:has-text('View Full Listing')"
        ]
        
        for selector in view_listing_selectors:
            try:
                element = page.locator(selector).first
                if element and element.count() > 0:
                    print(f"Found View Listing button with selector: {selector}")
                    element.click()
                    page.wait_for_load_state("networkidle")
                    return
            except Exception as e:
                print(f"Error with selector {selector}: {str(e)}")
        
        print("⚠️ Could not find View Listing button")
        return
    except Exception as e:
        print(f"❌ Error processing workspace URL: {str(e)}")
        return

def clean_tax_information(tax_info):
    """Extract and format tax information as currency string."""
    if not tax_info or tax_info == "-":
        return None
        
    # Extract the number after $ and before / or end of string
    match = re.search(r'\$([\d,]+)(?:\s*\/.*)?', tax_info)
    if match:
        # Get the number and remove commas
        amount = int(match.group(1).replace(',', ''))
        # Format as currency string with single $
        return "${:,}".format(amount)
    
    # Try another pattern if the first one fails
    match = re.search(r'([\d,]+)', tax_info)
    if match:
        # Get the number and remove commas
        amount = int(match.group(1).replace(',', ''))
        # Format as currency string with single $
        return "${:,}".format(amount)
    
    return None

def clean_mls_type(mls_type):
    """Clean MLS type to be either 'Attached' or 'Detached'"""
    if not mls_type or mls_type == "-":
        return None
        
    # Remove 'Residential-' prefix if present
    mls_type = mls_type.replace("Residential-", "")
    
    # Convert to proper format
    if "Attached" in mls_type:
        return "Attached"
    elif "Detached" in mls_type:
        return "Detached"
    elif mls_type == "Residential":
        return "Detached"  # Default to Detached if just "Residential"
    else:
        return None

def clean_price_per_sqft(price_per_sqft):
    """Clean price per square foot value"""
    if not price_per_sqft or price_per_sqft == "-":
        return None
        
    # Extract numeric value and remove $ and commas
    match = re.search(r'\$?([\d,]+)', price_per_sqft)
    if match:
        return int(match.group(1).replace(',', ''))
    return None

def extract_listing_details(page, listing_id):
    """Extract listing details from the page"""
    details = {}
    
    try:
        # Check if we're on a workspace page
        if "workspace" in page.url:
            print("📝 Detected workspace URL")
            if not process_workspace_url(page, page.url):
                details['error'] = "Could not access listing from workspace"
                return details
        
        # Check for Private Exclusive listing
        try:
            # Look for Private Exclusive in specific locations
            private_exclusive_selectors = [
                "div[class*='listing-badge']:has-text('Private Exclusive')",
                "div[class*='status-badge']:has-text('Private Exclusive')",
                "div[class*='listing-status']:has-text('Private Exclusive')"
            ]
            
            for selector in private_exclusive_selectors:
                element = page.locator(selector).first
                if element and element.count() > 0:
                    text = element.inner_text().strip().lower()
                    if "private exclusive" in text:
                        print("⚠️ Skipping Private Exclusive listing - data not available")
                        details['error'] = "Private Exclusive listing - data not available"
                        return details
        except Exception:
            pass  # Continue if we can't check for Private Exclusive
        
        # Wait for the iframe containing listing details
        iframe = page.frame_locator("iframe[title='Listing page']").first
        
        # Extract MLS Number (from iframe)
        try:
            mls_number_text = iframe.locator("tr:has(th:has-text('MLS #')) td").first.inner_text()
            if mls_number_text and mls_number_text != "-":
                details['mls_number'] = mls_number_text
                print(f"Found MLS #: {mls_number_text}")
        except Exception:
            print("⚠️ MLS # not found")
        
        # Extract MLS Type (from iframe)
        try:
            mls_type_text = iframe.locator("tr:has(th:has-text('MLS Type')) td").first.inner_text()
            if mls_type_text and mls_type_text != "-":
                cleaned_type = clean_mls_type(mls_type_text)
                if cleaned_type:
                    details['mls_type'] = cleaned_type
                    print(f"Found MLS Type: {mls_type_text} (cleaned to: {cleaned_type})")
        except Exception:
            print("⚠️ MLS Type not found")
        
        # Extract days on compass (from iframe)
        try:
            days_text = iframe.locator("tr:has(th:has-text('Days on Compass')) td").first.inner_text()
            # Handle special cases for fresh listings
            if any(phrase in days_text.lower() for phrase in ["listed today", "new today", "just listed", "new listing"]):
                details['days_on_compass'] = 0
                print("Found fresh listing - setting days_on_compass to 0")
            else:
                match = re.search(r'(\d+)', days_text)
                if match:
                    details['days_on_compass'] = int(match.group(1))
        except Exception:
            print("⚠️ Days on Compass not found")
        
        # Extract favorite status (try both iframe and main page)
        try:
            # Wait for any favorite-related elements to be loaded
            page.wait_for_selector("button[class*='favorite'], [class*='saved'], [aria-label*='favorite']", timeout=10000)
            
            # First check for property-specific favorite button
            favorite_button_selectors = [
                "button[class*='footer-favoriteBtn']",
                "button[class*='favorite'][class*='circle']",
                "button[class*='favorite'][class*='heart']",
                "button[aria-label*='favorite'][class*='circle']",
                "button[aria-label*='favorite'][class*='heart']"
            ]
            
            is_favorite = False
            
            # Try main page first
            for selector in favorite_button_selectors:
                try:
                    element = page.locator(selector).first
                    if element and element.count() > 0:
                        # Get all possible attributes
                        class_attr = element.get_attribute("class") or ""
                        aria_label = element.get_attribute("aria-label") or ""
                        data_testid = element.get_attribute("data-testid") or ""
                        text = element.inner_text().strip()
                        
                        print(f"Found favorite button in main page - selector: {selector}")
                        print(f"Attributes - class: {class_attr}, aria-label: {aria_label}, data-testid: {data_testid}, text: {text}")
                        
                        # Check various indicators
                        if any([
                            "saved" in text.lower(),
                            "saved" in class_attr.lower(),
                            "saved" in aria_label.lower(),
                            "saved" in data_testid.lower(),
                            "favorited" in class_attr.lower(),
                            "remove from favorites" in aria_label.lower(),
                            "active" in class_attr.lower()
                        ]):
                            is_favorite = True
                            print(f"Found saved state in main page with selector: {selector}")
                            break
                except Exception as e:
                    print(f"Error checking selector {selector} in main page: {str(e)}")
            
            # If not found in main page, try iframe
            if not is_favorite:
                for selector in favorite_button_selectors:
                    try:
                        element = iframe.locator(selector).first
                        if element and element.count() > 0:
                            # Get all possible attributes
                            class_attr = element.get_attribute("class") or ""
                            aria_label = element.get_attribute("aria-label") or ""
                            data_testid = element.get_attribute("data-testid") or ""
                            text = element.inner_text().strip()
                            
                            print(f"Found favorite button in iframe - selector: {selector}")
                            print(f"Attributes - class: {class_attr}, aria-label: {aria_label}, data_testid: {data_testid}, text: {text}")
                            
                            # Check various indicators
                            if any([
                                "saved" in text.lower(),
                                "saved" in class_attr.lower(),
                                "saved" in aria_label.lower(),
                                "saved" in data_testid.lower(),
                                "favorited" in class_attr.lower(),
                                "remove from favorites" in aria_label.lower(),
                                "active" in class_attr.lower()
                            ]):
                                is_favorite = True
                                print(f"Found saved state in iframe with selector: {selector}")
                                break
                    except Exception as e:
                        print(f"Error checking selector {selector} in iframe: {str(e)}")
            
            details['favorite'] = is_favorite
            print(f"Final favorite status determined: {is_favorite}")
            
        except Exception as e:
            print(f"⚠️ Error getting favorite status: {str(e)}")
            traceback.print_exc()
        
        # Extract last updated date (try both iframe and main page)
        try:
            # Try different selectors in both iframe and main page
            selectors = [
                "text=/LISTING UPDATED: .*/i",
                "text=/Updated: .*/i",
                "text=/Last Updated: .*/i",
                "div:has-text('LISTING UPDATED')",
                "div:has-text('Updated')",
                "div:has-text('Last Updated')",
                "span:has-text('LISTING UPDATED')",
                "span:has-text('Updated')",
                "span:has-text('Last Updated')"
            ]
            
            updated_text = None
            for selector in selectors:
                # Try iframe first
                element = iframe.locator(selector).first
                if element and element.count() > 0:
                    updated_text = element
                    print(f"Found update text in iframe with selector: {selector}")
                    break
                    
                # Try main page
                element = page.locator(selector).first
                if element and element.count() > 0:
                    updated_text = element
                    print(f"Found update text in main page with selector: {selector}")
                    break
            
            if updated_text:
                text = updated_text.inner_text()
                print(f"Found update text: {text}")
                if ":" in text:
                    date_part = text.split(":", 1)[1].strip()
                    details['last_updated'] = date_part
            else:
                print("⚠️ Last updated text not found with any selector")
        except Exception as e:
            print(f"⚠️ Error getting last updated date: {str(e)}")
            traceback.print_exc()
            
        # Extract status (try both iframe and main page)
        try:
            # Try different selectors for status
            status_selectors = [
                "tr:has(th:has-text('Status')) td",
                "div:has-text('Status')",
                "span:has-text('Status')",
                "div[class*='status']",
                "span[class*='status']",
                "div:has-text('Coming Soon')",
                "div:has-text('Active')",
                "div:has-text('Pending')",
                "div:has-text('Expired')",
                "div:has-text('Sold')"
            ]
            
            status_text = None
            for selector in status_selectors:
                # Try iframe first
                element = iframe.locator(selector).first
                if element and element.count() > 0:
                    status_text = element
                    print(f"Found status in iframe with selector: {selector}")
                    break
                    
                # Try main page
                element = page.locator(selector).first
                if element and element.count() > 0:
                    status_text = element
                    print(f"Found status in main page with selector: {selector}")
                    break
            
            if status_text:
                text = status_text.inner_text().strip()
                print(f"Found status text: {text}")
                # Clean up the status text
                if ":" in text:
                    text = text.split(":", 1)[1].strip()
                details['status'] = text
            else:
                print("⚠️ Status not found with any selector")
        except Exception as e:
            print(f"⚠️ Error getting status: {str(e)}")
            traceback.print_exc()
            
        # Extract tax information (try both iframe and main page)
        try:
            # Try different selectors for tax information
            tax_selectors = [
                "tr:has(th:has-text('Tax Information')) td",
                "tr:has(th:has-text('Taxes')) td",
                "div:has-text('Tax Information')",
                "div:has-text('Taxes')",
                "span:has-text('Tax Information')",
                "span:has-text('Taxes')"
            ]
            
            tax_text = None
            for selector in tax_selectors:
                # Try iframe first
                element = iframe.locator(selector).first
                if element and element.count() > 0:
                    tax_text = element
                    print(f"Found tax info in iframe with selector: {selector}")
                    break
                    
                # Try main page
                element = page.locator(selector).first
                if element and element.count() > 0:
                    tax_text = element
                    print(f"Found tax info in main page with selector: {selector}")
                    break
            
            if tax_text:
                text = tax_text.inner_text().strip()
                print(f"Found tax info text: {text}")
                # Clean up the tax information
                cleaned_tax = clean_tax_information(text)
                if cleaned_tax:
                    details['tax_information'] = cleaned_tax
            else:
                print("⚠️ Tax information not found with any selector")
        except Exception as e:
            print(f"⚠️ Error getting tax information: {str(e)}")
            traceback.print_exc()
            
    except Exception as e:
        print(f"❌ Error extracting details: {str(e)}")
        traceback.print_exc()
    
    return details

def calculate_price_per_sqft(price, sqft):
    """Calculate price per square foot from price and square feet"""
    if not price or not sqft or price <= 0 or sqft <= 0:
        return None
    return int(price / sqft)

def update_database(enriched_data):
    """Update the database with enriched data"""
    print("Updating database with enriched data...")
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        
        # Get current table structure
        c.execute("PRAGMA table_info(listings)")
        columns = c.fetchall()
        column_names = [col[1] for col in columns]
        
        updated_count = 0
        skipped_count = 0
        
        for listing in enriched_data:
            listing_id = listing.get('id')
            if not listing_id:
                print("⚠️ Skipping entry without ID")
                skipped_count += 1
                continue
            
            # Skip entries with errors
            if 'error' in listing:
                print(f"⚠️ Skipping listing ID {listing_id} due to error during scraping")
                skipped_count += 1
                continue
            
            # Calculate price per square foot if we have both price and sqft
            price = listing.get('price')
            sqft = listing.get('sqft')
            if price and sqft:
                listing['price_per_sqft'] = calculate_price_per_sqft(price, sqft)
                if listing['price_per_sqft']:
                    print(f"Calculated price per sqft: ${listing['price_per_sqft']}/sqft")
            
            # Extract fields that exist in the database and have values
            valid_fields = {k: v for k, v in listing.items() 
                          if k in column_names and v is not None}
            
            # Add new fields if they exist
            if 'walkscore_shorturl' in listing and 'walkscore_shorturl' in column_names:
                valid_fields['walkscore_shorturl'] = listing['walkscore_shorturl']
            if 'compass_shorturl' in listing and 'compass_shorturl' in column_names:
                valid_fields['compass_shorturl'] = listing['compass_shorturl']
            
            if valid_fields:
                set_clause = ", ".join(f"{key} = ?" for key in valid_fields.keys())
                values = list(valid_fields.values()) + [listing_id]
                
                print(f"   Updating fields: {', '.join(valid_fields.keys())}")
                c.execute(
                    f"UPDATE listings SET {set_clause} WHERE id = ?",
                    values
                )
                updated_count += 1
            else:
                print(f"⚠️ No valid fields to update for listing ID {listing_id}")
                skipped_count += 1
        
        conn.commit()
        print(f"\n✅ Database update complete:")
        print(f"   Updated: {updated_count} listings")
        print(f"   Skipped: {skipped_count} listings")
        
    except Exception as e:
        print(f"❌ Error updating database: {str(e)}")
        conn.rollback()
        
    finally:
        conn.close()

def enrich_listings_with_compass(output_file=None, max_listings=None, headless=False, update_db=False, specific_address=None):
    """Main function to enrich listings and optionally update database"""
    # Ensure directories exist
    setup_directories()
    
    # Fetch listings needing enrichment
    listings = fetch_listings_needing_enrichment(max_listings, specific_address)
    if not listings:
        print("✅ No listings need enrichment. Database is up to date.")
        return
    
    # Create default output filename if none provided
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUT_DIR / f"enriched_listings_{timestamp}.json"
    else:
        output_file = Path(output_file)
    
    # Store enriched data
    enriched_data = []
    
    with sync_playwright() as p:
        # Set up persistent context with saved authentication
        print("🌐 Launching browser...")
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(AUTH_STORAGE_PATH),
            headless=headless
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

        for listing in listings:
            listing_id = listing['id']
            url = listing['url']
            
            try:
                print(f"➡️ Processing listing ID {listing_id}: {listing.get('address', url)}")
                
                # Start with existing listing data
                enriched_listing = {k: v for k, v in listing.items()}
                enriched_listing["scraped_at"] = datetime.now().isoformat()
                
                # Visit the listing page
                page.goto(url)
                time.sleep(3)  # wait for page load
                
                # Extract details
                details = extract_listing_details(page, listing_id)
                if details:
                    enriched_listing.update(details)
                    print(f"✅ Extracted details: {', '.join(details.keys())}")
                else:
                    print("⚠️ Could not extract any details")
                
                # Add to enriched data
                enriched_data.append(enriched_listing)
                
                # Random delay to avoid rate limiting
                delay = random.uniform(2, 5)
                print(f"⏳ Waiting {delay:.1f}s before next listing...")
                time.sleep(delay)

            except Exception as e:
                print(f"❌ Error processing listing ID {listing_id}: {str(e)}")
                traceback.print_exc()
                # Add the listing with error info
                enriched_data.append({
                    "id": listing_id,
                    "url": url,
                    "error": str(e),
                    "scraped_at": datetime.now().isoformat()
                })

        context.close()
    
    # Save enriched data to JSON file
    with open(output_file, 'w') as f:
        json.dump(enriched_data, f, indent=2)
        f.flush()
    
    # Validate the JSON file
    try:
        with open(output_file, 'r') as f:
            json.load(f)
        print(f"✅ Successfully wrote valid JSON to {output_file}")
    except json.JSONDecodeError as e:
        print(f"⚠️ Warning: Written JSON file may not be valid: {e}")
    
    print(f"🏁 Enrichment process completed. Saved {len(enriched_data)} listings to {output_file}")
    
    # Update database if requested
    if update_db:
        update_database(enriched_data)
    
    # Print summary
    error_count = sum(1 for item in enriched_data if 'error' in item)
    success_count = len(enriched_data) - error_count
    print(f"📊 Summary: {success_count} successful, {error_count} failed")

def main():
    parser = argparse.ArgumentParser(description="Enrich Compass listings and save to JSON")
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--limit', type=int, help='Maximum number of listings to process')
    parser.add_argument('--output', help='Output JSON file path')
    parser.add_argument('--update-db', action='store_true', help='Update database with enriched data')
    parser.add_argument('--address', help='Process a specific address')
    
    args = parser.parse_args()
    
    enrich_listings_with_compass(
        output_file=args.output,
        max_listings=args.limit,
        headless=args.headless,
        update_db=args.update_db,
        specific_address=args.address
    )

if __name__ == "__main__":
    main() 