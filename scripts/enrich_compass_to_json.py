#!/usr/bin/env python3
"""
Compass Listing Enricher with JSON Output

This script fetches additional details for property listings from Compass.com
and saves the enriched information to a JSON file for review before database updates.
Uses Playwright's persistent context for authentication.

Usage:
    python enrich_compass_to_json.py [--headless] [--limit LIMIT] [--output OUTPUT]

Options:
    --headless           Run browser in headless mode (default: False)
    --limit LIMIT        Limit the number of listings to process (default: all)
    --output OUTPUT      Output JSON file (default: enriched_listings_{timestamp}.json)
    --update-db FILE     Update database with data from specified JSON file
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
from urllib.parse import urlparse, parse_qs

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"
AUTH_STORAGE_PATH = ROOT / ".auth" / "compass"
OUTPUT_DIR = ROOT / "data" / "enriched"

def setup_directories():
    """Ensure all necessary directories exist"""
    AUTH_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def inspect_database_schema():
    """
    Inspects the database schema and returns information about the listings table
    
    Returns:
        dict: Information about the listings table schema
    """
    print(f"Inspecting database schema at: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        
        # Check if listings table exists
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='listings'")
        if not c.fetchone():
            print("❌ Error: 'listings' table does not exist in the database")
            return None
            
        # Get all columns from the listings table
        c.execute("PRAGMA table_info(listings)")
        columns = [{"name": row[1], "type": row[2], "notnull": row[3], "pk": row[5]} for row in c.fetchall()]
        
        # Get a sample row to see data format
        c.execute("SELECT * FROM listings LIMIT 1")
        sample = c.fetchone()
        
        # Get total number of rows
        c.execute("SELECT COUNT(*) FROM listings")
        total_rows = c.fetchone()[0]
        
        # Get enrichment columns that exist in the table
        column_names = [col["name"] for col in columns]
        enrichment_columns = [
            col for col in ["mls_number", "tax_information", "mls_type", "year_built", "square_feet"]
            if col in column_names
        ]
        
        # Get count of rows needing enrichment
        if enrichment_columns:
            where_conditions = [f"{col} IS NULL" for col in enrichment_columns]
            where_clause = " OR ".join(where_conditions)
            c.execute(f"SELECT COUNT(*) FROM listings WHERE ({where_clause})")
            need_enrichment = c.fetchone()[0]
        else:
            need_enrichment = 0
        
        schema_info = {
            "columns": columns,
            "total_rows": total_rows,
            "need_enrichment": need_enrichment,
            "column_names": column_names,
            "enrichment_columns": enrichment_columns
        }
        
        # Log some useful information
        print(f"📊 Database summary:")
        print(f"   - Total rows: {total_rows}")
        print(f"   - Rows needing enrichment: {need_enrichment}")
        print(f"   - Columns: {', '.join(col['name'] for col in columns)}")
        
        return schema_info
    finally:
        conn.close()

def authenticate_browser(page):
    """Check and handle authentication if needed"""
    page.goto("https://www.compass.com/")
    if "login" in page.url:
        print("⚠️ Not authenticated. Please log in in the browser window...")
        page.wait_for_url("https://www.compass.com/**", timeout=0)  # Wait indefinitely for successful login
        print("✅ Authentication successful!")
    else:
        print("✅ Using saved authentication")

def clean_mls_type(mls_type):
    """Convert MLS type to simplified format"""
    if not mls_type or mls_type == "-":
        return None
        
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

def clean_square_feet(sq_ft):
    """Extract and clean square footage value"""
    if not sq_ft or sq_ft == "-":
        return None
        
    # Extract numeric value
    match = re.search(r'([\d,]+)', sq_ft)
    if match:
        return int(match.group(1).replace(',', ''))
    return None

def clean_year_built(year):
    """Clean year built value"""
    if not year or year == "-":
        return None
        
    # Extract year as 4-digit number
    match = re.search(r'(\d{4})', year)
    if match:
        return int(match.group(1))
    return None

def process_workspace_url(page, url):
    """
    Process a Compass workspace URL to extract listing data
    
    Args:
        page: Playwright page
        url: Original workspace URL
        
    Returns:
        dict: Extracted listing details
    """
    details = {}
    
    try:
        # First check if we're on a collection page or a specific listing page
        if "Collection" in page.title() or page.locator("text=Request a tour").count() > 1:
            print("📊 Detected collection page, looking for specific listing...")
            
            # Find the listing element that matches our address
            listing_links = page.locator('a:has-text("Request a tour")').all()
            
            if listing_links:
                # Click on the first listing to open it
                print(f"🔍 Found {len(listing_links)} listings, opening the first one")
                listing_links[0].click()
                
                # Wait for the details panel to load
                page.wait_for_selector('text=Request a tour', timeout=10000)
                time.sleep(3)  # Give extra time for content to load
        
        # Wait for the page to be fully loaded
        page.wait_for_load_state("networkidle", timeout=30000)
        time.sleep(2)  # Give extra time for dynamic content
        
        # Debug: Print page title and URL
        print(f"📄 Page title: {page.title()}")
        print(f"🔗 Current URL: {page.url}")
        
        # Debug: Try to find any text elements
        try:
            text_elements = page.locator('div, span, p').all()
            print(f"Found {len(text_elements)} text elements")
            for elem in text_elements[:10]:  # Print first 10 elements
                try:
                    text = elem.inner_text().strip()
                    if text:
                        print(f"Text element: {text[:100]}")  # Print first 100 chars
                except Exception:
                    continue
        except Exception as e:
            print(f"Error getting text elements: {str(e)}")
        
        # Try to find the listing details in various ways
        try:
            # First try to find the iframe element
            iframe_element = page.locator("iframe[title='Listing page']").first
            if iframe_element and iframe_element.count() > 0:
                print("🏠 Found listing iframe, extracting data...")
                # Get the frame from the iframe element
                frame = iframe_element.content_frame()
                if frame:
                    details = extract_listing_details_from_table(frame)
                else:
                    print("⚠️ Could not access iframe content")
            else:
                print("⚠️ No iframe found with title 'Listing page'")
                
                # Debug: Try to find any iframes
                iframes = page.locator('iframe').all()
                print(f"Found {len(iframes)} iframes")
                for iframe in iframes:
                    try:
                        title = iframe.get_attribute('title')
                        src = iframe.get_attribute('src')
                        print(f"iframe - title: {title}, src: {src}")
                    except Exception:
                        continue
        except Exception as e:
            print(f"⚠️ Error accessing iframe: {str(e)}")
        
        # If we couldn't get details from iframe, try the main page
        if not details:
            print("🔍 Looking for listing details in the main page...")
            
            # Debug: Try to find elements with specific classes
            try:
                class_elements = page.locator('[class*="property"], [class*="detail"], [class*="listing"]').all()
                print(f"Found {len(class_elements)} elements with property/detail/listing classes")
                for elem in class_elements[:10]:  # Print first 10 elements
                    try:
                        text = elem.inner_text().strip()
                        classes = elem.get_attribute('class')
                        if text:
                            print(f"Class element: {classes} - Text: {text[:100]}")
                    except Exception:
                        continue
            except Exception as e:
                print(f"Error getting class elements: {str(e)}")
            
            # Try different selectors for MLS number
            mls_selectors = [
                '[class*="propertyDetails"] div:has-text("MLS")',
                '[class*="details"] div:has-text("MLS")',
                '[class*="info"] div:has-text("MLS")',
                'div:has-text("MLS"):not(:has(div))',
                'span:has-text("MLS")',
                'div:has-text("MLS")',
                'div >> text=MLS'
            ]
            
            for selector in mls_selectors:
                try:
                    mls_elements = page.locator(selector).all()
                    print(f"Trying MLS selector: {selector} - Found {len(mls_elements)} elements")
                    for mls_element in mls_elements:
                        mls_text = mls_element.inner_text().strip()
                        if mls_text and mls_text != "-":
                            print(f"Found MLS text: {mls_text}")
                            # Extract just the MLS number using regex
                            match = re.search(r'(?:MLS\s*#?:?\s*)?([A-Z0-9]+)', mls_text)
                            if match:
                                details["mls_number"] = match.group(1)
                                print(f"✅ Found MLS #: {details['mls_number']}")
                                break
                    if "mls_number" in details:
                        break
                except Exception as e:
                    print(f"⚠️ Error with MLS selector {selector}: {str(e)}")
            
            # Try different selectors for Days on Market/Compass
            days_selectors = [
                '[class*="propertyDetails"] div:has-text("Days")',
                '[class*="details"] div:has-text("Days")',
                '[class*="info"] div:has-text("Days")',
                'div:has-text("Days"):not(:has(div))',
                'span:has-text("Days")',
                'div:has-text("Days on")',
                'div >> text="Days on"'
            ]
            
            for selector in days_selectors:
                try:
                    days_elements = page.locator(selector).all()
                    print(f"Trying Days selector: {selector} - Found {len(days_elements)} elements")
                    for days_element in days_elements:
                        days_text = days_element.inner_text().strip()
                        if days_text:
                            print(f"Found Days text: {days_text}")
                            # Extract just the number
                            match = re.search(r'\d+', days_text)
                            if match:
                                details["days_on_compass"] = int(match.group(0))
                                print(f"✅ Found Days on Market: {details['days_on_compass']}")
                                break
                    if "days_on_compass" in details:
                        break
                except Exception as e:
                    print(f"⚠️ Error with Days selector {selector}: {str(e)}")
            
            # Try different selectors for favorite status
            favorite_selectors = [
                'button[aria-label*="favorite"]',
                'button[class*="favorite"]',
                'button:has-text("Saved")',
                'button:has-text("Save")',
                'div[role="button"][aria-label*="favorite"]',
                'div[class*="favorite"]',
                'i[class*="favorite"]',
                'svg[data-testid*="favorite"]',
                'div[class*="save"]',
                '[aria-label*="favorite"]',
                '[class*="favorite"]'
            ]
            
            for selector in favorite_selectors:
                try:
                    favorite_elements = page.locator(selector).all()
                    print(f"Trying Favorite selector: {selector} - Found {len(favorite_elements)} elements")
                    for favorite_element in favorite_elements:
                        try:
                            classes = favorite_element.get_attribute('class')
                            aria_label = favorite_element.get_attribute('aria-label')
                            text = favorite_element.inner_text().strip()
                            print(f"Found favorite element - class: {classes}, aria-label: {aria_label}, text: {text}")
                            
                            # Check various ways to determine if favorited
                            is_favorite = (
                                "favorited" in (classes or "").lower() or
                                favorite_element.get_attribute("aria-pressed") == "true" or
                                "active" in (classes or "").lower() or
                                text == "Saved" or
                                "saved" in (classes or "").lower()
                            )
                            details["favorite"] = is_favorite
                            print(f"✅ Found Favorite status: {is_favorite}")
                            break
                        except Exception:
                            continue
                    if "favorite" in details:
                        break
                except Exception as e:
                    print(f"⚠️ Error with Favorite selector {selector}: {str(e)}")
    
    except Exception as e:
        print(f"❌ Error processing workspace URL: {str(e)}")
        traceback.print_exc()
    
    return details

def extract_listing_details_from_table(page_or_frame):
    """
    Extract listing details from a table structure
    
    Args:
        page_or_frame: Playwright page or frame
        
    Returns:
        dict: Extracted listing details
    """
    details = {}
    
    try:
        # Get all table rows
        rows = page_or_frame.locator('tr, .listingDetail, .propertyDetail').all()
        
        # Field mapping
        field_map = {
            "MLS #": "mls_number",
            "MLS": "mls_number",
            "Days on Compass": "days_on_compass",
            "Days on Market": "days_on_compass",
            "Taxes": "tax_information",
            "MLS Type": "mls_type",
            "Year Built": "year_built",
            "Lot Size": "lot_size",
            "Status": "status",
            "HOA Fees": "hoa_fee"
        }
        
        # Extract data from each row
        for row in rows:
            try:
                # Get the text content
                text = row.inner_text()
                
                # Skip empty rows
                if not text.strip():
                    continue
                
                # Check for table rows with th/td
                header_cell = row.locator('th, .label').first
                value_cell = row.locator('td, .value').first
                
                if header_cell and value_cell:
                    field = header_cell.inner_text().strip()
                    value = value_cell.inner_text().strip()
                    
                    # Map field to our database column
                    if field in field_map:
                        column = field_map[field]
                        if value and value != "-":
                            # Process specific field types
                            if column == "mls_type":
                                details[column] = clean_mls_type(value)
                            elif column == "tax_information":
                                details[column] = clean_tax_information(value)
                            elif column == "year_built":
                                details[column] = clean_year_built(value)
                            elif column == "days_on_compass":
                                # Extract just the number from days on market
                                match = re.search(r'\d+', value)
                                if match:
                                    details[column] = int(match.group(0))
                            else:
                                details[column] = value
                            print(f"✅ Extracted {column}: {value}")
                
                # Also check for label/value in the text
                for field, column in field_map.items():
                    if field in text:
                        parts = text.split(field, 1)
                        if len(parts) > 1:
                            # Extract the value after the field name
                            value_text = parts[1].strip()
                            if ":" in value_text[:5]:  # If there's a colon right after
                                value_text = value_text.split(":", 1)[1].strip()
                            
                            # Take the first line only
                            if "\n" in value_text:
                                value_text = value_text.split("\n")[0].strip()
                            
                            if value_text and value_text != "-" and column not in details:
                                if column == "mls_type":
                                    details[column] = clean_mls_type(value_text)
                                elif column == "tax_information":
                                    details[column] = clean_tax_information(value_text)
                                elif column == "year_built":
                                    details[column] = clean_year_built(value_text)
                                elif column == "days_on_compass":
                                    # Extract just the number from days on market
                                    match = re.search(r'\d+', value_text)
                                    if match:
                                        details[column] = int(match.group(0))
                                else:
                                    details[column] = value_text
                                print(f"✅ Extracted {column} from text: {value_text}")
            except Exception as e:
                print(f"⚠️ Error processing row: {str(e)}")
                continue
        
        # Look for "Updated" text
        updated_text = page_or_frame.locator('text=/LISTING UPDATED: .*/i, text=/Updated: .*/i').first
        if updated_text:
            text = updated_text.inner_text()
            if ":" in text:
                date_part = text.split(":", 1)[1].strip()
                details["last_updated"] = date_part
                print(f"✅ Extracted last_updated: {date_part}")
        
        # Enhanced extraction for favorite status
        favorite_selectors = [
            'button.favorite', 
            'button[aria-label*="favorite"]',
            'span[data-testid="favorite-button"]',
            'div[role="button"][aria-label*="favorite"]',
            'div.favorite',
            'i.favorite-icon',
            'svg[data-testid="favorite-icon"]',
            'button:has-text("Saved")',
            'button:has-text("Save")'
        ]

        for selector in favorite_selectors:
            favorite_button = page_or_frame.locator(selector).first
            if favorite_button and favorite_button.count() > 0:
                print(f"✅ Found Favorite element with selector: {selector}")
                # Check for various indicators of favorited status
                is_favorite = (
                    "favorited" in favorite_button.get_attribute("class", "") or
                    "selected" in favorite_button.get_attribute("class", "") or
                    favorite_button.get_attribute("aria-pressed") == "true" or
                    "active" in favorite_button.get_attribute("class", "") or
                    favorite_button.inner_text().strip() == "Saved"
                )
                details["favorite"] = is_favorite
                print(f"  → Extracted favorite status: {is_favorite}")
                break
    
    except Exception as e:
        print(f"❌ Error extracting from table: {str(e)}")
        traceback.print_exc()
    
    return details

def extract_listing_details(page_or_frame):
    """
    Extract listing details from a page or frame
    
    Args:
        page_or_frame: Playwright page or frame
        
    Returns:
        dict: Extracted listing details
    """
    # This is a wrapper function that calls extract_listing_details_from_table
    # In case we need to add more extraction methods in the future
    return extract_listing_details_from_table(page_or_frame)

def enrich_listings_with_compass(output_file=None, max_listings=None, headless=False):
    """
    Main function to enrich listings and save to JSON
    
    Args:
        output_file (str): Path to output JSON file
        max_listings (int): Maximum number of listings to process
        headless (bool): Whether to run browser in headless mode
    """
    # Ensure directories exist
    setup_directories()
    
    # First, inspect the database schema
    schema_info = inspect_database_schema()
    if not schema_info:
        print("❌ Cannot continue without valid database schema")
        return
        
    # Check if we have the necessary columns to work with
    if not schema_info["enrichment_columns"]:
        print("❌ No enrichment columns found in database. Please ensure your database has at least one of: " +
              "mls_number, tax_information, mls_type, year_built, square_feet")
        return
    
    # Create default output filename if none provided
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = OUTPUT_DIR / f"enriched_listings_{timestamp}.json"
    else:
        output_file = Path(output_file)
    
    # Fetch listings needing enrichment
    column_names = schema_info["column_names"]
    enrichment_columns = schema_info["enrichment_columns"]
    
    # Construct query to fetch listings needing enrichment
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        
        # Start with essential columns
        select_columns = ["id", "url"]
        
        # Add optional columns if they exist
        optional_columns = ["address", "price", "city", "state", "zip"]
        select_columns.extend([col for col in optional_columns if col in column_names])
        
        # Build WHERE clause for enrichment columns
        where_conditions = [f"{col} IS NULL" for col in enrichment_columns]
        where_clause = " OR ".join(where_conditions)
        
        # Build complete query
        query = f"SELECT {', '.join(select_columns)} FROM listings WHERE ({where_clause})"
        if max_listings:
            query += f" LIMIT {max_listings}"
            
        print(f"Executing query: {query}")
        c.execute(query)
        
        # Convert to list of dictionaries
        result_columns = [column[0] for column in c.description]
        listings = [dict(zip(result_columns, row)) for row in c.fetchall()]
        
        print(f"Found {len(listings)} listings needing enrichment")
        
        if not listings:
            print("✅ No listings need enrichment. Database is up to date.")
            return
    finally:
        conn.close()
    
    # Store enriched data
    enriched_data = []
    
    with sync_playwright() as p:
        # Set up persistent context with saved authentication
        print("🌐 Launching browser...")
        browser_args = []
        
        # Create a debug directory for screenshots
        debug_dir = OUTPUT_DIR / "debug"
        debug_dir.mkdir(exist_ok=True)
        
        # Add arguments to enable debugging if needed
        if not headless:
            browser_args.extend([
                "--window-size=1280,1024",
                "--auto-open-devtools-for-tabs"
            ])
        
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(AUTH_STORAGE_PATH),
            headless=headless,
            args=browser_args,
            ignore_https_errors=True,
            bypass_csp=True,
            viewport={"width": 1280, "height": 1024}
        )

        page = context.pages[0]  # use the first (blank) page
        
        # Authenticate if needed
        authenticate_browser(page)
        
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
                
                # Wait for page to load with exponential backoff
                backoff = 3
                for attempt in range(3):
                    try:
                        # Wait for some element that indicates the page is loaded
                        page.wait_for_load_state("networkidle", timeout=10000)
                        break
                    except Exception:
                        if attempt < 2:  # Don't sleep after the last attempt
                            print(f"⏳ Page loading slowly, waiting {backoff}s...")
                            time.sleep(backoff)
                            backoff *= 2
                
                # Take a screenshot for debugging
                try:
                    screenshot_path = debug_dir / f"listing_{listing_id}.png"
                    page.screenshot(path=str(screenshot_path))
                    print(f"📸 Saved screenshot to {screenshot_path}")
                except Exception as e:
                    print(f"⚠️ Could not save screenshot: {str(e)}")
                
                # Check if we're on a workspace page or direct listing page
                if "workspace" in page.url:
                    print("📝 Detected workspace URL")
                    details = process_workspace_url(page, url)
                else:
                    print("🏠 Detected direct listing URL")
                    # In case of direct URL, try to extract listing details directly
                    details = extract_listing_details_from_table(page)
                
                # Update the enriched listing with extracted details
                if details:
                    enriched_listing.update(details)
                    print(f"✅ Updated listing with extracted details: {', '.join(details.keys())}")
                else:
                    print("⚠️ Could not extract any details from the page")
                
                # Add to enriched data
                enriched_data.append(enriched_listing)
                
                # Print summary of what was enriched
                fields_extracted = [k for k, v in details.items() if v is not None]
                if fields_extracted:
                    print(f"💾 Saved details for listing ID {listing_id}: {', '.join(fields_extracted)}")
                else:
                    print(f"💾 Saved listing ID {listing_id} with no new details")
                
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
        # Make sure no extra characters are added
        f.flush()
    
    # Validate the JSON file
    try:
        with open(output_file, 'r') as f:
            json.load(f)  # This will raise an exception if JSON is invalid
        print(f"✅ Successfully wrote valid JSON to {output_file}")
    except json.JSONDecodeError as e:
        print(f"⚠️ Warning: Written JSON file may not be valid: {e}")
    
    print(f"🏁 Enrichment process completed. Saved {len(enriched_data)} listings to {output_file}")
    
    # Print summary
    error_count = sum(1 for item in enriched_data if 'error' in item)
    success_count = len(enriched_data) - error_count
    print(f"📊 Summary: {success_count} successful, {error_count} failed")

def update_database_from_json(json_file):
    """
    Update the database with data from the JSON file
    
    Args:
        json_file (str): Path to JSON file with enriched data
    """
    # First, inspect the database schema
    schema_info = inspect_database_schema()
    if not schema_info:
        print("❌ Cannot continue without valid database schema")
        return
        
    # Get column names from the database
    column_names = schema_info["column_names"]
    
    print(f"Loading enriched data from {json_file}")
    with open(json_file, 'r') as f:
        enriched_data = json.load(f)
    
    print(f"Found {len(enriched_data)} listings in JSON file")
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Fields we want to update in the database
    updateable_fields = [
        "mls_number", "mls_type", "tax_information", 
        "square_feet", "year_built", "lot_size",
        "hoa_dues", "property_type", "status"
    ]
    
    # Filter to fields that actually exist in the database
    updateable_fields = [field for field in updateable_fields if field in column_names]
    
    if not updateable_fields:
        print("❌ Error: None of the enrichment fields exist in the database. Cannot update.")
        conn.close()
        return
    
    print(f"Fields available for update: {', '.join(updateable_fields)}")
    
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
        
        # Extract fields that exist in the database and have values
        valid_fields = {k: v for k, v in listing.items() 
                      if k in column_names and k in updateable_fields and v is not None}
        
        if valid_fields:
            # Build the update query
            set_clause = ", ".join(f"{key} = ?" for key in valid_fields.keys())
            values = list(valid_fields.values()) + [listing_id]
            
            print(f"✏️ Updating listing ID {listing_id} with fields: {', '.join(valid_fields.keys())}")
            c.execute(
                f"UPDATE listings SET {set_clause} WHERE id = ?",
                values
            )
            updated_count += 1
        else:
            print(f"⚠️ No valid fields to update for listing ID {listing_id}")
            skipped_count += 1
    
    # Commit changes
    conn.commit()
    conn.close()
    
    print(f"🏁 Database update completed: {updated_count} listings updated, {skipped_count} skipped")

def main():
    parser = argparse.ArgumentParser(description="Enrich Compass listings and save to JSON")
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--limit', type=int, help='Maximum number of listings to process')
    parser.add_argument('--output', help='Output JSON file path')
    parser.add_argument('--update-db', help='Update database with data from JSON file')
    parser.add_argument('--inspect', action='store_true', help='Just inspect database schema and exit')
    
    args = parser.parse_args()
    
    if args.inspect:
        # Just inspect the database and exit
        inspect_database_schema()
    elif args.update_db:
        # Run database update mode
        update_database_from_json(args.update_db)
    else:
        # Run scraping mode
        enrich_listings_with_compass(
            output_file=args.output,
            max_listings=args.limit,
            headless=args.headless
        )

if __name__ == "__main__":
    main()
