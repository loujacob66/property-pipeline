#!/usr/bin/env python3
"""
Compass Listing Enricher with JSON Output

This script fetches additional details for property listings from Compass.com
and saves the enriched information to a JSON file for review before database updates.
Uses Playwright's persistent context for authentication.

Usage:
    python enrich_compass_to_json_new.py [--headless] [--limit LIMIT] [--output OUTPUT]

Options:
    --headless           Run browser in headless mode (default: False)
    --limit LIMIT        Limit the number of listings to process (default: all)
    --output OUTPUT      Output JSON file (default: enriched_listings_{timestamp}.json)
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

def fetch_listings_needing_enrichment(max_listings=None):
    """Fetch listings that need enrichment"""
    print(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        c = conn.cursor()
        
        # Query for listings missing any of our target fields
        query = """
            SELECT id, url, address, price, city, state, zip 
            FROM listings 
            WHERE days_on_compass IS NULL 
               OR favorite IS NULL 
               OR last_updated IS NULL
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
        # First check if we're on a collection page
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
        
    except Exception as e:
        print(f"❌ Error processing workspace URL: {str(e)}")
        traceback.print_exc()

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

def extract_listing_details(page, listing_id):
    """Extract listing details from the page"""
    details = {}
    
    try:
        # Check if we're on a workspace page
        if "workspace" in page.url:
            print("📝 Detected workspace URL")
            process_workspace_url(page, page.url)
        
        # Wait for the iframe containing listing details
        iframe = page.frame_locator("iframe[title='Listing page']").first
        
        # Extract days on compass (from iframe)
        try:
            days_text = iframe.locator("tr:has(th:has-text('Days on Compass')) td").first.inner_text()
            match = re.search(r'(\d+)', days_text)
            if match:
                details['days_on_compass'] = int(match.group(1))
        except Exception:
            print("⚠️ Days on Compass not found")
        
        # Extract favorite status (try both iframe and main page)
        try:
            # Try iframe first
            favorite_button = iframe.locator("button[aria-label*='favorite'], button[class*='favorite']").first
            if not favorite_button or favorite_button.count() == 0:
                # Try main page
                favorite_button = page.locator("button[aria-label*='favorite'], button[class*='favorite']").first
            
            if favorite_button and favorite_button.count() > 0:
                # Get class attribute
                class_attr = favorite_button.get_attribute("class") or ""
                aria_pressed = favorite_button.get_attribute("aria-pressed")
                button_text = favorite_button.inner_text().strip()
                
                is_favorite = (
                    "favorited" in class_attr or
                    "active" in class_attr or
                    aria_pressed == "true" or
                    button_text == "Saved"
                )
                details['favorite'] = is_favorite
                print(f"Found favorite button - class: {class_attr}, aria-pressed: {aria_pressed}, text: {button_text}")
            else:
                print("⚠️ Favorite button not found")
        except Exception as e:
            print(f"⚠️ Error getting favorite status: {str(e)}")
        
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

def enrich_listings_with_compass(output_file=None, max_listings=None, headless=False):
    """Main function to enrich listings and save to JSON"""
    # Ensure directories exist
    setup_directories()
    
    # Fetch listings needing enrichment
    listings = fetch_listings_needing_enrichment(max_listings)
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
    
    print(f"🏁 Enrichment process completed. Saved {len(enriched_data)} listings to {output_file}")
    
    # Print summary
    error_count = sum(1 for item in enriched_data if 'error' in item)
    success_count = len(enriched_data) - error_count
    print(f"📊 Summary: {success_count} successful, {error_count} failed")

def main():
    parser = argparse.ArgumentParser(description="Enrich Compass listings and save to JSON")
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--limit', type=int, help='Maximum number of listings to process')
    parser.add_argument('--output', help='Output JSON file path')
    
    args = parser.parse_args()
    
    enrich_listings_with_compass(
        output_file=args.output,
        max_listings=args.limit,
        headless=args.headless
    )

if __name__ == "__main__":
    main() 