#!/usr/bin/env python3
"""
Multi-Label Gmail Parser for Property Listings

This script accesses Gmail using OAuth, fetches emails from multiple configured labels,
parses them for real estate listings, and stores the data in an SQLite database.

Usage:
    python multi_label_gmail_parser.py [--dry-run] [--config CONFIG_FILE] [--max-emails MAX_EMAILS] [--credentials CREDENTIALS_PATH] [--token TOKEN_PATH]

Options:
    --dry-run           Preview extraction without database insertion
    --config            Path to the label configuration file (default: config/label_config.json)
    --max-emails MAX    Maximum number of emails to process per label (default: 10)
    --credentials PATH  Path to credentials.json file (default: project_root/credentials.json)
    --token PATH        Path to token.pickle file (default: project_root/token.pickle)
    --force             Force reprocessing of all emails
"""

import os
import sys
import re
import json
import argparse
import html
from bs4 import BeautifulSoup
import usaddress
import sqlite3
from pathlib import Path
import base64

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Define database path
DB_PATH = ROOT / "data" / "listings.db"

# Import project modules
from lib.gmail_utils import authenticate_gmail
from lib.zori_utils import load_zori_data
from lib.db_utils import insert_listings

# Email parsing functions
def parse_address_components(full_address):
    """Parse a full address into its components using usaddress library."""
    try:
        parsed = usaddress.tag(full_address)[0]
        street_parts = [
            parsed.get("AddressNumber", ""),
            parsed.get("StreetNamePreType", ""),
            parsed.get("StreetName", ""),
            parsed.get("StreetNamePostType", ""),
            parsed.get("OccupancyType", ""),
            parsed.get("OccupancyIdentifier", "")
        ]
        street = " ".join(filter(None, street_parts)).strip()
        city = parsed.get("PlaceName", "")
        state = parsed.get("StateName", "")
        zip_code = parsed.get("ZipCode", "")
        return street, city, state, zip_code
    except Exception as e:
        print(f"⚠️ Address parsing error: {e}")
        return full_address, "", "", ""

def parse_html_email(html_content):
    """Parse HTML email content to extract real estate listings."""
    soup = BeautifulSoup(html.unescape(html_content), "html.parser")
    listings = []
    
    # Try to parse as a collection email first
    collection_listings = parse_collection_format(soup)
    if collection_listings:
        for listing in collection_listings:
            listing["from_collection"] = True
        return collection_listings
    
    # Fall back to individual listing format
    individual_listings = parse_individual_format(soup)
    for listing in individual_listings:
        listing["from_collection"] = False
    
    return individual_listings

def parse_collection_format(soup):
    """Parse emails with a collection of listings (multiple properties)."""
    listings = []
    
    for row in soup.find_all("tr", class_="listingComponentV2"):
        a_tag = row.find("a", href=True)
        href = a_tag["href"] if a_tag else None
        
        # Extract base Compass URL without workspace parameters
        if href and "compass.com/listing" in href:
            # Extract just the base URL up to the listing ID
            url_parts = href.split("?")[0]
            href = url_parts
        
        next_tr = row.find_next_sibling("tr")
        if not next_tr:
            continue
            
        full_address, price, beds, baths, sqft = None, None, None, None, None
        city, state, zip_code = None, None, None
        tax_information, mls_type, mls_number = None, None, None
        days_on_compass, last_updated = None, None
        
        for div in next_tr.find_all("div"):
            text = div.get_text(" ", strip=True)
            
            if not full_address:
                a = div.find("a")
                if a and "," in a.get_text(strip=True):
                    full_address = a.get_text(strip=True)
                    street_address, city, state, zip_code = parse_address_components(full_address)
                    
            if "$" in text and not price:
                m = re.search(r"\$[\d,]+", text)
                if m:
                    price = int(m.group(0).replace("$", "").replace(",", ""))
                    
            # More specific matching for beds
            if "BD" in text and not beds:
                m = re.search(r"(\d+(?:\.\d+)?)\s*BD", text)
                if m:
                    beds = int(round(float(m.group(1))))
            # More specific matching for baths
            if "BA" in text and not baths:
                m = re.search(r"(\d+(?:\.\d+)?)\s*BA", text)
                if m:
                    baths = int(round(float(m.group(1))))
            # More specific matching for sqft
            if "Sq.Ft." in text and not sqft:
                m = re.search(r"([\d,]+)\s*Sq\.Ft\.", text)
                if m:
                    sqft = int(m.group(1).replace(",", ""))
            
            # Extract MLS number
            if "MLS#" in text and not mls_number:
                m = re.search(r"MLS#\s*([A-Z0-9-]+)", text)
                if m:
                    mls_number = m.group(1).strip()
            
            # Extract MLS type
            if "MLS Type:" in text and not mls_type:
                m = re.search(r"MLS Type:\s*([^,\n]+)", text)
                if m:
                    mls_type = m.group(1).strip()
            
            # Extract tax information
            if "Taxes:" in text and not tax_information:
                m = re.search(r"Taxes:\s*([^,\n]+)", text)
                if m:
                    tax_information = m.group(1).strip()
            
            # Extract days on compass
            if "Days on Market:" in text and not days_on_compass:
                m = re.search(r"Days on Market:\s*(\d+)", text)
                if m:
                    days_on_compass = int(m.group(1))
            
            # Extract last updated date
            if "Last Updated:" in text and not last_updated:
                m = re.search(r"Last Updated:\s*([\d/]+)", text)
                if m:
                    last_updated = m.group(1)
                    
        if href and full_address:
            listing = {
                "address": street_address,
                "url": href,
                "price": price,
                "beds": beds,
                "baths": baths,
                "sqft": sqft,
                "price_per_sqft": int(price / sqft) if price and sqft else None,
                "city": city,
                "state": state,
                "zip": zip_code,
                "estimated_rent": None,
                "rent_yield": None,
                "tax_information": tax_information,
                "mls_type": mls_type,
                "mls_number": mls_number,
                "days_on_compass": days_on_compass,
                "last_updated": last_updated,
                "favorite": 0
            }
            listings.append(listing)
            
    return listings

def parse_individual_format(soup):
    """Parse emails with individual listings format."""
    listings = []
    
    for row in soup.find_all("tr", class_="listingComponentV2"):
        a_tag = row.find("a", href=True)
        href = a_tag["href"] if a_tag else None
        
        # Extract base Compass URL without workspace parameters
        if href and "compass.com/listing" in href:
            # Extract just the base URL up to the listing ID
            url_parts = href.split("?")[0]
            href = url_parts
        
        price, beds, baths, sqft, full_address = None, None, None, None, None
        city, state, zip_code = None, None, None
        tax_information, mls_type, mls_number = None, None, None
        days_on_compass, last_updated = None, None
        
        price_tag = row.find("b")
        if price_tag:
            m = re.search(r"\$[\d,]+", price_tag.text)
            if m:
                price = int(m.group(0).replace("$", "").replace(",", ""))
                
        for span in row.find_all("span"):
            text = span.get_text(strip=True)
            # More specific matching for beds
            if "BD" in text and not beds:
                m = re.search(r"(\d+(?:\.\d+)?)\s*BD", text)
                if m:
                    beds = int(round(float(m.group(1))))
            # More specific matching for baths
            elif "BA" in text and not baths:
                m = re.search(r"(\d+(?:\.\d+)?)\s*BA", text)
                if m:
                    baths = int(round(float(m.group(1))))
            # More specific matching for sqft
            elif "Sq.Ft." in text and not sqft:
                m = re.search(r"([\d,]+)\s*Sq\.Ft\.", text)
                if m:
                    sqft = int(m.group(1).replace(",", ""))
            
            # Extract MLS number
            elif "MLS#" in text and not mls_number:
                m = re.search(r"MLS#\s*([A-Z0-9-]+)", text)
                if m:
                    mls_number = m.group(1).strip()
            
            # Extract MLS type
            elif "MLS Type:" in text and not mls_type:
                m = re.search(r"MLS Type:\s*([^,\n]+)", text)
                if m:
                    mls_type = m.group(1).strip()
            
            # Extract tax information
            elif "Taxes:" in text and not tax_information:
                m = re.search(r"Taxes:\s*([^,\n]+)", text)
                if m:
                    tax_information = m.group(1).strip()
            
            # Extract days on compass
            elif "Days on Market:" in text and not days_on_compass:
                m = re.search(r"Days on Market:\s*(\d+)", text)
                if m:
                    days_on_compass = int(m.group(1))
            
            # Extract last updated date
            elif "Last Updated:" in text and not last_updated:
                m = re.search(r"Last Updated:\s*([\d/]+)", text)
                if m:
                    last_updated = m.group(1)
                    
        for a in row.find_all("a"):
            text = a.get_text(strip=True)
            if "," in text and len(text) > 10:
                full_address = text
                street_address, city, state, zip_code = parse_address_components(full_address)
                break
                
        if href and full_address:
            listing = {
                "address": street_address,
                "url": href,
                "price": price,
                "beds": beds,
                "baths": baths,
                "sqft": sqft,
                "price_per_sqft": int(price / sqft) if price and sqft else None,
                "city": city,
                "state": state,
                "zip": zip_code,
                "estimated_rent": None,
                "rent_yield": None,
                "tax_information": tax_information,
                "mls_type": mls_type,
                "mls_number": mls_number,
                "days_on_compass": days_on_compass,
                "last_updated": last_updated,
                "favorite": 0
            }
            listings.append(listing)
            
    return listings

def fetch_emails_with_label(service, label_id, max_results=10):
    """Fetch emails with a specific label."""
    try:
        print(f"🔍 Searching for emails with label ID: {label_id}")
        
        # Ensure label ID has Label_ prefix
        if not label_id.startswith('Label_'):
            label_id = f'Label_{label_id}'
            print(f"ℹ️ Added Label_ prefix to label ID: {label_id}")
        
        # Get list of email IDs with this label
        results = service.users().messages().list(
            userId='me',
            labelIds=[label_id],
            maxResults=max_results
        ).execute()
        
        messages = results.get('messages', [])
        if not messages:
            print("⚠️ No messages found with this label")
            return []
        
        print(f"✅ Found {len(messages)} messages")
        
        # Fetch each email's content
        emails = []
        for message in messages:
            print(f"📧 Fetching message {message['id']}")
            msg = service.users().messages().get(
                userId='me',
                id=message['id'],
                format='full'
            ).execute()
            
            # Print message details for debugging
            if 'payload' in msg and 'headers' in msg['payload']:
                for header in msg['payload']['headers']:
                    if header['name'] in ['Subject', 'Date']:
                        print(f"{header['name']}: {header['value']}")
            
            # Get HTML content from payload
            html_content = None
            
            # First try to get HTML content directly from payload
            if 'payload' in msg and msg['payload'].get('mimeType') == 'text/html':
                data = msg['payload'].get('body', {}).get('data')
                if data:
                    html_content = base64.urlsafe_b64decode(data).decode('utf-8')
                    print("✅ Found HTML content in main payload")
            
            # If not found, look in parts
            if not html_content and 'payload' in msg and 'parts' in msg['payload']:
                for part in msg['payload']['parts']:
                    if part['mimeType'] == 'text/html':
                        data = part.get('body', {}).get('data')
                        if data:
                            html_content = base64.urlsafe_b64decode(data).decode('utf-8')
                            print("✅ Found HTML content in message parts")
                            break
            
            # If still not found, look in nested parts
            if not html_content and 'payload' in msg and 'parts' in msg['payload']:
                for part in msg['payload']['parts']:
                    if 'parts' in part:
                        for subpart in part['parts']:
                            if subpart['mimeType'] == 'text/html':
                                data = subpart.get('body', {}).get('data')
                                if data:
                                    html_content = base64.urlsafe_b64decode(data).decode('utf-8')
                                    print("✅ Found HTML content in nested parts")
                                    break
            
            if html_content:
                print("✅ Successfully extracted HTML content")
                emails.append({
                    'id': message['id'],
                    'html_content': html_content
                })
            else:
                print("⚠️ No HTML content found in message")
                # Print message structure for debugging
                print("Message structure:")
                print(json.dumps(msg['payload'], indent=2))
        
        print(f"✅ Successfully processed {len(emails)} emails")
        return emails
    except Exception as e:
        print(f"❌ Error fetching emails: {e}")
        import traceback
        traceback.print_exc()
        return []

def enrich_with_rent(listings, rent_data):
    """Add estimated rent and yield data to listings."""
    for listing in listings:
        zip_code = listing.get("zip")
        if zip_code and str(zip_code) in rent_data:
            rent = rent_data[str(zip_code)]
            listing["estimated_rent"] = rent
            if listing.get("price"):
                listing["rent_yield"] = round(12 * rent / listing["price"], 4)
        else:
            if zip_code:
                print(f"⚠️ No rent data available for ZIP: {zip_code}")

def print_listing_details(listing, label_name=None, changes=None):
    """Print formatted details of a listing."""
    if label_name:
        print(f"🏷️ Label: {label_name}")
    print("🏡 Address:", listing.get("address", "N/A"))
    
    # Print price with change indicator if it changed
    price = listing.get("price")
    if changes and "price" in changes:
        old_price = changes["price"][0]
        new_price = changes["price"][1]
        print(f"💲 Price: ${old_price:,} → ${new_price:,} (Changed)")
    else:
        print("💲 Price:", f"${price:,}" if price else "N/A")
    
    # Print other fields with change indicators
    for field, display in [
        ("beds", "🛏 Beds"),
        ("baths", "🛁 Baths"),
        ("sqft", "📐 Sqft"),
        ("mls_type", "🏷️ MLS Type"),
        ("tax_information", "💵 Tax Info")
    ]:
        value = listing.get(field)
        if changes and field in changes:
            old_val = changes[field][0]
            new_val = changes[field][1]
            print(f"{display}: {old_val} → {new_val} (Changed)")
        else:
            print(f"{display}:", value if value else "N/A")
    
    print("🏙 City/State/Zip:", 
          f"{listing.get('city', 'N/A')}, {listing.get('state', 'N/A')} {listing.get('zip', 'N/A')}")
    print("💰 Est. Rent:", 
          f"${listing.get('estimated_rent'):,}/mo" if listing.get("estimated_rent") else "N/A")
    print("📊 Rent Yield:", 
          f"{listing.get('rent_yield')*100:.2f}%" if listing.get("rent_yield") else "N/A")
    print("🔗 URL:", listing.get("url", "N/A"))
    print("-" * 60)

def load_label_config(config_file):
    """Load label configuration from JSON file."""
    try:
        config_path = Path(config_file)
        if not config_path.is_absolute():
            config_path = ROOT / config_path
        
        print(f"📋 Loading label configuration from {config_path}...")
        with open(config_path) as f:
            config = json.load(f)
        
        if "property_listings" not in config:
            print("❌ Invalid configuration: missing 'property_listings' key")
            return None
        
        # Get enabled labels and their IDs
        enabled_labels = {}
        for label in config["property_listings"]:
            if label.get("enabled", True):
                enabled_labels[label["name"]] = label["id"]
        
        if not enabled_labels:
            print("⚠️ No enabled labels found in configuration")
            return None
        
        print(f"✅ Found {len(enabled_labels)} enabled labels")
        return enabled_labels
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return None

def is_email_processed(email_id):
    """Check if an email has already been processed."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_emails WHERE message_id = ?", (email_id,))
    result = cursor.fetchone() is not None
    conn.close()
    return result

def mark_email_processed(email_id, label_id):
    """Mark an email as processed."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO processed_emails (message_id, label_id, source)
        VALUES (?, ?, 'gmail-multi-label')
    """, (email_id, label_id))
    conn.commit()
    conn.close()

def get_project_root():
    env_root = os.environ.get("PROPERTY_PIPELINE_ROOT")
    if env_root and (Path(env_root) / "config" / "credentials.json").exists():
        return Path(env_root).resolve()
    cur = Path(__file__).resolve().parent
    while True:
        candidate = cur / "config" / "credentials.json"
        if candidate.exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    raise FileNotFoundError("Could not find project root containing config/credentials.json")

PROJECT_ROOT = get_project_root()
DEFAULT_CREDENTIALS = str(PROJECT_ROOT / "config" / "credentials.json")
DEFAULT_TOKEN = str(PROJECT_ROOT / "config" / "token.pickle")

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Parse Gmail emails for property listings")
    parser.add_argument("--dry-run", action="store_true", help="Preview extraction without database insertion")
    parser.add_argument("--config", default="config/label_config.json", help="Path to label configuration file")
    parser.add_argument("--max-emails", type=int, default=10, help="Maximum number of emails to process per label")
    parser.add_argument("--credentials", default=DEFAULT_CREDENTIALS, help="Path to credentials.json file")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="Path to token.pickle file")
    parser.add_argument("--force", action="store_true", help="Force reprocessing of all emails")
    args = parser.parse_args()
    
    # Load label configuration
    label_config = load_label_config(args.config)
    if not label_config:
        return
    
    # Load Zillow rent data
    rent_data_path = ROOT / "data" / "zori_latest.csv"
    print(f"📊 Loading Zillow rent data from {rent_data_path}...")
    rent_data = load_zori_data(rent_data_path)
    if not rent_data:
        return
    
    # Authenticate with Gmail
    service = authenticate_gmail(args.credentials, args.token)
    if not service:
        return
    
    # Process each enabled label
    total_listings = []
    listings_per_label = {}
    
    for label_name, label_id in label_config.items():
        print(f"\n📩 Processing label: {label_name} (ID: {label_id})...")
        
        # Fetch emails for this label
        emails = fetch_emails_with_label(service, label_id, args.max_emails)
        if not emails:
            print(f"⚠️ No emails found for label: {label_name}")
            continue
        
        # Process each email
        label_listings = []
        for email in emails:
            email_id = email['id']
            
            # Skip if already processed (unless force flag is set)
            if not args.force and is_email_processed(email_id):
                print(f"ℹ️ Skipping already processed email: {email_id}")
                continue
            
            print(f"\n📝 Processing email {email_id}...")
            
            # Parse listings from HTML
            email_listings = parse_html_email(email['html_content'])
            if not email_listings:
                print("⚠️ No listings found in email")
                continue
            
            print(f"✅ Found {len(email_listings)} listings in email")
            
            # Add label information to each listing
            for listing in email_listings:
                listing["label"] = label_name
            
            label_listings.extend(email_listings)
            
            # Mark email as processed (unless dry run)
            if not args.dry_run:
                mark_email_processed(email_id, label_id)
        
        if label_listings:
            total_listings.extend(label_listings)
            listings_per_label[label_name] = len(label_listings)
            print(f"✅ Fetched {len(label_listings)} new emails")
        else:
            print("⚠️ No listings found in any emails")
    
    if not total_listings:
        print("\n⚠️ No listings found in any emails")
        return
    
    # Enrich listings with rent data
    enrich_with_rent(total_listings, rent_data)
    
    # Insert listings into database (unless dry run)
    if not args.dry_run:
        print("\n🧾 Processing {} listings...".format(len(total_listings)))
        insert_listings(total_listings)
    
    # Print summary
    print("\n📝 Summary:")
    print(f"   Total labels processed: {len(label_config)}")
    print(f"   Total listings parsed: {len(total_listings)}")
    print("   Listings found per label:")
    for label_name, count in listings_per_label.items():
        print(f"     - {label_name}: {count}")

if __name__ == "__main__":
    main()
