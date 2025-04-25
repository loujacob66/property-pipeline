#!/usr/bin/env python3
"""
Multi-Label Gmail Parser for Property Listings

This script accesses Gmail using OAuth, fetches emails from multiple configured labels,
parses them for real estate listings, and stores the data in an SQLite database.

Usage:
    python multi_label_gmail_parser.py [--dry-run] [--config CONFIG_FILE] [--max-emails MAX_EMAILS]

Options:
    --dry-run           Preview extraction without database insertion
    --config            Path to the label configuration file (default: config/label_config.json)
    --max-emails MAX    Maximum number of emails to process per label (default: 10)
"""

import os
import sys
import re
import json
import argparse
import html
from bs4 import BeautifulSoup
import usaddress

# Add project root to path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

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
        
        next_tr = row.find_next_sibling("tr")
        if not next_tr:
            continue
            
        full_address, price, beds, baths, sqft = None, None, None, None, None
        city, state, zip_code = None, None, None
        tax_information, mls_type = None, None  # New fields
        
        for div in next_tr.find_all("div"):
            text = div.get_text(" ", strip=True)
            
            if not full_address:
                a = div.find("a")
                if a and "," in a.get_text(strip=True):
                    full_address = a.get_text(strip=True)
                    # Parse address components
                    street_address, city, state, zip_code = parse_address_components(full_address)
                    
            if "$" in text and not price:
                m = re.search(r"\$[\d,]+", text)
                if m:
                    price = int(m.group(0).replace("$", "").replace(",", ""))
                    
            if "BD" in text and not beds:
                m = re.search(r"(\d+(\.\d+)?)", text)
                if m:
                    beds = float(m.group(1))
            if "BA" in text and not baths:
                m = re.search(r"(\d+(\.\d+)?)", text)
                if m:
                    baths = float(m.group(1))
            if "Sq.Ft." in text and not sqft:
                m = re.search(r"([\d,]+)", text)
                if m:
                    sqft = int(m.group(1).replace(",", ""))
            
            # Try to extract MLS type if available
            if "MLS Type:" in text and not mls_type:
                m = re.search(r"MLS Type:\s*([^,\n]+)", text)
                if m:
                    mls_type = m.group(1).strip()
                    
        if href and full_address:
            listing = {
                "address": street_address,
                "url": href,
                "price": price,
                "beds": beds,
                "baths": baths,
                "sqft": sqft,
                "city": city,
                "state": state,
                "zip": zip_code,
                "estimated_rent": None,
                "rent_yield": None,
                "tax_information": tax_information,  # New field
                "mls_type": mls_type  # New field
            }
            listings.append(listing)
            
    return listings

def parse_individual_format(soup):
    """Parse emails with individual listings format."""
    listings = []
    
    for row in soup.find_all("tr", class_="listingComponentV2"):
        a_tag = row.find("a", href=True)
        href = a_tag["href"] if a_tag else None
        
        price, beds, baths, sqft, full_address = None, None, None, None, None
        city, state, zip_code = None, None, None
        tax_information, mls_type = None, None  # New fields
        
        price_tag = row.find("b")
        if price_tag:
            m = re.search(r"\$[\d,]+", price_tag.text)
            if m:
                price = int(m.group(0).replace("$", "").replace(",", ""))
                
        for span in row.find_all("span"):
            text = span.get_text(strip=True)
            if "BD" in text and not beds:
                m = re.search(r"(\d+(\.\d+)?)", text)
                if m:
                    beds = float(m.group(1))
            elif "BA" in text and not baths:
                m = re.search(r"(\d+(\.\d+)?)", text)
                if m:
                    baths = float(m.group(1))
            elif "Sq.Ft." in text and not sqft:
                m = re.search(r"([\d,]+)", text)
                if m:
                    sqft = int(m.group(1).replace(",", ""))
            elif "MLS Type:" in text and not mls_type:
                m = re.search(r"MLS Type:\s*([^,\n]+)", text)
                if m:
                    mls_type = m.group(1).strip()
                    
        for a in row.find_all("a"):
            text = a.get_text(strip=True)
            if "," in text and len(text) > 10:
                full_address = text
                # Parse address components
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
                "city": city,
                "state": state,
                "zip": zip_code,
                "estimated_rent": None,
                "rent_yield": None,
                "tax_information": tax_information,  # New field
                "mls_type": mls_type  # New field
            }
            listings.append(listing)
            
    return listings

def fetch_emails_with_label(service, label_id, max_results=10):
    """Fetch emails with a specific label ID."""
    results = service.users().messages().list(
        userId='me',
        labelIds=[label_id],
        maxResults=max_results
    ).execute()
    messages = results.get('messages', [])
    email_bodies = []
    
    for msg in messages:
        msg_data = service.users().messages().get(
            userId='me',
            id=msg['id'],
            format='raw'
        ).execute()
        
        import base64
        from email import message_from_bytes
        
        raw_data = base64.urlsafe_b64decode(msg_data['raw'].encode('ASCII'))
        mime_msg = message_from_bytes(raw_data)
        
        if mime_msg.is_multipart():
            for part in mime_msg.walk():
                content_type = part.get_content_type()
                if content_type == 'text/html':
                    payload = part.get_payload(decode=True)
                    if payload:
                        email_bodies.append(payload.decode('utf-8', errors='ignore'))
                        break
        else:
            payload = mime_msg.get_payload(decode=True)
            if payload:
                email_bodies.append(payload.decode('utf-8', errors='ignore'))
    
    return email_bodies

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

def print_listing_details(listing, label_name=None):
    """Print formatted details of a listing."""
    if label_name:
        print(f"🏷️ Label: {label_name}")
    print("🏡 Address:", listing.get("address", "N/A"))
    print("💲 Price:", f"${listing.get('price'):,}" if listing.get("price") else "N/A")
    print("🛏 Beds:", listing.get("beds", "N/A"))
    print("🛁 Baths:", listing.get("baths", "N/A"))
    print("📐 Sqft:", listing.get("sqft", "N/A"))
    print("🏙 City/State/Zip:", 
          f"{listing.get('city', 'N/A')}, {listing.get('state', 'N/A')} {listing.get('zip', 'N/A')}")
    print("💰 Est. Rent:", 
          f"${listing.get('estimated_rent'):,}/mo" if listing.get("estimated_rent") else "N/A")
    print("📊 Rent Yield:", 
          f"{listing.get('rent_yield')*100:.2f}%" if listing.get("rent_yield") else "N/A")
    # New fields
    print("🏷️ MLS Type:", listing.get("mls_type", "N/A"))
    print("💵 Tax Info:", listing.get("tax_information", "N/A"))
    print("🔗 URL:", listing.get("url", "N/A"))
    print("-" * 60)

def load_label_config(config_file):
    """Load label configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"❌ Error loading config file: {e}")
        return None

def main():
    """Main function to run the script."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Multi-label Gmail parser for property listings")
    parser.add_argument("--dry-run", action="store_true", help="Preview without database insertion")
    parser.add_argument("--config", default=os.path.join(ROOT, "config", "label_config.json"), 
                        help="Path to the label configuration file")
    parser.add_argument("--max-emails", type=int, default=10, 
                        help="Maximum number of emails to process per label")
    args = parser.parse_args()
    
    # Load label configuration
    print(f"📋 Loading label configuration from {args.config}...")
    config = load_label_config(args.config)
    if not config or "property_listings" not in config:
        print("❌ Invalid or missing configuration")
        return
    
    enabled_labels = [label for label in config["property_listings"] if label.get("enabled", True)]
    if not enabled_labels:
        print("⚠️ No enabled labels found in configuration")
        return
    
    print(f"✅ Found {len(enabled_labels)} enabled labels")
    
    # Load rent data
    print("📊 Loading Zillow rent data...")
    rent_data = load_zori_data(os.path.join(ROOT, "data", "zori_latest.csv"))
    print(f"✅ Loaded rent data for {len(rent_data)} ZIP codes")
    
    # Authenticate with Gmail
    print("🔐 Authenticating with Gmail...")
    try:
        gmail_service = authenticate_gmail()
        print("✅ Authentication successful")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return
    
    # Process each label
    all_listings = []
    label_listing_counts = {}
    
    for label_config in enabled_labels:
        label_name = label_config.get("name", "Unknown")
        label_id = label_config.get("id")
        
        if not label_id:
            print(f"⚠️ No ID specified for label '{label_name}'. Skipping.")
            continue
        
        print(f"\n📩 Processing label: {label_name} (ID: {label_id})...")
        
        # Fetch emails
        try:
            email_bodies = fetch_emails_with_label(
                gmail_service, label_id, max_results=args.max_emails
            )
            print(f"✅ Fetched {len(email_bodies)} emails")
        except Exception as e:
            print(f"❌ Error fetching emails: {e}")
            continue
        
        # Parse emails and extract listings
        label_listings = []
        
        for idx, html_content in enumerate(email_bodies):
            print(f"📝 Parsing email {idx+1}/{len(email_bodies)}...")
            listings = parse_html_email(html_content)
            
            if listings:
                print(f"✅ Found {len(listings)} listings in email #{idx+1}")
                enrich_with_rent(listings, rent_data)
                
                for listing in listings:
                    listing["source_label"] = label_name
                    print_listing_details(listing, label_name)
                
                label_listings.extend(listings)
            else:
                print(f"⚠️ No listings found in email #{idx+1}")
        
        # Add to overall counts
        all_listings.extend(label_listings)
        label_listing_counts[label_name] = len(label_listings)
    
    # Process all listings
    if all_listings:
        if args.dry_run:
            print("\n🚫 Dry-run mode: Skipping database insert.")
        else:
            print(f"\n🧾 Inserting {len(all_listings)} listings into database...")
            insert_listings(all_listings, source="gmail-multi-label")
    else:
        print("\n⚠️ No listings found in any emails")
    
    # Print summary
    print("\n📝 Summary:")
    print(f"   Total labels processed: {len(label_listing_counts)}")
    print(f"   Total listings parsed: {sum(label_listing_counts.values())}")
    print(f"   Listings found per label:")
    for label, count in label_listing_counts.items():
        print(f"     - {label}: {count}")

if __name__ == "__main__":
    main()
