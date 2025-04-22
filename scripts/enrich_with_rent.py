import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import re
from lib.zori_utils import load_zori_data
from lib.db_utils import insert_listings

def extract_zip(address, url=None):
    match = re.search(r'\b\d{5}\b', address)
    if match:
        return match.group(0)
    if url:
        match = re.search(r'-(\d{5})(?:/|\b)', url)
        if match:
            return match.group(1)
    return None

def extract_city_state_from_url(url):
    match = re.search(r'/listing/[^/]+-([a-z]+)-([a-z]{2})-(\d{5})/', url)
    if match:
        city = match.group(1).replace("-", " ").title()
        state = match.group(2).upper()
        return city, state
    return None, None

def enrich_listings_with_rent(listings, zori_data):
    for listing in listings:
        zip_code = extract_zip(listing.get("address", ""), listing.get("url"))
        city, state = extract_city_state_from_url(listing.get("url", "")) if zip_code else (None, None)

        listing["zip"] = zip_code
        listing["city"] = city
        listing["state"] = state

        listing["estimated_rent"] = zori_data.get(zip_code)
        if listing["estimated_rent"] and listing.get("price"):
            listing["rent_yield"] = round(listing["estimated_rent"] / listing["price"], 5)
        else:
            listing["rent_yield"] = None
    return listings

if __name__ == "__main__":
    from lib.gmail_utils import authenticate_gmail, fetch_emails_with_label
    from lib.email_parser import parse_compass_email

    svc = authenticate_gmail()
    emails = fetch_emails_with_label(svc, "Label_4461353129661597509", 5)
    listings = parse_compass_email(emails[0])

    zori_data = load_zori_data("data/zori_latest.csv")
    enriched = enrich_listings_with_rent(listings, zori_data)

    print(f"💾 Inserting {len(enriched)} listings into the database...")
    insert_listings(enriched)

    for l in enriched:
        print(json.dumps(l, indent=2))
