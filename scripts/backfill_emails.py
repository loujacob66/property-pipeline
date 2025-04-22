import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
from lib.gmail_utils import authenticate_gmail, fetch_emails_with_label
from lib.email_parser import parse_compass_email
from lib.zori_utils import load_zori_data
from lib.db_utils import insert_listings
from scripts.enrich_with_rent import enrich_listings_with_rent

def main():
    parser = argparse.ArgumentParser(description="Backfill listings from Gmail")
    parser.add_argument("--label", type=str, default="Label_4461353129661597509", help="Gmail label ID or name")
    parser.add_argument("--max-emails", type=int, default=50, help="Maximum number of emails to process")
    args = parser.parse_args()

    print(f"📬 Fetching up to {args.max_emails} emails with label: {args.label}")
    svc = authenticate_gmail()
    emails = fetch_emails_with_label(svc, args.label, args.max_emails)

    zori_data = load_zori_data("data/zori_latest.csv")
    total = 0

    for email in emails:
        listings = parse_compass_email(email)
        enriched = enrich_listings_with_rent(listings, zori_data)
        insert_listings(enriched)
        total += len(enriched)

    print(f"✅ Backfill complete. Processed {len(emails)} emails, inserted {total} listings.")

if __name__ == "__main__":
    main()
