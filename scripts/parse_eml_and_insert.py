import re
import sys
import os
import glob
import html
from bs4 import BeautifulSoup
import quopri

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from lib.zori_utils import load_zori_data
from lib.db_utils import insert_listings  # Add this missing import

def clean_url(raw_url):
    url = raw_url.replace('3D"', "").replace("=\n", "").replace("=\r", "").strip()
    if url.endswith("="):
        url = url[:-1]
    return url.strip('"')

def parse_eml_file(filepath):
    """Parse an EML file to extract real estate listings."""
    with open(filepath, "r", encoding="utf-8") as f:
        raw_data = f.read()

    html_content = quopri.decodestring(raw_data).decode("utf-8", errors="ignore")
    soup = BeautifulSoup(html.unescape(html_content), "html.parser")
    listings = []

    # --- Collection Email Parsing ---
    for row in soup.find_all("tr", class_="listingComponentV2"):
        a_tag = row.find("a", href=True)
        href = a_tag["href"] if a_tag else None

        next_tr = row.find_next_sibling("tr")
        if not next_tr:
            continue

        address, price, beds, baths, sqft = None, None, None, None, None
        city, state, zip_code = None, None, None

        for div in next_tr.find_all("div"):
            text = div.get_text(" ", strip=True)

            if not address:
                a = div.find("a")
                if a and "," in a.get_text(strip=True):
                    address = a.get_text(strip=True)
                    parts = address.split(", ")
                    if len(parts) >= 3:
                        city = parts[-3]
                        state = parts[-2]
                        zip_code = parts[-1].split()[0]

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

        if href and address:
            listing = {
                "address": address,
                "url": href,
                "from_collection": True,
                "price": price,
                "beds": beds,
                "baths": baths,
                "sqft": sqft,
                "city": city,
                "state": state,
                "zip": zip_code,
                "estimated_rent": None,
                "rent_yield": None
            }
            listings.append(listing)

    # --- Individual Email Fallback ---
    if not listings:
        for row in soup.find_all("tr", class_="listingComponentV2"):
            a_tag = row.find("a", href=True)
            href = a_tag["href"] if a_tag else None

            price, beds, baths, sqft, address = None, None, None, None, None
            city, state, zip_code = None, None, None

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

            for a in row.find_all("a"):
                text = a.get_text(strip=True)
                if "," in text and len(text) > 10:
                    address = text
                    parts = address.split(", ")
                    if len(parts) >= 3:
                        city = parts[-3]
                        state = parts[-2]
                        zip_code = parts[-1].split()[0]
                    break

            if href and address:
                listing = {
                    "address": address,
                    "url": href,
                    "from_collection": False,
                    "price": price,
                    "beds": beds,
                    "baths": baths,
                    "sqft": sqft,
                    "city": city,
                    "state": state,
                    "zip": zip_code,
                    "estimated_rent": None,
                    "rent_yield": None
                }
                listings.append(listing)

    return listings

def enrich_with_rent(listings, rent_data):
    """Add estimated rent and yield data to listings."""
    for listing in listings:
        zip_code = listing.get("zip")
        if zip_code and str(zip_code) in rent_data:
            rent = rent_data[str(zip_code)]
            listing["estimated_rent"] = rent
            if listing.get("price"):
                listing["rent_yield"] = round(12 * rent / listing["price"], 4)

def main():
    import sys
    dry_run = "--dry-run" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    file_listing_counts = {}  # Track per-file listing counts
    
    if not args:
        print("Usage: python scripts/parse_eml_and_insert.py data/*.eml")
        return

    rent_data = load_zori_data(os.path.join(ROOT, "data", "zori_latest.csv"))

    all_listings = []
    processed_files = set()  # Track which files we've already processed
    
    for pattern in args:
        for filepath in glob.glob(pattern):
            # Skip if we've already processed this file
            if filepath in processed_files:
                continue
                
            processed_files.add(filepath)
            print(f"📥 Parsing {filepath}")
            listings = parse_eml_file(filepath)
            enrich_with_rent(listings, rent_data)
            
            if listings:
                print(f"✅ Found {len(listings)} listings in {os.path.basename(filepath)}")
                for listing in listings:
                    print("🏡 Address:", listing.get("address", "N/A"))
                    print("💲 Price:", f"${listing.get('price'):,}" if listing.get("price") else "N/A")
                    print("🛏 Beds:", listing.get("beds", "N/A"))
                    print("🛁 Baths:", listing.get("baths", "N/A"))
                    print("📐 Sqft:", listing.get("sqft", "N/A"))
                    print("🏙 City/State/Zip:", f"{listing.get('city', 'N/A')}, {listing.get('state', 'N/A')} {listing.get('zip', 'N/A')}")
                    print("🔗 URL:", listing.get("url", "N/A"))
                    print("-" * 60)
                
                all_listings.extend(listings)
                file_listing_counts[os.path.basename(filepath)] = len(listings)

    if all_listings:
        if dry_run:
            print("🚫 Dry-run mode: Skipping database insert.")
        else:
            print(f"🧾 Inserting {len(all_listings)} listings into database...")
            insert_listings(all_listings, source="eml-import")

    print("\n📝 Summary:")
    print(f"   Total files processed: {len(file_listing_counts)}")
    print(f"   Total listings parsed: {sum(file_listing_counts.values())}")
    print(f"   Listings found per file:")
    for file, count in file_listing_counts.items():
        print(f"     - {file}: {count}")

if __name__ == "__main__":
    main()
