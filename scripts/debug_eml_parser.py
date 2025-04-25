import sys
import os
import html
import glob
import quopri
import re
from bs4 import BeautifulSoup

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

def clean_url(raw_url):
    url = raw_url.replace('3D"', "").replace("=\n", "").replace("=\r", "").strip()
    if url.endswith("="):
        url = url[:-1]
    return url.strip('"')

def parse_eml_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        raw_data = f.read()

    html_content = quopri.decodestring(raw_data).decode("utf-8", errors="ignore")
    soup = BeautifulSoup(html.unescape(html_content), "html.parser")
    listings = []

    collection_rows = soup.find_all("tr", class_="listingComponentV2")
    if collection_rows:
        for row in collection_rows:
            a_tag = row.find("a", href=True)
            if not a_tag:
                continue
            href = clean_url(a_tag["href"])

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
                    m = re.search(r"(\d+(\.\d+)?)\s*BD", text)
                    if m:
                        beds = float(m.group(1))

                if "BA" in text and not baths:
                    m = re.search(r"(\d+(\.\d+)?)\s*BA", text)
                    if m:
                        baths = float(m.group(1))

                if "Sq.Ft." in text and not sqft:
                    m = re.search(r"([\d,]+)\s*Sq\.Ft\.", text)
                    if m:
                        sqft = int(m.group(1).replace(",", ""))

            if address:
                listings.append({
                    "address": address,
                    "url": href,
                    "from_collection": True,
                    "price": price,
                    "beds": beds,
                    "baths": baths,
                    "sqft": sqft,
                    "city": city,
                    "state": state,
                    "zip": zip_code
                })
    else:
        anchors = soup.find_all("a", href=True)
        listings_by_url = {}
        for a in anchors:
            href = clean_url(a["href"])
            text = a.get_text(strip=True)
            if "compass.com/listing" in href and text:
                url_key = href.split("?")[0]
                current = listings_by_url.get(url_key)
                if not current or (any(c.isdigit() for c in text) and not any(c.isdigit() for c in current["address"])):
                    listings_by_url[url_key] = {
                        "address": text,
                        "url": href,
                        "from_collection": False,
                        "price": None,
                        "beds": None,
                        "baths": None,
                        "sqft": None,
                        "city": None,
                        "state": None,
                        "zip": None
                    }
        listings = list(listings_by_url.values())

    return listings

def main():
    args = sys.argv[1:]
    if not args:
        print("Usage: python scripts/debug_eml_parser.py data/*.eml")
        return

    for pattern in args:
        for filepath in glob.glob(pattern):
            print(f"📂 {filepath}")
            listings = parse_eml_file(filepath)
            print(f"🔍 Found {len(listings)} listing(s)")
            for l in listings:
                print(f"  • {l['address']} → {l['url']} (from_collection={l['from_collection']})")
            print("------")

if __name__ == "__main__":
    main()