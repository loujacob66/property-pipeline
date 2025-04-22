
from bs4 import BeautifulSoup
import re
import html
import urllib.parse

def extract_address_from_url(url):
    parsed = urllib.parse.urlparse(html.unescape(url))
    query = urllib.parse.parse_qs(parsed.query)
    utm_content = query.get('utm_content')
    if utm_content:
        return urllib.parse.unquote(utm_content[0]).strip()
    return None

def parse_compass_email(html_content):
    soup = BeautifulSoup(html.unescape(html_content), 'html.parser')
    listings = []

    for listing_div in soup.find_all('tr', class_='listingComponentV2'):
        try:
            anchors = listing_div.find_all('a', href=True)
            address = None
            url = None

            for a in anchors:
                href = a['href']
                text = a.get_text(strip=True)
                if 'compass.com/listing' in href:
                    url = href
                    address = text
                    break

            if not address or address.strip() == "":
                address = extract_address_from_url(url)

            price_tag = listing_div.find('b', class_='displayPriceStyle')
            price_text = price_tag.get_text(strip=True).replace('$', '').replace(',', '') if price_tag else None
            price = int(price_text) if price_text and price_text.isdigit() else None

            details_div = listing_div.find('div', style=lambda val: val and 'color: #000' in val)
            details_text = details_div.get_text(" ", strip=True) if details_div else ""

            beds_match = re.search(r'(\d+) BD', details_text)
            baths_match = re.search(r'(\d+) BA', details_text)
            sqft_match = re.search(r'([\d,]+) Sq\.Ft\.', details_text)

            beds = int(beds_match.group(1)) if beds_match else None
            baths = int(baths_match.group(1)) if baths_match else None
            sqft = int(sqft_match.group(1).replace(',', '')) if sqft_match else None

            listings.append({
                'address': address,
                'price': price,
                'beds': beds,
                'baths': baths,
                'sqft': sqft,
                'url': url
            })
        except Exception as e:
            print(f"Error parsing one listing: {e}")
            continue

    return listings
