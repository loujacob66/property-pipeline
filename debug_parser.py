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

            print("📍Before fallback:", repr(address))
            if not address or address.strip() == "":
                address = extract_address_from_url(url)
                print("✅ After fallback:", repr(address))

            listings.append({
                'address': address,
                'url': url
            })
        except Exception as e:
            print(f"Error parsing one listing: {e}")
            continue

    return listings

