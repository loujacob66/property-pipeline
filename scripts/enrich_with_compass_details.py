import sqlite3
import time
import os
from playwright.sync_api import sync_playwright

def enrich_listings_with_compass():
    db_filename = os.path.join(os.path.dirname(__file__), '..', 'data', 'listings.db')
    conn = sqlite3.connect(db_filename)
    c = conn.cursor()

    c.execute("SELECT id, url FROM listings WHERE mls_number IS NULL OR tax_info IS NULL OR mls_type IS NULL")
    listings = c.fetchall()

    if not listings:
        print("✅ No listings need enrichment. Database is up to date.")
        conn.close()
        return

    print(f"🔎 Found {len(listings)} listing(s) needing enrichment.")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=os.path.join(os.path.dirname(__file__), '..', '.auth'),
            headless=False
        )

        page = context.pages[0]  # use the first (blank) page
        print("🌐 Navigating to Compass login page...")
        page.goto("https://www.compass.com/login/")

        input("✅ After logging in (or if already logged in), press Enter to continue scraping...")

        for listing_id, url in listings:
            try:
                print(f"➡️ Visiting listing ID {listing_id}")
                page.goto(url)
                time.sleep(3)  # wait for page load

                # Switch to the iframe containing the listing details
                iframe = page.frame_locator("iframe[title='Listing page']").first

                mls_number = None
                tax_info = None
                mls_type = None

                try:
                    mls_number = iframe.locator("tr:has(th:has-text('MLS')) td").first.inner_text()
                except Exception:
                    print("⚠️ MLS number not found on page.")

                try:
                    # Wait for the Taxes row to appear (up to 5 seconds)
                    iframe.locator("tr.keyDetails-text:has(th:has-text('Taxes')) td").first.wait_for(timeout=5000)
                    tax_info = iframe.locator("tr.keyDetails-text:has(th:has-text('Taxes')) td").first.inner_text()
                except Exception:
                    print("⚠️ Tax info not found on page.")

                try:
                    # Wait for the MLS Type row to appear (up to 5 seconds)
                    iframe.locator("tr.keyDetails-text:has(th:has-text('MLS Type')) td").first.wait_for(timeout=5000)
                    mls_type = iframe.locator("tr.keyDetails-text:has(th:has-text('MLS Type')) td").first.inner_text()
                except Exception:
                    print("⚠️ MLS Type not found on page.")

                if mls_number or tax_info or mls_type:
                    c.execute('''
                        UPDATE listings
                        SET mls_number = COALESCE(?, mls_number),
                            tax_info = COALESCE(?, tax_info),
                            mls_type = COALESCE(?, mls_type)
                        WHERE id = ?
                    ''', (mls_number, tax_info, mls_type, listing_id))
                    conn.commit()
                    print(f"✅ Updated listing ID {listing_id}: MLS#={mls_number}, Tax={tax_info}, MLS Type={mls_type}")
                else:
                    print(f"⚠️ No updates found for listing ID {listing_id}")

            except Exception as e:
                print(f"❌ Error processing listing ID {listing_id}: {e}")

        context.close()
    conn.close()
    print("🏁 Enrichment process completed.")

if __name__ == "__main__":
    enrich_listings_with_compass()