import sqlite3
from playwright.sync_api import PlaywrightTimeoutError
import traceback

def enrich_listings(headless=True, limit=None):
    """
    Enrich property listings with details from Compass.com.
    
    Args:
        headless (bool): Whether to run browser in headless mode
        limit (int): Optional limit on number of listings to process
    """
    # Initialize database connection
    conn = sqlite3.connect('data/listings.db')
    cursor = conn.cursor()
    
    # Check if we have all the columns we need
    cursor.execute("PRAGMA table_info(listings)")
    columns = [col[1] for col in cursor.fetchall()]
    
    # Add any missing columns
    new_columns = {
        'mls_number': 'TEXT',
        'mls_type': 'TEXT',
        'tax_information': 'TEXT',
        'days_on_compass': 'INTEGER',
        'last_updated': 'TEXT',
        'year_built': 'INTEGER',
        'lot_size': 'TEXT',
        'hoa_fee': 'TEXT',
        'parking': 'TEXT',
        'heating': 'TEXT',
        'cooling': 'TEXT',
        'style': 'TEXT',
        'construction': 'TEXT',
        'status': 'TEXT',
        'agent_name': 'TEXT',
        'agent_phone': 'TEXT',
        'agent_email': 'TEXT'
    }
    
    for col, col_type in new_columns.items():
        if col not in columns:
            cursor.execute(f"ALTER TABLE listings ADD COLUMN {col} {col_type}")
            print(f"Added column {col} to listings table")
    
    # Get listings that need enrichment
    cursor.execute("""
        SELECT id, url FROM listings 
        WHERE mls_number IS NULL 
        OR mls_type IS NULL 
        OR tax_information IS NULL 
        OR days_on_compass IS NULL 
        OR last_updated IS NULL
        ORDER BY id
    """)
    listings = cursor.fetchall()
    
    if limit:
        listings = listings[:limit]
    
    if not listings:
        print("No listings need enrichment")
        return
    
    print(f"Found {len(listings)} listings to enrich")
    
    # Authenticate with Compass
    page, context = authenticate_compass(headless=headless)
    
    try:
        for listing_id, url in listings:
            print(f"\nProcessing listing {listing_id}: {url}")
            
            try:
                # Navigate to the listing
                page.goto(url)
                page.wait_for_load_state("networkidle")
                
                # Wait for and switch to the iframe
                iframe = page.frame_locator("iframe").first
                iframe.wait_for_selector("tr.keyDetails-text", timeout=10000)
                
                # Extract details
                details = extract_listing_details(iframe)
                
                # Update database
                update_fields = []
                update_values = []
                
                for field, value in details.items():
                    if field in new_columns:
                        update_fields.append(field)
                        update_values.append(value)
                
                if update_fields:
                    placeholders = ','.join(['?'] * len(update_fields))
                    field_names = ','.join(update_fields)
                    cursor.execute(f"""
                        UPDATE listings 
                        SET ({field_names}) = ({placeholders})
                        WHERE id = ?
                    """, update_values + [listing_id])
                    
                    conn.commit()
                    print(f"Updated {len(update_fields)} fields for listing {listing_id}")
                
            except PlaywrightTimeoutError:
                print(f"Timeout processing listing {listing_id}")
                continue
            except Exception as e:
                print(f"Error processing listing {listing_id}: {str(e)}")
                traceback.print_exc()
                continue
    
    finally:
        # Clean up
        context.close()
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Enrich property listings with Compass details")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--limit", type=int, help="Limit number of listings to process")
    
    args = parser.parse_args()
    enrich_listings(headless=args.headless, limit=args.limit) 