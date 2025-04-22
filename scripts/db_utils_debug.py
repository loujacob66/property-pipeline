import sqlite3
from datetime import datetime

DB_PATH = "data/listings.db"

def insert_listings(listings, source="compass"):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    for listing in listings:
        try:
            print("🔍 Attempting to insert listing:")
            for k in ("address", "city", "state", "zip", "price", "beds", "baths", "sqft", "estimated_rent", "rent_yield", "url"):
                print(f"   {k}: {listing.get(k)}")

            cursor.execute("""
                INSERT INTO listings
                (address, city, state, zip, price, beds, baths, sqft, estimated_rent, rent_yield, url, source, imported_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                listing.get("address"),
                listing.get("city"),
                listing.get("state"),
                listing.get("zip"),
                listing.get("price"),
                listing.get("beds"),
                listing.get("baths"),
                listing.get("sqft"),
                listing.get("estimated_rent"),
                listing.get("rent_yield"),
                listing.get("url"),
                source,
                datetime.utcnow().isoformat()
            ))

            print("✅ Inserted successfully.
")

        except Exception as e:
            print("❌ Error inserting listing:", e)
    conn.commit()
    conn.close()
