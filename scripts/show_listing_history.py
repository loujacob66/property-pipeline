#!/usr/bin/env python3
"""
Script to show the history of changes for a specific listing.
Usage: python scripts/show_listing_history.py <address>
"""

import sqlite3
import sys
from pathlib import Path
from datetime import datetime
import pytz
from itertools import groupby

# Constants
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "listings.db"
MTN_TZ = pytz.timezone('America/Denver')
UTC_TZ = pytz.UTC

# Fields to exclude from output
EXCLUDED_FIELDS = {'url', 'rent_yield'}

# Define field name mappings for consistent capitalization
FIELD_NAMES = {
    "price": "Price",
    "beds": "Beds",
    "baths": "Baths",
    "sqft": "Sqft",
    "mls_type": "MLS Type",
    "tax_information": "Tax Info"
}

def format_value(field, value):
    """Format values for display based on field type"""
    if value == "None" or value is None:
        return "not set"
        
    if field == "price":
        return f"${int(float(value)):,}"
    elif field in ["beds", "baths", "sqft"]:
        return f"{int(float(value)):,}"
    elif field == "price_per_sqft":
        return f"${float(value):,.2f}/sqft"
    return value

def show_listing_history(address):
    """Show the history of changes for a specific listing."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get listing ID and first seen timestamp
    c.execute("""
        SELECT id, created_at 
        FROM listings 
        WHERE address = ?
    """, (address,))
    result = c.fetchone()
    if not result:
        print(f"❌ No listing found with address: {address}")
        return
    
    listing_id, first_seen = result
    
    # Get current listing details
    c.execute("""
        SELECT price, beds, baths, sqft, mls_type, tax_information
        FROM listings WHERE id = ?
    """, (listing_id,))
    current = c.fetchone()
    current_dict = dict(zip(["price", "beds", "baths", "sqft", "mls_type", "tax_information"], current))
    
    # Get changes, excluding URL changes and deduplicating price changes within 5 minutes
    c.execute("""
        WITH RankedChanges AS (
            SELECT 
                field_name,
                old_value,
                new_value,
                changed_at,
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        field_name,
                        CASE 
                            WHEN field_name = 'price' 
                            THEN strftime('%Y-%m-%d %H:%M', changed_at, 'unixepoch', 'localtime')
                            ELSE changed_at 
                        END
                    ORDER BY id DESC
                ) as rn
            FROM listing_changes
            WHERE listing_id = ?
            AND field_name NOT IN ('rent_yield', 'source_label', 'url')
        )
        SELECT field_name, old_value, new_value, changed_at
        FROM RankedChanges
        WHERE rn = 1
        ORDER BY changed_at DESC
    """, (listing_id,))
    
    changes = c.fetchall()
    
    # Convert first_seen to Mountain Time
    first_seen_dt = datetime.fromisoformat(first_seen.replace('Z', '+00:00'))
    first_seen_dt = first_seen_dt.astimezone(pytz.timezone("America/Denver"))
    first_seen_formatted = first_seen_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    
    # Print header
    print(f"\n🏠 Property History: {address}")
    print("=" * 60)
    print(f"📅 First seen in system: {first_seen_formatted}")
    print("=" * 60)
    
    if not changes:
        print("\nNo changes recorded since first seen.")
    else:
        # Group changes by timestamp
        for timestamp, group in groupby(changes, key=lambda x: x[3]):
            # Convert to Mountain Time
            mt_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            mt_timestamp = mt_timestamp.astimezone(pytz.timezone("America/Denver"))
            formatted_time = mt_timestamp.strftime("%Y-%m-%d %H:%M:%S %Z")
            
            print(f"\n📊 Changes on {formatted_time}")
            print("-" * 60)
            
            # Sort changes to group related fields
            sorted_changes = sorted(group, key=lambda x: x[0])
            
            for field, old_val, new_val, _ in sorted_changes:
                emoji = get_field_emoji(field)
                field_name = FIELD_NAMES.get(field, field.replace('_', ' ').title())
                
                # Format the values
                old_val = format_value(field, old_val)
                new_val = format_value(field, new_val)
                
                # Print with better alignment
                print(f"{emoji} {field_name:<20} {old_val:>15}  →  {new_val:<15}")
    
    # Print current values in a more organized way
    print("\n📌 Current Property Details")
    print("-" * 60)
    
    # Group related fields
    property_details = {
        "Basic Info": ["price", "beds", "baths", "sqft"],
        "Listing Info": ["mls_type", "tax_information"]
    }
    
    for section, fields in property_details.items():
        print(f"\n{section}:")
        for field in fields:
            if field in current_dict:
                field_name = FIELD_NAMES.get(field, field.replace('_', ' ').title())
                value = format_value(field, current_dict[field])
                print(f"  {field_name:<15} {value:>15}")

def get_field_emoji(field):
    """Return appropriate emoji for a field."""
    emoji_map = {
        "price": "💰",
        "beds": "🛏️",
        "baths": "🚿",
        "sqft": "📐",
        "mls_type": "🏷️",
        "tax_information": "💵",
        "status": "📊",
        "days_on_compass": "📅",
        "last_updated": "🕒",
        "mls_number": "🔢"
    }
    return emoji_map.get(field, "📝")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/show_listing_history.py <address>")
        sys.exit(1)
        
    show_listing_history(sys.argv[1]) 