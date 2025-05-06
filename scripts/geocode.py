#!/usr/bin/env python3
"""
Geocoding utility for property listings.
This script provides functions to geocode addresses and can be imported by other scripts.
"""

import requests
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def geocode_address(address):
    """
    Get latitude and longitude for an address using a geocoding service.
    
    Args:
        address (str): The address to geocode
    
    Returns:
        tuple: (latitude, longitude) or (None, None) if geocoding fails
    """
    try:
        # Using the free Nominatim geocoding service
        # Note: For production use, consider using a commercial geocoding service
        # with appropriate usage limits and terms of service compliance
        geocode_url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": address,
            "format": "json",
            "limit": 1
        }
        headers = {
            "User-Agent": "PropertyPipeline/1.0"  # Required by Nominatim ToS
        }
        
        response = requests.get(geocode_url, params=params, headers=headers)
        data = response.json()
        
        if data and len(data) > 0:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            logger.info(f"Successfully geocoded {address}: {lat}, {lon}")
            return lat, lon
        else:
            logger.warning(f"Could not geocode address: {address}")
            return None, None
    
    except Exception as e:
        logger.error(f"Geocoding error for {address}: {e}")
        return None, None

def update_listing_coordinates(conn, listing_id, lat, lon):
    """
    Update the listing with latitude and longitude coordinates.
    
    Args:
        conn: SQLite database connection
        listing_id (int): The ID of the listing to update
        lat (float): Latitude coordinate
        lon (float): Longitude coordinate
    
    Returns:
        bool: True if successful, False otherwise
    """
    cursor = conn.cursor()
    try:
        query = """
            UPDATE listings
            SET latitude = ?, longitude = ?
            WHERE id = ?
        """
        cursor.execute(query, (lat, lon, listing_id))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        return False

def get_listings_without_coordinates(conn):
    """
    Get all listings that don't have latitude and longitude coordinates.
    
    Args:
        conn: SQLite database connection
    
    Returns:
        list: List of tuples containing (id, address) for listings without coordinates
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, address
        FROM listings
        WHERE latitude IS NULL OR longitude IS NULL
    """)
    return cursor.fetchall()

if __name__ == "__main__":
    import sqlite3
    from pathlib import Path
    
    # Connect to database
    db_path = Path(__file__).parent.parent / "data" / "listings.db"
    conn = sqlite3.connect(db_path)
    
    try:
        # Get listings without coordinates
        listings = get_listings_without_coordinates(conn)
        print(f"Found {len(listings)} listings without coordinates")
        
        # Process each listing
        for listing_id, address in listings:
            print(f"\nProcessing listing {listing_id}: {address}")
            lat, lon = geocode_address(address)
            if lat and lon:
                success = update_listing_coordinates(conn, listing_id, lat, lon)
                if success:
                    print(f"✅ Updated coordinates for listing {listing_id}")
                else:
                    print(f"❌ Failed to update coordinates for listing {listing_id}")
            else:
                print(f"⚠️ Could not geocode address: {address}")
    
    finally:
        conn.close() 