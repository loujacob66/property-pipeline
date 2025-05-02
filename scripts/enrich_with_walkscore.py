#!/usr/bin/env python3
# enrich_with_walkscore.py
# 
# Script to enrich property listings database with WalkScore data
# This script connects to listings.db and adds WalkScore, BikeScore, and TransitScore
# for properties that don't have these scores yet.

import os
import json
import sqlite3
import requests
import time
import logging
from urllib.parse import quote
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("walkscore_enrichment.log"),
                             logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Configuration paths
BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / 'config' / 'walkscore_config.json'
DB_PATH = BASE_DIR / 'data' / 'listings.db'

# Add load_shortio_config function after load_config function
def load_config():
    """Load API configuration from config file."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
            return config
    except FileNotFoundError:
        logger.error(f"Config file not found: {CONFIG_PATH}")
        # Create an example config file if it doesn't exist
        example_config = {
            "api_key": "YOUR_WALKSCORE_API_KEY_HERE"
        }
        os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
        with open(CONFIG_PATH, 'w') as f:
            json.dump(example_config, f, indent=4)
        logger.info(f"Created example config file at {CONFIG_PATH}")
        logger.info("Please add your WalkScore API key to the config file and run the script again.")
        exit(1)
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in config file: {CONFIG_PATH}")
        exit(1)


def load_shortio_config():
    """Load Short.io API configuration from config file."""
    shortio_config_path = BASE_DIR / 'config' / 'shortio_config.json'
    try:
        with open(shortio_config_path, 'r') as f:
            config = json.load(f)
            return config
    except FileNotFoundError:
        logger.error(f"Short.io config file not found: {shortio_config_path}")
        # Create an example config file if it doesn't exist
        example_config = {
            "api_key": "YOUR_SHORTIO_API_KEY_HERE",
            "domain": "YOUR_SHORTIO_DOMAIN_HERE"
        }
        os.makedirs(os.path.dirname(shortio_config_path), exist_ok=True)
        with open(shortio_config_path, 'w') as f:
            json.dump(example_config, f, indent=4)
        logger.info(f"Created example Short.io config file at {shortio_config_path}")
        logger.info("Please add your Short.io API key and domain to the config file.")
        return example_config
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in Short.io config file: {shortio_config_path}")
        return {}
        
def update_listing_scores(conn, listing_id, scores):
    """
    Update the listing with WalkScore data.
    
    Args:
        conn: SQLite database connection
        listing_id (int): The ID of the listing to update
        scores (dict): Dictionary with walk_score, transit_score, bike_score, and walkscore_shorturl
    
    Returns:
        bool: True if successful, False otherwise
    """
    cursor = conn.cursor()
    try:
        query = """
            UPDATE listings
            SET walk_score = ?, transit_score = ?, bike_score = ?, walkscore_shorturl = ?
            WHERE id = ?
        """
        cursor.execute(query, (
            scores.get("walk_score"),
            scores.get("transit_score"),
            scores.get("bike_score"),
            scores.get("walkscore_shorturl"),
            listing_id
        ))
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        return False

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
        geocode_url = f"https://nominatim.openstreetmap.org/search"
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

def get_walkscore(address, lat=None, lon=None, api_key=None):
    """
    Get WalkScore, TransitScore, and BikeScore for an address.
    
    Args:
        address (str): The address to look up
        lat (float, optional): Latitude coordinate
        lon (float, optional): Longitude coordinate
        api_key (str): WalkScore API key
    
    Returns:
        dict: Dictionary containing walk_score, transit_score, and bike_score
    """
    if not api_key:
        logger.error("No API key provided")
        return None
    
    # Get coordinates if not provided
    if lat is None or lon is None:
        lat, lon = geocode_address(address)
        if lat is None or lon is None:
            logger.error(f"Could not get coordinates for {address}, WalkScore API requires lat/lon")
            scores = {
                "walk_score": None,
                "transit_score": None,
                "bike_score": None
            }
            return scores
    
    # Prepare the address for the API request
    encoded_address = quote(address)
    
    # Base URL for the WalkScore API
    base_url = "https://api.walkscore.com/score"
    
    # Parameters for the API request - WalkScore API requires lat, lon, and address
    params = {
        "format": "json",
        "address": encoded_address,
        "lat": lat,
        "lon": lon,
        "wsapikey": api_key,
        "transit": 1,  # Request Transit Score
        "bike": 1      # Request Bike Score
    }
    
    try:
        response = requests.get(base_url, params=params)
        data = response.json()
        
        # Initialize scores to None
        scores = {
            "walk_score": None,
            "transit_score": None,
            "bike_score": None,
            "walkscore_shorturl": None
        }
        
        # Check if the request was successful
        if response.status_code == 200 and data.get("status") == 1:
            scores["walk_score"] = data.get("walkscore")
            scores["walkscore_shorturl"] = data.get("ws_link")
            
            # Transit and bike scores might not be available for all locations
            if "transit" in data:
                scores["transit_score"] = data.get("transit", {}).get("score")
            
            if "bike" in data:
                scores["bike_score"] = data.get("bike", {}).get("score")
            
            logger.info(f"Successfully retrieved scores for {address}")
            return scores
        else:
            error_msg = data.get('description', f"Error code: {data.get('status', 'Unknown')}")
            logger.warning(f"Failed to get scores for {address}: {error_msg}")
            return scores
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in response for {address}")
        return None

def get_listings_without_scores(conn):
    """
    Get listings that are missing any of the score fields or walkscore_shorturl.
    
    Args:
        conn: SQLite database connection
    
    Returns:
        list: List of tuples containing (id, address, city, state, zip)
    """
    cursor = conn.cursor()
    query = """
        SELECT id, address, city, state, zip
        FROM listings
        WHERE walk_score IS NULL 
           OR transit_score IS NULL 
           OR bike_score IS NULL
           OR walkscore_shorturl IS NULL
    """
    cursor.execute(query)
    return cursor.fetchall()

def get_listings_needing_url_shortening(conn):
    """
    Get listings that have long WalkScore URLs that need shortening.
    
    Args:
        conn: SQLite database connection
    
    Returns:
        list: List of tuples containing (id, address, city, state, zip, walkscore_shorturl)
    """
    cursor = conn.cursor()
    query = """
        SELECT id, address, city, state, zip, walkscore_shorturl
        FROM listings
        WHERE walkscore_shorturl LIKE 'https://www.walkscore.com/%'
    """
    cursor.execute(query)
    return cursor.fetchall()

def update_listing_url(conn, listing_id, short_url):
    """
    Update just the walkscore_shorturl field for a listing.
    
    Args:
        conn: SQLite database connection
        listing_id (int): The ID of the listing to update
        short_url (str): The shortened URL
    
    Returns:
        bool: True if successful, False otherwise
    """
    cursor = conn.cursor()
    try:
        query = """
            UPDATE listings
            SET walkscore_shorturl = ?
            WHERE id = ?
        """
        cursor.execute(query, (short_url, listing_id))
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        return False

def shorten_url(long_url, api_key=None, domain=None):
    """
    Shorten a URL using Short.io API.
    
    Args:
        long_url (str): The URL to shorten
        api_key (str): Short.io API key
        domain (str): Short.io domain
        
    Returns:
        str: Shortened URL or None if failed
    """
    if not api_key or not domain:
        logger.error("No Short.io API key or domain provided")
        return None
        
    try:
        url = "https://api.short.io/links"
        
        payload = json.dumps({
            "originalURL": long_url,
            "domain": domain
        })
        
        headers = {
            "authorization": api_key,
            "content-type": "application/json"
        }
        
        response = requests.post(url, data=payload, headers=headers)
        data = response.json()
        
        if response.status_code == 200 and data.get("success", True):
            logger.info(f"Successfully shortened URL: {long_url}")
            return data.get("shortURL")
        else:
            error_message = data.get("message", "Unknown error")
            status_code = data.get("statusCode", response.status_code)
            logger.error(f"Failed to shorten URL: {error_message} (Status: {status_code})")
            
            # Check for common issues and provide more helpful messages
            if status_code == 404 and "Domain not found" in error_message:
                logger.error(f"The domain '{domain}' does not exist in your Short.io account or is not properly configured.")
                logger.error("To fix this: 1) Log into Short.io, 2) Verify the domain exists, 3) Check domain configuration")
            elif status_code == 401:
                logger.error("API key authentication failed. Please check your API key.")
            
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        return None
    except json.JSONDecodeError:
        logger.error("Invalid JSON in Short.io API response")
        return None

def main():
    """Main function to enrich listings with WalkScore data."""
    # Load WalkScore configuration
    config = load_config()
    walkscore_api_key = config.get("api_key")
    
    if not walkscore_api_key or walkscore_api_key == "YOUR_WALKSCORE_API_KEY_HERE":
        logger.error("Please set your WalkScore API key in the config file")
        return
    
    # Load Short.io configuration
    shortio_config = load_shortio_config()
    shortio_api_key = shortio_config.get("api_key")
    shortio_domain = shortio_config.get("domain")
    
    if (shortio_api_key == "YOUR_SHORTIO_API_KEY_HERE" or 
        shortio_domain == "YOUR_SHORTIO_DOMAIN_HERE"):
        logger.warning("Short.io API key or domain not set properly, URL shortening will be skipped")
        use_url_shortener = False
    else:
        # Verify domain exists by making a test API call
        test_url = "https://example.com"
        short_url = shorten_url(test_url, shortio_api_key, shortio_domain)
        if short_url:
            use_url_shortener = True
            logger.info(f"Short.io API connection successful, URL shortening enabled")
        else:
            logger.warning("Short.io API connection failed or domain not found, URL shortening disabled")
            use_url_shortener = False
    
    # Connect to the database
    try:
        conn = sqlite3.connect(DB_PATH)
        logger.info(f"Connected to database: {DB_PATH}")
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        return
    
    # First, process listings with missing score data
    process_missing_scores(conn, walkscore_api_key, shortio_api_key, shortio_domain, use_url_shortener)
    
    # Then, fix listings with long WalkScore URLs
    if use_url_shortener:
        fix_long_urls(conn, shortio_api_key, shortio_domain)
    
    # Close the database connection
    conn.close()
    
def process_missing_scores(conn, walkscore_api_key, shortio_api_key, shortio_domain, use_url_shortener):
    """Process listings missing WalkScore data."""
    # Get listings that need scores
    listings = get_listings_without_scores(conn)
    total_listings = len(listings)
    logger.info(f"Found {total_listings} listings that need WalkScore data")
    
    # Process each listing
    success_count = 0
    for i, (listing_id, address, city, state, zip_code) in enumerate(listings, 1):
        # Combine address components
        full_address = f"{address}, {city}, {state} {zip_code}"
        
        logger.info(f"Processing [{i}/{total_listings}] {full_address}")
        
        # First, get coordinates for the address
        lat, lon = geocode_address(full_address)
        
        # Get scores from WalkScore API using coordinates
        scores = get_walkscore(full_address, lat=lat, lon=lon, api_key=walkscore_api_key)
        
        if scores:
            # Shorten WalkScore URL if enabled
            if use_url_shortener and scores.get("walkscore_shorturl"):
                original_url = scores.get("walkscore_shorturl")
                short_url = shorten_url(original_url, shortio_api_key, shortio_domain)
                
                if short_url:
                    scores["walkscore_shorturl"] = short_url
                    logger.info(f"Shortened WalkScore URL for {full_address}: {short_url}")
            
            # Update the database
            if update_listing_scores(conn, listing_id, scores):
                success_count += 1
                logger.info(f"Updated listing {listing_id} with scores: {scores}")
            else:
                logger.warning(f"Failed to update listing {listing_id}")
        
        # Throttle requests to avoid hitting API limits (max 5000/day, recommend 1/sec)
        time.sleep(1)
    
    logger.info(f"Score enrichment complete. Updated {success_count} out of {total_listings} listings.")

def fix_long_urls(conn, shortio_api_key, shortio_domain):
    """Fix listings with long WalkScore URLs by shortening them."""
    # Get listings with long WalkScore URLs
    listings = get_listings_needing_url_shortening(conn)
    total_listings = len(listings)
    logger.info(f"Found {total_listings} listings with long WalkScore URLs that need shortening")
    
    # Process each listing
    success_count = 0
    for i, (listing_id, address, city, state, zip_code, long_url) in enumerate(listings, 1):
        # Combine address components
        full_address = f"{address}, {city}, {state} {zip_code}"
        
        logger.info(f"Processing [{i}/{total_listings}] {full_address}")
        
        # Shorten the long URL
        short_url = shorten_url(long_url, shortio_api_key, shortio_domain)
        
        if short_url:
            # Update just the URL in the database
            if update_listing_url(conn, listing_id, short_url):
                success_count += 1
                logger.info(f"Updated URL for listing {listing_id}: {short_url}")
            else:
                logger.warning(f"Failed to update URL for listing {listing_id}")
        
        # Throttle requests to avoid hitting API limits
        time.sleep(1)
    
    logger.info(f"URL shortening complete. Updated {success_count} out of {total_listings} listings.")

if __name__ == "__main__":
    main()
