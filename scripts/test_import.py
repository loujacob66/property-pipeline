#!/usr/bin/env python3
"""
Test script to verify duplicate handling in import_compass_to_db.py
"""

import os
import sys
from pathlib import Path

# Add parent directory to path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from scripts.import_compass_to_db import import_listing_to_db

# Test data - same address but different details
test_listing = {
    'address': '3250 Ames Street',
    'city': 'Denver',
    'state': 'CO',
    'zip': '80238',
    'price': 550000,  # Changed price
    'beds': 2,
    'baths': 1,
    'sqft': 1200,
    'price_per_sqft': 458,
    'url': 'https://www.compass.com/listing/3250-ames-street',
    'source': 'test_import'
}

def main():
    print(f"Testing import of listing: {test_listing['address']}")
    print(f"New price: ${test_listing['price']:,}")
    
    success = import_listing_to_db(test_listing)
    if success:
        print("✅ Import completed successfully")
    else:
        print("❌ Import failed")

if __name__ == "__main__":
    main() 