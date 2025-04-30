#!/usr/bin/env python3
"""
Test script to verify field extraction from Compass listings
"""

from playwright.sync_api import sync_playwright
import json
import time
import os
import re
import sqlite3
from datetime import datetime

def clean_mls_type(mls_type):
    """Convert MLS type to simplified format"""
    if mls_type == "Residential-Detached":
        return "Detached"
    elif mls_type == "Residential-Attached":
        return "Attached"
    return mls_type

def clean_tax_info(tax_info):
    """Extract just the dollar amount from tax information"""
    match = re.search(r'\$([\d,]+)', tax_info)
    if match:
        return int(match.group(1).replace(',', ''))
    return None

def clean_last_updated(last_updated):
    """Extract just the date from last updated field"""
    match = re.search(r'(\d{2}/\d{2}/\d{4})', last_updated)
    if match:
        return match.group(1)  # Return the date string directly
    return None

def update_database(url, details):
    """Update the database with extracted details"""
    db_path = os.path.join(os.path.dirname(__file__), 'data', 'listings.db')
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        update_fields = []
        update_values = []
        
        if 'last_updated' in details:
            update_fields.append("last_updated = ?")
            update_values.append(details['last_updated'])
        
        if 'favorite' in details:
            update_fields.append("favorite = ?")
            update_values.append(details['favorite'])
        
        if update_fields:
            update_values.append(url)
            cursor.execute(
                f"UPDATE listings SET {', '.join(update_fields)} WHERE url = ?",
                update_values
            )
            conn.commit()
            print(f"✅ Updated database for {url}")
    finally:
        conn.close()

def test_extraction():
    # Use a direct listing URL instead of workspace URL
    url = "https://www.compass.com/listing/1822113986984183057"
    
    print(f"Testing extraction for listing: {url}")
    
    with sync_playwright() as p:
        # Launch browser with authentication state
        ROOT = os.path.abspath(os.path.dirname(__file__))
        AUTH_STORAGE_PATH = os.path.join(ROOT, ".auth", "compass")
        
        print("\nLaunching browser with saved authentication...")
        browser_context = p.chromium.launch_persistent_context(
            user_data_dir=AUTH_STORAGE_PATH,
            headless=False
        )
        page = browser_context.new_page()
        
        try:
            # Navigate to the listing
            print("\nLoading listing...")
            page.goto(url)
            print("Waiting for network idle...")
            page.wait_for_load_state("networkidle")
            
            # Wait for and get the iframe
            print("\nWaiting for iframe...")
            page.wait_for_selector("iframe", state="attached", timeout=10000)
            
            print("Found iframe, getting frame...")
            frames = page.frames
            print(f"Available frames: {len(frames)}")
            
            # Find the frame containing the listing details
            listing_frame = None
            for frame in frames:
                try:
                    # Try to find a unique element that should be in the listing details
                    if frame.locator("tr.keyDetails-text").count() > 0:
                        listing_frame = frame
                        print(f"Found listing details in frame: {frame.url}")
                        break
                except:
                    continue
            
            if not listing_frame:
                print("Could not find frame with listing details")
                return
            
            # Extract key details
            print("\nExtracting details...")
            details = {}
            
            # Extract listing details using the existing extraction logic
            try:
                # MLS Number
                mls_row = listing_frame.locator("tr:has-text('MLS #')").first
                if mls_row:
                    details["mls_number"] = mls_row.locator("td").first.inner_text().strip()
                
                # MLS Type
                mls_type_row = listing_frame.locator("tr:has-text('MLS Type')").first
                if mls_type_row:
                    raw_mls_type = mls_type_row.locator("td").first.inner_text().strip()
                    details["mls_type"] = clean_mls_type(raw_mls_type)
                
                # Tax Information
                tax_row = listing_frame.locator("tr:has-text('Taxes')").first
                if tax_row:
                    raw_tax_info = tax_row.locator("td").first.inner_text().strip()
                    details["tax_info"] = clean_tax_info(raw_tax_info)
                
                # Days on Compass
                dom_row = listing_frame.locator("tr:has-text('Days on Compass')").first
                if dom_row:
                    details["days_on_compass"] = dom_row.locator("td").first.inner_text().strip()
                
                # Last Updated
                updated_text = listing_frame.locator("text=LISTING UPDATED").first
                if updated_text:
                    raw_updated = updated_text.inner_text().strip()
                    details["last_updated"] = clean_last_updated(raw_updated)
                
                # Listing Status
                status_row = listing_frame.locator("tr:has-text('Status')").first
                if status_row:
                    details["listing_status"] = status_row.locator("td").first.inner_text().strip()
                
                # Check if listing is saved/favorited
                saved_element = listing_frame.locator("button:has-text('Saved')").first
                details["favorite"] = 1 if saved_element else 0
                
                # Print extracted details
                print("\nExtracted details:")
                print(json.dumps(details, indent=2, default=str))
                
                # Update database
                update_database(url, details)
                
            except Exception as e:
                print(f"Error during extraction: {str(e)}")
                
        except Exception as e:
            print(f"\nError: {str(e)}")
        finally:
            print("\nClosing browser...")
            browser_context.close()

if __name__ == "__main__":
    test_extraction() 