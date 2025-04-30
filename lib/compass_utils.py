#!/usr/bin/env python3
"""
Compass Utilities

This module provides utility functions for authenticating with Compass.com
and retrieving property listing data.
"""

import os
import json
import time
import traceback
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import re
from datetime import datetime

def authenticate_compass(playwright, headless=False, max_retries=3):
    """
    Authenticate with Compass using browser persistence.
    
    Args:
        playwright: Playwright instance
        headless (bool): Whether to run browser in headless mode
        max_retries (int): Maximum number of authentication retries
    
    Returns:
        tuple: (page, browser_context) - Authenticated page and context objects
    """
    # Config paths
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    AUTH_STORAGE_PATH = os.path.join(ROOT, ".auth", "compass")
    SESSION_FILE = os.path.join(AUTH_STORAGE_PATH, "session.json")
    
    # Ensure auth storage directory exists
    os.makedirs(AUTH_STORAGE_PATH, exist_ok=True)
    
    def save_session_state():
        """Save current session state to file"""
        try:
            with open(SESSION_FILE, 'w') as f:
                json.dump({
                    'last_auth': time.time(),
                    'status': 'authenticated'
                }, f)
        except Exception as e:
            print(f"⚠️ Warning: Could not save session state: {e}")
    
    def load_session_state():
        """Load session state from file"""
        try:
            if os.path.exists(SESSION_FILE):
                with open(SESSION_FILE, 'r') as f:
                    return json.load(f)
        except Exception as e:
            print(f"⚠️ Warning: Could not load session state: {e}")
        return None
    
    def is_session_valid():
        """Check if current session is still valid"""
        session = load_session_state()
        if not session:
            return False
        # Consider session valid if it's less than 24 hours old
        return time.time() - session.get('last_auth', 0) < 86400
    
    def verify_authentication(page):
        """Verify that the page is properly authenticated"""
        try:
            # Try accessing a protected page
            page.goto("https://www.compass.com/workspace/", timeout=30000)
            return "/login/" not in page.url
        except Exception:
            return False
    
    browser_context = None
    page = None
    
    try:
        # Launch persistent context
        print("Launching browser with persistent context...")
        browser_context = playwright.chromium.launch_persistent_context(
            user_data_dir=AUTH_STORAGE_PATH,
            headless=headless
        )
        
        # Use existing page or create a new one
        if browser_context.pages:
            page = browser_context.pages[0]
        else:
            page = browser_context.new_page()
        
        # Check if we have a valid session
        if is_session_valid():
            print("Checking existing session...")
            if verify_authentication(page):
                print("✅ Using existing valid session")
                return page, browser_context
            else:
                print("⚠️ Existing session is invalid")
        
        # Need to authenticate
        print("\n🔐 Authentication required")
        print("⚠️ Please complete the login process in the opened browser")
        print("   This may include logging in with Google or other authentication methods")
        
        # Navigate to workspace login page
        page.goto("https://www.compass.com/workspace/", timeout=30000)
        
        # Wait for user to complete authentication
        max_wait_seconds = 300  # 5 minutes timeout
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            time.sleep(2)
            current_url = page.url
            
            if "/login/" not in current_url and "compass.com" in current_url:
                if verify_authentication(page):
                    print("✅ Login successful")
                    save_session_state()
                    return page, browser_context
        
        print("⚠️ Authentication timed out")
        raise Exception("Authentication timed out")
        
    except Exception as e:
        print(f"❌ Error during authentication: {e}")
        traceback.print_exc()
        if browser_context:
            browser_context.close()
        raise

def safe_extract(iframe, selector, timeout=5000):
    """Helper function to safely extract text from an element with error handling and logging."""
    try:
        element = iframe.locator(selector).first
        element.wait_for(timeout=timeout)
        return element.inner_text().strip()
    except Exception as e:
        print(f"⚠️ Could not extract element with selector '{selector}': {e}")
        return None

def extract_listing_details(page, iframe):
    """Extract listing details from the iframe content."""
    details = {}
    
    try:
        # Wait for the main content to load
        iframe.wait_for_selector("tr.keyDetails-text", timeout=10000)
        
        # Get the page content for debugging
        content = iframe.content()
        print(f"Page content: {content}")
        
        # Extract MLS number
        print("Extracting MLS number...")
        mls_selectors = [
            "tr.keyDetails-text:has(th:has-text('MLS#')) td",
            "tr:has(th:has-text('MLS#')) td",
            "tr:has(th:has-text('MLS Number')) td",
            "div:has-text('MLS#')",
            "span:has-text('MLS#')"
        ]
        details['mls_number'] = extract_with_selectors(iframe, mls_selectors)
        
        # Extract MLS Type
        print("Extracting MLS Type...")
        mls_type_selectors = [
            "tr.keyDetails-text:has(th:has-text('Property Type')) td",
            "tr:has(th:has-text('Property Type')) td",
            "div:has-text('Property Type')",
            "span:has-text('Property Type')"
        ]
        mls_type = extract_with_selectors(iframe, mls_type_selectors)
        if mls_type and 'Residential' in mls_type:
            details['mls_type'] = 'Residential'
        
        # Extract Tax Information
        print("Extracting Tax Information...")
        tax_selectors = [
            "tr.keyDetails-text:has(th:has-text('Tax Information')) td",
            "tr:has(th:has-text('Tax Information')) td",
            "tr:has(th:has-text('Taxes')) td",
            "div:has-text('Tax Information')",
            "span:has-text('Tax Information')"
        ]
        tax_info = extract_with_selectors(iframe, tax_selectors)
        if tax_info:
            # Extract just the dollar amount
            match = re.search(r'\$[\d,]+', tax_info)
            if match:
                details['tax_info'] = match.group(0)
        
        # Extract Year Built
        print("Extracting Year Built...")
        year_built_selectors = [
            "tr.keyDetails-text:has(th:has-text('Year Built')) td",
            "tr:has(th:has-text('Year Built')) td",
            "div:has-text('Year Built')",
            "span:has-text('Year Built')"
        ]
        details['year_built'] = extract_with_selectors(iframe, year_built_selectors)
        
        # Extract Lot Size
        print("Extracting Lot Size...")
        lot_size_selectors = [
            "tr.keyDetails-text:has(th:has-text('Lot Size')) td",
            "tr:has(th:has-text('Lot Size')) td",
            "div:has-text('Lot Size')",
            "span:has-text('Lot Size')"
        ]
        details['lot_size'] = extract_with_selectors(iframe, lot_size_selectors)
        
        # Extract HOA Fee
        print("Extracting HOA Fee...")
        hoa_fee_selectors = [
            "tr.keyDetails-text:has(th:has-text('HOA Fee')) td",
            "tr:has(th:has-text('HOA Fee')) td",
            "div:has-text('HOA Fee')",
            "span:has-text('HOA Fee')"
        ]
        details['hoa_fee'] = extract_with_selectors(iframe, hoa_fee_selectors) or '-'
        
        # Extract Status
        print("Extracting Status...")
        status_selectors = [
            "tr.keyDetails-text:has(th:has-text('Status')) td",
            "tr:has(th:has-text('Status')) td",
            "div:has-text('Status')",
            "span:has-text('Status')",
            ".cx-solidListingBadge"
        ]
        details['status'] = extract_with_selectors(iframe, status_selectors)
        
        # Extract Days on Compass
        print("Extracting Days on Compass...")
        days_on_compass_selectors = [
            "tr.keyDetails-text:has(th:has-text('Days on Compass')) td",
            "tr:has(th:has-text('Days on Compass')) td",
            "div:has-text('Days on Compass')",
            "span:has-text('Days on Compass')",
            "div:has-text('Days on Market')",
            "span:has-text('Days on Market')"
        ]
        days_on_compass = extract_with_selectors(iframe, days_on_compass_selectors)
        if days_on_compass:
            # Extract just the number
            match = re.search(r'\d+', days_on_compass)
            if match:
                details['days_on_compass'] = match.group(0)
        
        # Extract Last Updated
        print("Extracting Last Updated...")
        try:
            updated_text = iframe.locator("text=LISTING UPDATED").first
            if updated_text:
                raw_updated = updated_text.inner_text().strip()
                # Extract just the date
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})', raw_updated)
                if date_match:
                    details['last_updated'] = date_match.group(1)
                else:
                    details['last_updated'] = raw_updated
                print(f"Found Last Updated: {raw_updated} -> {details['last_updated']}")
        except Exception as e:
            print(f"Error getting Last Updated: {e}")
            details['last_updated'] = None
        
        # Extract Favorite status
        print("Extracting Favorite status...")
        try:
            # Look for a heart icon or "Saved" text
            saved_element = iframe.locator("button:has-text('Saved')").first
            if saved_element:
                details['favorite'] = "1"
                print("Found listing is saved/favorited")
            else:
                details['favorite'] = "0"
                print("Listing is not saved/favorited")
        except Exception as e:
            print(f"Error checking saved status: {e}")
            details['favorite'] = "0"
        
    except Exception as e:
        print(f"⚠️ Error extracting listing details: {str(e)}")
        traceback.print_exc()
    
    print(f"Extracted details: {details}")
    return details