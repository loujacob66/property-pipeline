#!/usr/bin/env python3
"""
Test script for Compass authentication
"""

from lib.compass_utils import authenticate_compass

def test_authentication():
    """
    Test the authentication flow with Compass
    """
    print("Starting authentication test...")
    
    try:
        # Try authentication with visible browser first
        print("\nTesting authentication with visible browser...")
        page, context, playwright = authenticate_compass(headless=False)
        
        # Verify we can access a protected page
        print("\nVerifying authenticated session...")
        page.goto("https://www.compass.com/account/saved/")
        
        if "/login/" not in page.url:
            print("✅ Authentication successful!")
        else:
            print("❌ Authentication failed - redirected to login page")
        
        # Clean up
        context.close()
        playwright.stop()
        
    except Exception as e:
        print(f"❌ Authentication test failed: {e}")
        raise

if __name__ == "__main__":
    test_authentication() 