#!/usr/bin/env python3
"""
List Gmail Labels

This script lists all Gmail labels to help verify label IDs.
"""

import os
import sys
from pathlib import Path

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from lib.gmail_utils import authenticate_gmail

def main():
    """Main function to list Gmail labels."""
    # Authenticate with Gmail
    service = authenticate_gmail()
    if not service:
        return
    
    try:
        # List all labels
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        
        if not labels:
            print('No labels found.')
            return
            
        print('Labels:')
        for label in labels:
            print(f"Name: {label['name']}")
            print(f"ID: {label['id']}")
            print('-' * 40)
            
    except Exception as e:
        print(f'An error occurred: {e}')

if __name__ == '__main__':
    main()
