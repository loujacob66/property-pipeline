#!/usr/bin/env python3
"""
Gmail Label Lister

This script lists all available Gmail labels and their IDs to help with
setting up filtering for the property pipeline.

Usage: python list_gmail_labels.py [--output OUTPUT_FILE]
"""

import os
import sys
import json
import argparse

# Add project root to path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

# Import project modules
from lib.gmail_utils import authenticate_gmail

def list_all_labels(output_file=None):
    """List all Gmail labels and optionally save to a JSON file."""
    print("🔐 Authenticating with Gmail...")
    try:
        service = authenticate_gmail()
        print("✅ Authentication successful")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return
    
    print("📋 Retrieving labels...")
    try:
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        
        if not labels:
            print('No labels found.')
            return
            
        # Create a dictionary for labels
        label_dict = {}
        
        print("\n📝 Available Gmail Labels:")
        print("-" * 60)
        print(f"{'NAME':<40} {'ID':<30}")
        print("-" * 60)
        
        for label in labels:
            name = label.get('name', 'Unknown')
            label_id = label.get('id', 'Unknown')
            
            # Store in dictionary
            label_dict[name] = label_id
            
            # Print to console
            print(f"{name:<40} {label_id:<30}")
        
        # Save to file if requested
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(label_dict, f, indent=2)
            print(f"\n✅ Labels saved to {output_file}")
        
        return label_dict
            
    except Exception as e:
        print(f"❌ Error retrieving labels: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="List all Gmail labels and their IDs")
    parser.add_argument("--output", help="Output JSON file to save labels")
    args = parser.parse_args()
    
    list_all_labels(args.output)

if __name__ == "__main__":
    main()
